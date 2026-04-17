from collections import defaultdict
import time

import pytest
import uuid
import json

from unittest import mock
from requests.exceptions import HTTPError
from sentry_sdk.envelope import Envelope, Item, PayloadRef

from .test_store import make_transaction


def test_attachments_400(mini_sentry, relay_with_processing, attachments_consumer):
    proj_id = 42
    relay = relay_with_processing()
    mini_sentry.add_full_project_config(proj_id)
    attachments_consumer = attachments_consumer()

    event_id = "123abc"

    with pytest.raises(HTTPError) as excinfo:
        relay.send_attachments(proj_id, event_id, [])

    assert excinfo.value.response.status_code == 400


def test_mixed_attachments_with_processing(
    mini_sentry, relay_with_processing, attachments_consumer, outcomes_consumer
):
    project_id = 42
    event_id = "515539018c9b4260a6f999572f1661ee"

    options = {"processing": {"attachment_chunk_size": "100KB"}}
    mini_sentry.add_full_project_config(project_id)
    relay = relay_with_processing(options)
    attachments_consumer = attachments_consumer()
    outcomes_consumer = outcomes_consumer()

    large_content = b"heavens no" * 20_000
    attachments = [
        ("att_1", "foo.txt", large_content),
        ("att_2", "bar.txt", b"hell yeah"),
        ("att_3", "foobar.txt", b""),
    ]
    relay.send_attachments(project_id, event_id, attachments)

    chunked_data_per_id = defaultdict(bytes)
    n_chunks = 0
    inline_attachments = []
    start = time.time()
    while (
        set(chunked_data_per_id.values()) != {large_content}
        or len(inline_attachments) < 3
    ):
        if time.time() - start > 10:
            raise TimeoutError()
        _, m = attachments_consumer.get_message()
        if m["type"] == "attachment_chunk":
            chunked_data_per_id[m["id"]] += m["payload"]
            assert m["chunk_index"] == n_chunks
            n_chunks += 1
        elif m["type"] == "attachment":
            inline_attachments.append(m)
        else:
            raise AssertionError(f"Unexpected message type: {m['type']}")
    inline_attachments.sort(key=lambda a: a["attachment"]["name"])

    assert len(chunked_data_per_id) == 1
    (foo_id,) = chunked_data_per_id
    assert chunked_data_per_id[foo_id] == large_content
    assert n_chunks > 1

    # Inlined attachment bar
    bar = inline_attachments[0]
    # The ID is random. Just assert that it is there and non-zero.
    assert bar["attachment"].pop("id")
    assert bar == {
        "type": "attachment",
        "attachment": {
            "name": "bar.txt",
            "rate_limited": False,
            "attachment_type": "event.attachment",
            "size": len(b"hell yeah"),
            "retention_days": 90,
            "data": b"hell yeah",
        },
        "event_id": event_id,
        "project_id": project_id,
    }

    # Inlined metadata for chunked attachment foo
    foo = inline_attachments[1]
    assert foo == {
        "type": "attachment",
        "attachment": {
            "id": foo_id,
            "name": "foo.txt",
            "rate_limited": False,
            "attachment_type": "event.attachment",
            "size": len(large_content),
            "retention_days": 90,
            "chunks": n_chunks,
        },
        "event_id": event_id,
        "project_id": project_id,
    }

    # Empty attachment foobar
    foobar = inline_attachments[2]
    # The ID is random. Just assert that it is there and non-zero.
    assert foobar["attachment"].pop("id")
    assert foobar == {
        "type": "attachment",
        "attachment": {
            "name": "foobar.txt",
            "rate_limited": False,
            "attachment_type": "event.attachment",
            "size": 0,
            "retention_days": 90,
            "chunks": 0,
        },
        "event_id": event_id,
        "project_id": project_id,
    }

    outcomes_consumer.assert_empty()


def test_attachments_with_objectstore(
    mini_sentry,
    relay_with_processing,
    attachments_consumer,
    outcomes_consumer,
    objectstore,
):
    project_id = 42
    event_id = "515539018c9b4260a6f999572f1661ee"

    mini_sentry.global_config["options"][
        "relay.objectstore-attachments.sample-rate"
    ] = 1.0
    mini_sentry.add_full_project_config(project_id)

    options = {
        "processing": {
            "attachment_chunk_size": "100KB",
        }
    }
    relay = relay_with_processing(options)
    attachments_consumer = attachments_consumer()
    outcomes_consumer = outcomes_consumer()

    chunked_contents = b"heavens no" * 20_000
    relay.send_attachments(
        project_id,
        event_id,
        [("att_1", "foo.txt", chunked_contents), ("att_2", "foobar.txt", b"")],
    )

    attachments_by_name = {}
    for _ in range(2):
        att = attachments_consumer.get_individual_attachment()
        attachments_by_name[att["attachment"]["name"]] = att

    # Large attachments are stored in objectstore
    stored = attachments_by_name["foo.txt"]
    objectstore_key = stored["attachment"].pop("stored_id")
    objectstore = objectstore("attachments", project_id)
    assert objectstore.get(objectstore_key).payload.read() == chunked_contents
    assert stored == {
        "type": "attachment",
        "attachment": {
            "id": mock.ANY,
            "name": "foo.txt",
            "rate_limited": False,
            "attachment_type": "event.attachment",
            "size": len(chunked_contents),
            "retention_days": 90,
        },
        "event_id": event_id,
        "project_id": project_id,
    }

    # Empty attachments are still transmitted with zero chunks,
    # and not stored on objectstore
    empty = attachments_by_name["foobar.txt"]
    assert empty == {
        "type": "attachment",
        "attachment": {
            "id": mock.ANY,
            "name": "foobar.txt",
            "rate_limited": False,
            "attachment_type": "event.attachment",
            "size": 0,
            "retention_days": 90,
            "chunks": 0,
        },
        "event_id": event_id,
        "project_id": project_id,
    }


@pytest.mark.parametrize("rate_limits", [[], ["attachment"], ["attachment_item"]])
def test_attachments_ratelimit(
    mini_sentry, relay_with_processing, outcomes_consumer, rate_limits
):
    event_id = "515539018c9b4260a6f999572f1661ee"

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["quotas"] = [
        {"categories": rate_limits, "limit": 0, "reasonCode": "static_disabled_quota"}
    ]
    relay = relay_with_processing()

    outcomes_consumer = outcomes_consumer()
    attachments = [("att_1", "foo.txt", b"this is an attachment")]

    # First attachment returns 200 but is rate limited in processing
    relay.send_attachments(project_id, event_id, attachments)

    outcomes_consumer.assert_rate_limited(
        "static_disabled_quota", categories=["attachment", "attachment_item"]
    )

    # Second attachment returns 429 in endpoint
    with pytest.raises(HTTPError) as excinfo:
        relay.send_attachments(project_id, event_id, attachments)
    assert excinfo.value.response.status_code == 429
    outcomes_consumer.assert_rate_limited(
        "static_disabled_quota", categories=["attachment", "attachment_item"]
    )


def test_attachments_pii(mini_sentry, relay):
    event_id = "515539018c9b4260a6f999572f1661ee"

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["piiConfig"] = {
        "rules": {"0": {"type": "ip", "redaction": {"method": "remove"}}},
        "applications": {"$attachments.'foo.txt'": ["0"]},
    }
    relay = relay(mini_sentry)

    attachments = [
        ("att_1", "foo.txt", b"here's an IP that should get masked -> 127.0.0.1 <-"),
        (
            "att_2",
            "bar.txt",
            b"here's an IP that should not get scrubbed -> 127.0.0.1 <-",
        ),
    ]

    for attachment in attachments:
        relay.send_attachments(project_id, event_id, [attachment])

    payloads = {
        mini_sentry.get_captured_envelope().items[0].payload.bytes for _ in range(2)
    }
    assert payloads == {
        b"here's an IP that should get masked -> ********* <-",
        b"here's an IP that should not get scrubbed -> 127.0.0.1 <-",
    }


@pytest.mark.parametrize(
    "feature_flags, expected",
    [
        ([], "************"),
        (["organizations:view-hierarchy-scrubbing"], "************"),
    ],
)
def test_view_hierarchy_scrubbing(mini_sentry, relay, feature_flags, expected):
    event_id = "515539018c9b4260a6f999572f1661ee"
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = feature_flags
    project_config["config"]["piiConfig"] = {
        "rules": {"0": {"type": "ip", "redaction": {"method": "mask"}}},
        "applications": {"$attachments.'view-hierarchy.json'": ["0"], "$string": ["0"]},
    }
    relay = relay(mini_sentry)

    json_payload = {
        "rendering_system": "UIKIT",
        "identifier": "129.16.41.92",
    }

    envelope = Envelope(headers=[["event_id", event_id]])
    envelope.add_item(
        Item(
            headers=[["attachment_type", "event.view_hierarchy"]],
            type="attachment",
            payload=PayloadRef(json=json_payload),
            filename="view-hierarchy.json",
        )
    )

    relay.send_envelope(project_id, envelope)

    relay.send_envelope(project_id, envelope)
    payload = json.loads(mini_sentry.get_captured_envelope().items[0].payload.bytes)
    assert payload == {"rendering_system": "UIKIT", "identifier": expected}


@pytest.mark.parametrize(
    "feature_flags, expected",
    [
        ([], b"**************************************************"),
        (
            ["organizations:view-hierarchy-scrubbing"],
            b'{"rendering_system":"UIKIT","password":""}',
        ),
    ],
)
def test_attachment_scrubbing_with_event_with_fallback(
    mini_sentry, relay, feature_flags, expected
):
    """
    Like test_attachment_scrubbing_with_fallback, but goes through the ErrorsProcessor
    """
    event_id = "515539018c9b4260a6f999572f1661ee"
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"].setdefault("features", []).extend(feature_flags)
    project_config["config"]["piiConfig"] = {
        "rules": {"0": {"type": "password", "redaction": {"method": "remove"}}},
        "applications": {
            "$string": ["@password:remove"],
            "$attachments.'view-hierarchy.json'": ["0"],
        },
    }
    relay = relay(mini_sentry)
    json_payload = {
        "rendering_system": "UIKIT",
        "password": "hunter42",
    }
    envelope = Envelope(headers=[["event_id", event_id]])
    envelope.add_event({"message": "Hello, World!"})
    envelope.add_item(
        Item(
            headers=[["attachment_type", "event.view_hierarchy"]],
            type="attachment",
            payload=PayloadRef(json=json_payload),
            filename="view-hierarchy.json",
        )
    )
    relay.send_envelope(project_id, envelope)
    envelope = mini_sentry.get_captured_envelope()
    attachment = next(item for item in envelope.items if item.type == "attachment")
    payload = attachment.payload.bytes
    assert payload == expected


@pytest.mark.parametrize(
    "feature_flags, expected",
    [
        ([], b"**************************************************"),
        (
            ["organizations:view-hierarchy-scrubbing"],
            b'{"rendering_system":"UIKIT","password":""}',
        ),
    ],
)
def test_attachment_scrubbing_with_fallback(
    mini_sentry, relay, feature_flags, expected
):
    """
    If the feature flag is disabled, it will be scrubbed as binary file so it masks the entire attachment.
    If the feature flag is enabled, it will understand that it's json and only remove the password content
    """
    event_id = "515539018c9b4260a6f999572f1661ee"
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = feature_flags
    project_config["config"]["piiConfig"] = {
        "rules": {"0": {"type": "password", "redaction": {"method": "remove"}}},
        "applications": {
            "$string": ["@password:remove"],
            "$attachments.'view-hierarchy.json'": ["0"],
        },
    }

    relay = relay(mini_sentry)
    json_payload = {
        "rendering_system": "UIKIT",
        "password": "hunter42",
    }

    envelope = Envelope(headers=[["event_id", event_id]])
    envelope.add_item(
        Item(
            headers=[["attachment_type", "event.view_hierarchy"]],
            type="attachment",
            payload=PayloadRef(json=json_payload),
            filename="view-hierarchy.json",
        )
    )

    relay.send_envelope(project_id, envelope)

    payload = mini_sentry.get_captured_envelope().items[0].payload.bytes
    assert payload == expected


def test_view_hierarchy_not_scrubbed_without_config(mini_sentry, relay):
    event_id = "515539018c9b4260a6f999572f1661ee"
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["piiConfig"] = {}
    relay = relay(mini_sentry)

    json_payload = {"rendering_system": "UIKIT", "identifier": "129.16.41.92"}

    envelope = Envelope(headers=[["event_id", event_id]])
    envelope.add_item(
        Item(
            headers=[["attachment_type", "event.view_hierarchy"]],
            type="attachment",
            payload=PayloadRef(json=json_payload),
            filename="view-hierarchy.json",
        )
    )

    relay.send_envelope(project_id, envelope)
    payload = json.loads(mini_sentry.get_captured_envelope().items[0].payload.bytes)
    assert payload == json_payload


def test_attachments_pii_logfile(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["piiConfig"] = {
        "rules": {
            "0": {"type": "email", "redaction": {"method": "mask"}},
            "1": {"type": "userpath", "redaction": {"method": "remove"}},
        },
        "applications": {"$attachments.'logfile.txt'": ["0", "1"]},
    }
    relay = relay(mini_sentry)

    attachment = r"""Alice Johnson
alice.johnson@example.com
+1234567890
4111 1111 1111 1111
Bob Smith bob.smith@example.net +9876543210 5500 0000 0000 0004
Charlie Brown charlie.brown@example.org +1928374650 3782 822463 10005
Dana White dana.white@example.co.uk +1029384756 6011 0009 9013 9424
path=c:\Users\yan\mylogfile.txt
password=mysupersecretpassword123"""

    envelope = Envelope()
    item = Item(
        payload=attachment, type="attachment", headers={"filename": "logfile.txt"}
    )
    envelope.add_item(item)

    relay.send_envelope(project_id, envelope)

    scrubbed_payload = mini_sentry.get_captured_envelope().items[0].payload.bytes

    assert scrubbed_payload == rb"""Alice Johnson
*************************
+1234567890
4111 1111 1111 1111
Bob Smith ********************* +9876543210 5500 0000 0000 0004
Charlie Brown ************************* +1928374650 3782 822463 10005
Dana White ************************ +1029384756 6011 0009 9013 9424
path=c:\Users\***\mylogfile.txt
password=mysupersecretpassword123"""


def test_attachments_quotas(
    mini_sentry,
    relay_with_processing,
    attachments_consumer,
    outcomes_consumer,
):
    event_id = "515539018c9b4260a6f999572f1661ee"
    attachment_body = b"blabla"

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["quotas"] = [
        {
            "id": f"test_rate_limiting_{uuid.uuid4().hex}",
            "categories": ["attachment"],
            "window": 3600,
            "limit": 5 * len(attachment_body),
            "reasonCode": "attachments_exceeded",
        }
    ]
    relay = relay_with_processing()

    attachments_consumer = attachments_consumer()
    outcomes_consumer = outcomes_consumer()
    attachments = [("att_1", "foo.txt", attachment_body)]

    for i in range(5):
        relay.send_attachments(
            project_id, event_id, [("att_1", "%s.txt" % i, attachment_body)]
        )
        attachment = attachments_consumer.get_individual_attachment()
        assert attachment["attachment"]["name"] == "%s.txt" % i
        assert attachment["attachment"]["data"] == attachment_body

    # First attachment returns 200 but is rate limited in processing
    relay.send_attachments(project_id, event_id, attachments)

    outcomes_consumer.assert_rate_limited(
        "attachments_exceeded",
        categories={"attachment": len(attachment_body), "attachment_item": 1},
    )

    # Second attachment returns 429 in endpoint
    with pytest.raises(HTTPError) as excinfo:
        relay.send_attachments(42, event_id, attachments)
    assert excinfo.value.response.status_code == 429
    outcomes_consumer.assert_rate_limited(
        "attachments_exceeded",
        categories={"attachment": len(attachment_body), "attachment_item": 1},
    )


def test_attachments_quotas_items(
    mini_sentry,
    relay_with_processing,
    attachments_consumer,
    outcomes_consumer,
):
    event_id = "515539018c9b4260a6f999572f1661ee"
    attachment_body = b"blabla"

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["quotas"] = [
        {
            "id": f"test_rate_limiting_{uuid.uuid4().hex}",
            "categories": ["attachment_item"],
            "window": 3600,
            "limit": 5,
            "reasonCode": "attachments_exceeded",
        }
    ]
    relay = relay_with_processing()

    attachments_consumer = attachments_consumer()
    outcomes_consumer = outcomes_consumer()
    attachments = [("att_1", "foo.txt", attachment_body)]

    for i in range(5):
        relay.send_attachments(
            project_id, event_id, [("att_1", "%s.txt" % i, attachment_body)]
        )
        attachment = attachments_consumer.get_individual_attachment()
        assert attachment["attachment"]["name"] == "%s.txt" % i
        assert attachment["attachment"]["data"] == attachment_body

    # First attachment returns 200 but is rate limited in processing
    relay.send_attachments(project_id, event_id, attachments)

    outcomes_consumer.assert_rate_limited(
        "attachments_exceeded",
        categories={"attachment": len(attachment_body), "attachment_item": 1},
    )

    # Second attachment returns 429 in endpoint
    with pytest.raises(HTTPError) as excinfo:
        relay.send_attachments(42, event_id, attachments)
    assert excinfo.value.response.status_code == 429

    outcomes_consumer.assert_rate_limited(
        "attachments_exceeded",
        categories={"attachment": len(attachment_body), "attachment_item": 1},
    )


def test_view_hierarchy_processing(
    mini_sentry, relay_with_processing, attachments_consumer, outcomes_consumer
):
    project_id = 42
    event_id = "515539018c9b4260a6f999572f1661ee"

    mini_sentry.add_full_project_config(project_id)
    relay = relay_with_processing()
    attachments_consumer = attachments_consumer()
    outcomes_consumer = outcomes_consumer()

    json_payload = {"rendering_system": "compose", "windows": []}
    # separators are used to produce json without whitespaces
    expected_payload = json.dumps(json_payload, separators=(",", ":")).encode()

    envelope = Envelope(headers=[["event_id", event_id]])
    envelope.add_item(
        Item(
            headers=[["attachment_type", "event.view_hierarchy"]],
            type="attachment",
            payload=PayloadRef(json=json_payload, bytes=expected_payload),
        )
    )

    relay.send_envelope(project_id, envelope)

    attachment = attachments_consumer.get_individual_attachment()

    # The ID is random. Just assert that it is there and non-zero.
    assert attachment["attachment"].pop("id")

    assert attachment == {
        "type": "attachment",
        "attachment": {
            "name": "Unnamed Attachment",
            "rate_limited": False,
            "content_type": "application/json",
            "attachment_type": "event.view_hierarchy",
            "size": len(expected_payload),
            "retention_days": 90,
            "data": expected_payload,
        },
        "event_id": event_id,
        "project_id": project_id,
    }

    outcomes_consumer.assert_empty()


@pytest.mark.parametrize("use_objectstore", [False, True])
def test_event_with_attachment(
    mini_sentry,
    relay_with_processing,
    attachments_consumer,
    transactions_consumer,
    use_objectstore,
    objectstore,
):
    project_id = 42
    event_id = "515539018c9b4260a6f999572f1661ee"

    mini_sentry.add_full_project_config(project_id)

    if use_objectstore:
        mini_sentry.global_config["options"][
            "relay.objectstore-attachments.sample-rate"
        ] = 1.0

    relay = relay_with_processing()
    attachments_consumer = attachments_consumer()
    transactions_consumer = transactions_consumer()
    objectstore = objectstore("attachments", project_id)

    # event attachments are always sent as chunks, and added to events
    envelope = Envelope(headers=[["event_id", event_id]])
    envelope.add_event({"message": "Hello, World!"})
    envelope.add_item(
        Item(
            type="attachment",
            payload=PayloadRef(bytes=b"event attachment"),
        )
    )

    relay.send_envelope(project_id, envelope)

    if not use_objectstore:
        chunk, _ = attachments_consumer.get_attachment_chunk()
        assert chunk == b"event attachment"

    _, event_message = attachments_consumer.get_event()

    assert event_message["attachments"][0].pop("id")
    assert list(event_message["attachments"]) == [
        {
            "name": "Unnamed Attachment",
            "rate_limited": False,
            "content_type": "application/octet-stream",
            "attachment_type": "event.attachment",
            "size": len(b"event attachment"),
            "retention_days": 90,
            **({"stored_id": mock.ANY} if use_objectstore else {"chunks": 1}),
        }
    ]

    if use_objectstore:
        stored_id = event_message["attachments"][0]["stored_id"]
        assert objectstore.get(stored_id).payload.read() == b"event attachment"

    # transaction attachments are sent as individual attachments,
    # either using chunks by default, or contents inlined
    envelope = Envelope(headers=[["event_id", event_id]])
    envelope.add_transaction(make_transaction({"event_id": event_id}))
    envelope.add_item(
        Item(
            type="attachment",
            payload=PayloadRef(bytes=b"transaction attachment"),
        )
    )

    relay.send_envelope(project_id, envelope)

    expected_attachment = {
        "name": "Unnamed Attachment",
        "rate_limited": False,
        "content_type": "application/octet-stream",
        "attachment_type": "event.attachment",
        "size": len(b"transaction attachment"),
        "retention_days": 90,
        **(
            {"stored_id": mock.ANY}
            if use_objectstore
            else {"data": b"transaction attachment"}
        ),
    }

    attachment = attachments_consumer.get_individual_attachment()
    assert attachment["attachment"].pop("id")

    if use_objectstore:
        stored_id = attachment["attachment"]["stored_id"]
        assert objectstore.get(stored_id).payload.read() == b"transaction attachment"

    assert attachment == {
        "type": "attachment",
        "attachment": expected_attachment,
        "event_id": event_id,
        "project_id": project_id,
    }

    _, event = transactions_consumer.get_event()
    assert event["event_id"] == event_id


def test_form_data_is_rejected(
    mini_sentry, relay_with_processing, attachments_consumer, outcomes_consumer
):
    """
    Test that form data entries (those without filenames) are rejected and generate outcomes.
    """
    project_id = 42
    event_id = "515539018c9b4260a6f999572f1661ee"

    mini_sentry.add_full_project_config(project_id)
    relay = relay_with_processing()
    attachments_consumer = attachments_consumer()

    # Create attachments with both file content and form data
    attachments = [
        ("att_1", "foo.txt", b"file content"),  # Valid file attachment
        ("form_key", None, b"form value"),  # Form data that should be rejected
    ]

    relay.send_attachments(project_id, event_id, attachments)

    # Check that only the file attachment was processed
    attachment = attachments_consumer.get_individual_attachment()
    assert attachment["attachment"].pop("id")  # ID is random
    assert attachment == {
        "type": "attachment",
        "attachment": {
            "name": "foo.txt",
            "rate_limited": False,
            "attachment_type": "event.attachment",
            "size": len(b"file content"),
            "retention_days": 90,
            "data": b"file content",
        },
        "event_id": event_id,
        "project_id": project_id,
    }

    # Verify no more attachments were processed
    attachments_consumer.assert_empty()
