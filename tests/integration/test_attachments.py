import pytest
import time
import uuid
import json

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
    mini_sentry.set_global_config_option("relay.inline-attachments.rollout-rate", 1.0)
    relay = relay_with_processing(options)
    attachments_consumer = attachments_consumer()
    outcomes_consumer = outcomes_consumer()

    chunked_contents = b"heavens no" * 20_000
    attachments = [
        ("att_1", "foo.txt", chunked_contents),
        ("att_2", "bar.txt", b"hell yeah"),
        ("att_3", "foobar.txt", b""),
    ]
    relay.send_attachments(project_id, event_id, attachments)

    # A chunked attachment
    attachment_contents = {}
    attachment_ids = []
    attachment_num_chunks = {}

    while set(attachment_contents.values()) != {chunked_contents}:
        chunk, v = attachments_consumer.get_attachment_chunk()
        attachment_contents[v["id"]] = attachment_contents.get(v["id"], b"") + chunk
        if v["id"] not in attachment_ids:
            attachment_ids.append(v["id"])
        num_chunks = 1 + attachment_num_chunks.get(v["id"], 0)
        assert v["chunk_index"] == num_chunks - 1
        attachment_num_chunks[v["id"]] = num_chunks

    (id1,) = attachment_ids
    assert attachment_contents[id1] == chunked_contents
    assert attachment_num_chunks[id1] > 1

    attachment = attachments_consumer.get_individual_attachment()
    assert attachment == {
        "type": "attachment",
        "attachment": {
            "attachment_type": "event.attachment",
            "chunks": attachment_num_chunks[id1],
            "id": id1,
            "name": "foo.txt",
            "size": len(chunked_contents),
            "rate_limited": False,
        },
        "event_id": event_id,
        "project_id": project_id,
    }

    # An inlined attachment
    attachment = attachments_consumer.get_individual_attachment()

    # The ID is random. Just assert that it is there and non-zero.
    assert attachment["attachment"].pop("id")

    assert attachment == {
        "type": "attachment",
        "attachment": {
            "attachment_type": "event.attachment",
            "chunks": 0,
            "data": b"hell yeah",
            "name": "bar.txt",
            "size": len(b"hell yeah"),
            "rate_limited": False,
        },
        "event_id": event_id,
        "project_id": project_id,
    }

    outcomes_consumer.assert_empty()

    # An empty attachment
    attachment = attachments_consumer.get_individual_attachment()

    # The ID is random. Just assert that it is there and non-zero.
    assert attachment["attachment"].pop("id")

    assert attachment == {
        "type": "attachment",
        "attachment": {
            "attachment_type": "event.attachment",
            "chunks": 0,
            "name": "foobar.txt",
            "size": 0,
            "rate_limited": False,
        },
        "event_id": event_id,
        "project_id": project_id,
    }


@pytest.mark.parametrize("rate_limits", [[], ["attachment"]])
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
    attachments = [("att_1", "foo.txt", b"")]

    # First attachment returns 200 but is rate limited in processing
    relay.send_attachments(project_id, event_id, attachments)
    # TODO: There are no outcomes emitted for attachments yet. Instead, sleep to allow Relay to
    # process the event and cache the rate limit
    # outcomes_consumer.assert_rate_limited("static_disabled_quota")
    time.sleep(0.2)

    # Second attachment returns 429 in endpoint
    with pytest.raises(HTTPError) as excinfo:
        relay.send_attachments(project_id, event_id, attachments)
    assert excinfo.value.response.status_code == 429
    # outcomes_consumer.assert_rate_limited("static_disabled_quota")


def test_attachments_quotas(
    mini_sentry,
    relay_with_processing,
    attachments_consumer,
    outcomes_consumer,
):
    event_id = "515539018c9b4260a6f999572f1661ee"
    attachment_body = b"blabla"

    project_id = 42
    mini_sentry.set_global_config_option("relay.inline-attachments.rollout-rate", 1.0)
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
    # TODO: There are no outcomes emitted for attachments yet. Instead, sleep to allow Relay to
    # process the event and cache the rate limit
    # outcomes_consumer.assert_rate_limited("static_disabled_quota")
    time.sleep(1)

    # Second attachment returns 429 in endpoint
    with pytest.raises(HTTPError) as excinfo:
        relay.send_attachments(42, event_id, attachments)
    assert excinfo.value.response.status_code == 429
    # outcomes_consumer.assert_rate_limited("static_disabled_quota")


def test_view_hierarchy_processing(
    mini_sentry, relay_with_processing, attachments_consumer, outcomes_consumer
):
    project_id = 42
    event_id = "515539018c9b4260a6f999572f1661ee"

    mini_sentry.add_full_project_config(project_id)
    mini_sentry.set_global_config_option("relay.inline-attachments.rollout-rate", 1.0)
    relay = relay_with_processing()
    attachments_consumer = attachments_consumer()
    outcomes_consumer = outcomes_consumer()

    json_payload = {"rendering_system": "compose", "windows": []}
    expected_payload = json.dumps(json_payload).encode()

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
            "attachment_type": "event.view_hierarchy",
            "chunks": 0,
            "data": expected_payload,
            "content_type": "application/json",
            "name": "Unnamed Attachment",
            "size": len(expected_payload),
            "rate_limited": False,
        },
        "event_id": event_id,
        "project_id": project_id,
    }

    outcomes_consumer.assert_empty()


@pytest.mark.parametrize("use_inline_attachments", (False, True))
def test_event_with_attachment(
    mini_sentry,
    relay_with_processing,
    attachments_consumer,
    outcomes_consumer,
    use_inline_attachments,
):
    project_id = 42
    event_id = "515539018c9b4260a6f999572f1661ee"

    mini_sentry.add_full_project_config(project_id)
    if use_inline_attachments:
        mini_sentry.set_global_config_option(
            "relay.inline-attachments.rollout-rate", 1.0
        )
    relay = relay_with_processing()
    attachments_consumer = attachments_consumer()
    outcomes_consumer = outcomes_consumer()

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

    chunk, _ = attachments_consumer.get_attachment_chunk()
    assert chunk == b"event attachment"

    _, event_message = attachments_consumer.get_event()

    assert event_message["attachments"][0].pop("id")
    assert list(event_message["attachments"]) == [
        {
            "attachment_type": "event.attachment",
            "chunks": 1,
            "content_type": "application/octet-stream",
            "name": "Unnamed Attachment",
            "size": len(b"event attachment"),
            "rate_limited": False,
        }
    ]

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

    if not use_inline_attachments:
        chunk, _ = attachments_consumer.get_attachment_chunk()
        assert chunk == b"transaction attachment"

    expected_attachment = {
        "attachment_type": "event.attachment",
        "chunks": 1,
        "content_type": "application/octet-stream",
        "name": "Unnamed Attachment",
        "size": len(b"transaction attachment"),
        "rate_limited": False,
    }
    if use_inline_attachments:
        expected_attachment["chunks"] = 0
        expected_attachment["data"] = b"transaction attachment"

    attachment = attachments_consumer.get_individual_attachment()
    assert attachment["attachment"].pop("id")
    assert attachment == {
        "type": "attachment",
        "attachment": expected_attachment,
        "event_id": event_id,
        "project_id": project_id,
    }
