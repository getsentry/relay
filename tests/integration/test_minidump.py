import os
from unittest import mock

import msgpack

import json
import pytest
from requests import HTTPError
from sentry_sdk.envelope import Envelope
from uuid import UUID

from sentry_relay.consts import DataCategory
from .test_attachment_ref import upload_and_make_ref
from .test_upload import dummy_upload, DUMMY_UPLOAD_LOCATION  # noqa: F401

MINIDUMP_ATTACHMENT_NAME = "upload_file_minidump"
EVENT_ATTACHMENT_NAME = "__sentry-event"
BREADCRUMB_ATTACHMENT_NAME1 = "__sentry-breadcrumb1"
BREADCRUMB_ATTACHMENT_NAME2 = "__sentry-breadcrumb2"


def assert_minidump(minidump_item, assert_payload=True):
    assert minidump_item

    attachment_type = minidump_item.headers.get("attachment_type")
    assert attachment_type == "event.minidump"

    minidump_payload = minidump_item.payload.get_bytes()
    assert minidump_payload

    if assert_payload:
        assert minidump_payload.decode("utf-8") == "MDMP content"


def assert_only_minidump(envelope, assert_payload=True):
    minidump_item = None
    for item in envelope.items:
        item_type = item.headers.get("type")
        if item_type == "attachment":
            attachment_type = item.headers.get("attachment_type")
            assert attachment_type == "event.minidump"
            minidump_item = item

    assert_minidump(minidump_item, assert_payload=assert_payload)


def test_minidump(mini_sentry, relay):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_full_project_config(project_id)

    attachments = [
        (MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "MDMP content"),
    ]

    response = relay.send_minidump(project_id=project_id, files=attachments)

    # response body must be text containing the hyphenated event id
    body = response.text.strip()
    event_id = UUID(body)
    assert str(event_id) == body

    envelope = mini_sentry.get_captured_envelope()
    assert envelope

    # the event id from the response should match the envelope
    assert UUID(envelope.headers.get("event_id")) == event_id
    assert_only_minidump(envelope)


def test_minidump_attachments(mini_sentry, relay):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_full_project_config(project_id)

    event = {"event_id": "2dd132e467174db48dbaddabd3cbed57", "user": {"id": "123"}}
    breadcrumbs1 = {
        "timestamp": 1461185755,
        "message": "A",
    }
    breadcrumbs2 = {
        "timestamp": 1461185750,
        "message": "B",
    }

    sentry_event = msgpack.packb(event)
    sentry_breadcrumbs1 = msgpack.packb(breadcrumbs1)
    sentry_breadcrumbs2 = msgpack.packb(breadcrumbs2)

    attachments = [
        (MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "MDMP content"),
        (EVENT_ATTACHMENT_NAME, EVENT_ATTACHMENT_NAME, sentry_event),
        (BREADCRUMB_ATTACHMENT_NAME1, BREADCRUMB_ATTACHMENT_NAME1, sentry_breadcrumbs1),
        (BREADCRUMB_ATTACHMENT_NAME2, BREADCRUMB_ATTACHMENT_NAME2, sentry_breadcrumbs2),
        ("attachment1", "attach1.txt", "attachment content"),
    ]

    relay.send_minidump(project_id=project_id, files=attachments)
    envelope = mini_sentry.get_captured_envelope()
    assert envelope

    # Check that the envelope assumes the given event id
    assert envelope.headers.get("event_id") == "2dd132e467174db48dbaddabd3cbed57"

    # Check that event payload is applied
    event_item = envelope.get_event()
    assert event_item["event_id"] == "2dd132e467174db48dbaddabd3cbed57"
    assert event_item["user"]["id"] == "123"

    # Breadcrumbs are truncated to the length of the longer attachment (1)
    assert event_item["breadcrumbs"]["values"][0]["message"] == "A"

    minidump_item = None
    attachment_item = None

    # Sentry attachments must be removed from the envelope
    for item in envelope.items:
        if item.headers.get("type") != "attachment":
            continue

        name = item.headers.get("filename")
        if name == "minidump.dmp":
            minidump_item = item
            assert item.headers.get("attachment_type") == "event.minidump"
        elif name == "attach1.txt":
            attachment_item = item
            assert item.headers.get("attachment_type") == "event.attachment"
        else:
            raise AssertionError("Unexpected attachment")

    assert_minidump(minidump_item)

    assert attachment_item
    attachment_payload = attachment_item.payload.get_bytes()
    assert attachment_payload
    assert attachment_payload.decode("utf-8") == "attachment content"


def test_minidump_multipart(mini_sentry, relay):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_full_project_config(project_id)

    attachments = [
        (MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "MDMP content"),
    ]

    params = [
        ("sentry[event_id]", "2dd132e467174db48dbaddabd3cbed57"),
        ("sentry[user][id]", "123"),
    ]

    relay.send_minidump(project_id=project_id, files=attachments, params=params)
    envelope = mini_sentry.get_captured_envelope()

    assert envelope
    assert_only_minidump(envelope)

    # Check that the envelope assumes the given event id
    assert envelope.headers.get("event_id") == "2dd132e467174db48dbaddabd3cbed57"

    # Check that event payload is applied
    event_item = envelope.get_event()
    assert event_item["event_id"] == "2dd132e467174db48dbaddabd3cbed57"
    assert event_item["user"]["id"] == "123"


def test_minidump_sentry_json(mini_sentry, relay):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_full_project_config(project_id)

    attachments = [
        (MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "MDMP content"),
    ]

    event_json = '{"event_id":"2dd132e467174db48dbaddabd3cbed57","user":{"id":"123"}}'
    params = [
        ("sentry", event_json),
    ]

    relay.send_minidump(project_id=project_id, files=attachments, params=params)
    envelope = mini_sentry.get_captured_envelope()

    assert envelope
    assert_only_minidump(envelope)

    # Check that the envelope assumes the given event id
    assert envelope.headers.get("event_id") == "2dd132e467174db48dbaddabd3cbed57"

    # Check that event payload is applied
    event_item = envelope.get_event()
    assert event_item["event_id"] == "2dd132e467174db48dbaddabd3cbed57"
    assert event_item["user"]["id"] == "123"


def test_minidump_sentry_namespace_json(mini_sentry, relay):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_full_project_config(project_id)

    attachments = [
        (MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "MDMP content"),
    ]

    event_json = '{"event_id":"2dd132e467174db48dbaddabd3cbed57"}'
    namespace_json = '{"user":{"id":"123"}}'
    params = [("sentry", event_json), ("sentry___global", namespace_json)]

    relay.send_minidump(project_id=project_id, files=attachments, params=params)
    envelope = mini_sentry.get_captured_envelope()

    assert envelope
    assert_only_minidump(envelope)

    # Check that the envelope assumes the given event id
    assert envelope.headers.get("event_id") == "2dd132e467174db48dbaddabd3cbed57"

    # Check that event payload is applied
    event_item = envelope.get_event()
    assert event_item["event_id"] == "2dd132e467174db48dbaddabd3cbed57"
    assert event_item["user"]["id"] == "123"


def test_minidump_sentry_json_chunked(mini_sentry, relay):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_full_project_config(project_id)

    attachments = [
        (MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "MDMP content"),
    ]

    event_json = '{"event_id":"2dd132e467174db48dbaddabd3cbed57","user":{"id":"123"}}'
    params = [
        ("sentry__1", event_json[:30]),
        ("sentry__2", event_json[30:]),
    ]

    response = relay.send_minidump(
        project_id=project_id, files=attachments, params=params
    )
    envelope = mini_sentry.get_captured_envelope()

    assert envelope
    assert_only_minidump(envelope)

    # With chunked JSON payloads, inferring event ids is not supported.
    # The event id is randomized by Relay and overwritten.
    event_id = UUID(response.text.strip()).hex
    assert event_id != "2dd132e467174db48dbaddabd3cbed57"
    assert envelope.headers.get("event_id") == event_id

    # Check that event payload is applied
    event_item = envelope.get_event()
    assert event_item["event_id"] == event_id
    assert event_item["user"]["id"] == "123"


def test_minidump_invalid_json(mini_sentry, relay):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_full_project_config(project_id)

    attachments = [
        (MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "MDMP content"),
    ]

    params = [
        ("sentry", "{{{{"),
    ]

    relay.send_minidump(project_id=project_id, files=attachments, params=params)
    envelope = mini_sentry.get_captured_envelope()

    assert envelope
    assert_only_minidump(envelope)


def test_minidump_invalid_magic(mini_sentry, relay):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_full_project_config(project_id)

    attachments = [
        (MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "content without MDMP magic"),
    ]

    with pytest.raises(HTTPError):
        relay.send_minidump(project_id=project_id, files=attachments)


def test_minidump_invalid_field(mini_sentry, relay):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_full_project_config(project_id)

    attachments = [
        ("unknown_field_name", "minidump.dmp", "MDMP content"),
    ]

    with pytest.raises(HTTPError):
        relay.send_minidump(project_id=project_id, files=attachments)


@pytest.mark.parametrize(
    "content_type", ("application/octet-stream", "application/x-dmp")
)
def test_minidump_raw(mini_sentry, relay, content_type):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_full_project_config(project_id)

    relay.request(
        "post",
        "/api/{}/minidump?sentry_key={}".format(
            project_id, mini_sentry.get_dsn_public_key(project_id)
        ),
        headers={"Content-Type": content_type},
        data="MDMP content",
    )

    envelope = mini_sentry.get_captured_envelope()

    assert envelope
    assert_only_minidump(envelope)


@pytest.mark.parametrize("test_file_name", ("electron_simple.dmp", "electron.dmp"))
def test_minidump_nested_formdata(mini_sentry, relay, test_file_name):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_full_project_config(project_id)

    dmp_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "native", test_file_name
    )

    with open(dmp_path, "rb") as f:
        dmp_file = f.read()

    attachments = [(MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", dmp_file)]

    relay.send_minidump(project_id=project_id, files=attachments)
    envelope = mini_sentry.get_captured_envelope()

    assert envelope
    assert_only_minidump(envelope, assert_payload=False)


def test_minidump_invalid_nested_formdata(mini_sentry, relay):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_full_project_config(project_id)

    dmp_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "native", "bad_electron_simple.dmp"
    )

    with open(dmp_path, "rb") as f:
        dmp_file = f.read()

    attachments = [(MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", dmp_file)]

    with pytest.raises(HTTPError):
        relay.send_minidump(project_id=project_id, files=attachments)


@pytest.mark.parametrize(
    "rate_limit,minidump_filename,use_objectstore",
    [
        (None, "minidump.dmp", True),
        (None, "minidump.dmp", False),
        ("attachment", "minidump.dmp", True),
        ("attachment", "minidump.dmp", False),
        ("transaction", "minidump.dmp", False),
        (None, "minidump.dmp.gz", False),
        (None, "minidump.dmp.xz", False),
        (None, "minidump.dmp.bz2", False),
        (None, "minidump.dmp.zst", False),
    ],
)
def test_minidump_with_processing(
    mini_sentry,
    relay_with_processing,
    attachments_consumer,
    outcomes_consumer,
    rate_limit,
    minidump_filename,
    use_objectstore,
    objectstore,
):
    dmp_path = os.path.join(os.path.dirname(__file__), "fixtures/native/minidump.dmp")
    with open(dmp_path, "rb") as f:
        content = f.read()

    # if we test a compressed minidump fixture we load both, the plain dump and the compressed one.
    if minidump_filename != "minidump.dmp":
        compressed_dmp_path = os.path.join(
            os.path.dirname(__file__), f"fixtures/native/{minidump_filename}"
        )
        with open(compressed_dmp_path, "rb") as f:
            compressed_content = f.read()

    if use_objectstore:
        mini_sentry.global_config["options"][
            "relay.objectstore-attachments.sample-rate"
        ] = 1.0
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["eventRetention"] = 50000

    options = {
        "processing": {
            "objectstore": {
                "objectstore_url": "http://127.0.0.1:8888/" if use_objectstore else None
            }
        }
    }
    relay = relay_with_processing(options)

    # Disable scurbbing, the basic and full project configs from the mini_sentry fixture
    # will modify the minidump since it contains user paths in the module list.  This breaks
    # get_attachment_chunk() below.
    del project_config["config"]["piiConfig"]

    # Configure rate limits. The transaction rate limit does not affect minidumps. The attachment
    # rate limit would affect them, but since minidumps are required for processing they are still
    # passed through. Only when "error" is limited will the minidump be rejected.
    if rate_limit:
        project_config["config"]["quotas"] = [
            {
                "categories": [rate_limit],
                "limit": 0,
                "reasonCode": "static_disabled_quota",
            }
        ]

    attachments_consumer = attachments_consumer()
    outcomes_consumer = outcomes_consumer()

    # if we test a compressed minidump fixture we upload the compressed content
    # but retrieve the uncompressed minidump content from the `attachments_consumer` below.
    attachments = [
        (
            MINIDUMP_ATTACHMENT_NAME,
            minidump_filename,
            content if minidump_filename == "minidump.dmp" else compressed_content,
        )
    ]

    response = relay.send_minidump(project_id=project_id, files=attachments)

    attachment = b""
    num_chunks = 0
    attachment_id = None

    if not use_objectstore:
        while attachment != content:
            chunk, message = attachments_consumer.get_attachment_chunk()
            attachment_id = attachment_id or message["id"]
            attachment += chunk
            num_chunks += 1

    event, message = attachments_consumer.get_event()

    assert UUID(event["event_id"]) == UUID(response.text)

    # Check the placeholder payload
    assert event["platform"] == "native"
    assert event["exception"]["values"][0]["mechanism"]["type"] == "minidump"

    # Check information extracted from the minidump
    assert event["timestamp"] == 1574692481.0  # 11/25/2019 @ 2:34pm (UTC)

    # Check that the SDK name is correctly detected
    assert event["sdk"]["name"] == "minidump.unknown"

    if not use_objectstore:
        assert list(message["attachments"]) == [
            {
                "id": attachment_id,
                "name": "minidump.dmp",
                "rate_limited": rate_limit == "attachment",
                "attachment_type": "event.minidump",
                "content_type": "application/x-dmp",
                "size": len(content),
                "retention_days": 50000,
                "chunks": num_chunks,
            }
        ]
    else:
        (attachment,) = message["attachments"]

        objectstore_key = attachment.pop("stored_id")
        objectstore = objectstore("attachments", project_id)
        assert objectstore.get(objectstore_key).payload.read() == content

        assert attachment.pop("id")
        assert attachment == {
            "name": "minidump.dmp",
            "rate_limited": rate_limit == "attachment",
            "attachment_type": "event.minidump",
            "content_type": "application/x-dmp",
            "size": len(content),
            "retention_days": 50000,
        }

    assert "errors" not in event

    if rate_limit == "attachment":
        assert outcomes_consumer.get_aggregated_outcomes(n=2) == [
            {
                "category": DataCategory.ATTACHMENT.value,
                "key_id": 123,
                "org_id": 1,
                "outcome": 2,
                "project_id": 42,
                "quantity": len(content),
                "reason": "static_disabled_quota",
            },
            {
                "category": DataCategory.ATTACHMENT_ITEM.value,
                "key_id": 123,
                "org_id": 1,
                "outcome": 2,
                "project_id": 42,
                "quantity": 1,
                "reason": "static_disabled_quota",
            },
        ]


def test_minidump_with_processing_invalid(
    mini_sentry, relay_with_processing, attachments_consumer
):
    content = b"MDMP invalid garbage"

    relay = relay_with_processing()

    project_id = 42
    mini_sentry.add_full_project_config(project_id)

    attachments_consumer = attachments_consumer()

    attachments = [(MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", content)]
    relay.send_minidump(project_id=42, files=attachments)

    attachment = b""
    num_chunks = 0
    attachment_id = None

    while attachment != content:
        chunk, message = attachments_consumer.get_attachment_chunk()
        attachment_id = attachment_id or message["id"]
        attachment += chunk
        num_chunks += 1

    event, message = attachments_consumer.get_event()

    # Check the placeholder payload
    assert event["platform"] == "native"
    assert event["exception"]["values"][0]["mechanism"]["type"] == "minidump"

    assert list(message["attachments"]) == [
        {
            "id": attachment_id,
            "name": "minidump.dmp",
            "rate_limited": False,
            "content_type": "application/x-dmp",
            "attachment_type": "event.minidump",
            "size": len(content),
            "retention_days": 90,
            "chunks": num_chunks,
        }
    ]


@pytest.mark.parametrize("rate_limits", [[], ["error"], ["error", "attachment"]])
def test_minidump_ratelimit(
    mini_sentry, relay_with_processing, outcomes_consumer, rate_limits
):
    relay = relay_with_processing()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["quotas"] = [
        {"categories": rate_limits, "limit": 0, "reasonCode": "static_disabled_quota"}
    ]

    outcomes_consumer = outcomes_consumer()
    attachments = [(MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "MDMP content")]

    # First minidump returns 200 but is rate limited in processing
    relay.send_minidump(project_id=project_id, files=attachments)
    outcomes_consumer.assert_rate_limited(
        "static_disabled_quota", categories=["error", "attachment", "attachment_item"]
    )

    # Minidumps never return rate limits
    relay.send_minidump(project_id=project_id, files=attachments)
    outcomes_consumer.assert_rate_limited(
        "static_disabled_quota", categories=["error", "attachment", "attachment_item"]
    )


def test_crashpad_annotations(mini_sentry, relay_with_processing, attachments_consumer):
    dmp_path = os.path.join(
        os.path.dirname(__file__), "fixtures/native/annotations.dmp"
    )
    with open(dmp_path, "rb") as f:
        content = f.read()

    relay = relay_with_processing()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)

    # Disable scurbbing, the basic and full project configs from the mini_sentry fixture
    # will modify the minidump since it contains user paths in the module list.  This breaks
    # get_attachment_chunk() below.
    del project_config["config"]["piiConfig"]

    attachments_consumer = attachments_consumer()
    attachments = [(MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", content)]
    relay.send_minidump(project_id=project_id, files=attachments)

    # Only one attachment chunk expected
    attachments_consumer.get_attachment_chunk()
    event, _ = attachments_consumer.get_event()

    # Check the placeholder payload
    assert event["contexts"]["crashpad"] == {"hello": "world"}
    assert event["contexts"]["dyld"] == {
        "annotations": ["dyld2 mode"],
        "type": "crashpad",
    }


def test_chromium_stability_report(
    mini_sentry, relay_with_processing, attachments_consumer
):
    dmp_path = os.path.join(
        os.path.dirname(__file__), "fixtures/native/stability_report.dmp"
    )
    with open(dmp_path, "rb") as f:
        content = f.read()

    relay = relay_with_processing()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)

    # Disable scurbbing, the basic and full project configs from the mini_sentry fixture
    # will modify the minidump since it contains user paths in the module list.  This breaks
    # get_attachment_chunk() below.
    del project_config["config"]["piiConfig"]

    attachments_consumer = attachments_consumer()
    attachments = [(MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", content)]
    relay.send_minidump(project_id=project_id, files=attachments)

    # Only one attachment chunk expected
    attachments_consumer.get_attachment_chunk()
    event, _ = attachments_consumer.get_event()

    # Check the stability_report context
    assert event["contexts"]["chromium_stability_report"] == {
        "type": "chromiumstabilityreport",
        "process_states": [
            {
                "file_system_state": {
                    "windows_file_system_state": {"process_handle_count": 302}
                },
                "memory_state": {
                    "windows_memory": {
                        "process_peak_pagefile_usage": 44146688,
                        "process_peak_workingset_size": 84172800,
                        "process_private_usage": 44101632,
                    }
                },
                "process_id": 15212,
            }
        ],
        "system_memory_state": {
            "windows_memory": {
                "system_commit_limit": 26557411328,
                "system_commit_remaining": 14468075520,
                "system_handle_count": 119009,
            }
        },
    }


def test_minidump_placeholder(
    mini_sentry, relay_with_processing, attachments_consumer, objectstore
):
    """
    When a minidump comes in as an attachment placeholder (attachment ref),
    verify that:
    - The event placeholder is created with correct default values
    - The placeholder payload is not parsed as a minidump
    - Default values are used instead of values extracted from a real minidump
    """
    event_id = "515539018c9b4260a6f999572f1661ee"
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"].setdefault("features", []).append(
        "projects:relay-upload-endpoint"
    )
    mini_sentry.global_config["options"][
        "relay.objectstore-attachments.sample-rate"
    ] = 1.0

    relay = relay_with_processing()
    attachments_consumer = attachments_consumer()
    project_key = mini_sentry.get_dsn_public_key(project_id)

    # Upload data via TUS and create an attachment ref (placeholder) for a minidump.
    # The actual bytes here are irrelevant — the key point is that the attachment ref
    # payload (JSON with a signed location) must not be parsed as minidump binary data.
    minidump_data = b"MDMP fake minidump content"
    envelope = Envelope(headers=[["event_id", event_id]])
    envelope.add_item(
        upload_and_make_ref(
            relay,
            project_id,
            project_key,
            minidump_data,
            filename="minidump.dmp",
            content_type="application/x-dmp",
            attachment_type="event.minidump",
        )
    )

    relay.send_envelope(project_id, envelope)
    event, message = attachments_consumer.get_event()

    assert event == {
        "_metrics": {
            "bytes.ingested.event.minidump": len(minidump_data),
        },
        "event_id": "515539018c9b4260a6f999572f1661ee",
        "exception": {
            "values": [
                {
                    "mechanism": {
                        "handled": False,
                        "synthetic": True,
                        "type": "minidump",
                    },
                    "type": "Minidump",
                    "value": "Invalid Minidump",
                },
            ],
        },
        "grouping_config": mock.ANY,
        "key_id": "123",
        "level": "fatal",
        "logger": "",
        "platform": "native",
        "project": 42,
        "received": mock.ANY,
        "sdk": {
            "name": "minidump.upload",
            "version": "0.0.0",
        },
        "timestamp": mock.ANY,
        "type": "error",
        "version": "5",
    }

    # The attachment metadata must reference the minidump stored via the placeholder.
    assert len(message["attachments"]) == 1
    attachment = message["attachments"][0]

    assert attachment == {
        "attachment_type": "event.minidump",
        "content_type": "application/x-dmp",
        "id": mock.ANY,
        "name": "minidump.dmp",
        "rate_limited": False,
        "retention_days": 90,
        "size": 26,
        "stored_id": mock.ANY,
    }

    # Verify the actual data is retrievable from the objectstore.
    stored_id = attachment["stored_id"]
    objectstore_session = objectstore("attachments", project_id)
    assert objectstore_session.get(stored_id).payload.read() == minidump_data


@pytest.mark.parametrize(
    "limit,expected_status_code",
    [("max_attachment_size", 400), ("max_attachments_size", 413)],
)
def test_size_limits(mini_sentry, relay, limit, expected_status_code):
    project_id = 42
    relay = relay(
        mini_sentry,
        {
            "limits": {
                limit: 10,
            }
        },
    )
    mini_sentry.add_full_project_config(project_id)

    attachments = [
        (MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "MDMP content"),
    ]

    params = [
        ("sentry[event_id]", "2dd132e467174db48dbaddabd3cbed57"),
        ("sentry[user][id]", "123"),
    ]

    response = relay.send_minidump(
        project_id=project_id, files=attachments, params=params, raise_for_status=False
    )
    assert response.status_code == expected_status_code


@pytest.mark.parametrize(
    "rollout_enabled,feature_enabled",
    [
        (False, False),
        (True, False),
        (True, True),
    ],
    ids=["no-option", "no-feature", "both-enabled"],
)
def test_minidump_objectstore_uploads(
    mini_sentry,
    relay,
    dummy_upload,  # noqa: F811
    rollout_enabled,
    feature_enabled,
):
    project_id = 42
    minidump_content = b"MDMP content"
    log_content = b"Some log file content"

    project_config = mini_sentry.add_full_project_config(project_id)
    if feature_enabled:
        project_config["config"].setdefault("features", []).append(
            "projects:relay-minidump-attachment-uploads"
        )
    mini_sentry.global_config["options"][
        "relay.minidump-endpoint-fetch-config.rollout-rate"
    ] = (1.0 if rollout_enabled else 0.0)

    relay = relay(mini_sentry)

    response = relay.send_minidump(
        project_id=project_id,
        files=[
            (MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", minidump_content),
            ("logs", "log.txt", log_content),
        ],
    )
    assert response.ok

    envelope = mini_sentry.get_captured_envelope()
    by_name = {
        i.headers.get("filename"): i
        for i in envelope.items
        if i.headers.get("type") == "attachment"
    }
    minidump = by_name["minidump.dmp"]
    logs = by_name["log.txt"]

    assert (
        minidump.headers.get("content_type")
        != "application/vnd.sentry.attachment-ref+json"
    )
    assert minidump.payload.bytes == minidump_content

    if rollout_enabled and feature_enabled:
        assert (
            logs.headers["content_type"] == "application/vnd.sentry.attachment-ref+json"
        )
        assert json.loads(logs.payload.bytes) == {
            "location": DUMMY_UPLOAD_LOCATION,
        }
    else:
        assert (
            logs.headers.get("content_type")
            != "application/vnd.sentry.attachment-ref+json"
        )
        assert logs.payload.bytes == log_content
