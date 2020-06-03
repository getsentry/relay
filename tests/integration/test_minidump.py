import os

import msgpack

import pytest
from requests import HTTPError
import re
from uuid import UUID

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
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[project_id] = mini_sentry.full_project_config()

    attachments = [
        (MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "MDMP content"),
    ]

    response = relay.send_minidump(project_id=project_id, files=attachments)

    # response body must be text containing the hyphenated event id
    body = response.text.strip()
    event_id = UUID(body)
    assert str(event_id) == body

    envelope = mini_sentry.captured_events.get(timeout=1)
    assert envelope

    # the event id from the response should match the envelope
    assert UUID(envelope.headers.get("event_id")) == event_id
    assert_only_minidump(envelope)


def test_minidump_attachments(mini_sentry, relay):
    project_id = 42
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[project_id] = mini_sentry.full_project_config()

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
    envelope = mini_sentry.captured_events.get(timeout=1)
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
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[project_id] = mini_sentry.full_project_config()

    attachments = [
        (MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "MDMP content"),
    ]

    params = [
        ("sentry[event_id]", "2dd132e467174db48dbaddabd3cbed57"),
        ("sentry[user][id]", "123"),
    ]

    relay.send_minidump(project_id=project_id, files=attachments, params=params)
    envelope = mini_sentry.captured_events.get(timeout=1)

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
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[project_id] = mini_sentry.full_project_config()

    attachments = [
        (MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "MDMP content"),
    ]

    event_json = '{"event_id":"2dd132e467174db48dbaddabd3cbed57","user":{"id":"123"}}'
    params = [
        ("sentry", event_json),
    ]

    relay.send_minidump(project_id=project_id, files=attachments, params=params)
    envelope = mini_sentry.captured_events.get(timeout=1)

    assert envelope
    assert_only_minidump(envelope)

    # Check that the envelope assumes the given event id
    assert envelope.headers.get("event_id") == "2dd132e467174db48dbaddabd3cbed57"

    # Check that event payload is applied
    event_item = envelope.get_event()
    assert event_item["event_id"] == "2dd132e467174db48dbaddabd3cbed57"
    assert event_item["user"]["id"] == "123"


def test_minidump_invalid_json(mini_sentry, relay):
    project_id = 42
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[project_id] = mini_sentry.full_project_config()

    attachments = [
        (MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "MDMP content"),
    ]

    params = [
        ("sentry", "{{{{"),
    ]

    relay.send_minidump(project_id=project_id, files=attachments, params=params)
    envelope = mini_sentry.captured_events.get(timeout=1)

    assert envelope
    assert_only_minidump(envelope)


def test_minidump_invalid_magic(mini_sentry, relay):
    project_id = 42
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[project_id] = mini_sentry.full_project_config()

    attachments = [
        (MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "content without MDMP magic"),
    ]

    with pytest.raises(HTTPError):
        relay.send_minidump(project_id=project_id, files=attachments)


def test_minidump_invalid_field(mini_sentry, relay):
    project_id = 42
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[project_id] = mini_sentry.full_project_config()

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
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[project_id] = mini_sentry.full_project_config()

    relay.request(
        "post",
        "/api/42/minidump?sentry_key={}".format(relay.dsn_public_key),
        headers={"Content-Type": content_type},
        data="MDMP content",
    )

    envelope = mini_sentry.captured_events.get(timeout=1)

    assert envelope
    assert_only_minidump(envelope)


@pytest.mark.parametrize("test_file_name", ("electron_simple.dmp", "electron.dmp"))
def test_minidump_nested_formdata(mini_sentry, relay, test_file_name):
    project_id = 42
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[project_id] = mini_sentry.full_project_config()

    dmp_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "native", test_file_name
    )

    with open(dmp_path, "rb") as f:
        dmp_file = f.read()

    attachments = [(MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", dmp_file)]

    relay.send_minidump(project_id=project_id, files=attachments)
    envelope = mini_sentry.captured_events.get(timeout=1)

    assert envelope
    assert_only_minidump(envelope, assert_payload=False)


def test_minidump_invalid_nested_formdata(mini_sentry, relay):
    project_id = 42
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[project_id] = mini_sentry.full_project_config()

    dmp_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "native", "bad_electron_simple.dmp"
    )

    with open(dmp_path, "rb") as f:
        dmp_file = f.read()

    attachments = [(MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", dmp_file)]

    with pytest.raises(HTTPError):
        relay.send_minidump(project_id=project_id, files=attachments)


def test_minidump_with_processing(
    mini_sentry, relay_with_processing, attachments_consumer
):
    project_id = 42
    relay = relay_with_processing()
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[project_id] = mini_sentry.full_project_config()
    attachments_consumer = attachments_consumer()

    attachments = [(MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "MDMP content")]
    relay.send_minidump(project_id=project_id, files=attachments)

    attachment = b""
    num_chunks = 0
    attachment_id = None

    while attachment != b"MDMP content":
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
            "content_type": "application/octet-stream",
            "attachment_type": "event.minidump",
            "chunks": num_chunks,
        }
    ]


def test_minidump_with_processing_and_exception(
    mini_sentry, relay_with_processing, attachments_consumer
):
    project_id = 42
    relay = relay_with_processing()
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[project_id] = mini_sentry.full_project_config()
    attachments_consumer = attachments_consumer()

    attachments = [(MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "MDMP content")]
    params = [("sentry", '{"exception":{"type":"hello","value":"world"}}')]
    relay.send_minidump(project_id=project_id, params=params, files=attachments)

    attachment = b""
    num_chunks = 0
    attachment_id = None

    while attachment != b"MDMP content":
        chunk, message = attachments_consumer.get_attachment_chunk()
        attachment_id = attachment_id or message["id"]
        attachment += chunk
        num_chunks += 1

    event, message = attachments_consumer.get_event()

    # Check that the original exception is there, but so is the mechanism
    assert event["platform"] == "native"
    assert event["exception"]["values"][0]["mechanism"]["type"] == "minidump"
    assert event["exception"]["values"][0]["type"] == "hello"
    assert event["exception"]["values"][0]["value"] == "world"


def test_minidump_ratelimit(mini_sentry, relay_with_processing, outcomes_consumer):
    relay = relay_with_processing()
    relay.wait_relay_healthcheck()

    project_config = mini_sentry.project_configs[42] = mini_sentry.full_project_config()
    project_config["config"]["quotas"] = [
        {"limit": 0, "reasonCode": "static_disabled_quota",}
    ]

    outcomes_consumer = outcomes_consumer()
    attachments = [(MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "MDMP content")]

    # First minidump returns 200 but is rate limited in processing
    relay.send_minidump(project_id=42, files=attachments)
    outcomes_consumer.assert_rate_limited("static_disabled_quota")

    # Second minidump returns 429 in endpoint
    with pytest.raises(HTTPError) as excinfo:
        relay.send_minidump(project_id=42, files=attachments)
    assert excinfo.value.response.status_code == 429
    outcomes_consumer.assert_rate_limited("static_disabled_quota")
