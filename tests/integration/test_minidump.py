import os

import msgpack

import pytest
from requests import HTTPError
import re

MINIDUMP_ATTACHMENT_NAME = "upload_file_minidump"
EVENT_ATTACHMENT_NAME = "__sentry-event"


def _get_item_file_name(item):
    if item is not None and getattr(item, "headers"):
        headers = item.headers
        return headers.get("filename")
    return None


def _get_item_by_file_name(items, filename):
    filtered_items = [item for item in items if _get_item_file_name(item) == filename]
    if len(filtered_items) >= 1:
        return filtered_items[0]
    return None


@pytest.mark.parametrize(
    "input_event",
    [{"event_id": "2dd132e467174db48dbaddabd3cbed57"}, {"user": {"id": "123"}}, None],
)
def test_minidump_returns_the_correct_result(mini_sentry, relay, input_event):
    proj_id = 42
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[proj_id] = mini_sentry.full_project_config()

    files = [
        # add the minidump attachment with the magic header MDMP
        (MINIDUMP_ATTACHMENT_NAME, "minidump.txt", "MDMPminidump content"),
    ]

    if input_event is not None:
        files.append((EVENT_ATTACHMENT_NAME, "foo.txt", msgpack.packb(input_event)))

    response = relay.send_minidump(project_id=proj_id, files=files,)

    # a result for a successful request should be text consisting of a hyphenated uuid (and nothing else).
    response_body = response.text.strip()
    re_hex = lambda x: "[0-9a-fA-F]{{{}}}".format(x)
    hyphenated_uuid = "^{hex_8}-{hex_4}-{hex_4}-{hex_4}-{hex_12}$".format(
        hex_4=re_hex(4), hex_8=re_hex(8), hex_12=re_hex(12)
    )
    assert re.match(hyphenated_uuid, response_body)

    # the event id from the event should match the id in the response (minus the formatting)
    envelope = mini_sentry.captured_events.get(timeout=1)
    assert envelope
    event_id = envelope.headers.get("event_id")
    assert event_id == response_body.replace("-", "")

    event = envelope.get_event()
    assert event["exception"]["values"][0]["mechanism"]["type"] == "minidump"

    if input_event is not None:
        for key, value in input_event.items():
            assert event[key] == value


def test_minidump_attachments_are_added_to_the_envelope(mini_sentry, relay):
    proj_id = 42
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[proj_id] = mini_sentry.full_project_config()

    relay.send_minidump(
        project_id=proj_id,
        params=(("sentry[a][b]", "val1"), ("sentry[x][y]", "val2")),
        files=(
            # add the minidump attachment with the magic header MDMP
            (MINIDUMP_ATTACHMENT_NAME, "minidump.txt", "MDMPminidump content"),
            ("attachment1", "attach1.txt", "attachment 1 content"),
        ),
    )
    items = mini_sentry.captured_events.get(timeout=1).items

    mini_dump = _get_item_by_file_name(items, "minidump.txt")
    assert mini_dump is not None
    assert mini_dump.headers.get("name") == MINIDUMP_ATTACHMENT_NAME
    payload = mini_dump.payload.get_bytes()
    assert payload is not None
    assert payload.decode("utf-8") == "MDMPminidump content"
    attachment = _get_item_by_file_name(items, "attach1.txt")
    assert attachment is not None
    assert attachment.headers.get("name") == "attachment1"


def test_minidump_endpoint_checks_minidump_header(mini_sentry, relay):
    proj_id = 42
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[proj_id] = mini_sentry.full_project_config()

    with pytest.raises(HTTPError):
        relay.send_minidump(
            project_id=proj_id,
            files=(
                # add content without magic header 'MDMP'
                (MINIDUMP_ATTACHMENT_NAME, "minidump.txt", "minidump content"),
            ),
        )


def test_minidump_endpoint_checks_minidump_is_attached(mini_sentry, relay):
    proj_id = 42
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[proj_id] = mini_sentry.full_project_config()

    with pytest.raises(HTTPError):
        relay.send_minidump(
            project_id=proj_id,
            files=(
                # add content without magic header 'MDMP'
                ("some_unknown_attachment_name", "minidump.txt", "minidump content"),
            ),
        )


@pytest.mark.parametrize(
    "content_type", ("application/octet-stream", "application/x-dmp")
)
def test_minidump_endpoint_accepts_raw_minidump(mini_sentry, relay, content_type):
    proj_id = 42
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[proj_id] = mini_sentry.full_project_config()

    relay.request(
        "post",
        "/api/42/minidump?sentry_key={}".format(relay.dsn_public_key),
        headers={"Content-Type": content_type},
        data="MDMPminidump content",
    )

    items = mini_sentry.captured_events.get(timeout=1).items


@pytest.mark.parametrize("test_file_name", ("electron_simple.dmp", "electron.dmp"))
def test_minidump_endpoint_accepts_doubly_nested_formdata(
    mini_sentry, relay, test_file_name
):
    proj_id = 42
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[proj_id] = mini_sentry.full_project_config()

    with open(
        os.path.join(os.path.dirname(__file__), "fixtures/native", test_file_name),
        "rb",
    ) as f:
        # Yes, electron really sends us nested formdata
        dmpfile = f.read()

    relay.send_minidump(
        project_id=proj_id,
        files=((MINIDUMP_ATTACHMENT_NAME, "minidump.txt", dmpfile),),
    )

    items = mini_sentry.captured_events.get(timeout=1).items

    mini_dump = _get_item_by_file_name(items, "minidump.txt")
    assert mini_dump is not None
    assert mini_dump.headers.get("name") == MINIDUMP_ATTACHMENT_NAME


def test_minidump_endpoint_rejects_invalid_doubly_nested_formdata(mini_sentry, relay):
    proj_id = 42
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[proj_id] = mini_sentry.full_project_config()

    with open(
        os.path.join(
            os.path.dirname(__file__), "fixtures/native/bad_electron_simple.dmp"
        ),
        "rb",
    ) as f:
        # Yes, electron really sends us nested formdata
        dmpfile = f.read()

    with pytest.raises(HTTPError):
        relay.send_minidump(
            project_id=proj_id,
            files=((MINIDUMP_ATTACHMENT_NAME, "minidump.txt", dmpfile),),
        )


def test_minidump_with_processing(
    mini_sentry, relay_with_processing, attachments_consumer
):
    proj_id = 42
    relay = relay_with_processing()
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[proj_id] = mini_sentry.full_project_config()
    attachments_consumer = attachments_consumer()

    relay.send_minidump(
        project_id=proj_id,
        files=(("upload_file_minidump", "minidump.txt", "MDMPminidump content"),),
    )

    attachment = b""
    num_chunks = 0
    attachment_id = None

    while attachment != b"MDMPminidump content":
        print("ATTACHMENT", attachment)
        chunk, v = attachments_consumer.get_attachment_chunk()
        attachment_id = attachment_id or v["id"]
        attachment += chunk
        num_chunks += 1

    event, v = attachments_consumer.get_event()

    assert event["platform"] == "native"
    assert event["exception"]["values"][0]["mechanism"]["type"] == "minidump"

    assert list(v["attachments"]) == [
        {
            "id": attachment_id,
            "name": "minidump.txt",
            "content_type": "application/octet-stream",
            "attachment_type": "event.minidump",
            "chunks": num_chunks,
        }
    ]
