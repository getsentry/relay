from datetime import datetime, timedelta, timezone
import io
import os
import pytest

from sentry_sdk.envelope import Envelope, Item, PayloadRef
from .asserts import time_within_delta


def make_dying_message(*items) -> bytes:
    """Creates an uncompressed dying message
    out of the given items.

    See dying_message.md for details.
    """
    payload_writer = io.BytesIO()

    for item in items:
        item.serialize_into(payload_writer)

    payload = payload_writer.getvalue()
    payload_size = len(payload)

    assert payload_size < 2**16

    out = io.BytesIO()
    # File magic
    out.write(b"sntr")
    # Format version
    out.write(b"\x00")
    # Format & compression
    out.write(b"\x00")
    # Payload size
    out.write(payload_size.to_bytes(2, byteorder="big"))
    # Payload
    out.write(payload_writer.read())

    return out.getvalue()


@pytest.mark.parametrize("variant", ["plain", "zstandard"])
def test_nnswitch(
    mini_sentry,
    relay,
    relay_with_processing,
    outcomes_consumer,
    attachments_consumer,
    events_consumer,
    variant,
):
    PROJECT_ID = 42
    mini_sentry.add_full_project_config(PROJECT_ID)
    events_consumer = events_consumer()
    outcomes_consumer = outcomes_consumer()
    attachments_consumer = attachments_consumer()
    relay = relay(relay_with_processing())

    dying_message_path = os.path.join(
        os.path.dirname(__file__),
        "fixtures",
        "native",
        "nnswitch_dying_message_%s.dat" % variant,
    )

    bogus_error = {
        "event_id": "cbf6960622e14a45abc1f03b2055b186",
        "type": "error",
        "exception": {"values": [{"type": "ValueError", "value": "Should not happen"}]},
    }
    envelope = Envelope()
    envelope.add_event(bogus_error)

    envelope.add_item(
        Item(
            type="attachment",
            payload=PayloadRef(path=dying_message_path),
            headers={
                "filename": "dying_message.dat",
                "content_type": "application/octet-stream",
            },
        )
    )
    relay.send_envelope(PROJECT_ID, envelope)

    event, _ = events_consumer.get_event()
    assert event["sdk"]["name"] == "sentry.native.switch"
    assert event["user"]["id"] == "user-id"
    assert event["contexts"]["os"]["name"] == "Nintendo"
    assert event["breadcrumbs"]["values"][0]["type"] == "bread"
    assert event["breadcrumbs"]["values"][0]["message"] == "crumb"

    outcomes_consumer.assert_empty()


def test_nnswitch_with_session(
    mini_sentry,
    relay,
    relay_with_processing,
    outcomes_consumer,
    attachments_consumer,
    events_consumer,
    metrics_consumer,
):
    PROJECT_ID = 42
    mini_sentry.add_full_project_config(PROJECT_ID)
    events_consumer = events_consumer()
    outcomes_consumer = outcomes_consumer()
    attachments_consumer = attachments_consumer()
    metrics_consumer = metrics_consumer()
    relay = relay(relay_with_processing())

    started = datetime.now(tz=timezone.utc) - timedelta(hours=1)

    bogus_error = {
        "event_id": "cbf6960622e14a45abc1f03b2055b186",
        "type": "error",
        "exception": {"values": [{"type": "ValueError", "value": "Should not happen"}]},
    }
    envelope = Envelope()
    envelope.add_event(bogus_error)

    event_payload = {
        "platform": "native",
        "release": "sentry-switch-sample@0.1.0",
        "environment": "production",
        "level": "error",
        "user": {"id": "user-id", "username": "user-name", "email": "user-email"},
        "sdk": {
            "name": "sentry.native.switch",
            "version": "0.8.2",
            "packages": [
                {"name": "github:getsentry/sentry-native", "version": "0.8.2"}
            ],
            "integrations": ["nx"],
        },
        "tags": {"tag-name": "tag value"},
        "extra": {"extra-name": "extra value"},
        "contexts": {"os": {"name": "Nintendo"}},
        "breadcrumbs": [
            {
                "timestamp": "2025-04-07T18:17:10.016102Z",
                "type": "bread",
                "message": "crumb",
            }
        ],
    }

    session_payload = {
        "init": True,
        "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
        "status": "crashed",
        "did": "foobarbaz",
        "errors": 1,
        "started": started.isoformat(),
        "attrs": {"release": "sentry-test@1.0.0", "environment": "production"},
    }

    dying_message = make_dying_message(
        Item(type="event", payload=PayloadRef(json=event_payload)),
        Item(type="session", payload=PayloadRef(json=session_payload)),
    )
    with open("dying_message_new.dat", "wb") as f:
        f.write(dying_message)

    envelope.add_item(
        Item(
            type="attachment",
            payload=PayloadRef(bytes=dying_message),
            headers={
                "filename": "dying_message.dat",
                "content_type": "application/octet-stream",
            },
        )
    )
    relay.send_envelope(PROJECT_ID, envelope)

    event, _ = events_consumer.get_event()
    assert event["sdk"]["name"] == "sentry.native.switch"
    assert event["user"]["id"] == "user-id"
    assert event["contexts"]["os"]["name"] == "Nintendo"
    assert event["breadcrumbs"]["values"][0]["type"] == "bread"
    assert event["breadcrumbs"]["values"][0]["message"] == "crumb"

    metrics = metrics_consumer.get_metrics(with_headers=False)

    assert metrics == [
        {
            "name": "c:sessions/session@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {
                "environment": "production",
                "release": "sentry-test@1.0.0",
                "sdk": "raven-node/2.6.3",
                "session.status": "crashed",
            },
            "timestamp": time_within_delta(started),
            "type": "c",
            "value": 1.0,
        },
        {
            "name": "c:sessions/session@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {
                "environment": "production",
                "release": "sentry-test@1.0.0",
                "sdk": "raven-node/2.6.3",
                "session.status": "init",
            },
            "timestamp": time_within_delta(started),
            "type": "c",
            "value": 1.0,
        },
        {
            "name": "s:sessions/error@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {
                "environment": "production",
                "release": "sentry-test@1.0.0",
                "sdk": "raven-node/2.6.3",
            },
            "timestamp": time_within_delta(started),
            "type": "s",
            "value": [
                256847825,
            ],
        },
        {
            "name": "s:sessions/user@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {
                "environment": "production",
                "release": "sentry-test@1.0.0",
                "sdk": "raven-node/2.6.3",
                "session.status": "crashed",
            },
            "timestamp": time_within_delta(started),
            "type": "s",
            "value": [
                1617781333,
            ],
        },
        {
            "name": "s:sessions/user@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {
                "environment": "production",
                "release": "sentry-test@1.0.0",
                "sdk": "raven-node/2.6.3",
                "session.status": "errored",
            },
            "timestamp": time_within_delta(started),
            "type": "s",
            "value": [
                1617781333,
            ],
        },
    ]

    outcomes_consumer.assert_empty()
