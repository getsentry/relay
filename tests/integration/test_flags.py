import uuid

from sentry_sdk.envelope import Envelope, Item, PayloadRef


def test_event_with_flags(relay, mini_sentry):
    """Assert the flags context is parsed and has the expected output. Assert extra keys are
    retained and passed through.
    """
    mini_sentry.add_full_project_config(42)
    relay = relay(mini_sentry)

    event_id = uuid.uuid1().hex
    error_payload = {
        "event_id": event_id,
        "message": "test",
        "contexts": {
            "flags": {
                "abc": "def",
                "values": [{"flag": "hello", "result": "world", "key": "value"}],
            }
        },
        "type": "error",
        "environment": "production",
        "release": "foo@1.2.3",
    }

    envelope = Envelope(headers={"event_id": event_id})
    envelope.add_item(Item(PayloadRef(json=error_payload), type="event"))

    relay.send_envelope(42, envelope)

    envelope = mini_sentry.get_captured_event()
    assert envelope
    assert envelope.items[0].payload.json["contexts"] == {
        "flags": {
            "values": [{"flag": "hello", "result": "world", "key": "value"}],
            "abc": "def",
            "type": "flags",
        }
    }
    assert mini_sentry.captured_events.empty()


def test_event_with_flags_malformed(relay, mini_sentry):
    """Assert malformed fields are coerced to null."""
    mini_sentry.add_full_project_config(42)
    relay = relay(mini_sentry)

    event_id = uuid.uuid1().hex
    error_payload = {
        "event_id": event_id,
        "message": "test",
        "contexts": {"flags": {"values": "test", "key": "value"}},
        "type": "error",
        "environment": "production",
        "release": "foo@1.2.3",
    }

    envelope = Envelope(headers={"event_id": event_id})
    envelope.add_item(Item(PayloadRef(json=error_payload), type="event"))

    relay.send_envelope(42, envelope)

    # Malformed fields are removed. Unknown fields are passed through.
    envelope = mini_sentry.get_captured_event()
    assert envelope
    assert envelope.items[0].payload.json["contexts"] == {
        "flags": {"values": None, "key": "value", "type": "flags"}
    }
    assert mini_sentry.captured_events.empty()


def test_event_with_flags_malformed_inner_object(relay, mini_sentry):
    """Assert malformed fields are removed from the object.

    Flag is mistyped so it is transformed to null. Value is sent as null so it is
    removed from the object.
    """
    mini_sentry.add_full_project_config(42)
    relay = relay(mini_sentry)

    event_id = uuid.uuid1().hex
    error_payload = {
        "event_id": event_id,
        "message": "test",
        "contexts": {
            "flags": {"values": [{"flag": 123, "value": None, "key": "key"}, "hello"]}
        },
        "type": "error",
        "environment": "production",
        "release": "foo@1.2.3",
    }

    envelope = Envelope(headers={"event_id": event_id})
    envelope.add_item(Item(PayloadRef(json=error_payload), type="event"))

    relay.send_envelope(42, envelope)

    # Malformed fields are removed. Unknown fields are passed through.
    envelope = mini_sentry.get_captured_event()
    assert envelope
    assert envelope.items[0].payload.json["contexts"] == {
        "flags": {"values": [{"flag": None, "key": "key"}, None], "type": "flags"}
    }
    assert mini_sentry.captured_events.empty()
