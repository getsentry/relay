import uuid

from sentry_sdk.envelope import Envelope, Item, PayloadRef


def test_event_with_flags(relay, mini_sentry):
    mini_sentry.add_full_project_config(42)
    relay = relay(mini_sentry)

    event_id = uuid.uuid1().hex
    error_payload = {
        "event_id": event_id,
        "message": "test",
        "contexts": {"flags": {"values": [{"flag": "hello", "result": "world"}]}},
        "type": "error",
        "environment": "production",
        "release": "foo@1.2.3",
    }

    envelope = Envelope(headers={"event_id": event_id})
    envelope.add_item(Item(PayloadRef(json=error_payload), type="event"))

    relay.send_envelope(42, envelope)

    envelope = mini_sentry.captured_events.get(timeout=1)
    assert envelope
    assert envelope.items[0].payload.json["contexts"] == {
        "flags": {
            "values": [{"flag": "hello", "result": "world"}],
            "type": "flags",
        }
    }
    assert mini_sentry.captured_events.empty()


def test_event_with_flags_approximate(relay, mini_sentry):
    mini_sentry.add_full_project_config(42)
    relay = relay(mini_sentry)

    event_id = uuid.uuid1().hex
    error_payload = {
        "event_id": event_id,
        "message": "test",
        "contexts": {"flags": {"values": [{"key": "k", "value": "v"}]}},
        "type": "error",
        "environment": "production",
        "release": "foo@1.2.3",
    }

    envelope = Envelope(headers={"event_id": event_id})
    envelope.add_item(Item(PayloadRef(json=error_payload), type="event"))

    relay.send_envelope(42, envelope)

    # Malformed flags context is emptied and passed with only the type key.
    envelope = mini_sentry.captured_events.get(timeout=1)
    assert envelope
    assert envelope.items[0].payload.json["contexts"] == {
        "flags": {"values": [{"key": "k", "value": "v"}], "type": "flags"}
    }
    assert mini_sentry.captured_events.empty()


def test_event_with_flags_unknown(relay, mini_sentry):
    mini_sentry.add_full_project_config(42)
    relay = relay(mini_sentry)

    event_id = uuid.uuid1().hex
    error_payload = {
        "event_id": event_id,
        "message": "test",
        "contexts": {"flags": {"what": "ever"}},
        "type": "error",
        "environment": "production",
        "release": "foo@1.2.3",
    }

    envelope = Envelope(headers={"event_id": event_id})
    envelope.add_item(Item(PayloadRef(json=error_payload), type="event"))

    relay.send_envelope(42, envelope)

    # Malformed flags context is emptied and passed with only the type key.
    envelope = mini_sentry.captured_events.get(timeout=1)
    assert envelope
    assert envelope.items[0].payload.json["contexts"] == {
        "flags": {"what": "ever", "type": "flags"}
    }
    assert mini_sentry.captured_events.empty()
