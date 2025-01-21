import json
import uuid

from sentry_sdk.envelope import Envelope, Item, PayloadRef


def test_user_report_with_event(relay_with_processing, mini_sentry):
    project_id = 42
    relay = relay_with_processing()
    mini_sentry.add_full_project_config(project_id)

    event_id = uuid.uuid1().hex
    error_payload = {
        "event_id": event_id,
        "message": "test",
        "context": {"flags": {"values": [{"flag": "hello", "result": "world"}]}},
        "type": "error",
        "environment": "production",
        "release": "foo@1.2.3",
    }

    envelope = Envelope(headers={"event_id": event_id})
    envelope.add_item(Item(PayloadRef(json=error_payload), type="event"))

    relay.send_envelope(project_id, envelope)

    envelope = mini_sentry.captured_events.get(timeout=1)
    assert envelope
