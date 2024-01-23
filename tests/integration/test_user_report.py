import json
import uuid

from sentry_sdk.envelope import Envelope, Item, PayloadRef


def test_standalone_user_report(
    relay_with_processing, mini_sentry, attachments_consumer
):
    project_id = 42
    relay = relay_with_processing()
    mini_sentry.add_full_project_config(project_id)

    report_payload = {
        "name": "Josh",
        "email": "",
        "comments": "I'm having fun",
        "event_id": "4cec9f3e1f214073b816e0f4de5f59b1",
    }

    relay.send_user_report(
        project_id,
        report_payload,
    )

    attachments_consumer = attachments_consumer()
    report = attachments_consumer.get_user_report(timeout=5)
    assert json.loads(report["payload"]) == report_payload


def test_user_report_with_event(
    relay_with_processing, mini_sentry, attachments_consumer
):
    project_id = 42
    relay = relay_with_processing()
    mini_sentry.add_full_project_config(project_id)

    event_id = uuid.uuid1().hex

    error_payload = {
        "event_id": event_id,
        "message": "test",
        "extra": {"msg_text": "test"},
        "type": "error",
        "environment": "production",
        "release": "foo@1.2.3",
    }

    report_payload = {
        "name": "Josh",
        "email": "",
        "comments": "I'm having fun",
        "event_id": "4cec9f3e1f214073b816e0f4de5f59b1",
    }

    envelope = Envelope()
    envelope.add_item(Item(PayloadRef(json=error_payload), type="event"))
    envelope.add_item(Item(PayloadRef(json=report_payload), type="user_report"))

    relay.send_envelope(project_id, envelope)

    attachments_consumer = attachments_consumer()
    report = attachments_consumer.get_user_report(timeout=5)
    assert json.loads(report["payload"]) == report_payload
