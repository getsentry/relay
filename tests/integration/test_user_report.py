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
        "event_id": event_id,
    }

    envelope = Envelope()
    envelope.add_item(Item(PayloadRef(json=error_payload), type="event"))
    envelope.add_item(Item(PayloadRef(json=report_payload), type="user_report"))

    relay.send_envelope(project_id, envelope)

    attachments_consumer = attachments_consumer()
    report = attachments_consumer.get_user_report(timeout=5)
    assert json.loads(report["payload"]) == report_payload


def test_user_reports_quotas(
    mini_sentry,
    relay_with_processing,
    attachments_consumer,
    events_consumer,
    outcomes_consumer,
):
    project_id = 42
    event_id = uuid.uuid1().hex
    relay = relay_with_processing()
    outcomes_consumer = outcomes_consumer(timeout=10)
    attachments_consumer = attachments_consumer()
    events_consumer = events_consumer()

    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["quotas"] = [
        {
            "id": f"test_rate_limiting_{event_id}",
            "categories": ["error"],
            "window": 3600,
            "limit": 0,
            "reasonCode": "drop_all",
        }
    ]

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
        "event_id": event_id,
    }

    envelope = Envelope()
    envelope.add_item(Item(PayloadRef(json=error_payload), type="event"))
    envelope.add_item(Item(PayloadRef(json=report_payload), type="user_report"))

    relay.send_envelope(project_id, envelope)

    # Becuase of the quotas, we should drop error, and since the user_report is in the same envelope, we should also drop it.
    attachments_consumer.assert_empty()
    events_consumer.assert_empty()

    # We must have 1 outcome with provided reason.
    outcomes = outcomes_consumer.get_outcomes()
    assert len(outcomes) == 1
    assert outcomes[0]["reason"] == "drop_all"
