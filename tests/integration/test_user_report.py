import json
import uuid

from sentry_relay.consts import DataCategory
from sentry_sdk.envelope import Envelope, Item, PayloadRef


def test_standalone_user_report(
    relay_with_processing, mini_sentry, attachments_consumer
):
    attachments_consumer = attachments_consumer()

    project_id = 42
    relay = relay_with_processing()
    mini_sentry.add_full_project_config(project_id)

    event_id = "4cec9f3e1f214073b816e0f4de5f59b1"

    report_payload = {
        "name": "Josh",
        "email": "",
        "comments": "I'm having fun",
        "event_id": event_id,
    }

    envelope = Envelope(headers={"event_id": event_id})
    envelope.add_item(Item(PayloadRef(json=report_payload), type="user_report"))

    relay.send_envelope(project_id, envelope)

    report = attachments_consumer.get_user_report(timeout=5)
    assert json.loads(report["payload"]) == report_payload


def test_user_report_with_event(
    relay_with_processing, mini_sentry, attachments_consumer
):
    attachments_consumer = attachments_consumer()

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

    envelope = Envelope(headers={"event_id": event_id})
    envelope.add_item(Item(PayloadRef(json=error_payload), type="event"))
    envelope.add_item(Item(PayloadRef(json=report_payload), type="user_report"))

    relay.send_envelope(project_id, envelope)

    report = attachments_consumer.get_user_report(timeout=5)
    assert json.loads(report["payload"]) == report_payload

    event, _ = attachments_consumer.get_event(timeout=5)
    assert event["event_id"] == event_id


def test_user_report_urv2_quota_with_event(
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
            "categories": ["user_report_v2"],
            "window": 3600,
            "limit": 0,
            "reasonCode": "user_report_v2_quota",
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

    # The user report is dropped, but the error event is not.
    attachments_consumer.assert_empty()
    events_consumer.get_event()

    outcomes_consumer.assert_rate_limited(
        "user_report_v2_quota", categories={DataCategory.USER_REPORT_V2: 1}
    )


def test_user_report_urv2_quota_standalone(
    mini_sentry,
    relay_with_processing,
    attachments_consumer,
    outcomes_consumer,
):
    project_id = 42
    event_id = uuid.uuid1().hex
    relay = relay_with_processing()
    outcomes_consumer = outcomes_consumer(timeout=10)
    attachments_consumer = attachments_consumer()

    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["quotas"] = [
        {
            "id": f"test_rate_limiting_{event_id}",
            "categories": ["user_report_v2"],
            "window": 3600,
            "limit": 0,
            "reasonCode": "user_report_v2_quota",
        }
    ]

    report_payload = {
        "name": "Josh",
        "email": "",
        "comments": "I'm having fun",
        "event_id": event_id,
    }

    envelope = Envelope()
    envelope.add_item(Item(PayloadRef(json=report_payload), type="user_report"))
    relay.send_envelope(project_id, envelope)

    attachments_consumer.assert_empty()

    outcomes_consumer.assert_rate_limited(
        "user_report_v2_quota", categories={DataCategory.USER_REPORT_V2: 1}
    )


def test_user_report_error_quota(
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
            "reasonCode": "error_quota",
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

    # Rejecting the error also rejects its accompanying feedback.
    attachments_consumer.assert_empty()
    events_consumer.assert_empty()

    # Both rejected items must be accounted for, using the parent error's reason.
    outcomes_consumer.assert_rate_limited(
        "error_quota",
        categories={DataCategory.ERROR: 1, DataCategory.USER_REPORT_V2: 1},
    )
