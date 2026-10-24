import json
import uuid
from collections import Counter
from dataclasses import dataclass

import pytest

from sentry_relay.consts import DataCategory
from sentry_sdk.envelope import Envelope, Item, PayloadRef

from .consts import Outcome
from .test_feedback import generate_feedback_sdk_event


@dataclass(frozen=True, kw_only=True)
class Case:
    id: str
    categories: list[str]
    expected_items: list[str]
    expected_outcomes: dict[tuple[DataCategory, str], int]


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


@pytest.mark.parametrize(
    "reverse_items", [False, True], ids=["error-first", "report-first"]
)
@pytest.mark.parametrize(
    "case",
    [
        Case(
            id="feedback-quota-keeps-error",
            categories=["user_report_v2"],
            expected_items=["event"],
            expected_outcomes={
                (DataCategory.USER_REPORT_V2, "user_report_v2_quota"): 1
            },
        ),
        Case(
            id="error-quota-drops-feedback",
            categories=["error"],
            expected_items=[],
            expected_outcomes={
                (DataCategory.ERROR, "error_quota"): 1,
                (DataCategory.USER_REPORT_V2, "error_quota"): 1,
            },
        ),
        Case(
            id="error-reason-takes-precedence",
            categories=["user_report_v2", "error"],
            expected_items=[],
            expected_outcomes={
                (DataCategory.ERROR, "error_quota"): 1,
                (DataCategory.USER_REPORT_V2, "error_quota"): 1,
            },
        ),
    ],
    ids=lambda case: case.id,
)
def test_user_report_error_quota(
    mini_sentry,
    relay,
    reverse_items,
    case: Case,
):
    project = mini_sentry.add_full_project_config(42)
    project["config"]["quotas"] = [
        {
            "id": category,
            "categories": [category],
            "window": 3600,
            "limit": 0,
            "reasonCode": f"{category}_quota",
        }
        for category in case.categories
    ]
    event_id = uuid.uuid4().hex
    envelope = Envelope(headers={"event_id": event_id})
    envelope.add_event({"event_id": event_id, "message": "test"})
    envelope.add_item(
        Item(
            type="user_report",
            payload=PayloadRef(
                json={
                    "event_id": event_id,
                    "name": "Josh",
                    "email": "",
                    "comments": "test",
                }
            ),
        )
    )
    if reverse_items:
        envelope.items.reverse()
    relay = relay(mini_sentry)
    relay.send_envelope(42, envelope)

    if case.expected_items:
        forwarded = mini_sentry.get_captured_envelope()
        assert [item.type for item in forwarded.items] == case.expected_items
        assert forwarded.get_event()["event_id"] == event_id

    # V1 reports count as V2 feedback. If both quotas apply, the parent's reason wins.
    actual: Counter[tuple[DataCategory, str]] = Counter()
    for outcome in mini_sentry.get_outcomes():
        assert outcome["outcome"] == Outcome.RATE_LIMITED
        actual[outcome["category"], outcome["reason"]] += outcome["quantity"]
    assert actual == case.expected_outcomes
    assert mini_sentry.captured_envelopes.empty()
    assert mini_sentry.captured_outcomes.empty()


@pytest.mark.parametrize("item_type", ["user_report", "feedback"])
@pytest.mark.parametrize("category", ["error", "user_report_v2"])
def test_user_report_quota_standalone(mini_sentry, relay, item_type, category):
    project = mini_sentry.add_full_project_config(42)
    project["config"]["quotas"] = [
        {
            "id": "report_quota",
            "categories": [category],
            "window": 3600,
            "limit": 0,
            "reasonCode": "report_quota",
        }
    ]
    payload = (
        generate_feedback_sdk_event()
        if item_type == "feedback"
        else {
            "event_id": uuid.uuid4().hex,
            "name": "Josh",
            "email": "",
            "comments": "test",
        }
    )
    envelope = Envelope(headers={"event_id": payload["event_id"]})
    envelope.add_item(Item(type=item_type, payload=PayloadRef(json=payload)))
    relay = relay(mini_sentry)
    relay.send_envelope(42, envelope)

    # Both report formats use the feedback quota; an error quota does not apply.
    if category == "error":
        forwarded = mini_sentry.get_captured_envelope()
        assert [item.type for item in forwarded.items] == [item_type]
    outcomes = mini_sentry.get_outcomes(
        timeout=10 if category == "user_report_v2" else 1
    )
    assert [
        (o["category"], o["outcome"], o["reason"], o["quantity"]) for o in outcomes
    ] == (
        [(DataCategory.USER_REPORT_V2, Outcome.RATE_LIMITED, "report_quota", 1)]
        if category == "user_report_v2"
        else []
    )
    assert mini_sentry.captured_envelopes.empty()
    assert mini_sentry.captured_outcomes.empty()
