"""Characterize transaction storage after extracted span quota rejection."""

import uuid
from collections import Counter
from dataclasses import dataclass

import pytest
from sentry_relay.consts import DataCategory

from .consts import Outcome
from .test_store import make_transaction


@dataclass(frozen=True, kw_only=True)
class Case:
    id: str
    limit: int
    expected_extracted_spans: int
    expected_outcomes: dict[tuple[DataCategory, Outcome], int]


@pytest.mark.parametrize("category", ["span", "span_indexed"])
@pytest.mark.parametrize(
    "case",
    [
        Case(
            id="batch-exceeds-quota",
            limit=10,
            expected_extracted_spans=0,
            # Either quota rejects the whole batch, which still carries
            # both total and indexed quantities at this processing stage.
            expected_outcomes={
                (DataCategory.SPAN, Outcome.RATE_LIMITED): 11,
                (DataCategory.SPAN_INDEXED, Outcome.RATE_LIMITED): 11,
            },
        ),
        Case(
            id="batch-fits-quota",
            limit=11,
            expected_extracted_spans=11,
            expected_outcomes={
                (DataCategory.SPAN, Outcome.ACCEPTED): 11,
                (DataCategory.TRANSACTION, Outcome.ACCEPTED): 1,
            },
        ),
    ],
    ids=lambda case: case.id,
)
def test_transaction_storage_under_span_quota(
    mini_sentry,
    relay_with_processing,
    transactions_consumer,
    spans_consumer,
    outcomes_consumer,
    category,
    case: Case,
):
    """Eleven extracted spans can be rejected while their embedded payload is stored."""
    project = mini_sentry.add_full_project_config(42)
    project["config"]["quotas"] = [
        {
            "id": uuid.uuid4().hex,
            "categories": [category],
            "window": 3600,
            "limit": case.limit,
            "reasonCode": "span_quota_verification",
        }
    ]
    transactions = transactions_consumer()
    spans = spans_consumer()
    outcomes = outcomes_consumer()
    relay = relay_with_processing()

    event = make_transaction({"event_id": uuid.uuid4().hex})
    trace = event["contexts"]["trace"]
    event["spans"] = [
        {
            "description": f"child {index}",
            "op": "http",
            "parent_span_id": trace["span_id"],
            "span_id": f"{index + 1:016x}",
            "trace_id": trace["trace_id"],
            "start_timestamp": event["start_timestamp"],
            "timestamp": event["timestamp"],
        }
        for index in range(10)
    ]
    relay.send_event(42, event)

    stored, _ = transactions.get_event()
    assert stored["event_id"] == event["event_id"]
    assert {span["span_id"] for span in stored["spans"]} == {
        span["span_id"] for span in event["spans"]
    }
    assert len(stored["spans"]) == 10
    transactions.assert_empty()

    assert (
        len(spans.get_spans(n=case.expected_extracted_spans))
        == case.expected_extracted_spans
    )

    actual: Counter[tuple[DataCategory, Outcome]] = Counter()
    for outcome in outcomes.get_outcomes():
        actual[outcome["category"], outcome["outcome"]] += outcome["quantity"]
        if outcome["outcome"] == Outcome.RATE_LIMITED:
            assert outcome["reason"] == "span_quota_verification"
    assert actual == case.expected_outcomes
