"""Characterize transaction storage after extracted span quota rejection."""

import uuid
from collections import Counter

import pytest
from sentry_relay.consts import DataCategory

from .consts import Outcome
from .test_store import make_transaction


@pytest.mark.parametrize("category", ["span", "span_indexed"])
@pytest.mark.parametrize("limit", [10, 11])
def test_transaction_storage_under_span_quota(
    mini_sentry,
    relay_with_processing,
    transactions_consumer,
    spans_consumer,
    outcomes_consumer,
    category,
    limit,
):
    """Eleven extracted spans can be rejected while their embedded payload is stored."""
    project = mini_sentry.add_full_project_config(42)
    project["config"].setdefault("features", []).append(
        "organizations:relay-generate-billing-outcome"
    )
    project["config"]["quotas"] = [
        {
            "id": uuid.uuid4().hex,
            "categories": [category],
            "window": 3600,
            "limit": limit,
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

    if limit == 10:
        spans.assert_empty()
    else:
        assert len(spans.get_spans(n=11)) == 11

    rejected = Counter()
    for outcome in outcomes.get_outcomes():
        if outcome["outcome"] == Outcome.RATE_LIMITED:
            assert outcome["reason"] == "span_quota_verification"
            rejected[outcome["category"]] += outcome["quantity"]
    expected = {}
    if limit == 10:
        expected[DataCategory.SPAN_INDEXED] = 11
        # Either limit rejects the whole ExtractedSpans batch, which still
        # carries both total and indexed quantities at this processing stage.
        expected[DataCategory.SPAN] = 11
    assert rejected == expected
