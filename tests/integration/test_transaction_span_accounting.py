"""Audit span quota accounting for Python's mixed transaction/span envelopes."""

import uuid
from collections import Counter
from datetime import datetime

import pytest
from requests import HTTPError
from sentry_relay.consts import DataCategory
from sentry_sdk.envelope import Envelope, Item, PayloadRef

from .consts import Outcome
from .test_store import make_transaction


@pytest.mark.parametrize(
    "category,limit,kept_spans,kept_transaction,rejected",
    [
        pytest.param(None, None, {"root", "child", "gen_ai"}, True, {}, id="no-quota"),
        pytest.param("span", 3, {"root", "child", "gen_ai"}, True, {}, id="span-fits"),
        pytest.param(
            "span",
            2,
            {"gen_ai"},
            True,
            {DataCategory.SPAN: 2, DataCategory.SPAN_INDEXED: 2},
            id="span-batch-exceeds",
        ),
        pytest.param(
            "span_indexed",
            2,
            {"gen_ai"},
            True,
            {DataCategory.SPAN: 2, DataCategory.SPAN_INDEXED: 2},
            id="indexed-span-batch-exceeds",
        ),
        pytest.param(
            "transaction",
            0,
            set(),
            False,
            {
                DataCategory.TRANSACTION: 1,
                DataCategory.TRANSACTION_INDEXED: 1,
                DataCategory.SPAN: 3,
                DataCategory.SPAN_INDEXED: 3,
            },
            id="transaction-blocked",
        ),
    ],
)
def test_python_transaction_with_separate_span_container(
    mini_sentry,
    relay_with_processing,
    transactions_consumer,
    spans_consumer,
    outcomes_consumer,
    category,
    limit,
    kept_spans,
    kept_transaction,
    rejected,
):
    """Match Python's GenAI split: a transaction with embedded spans plus a V2 span container."""
    project = mini_sentry.add_full_project_config(42)
    project["config"].setdefault("features", []).append(
        "organizations:relay-generate-billing-outcome"
    )
    if category is not None:
        project["config"]["quotas"] = [
            {
                "id": uuid.uuid4().hex,
                "categories": [category],
                "window": 3600,
                "limit": limit,
                "reasonCode": "python_mixed_quota",
            }
        ]
    transactions = transactions_consumer()
    spans = spans_consumer()
    outcomes = outcomes_consumer()
    relay = relay_with_processing()

    event, envelope, ids = python_mixed_transaction(project)
    relay.send_envelope(42, envelope)

    if kept_transaction:
        stored, _ = transactions.get_event()
        assert stored["event_id"] == event["event_id"]
        assert [span["span_id"] for span in stored["spans"]] == [ids["child"]]
    else:
        transactions.assert_empty()

    assert {span["span_id"] for span in spans.get_spans()} == {
        ids[name] for name in kept_spans
    }
    actual_rejected = Counter()
    for outcome in outcomes.get_outcomes():
        if outcome["outcome"] == Outcome.RATE_LIMITED:
            assert outcome["reason"] == "python_mixed_quota"
            actual_rejected[outcome["category"]] += outcome["quantity"]
    assert actual_rejected == rejected


def python_mixed_transaction(project):
    """Mirror sentry-python/client.py's transaction/GenAI split on the wire."""
    event = make_transaction({"event_id": uuid.uuid4().hex})
    trace = event["contexts"]["trace"]
    ids = {
        "root": trace["span_id"],
        "child": "bbbbbbbbbbbbbbbb",
        "gen_ai": "cccccccccccccccc",
    }
    event["spans"] = [
        {
            "description": "remaining HTTP child",
            "op": "http.client",
            "parent_span_id": ids["root"],
            "span_id": ids["child"],
            "trace_id": trace["trace_id"],
            "start_timestamp": event["start_timestamp"],
            "timestamp": event["timestamp"],
        }
    ]
    envelope = Envelope(
        headers={
            "event_id": event["event_id"],
            "trace": {
                "trace_id": trace["trace_id"],
                "public_key": project["publicKeys"][0]["publicKey"],
            },
        }
    )
    envelope.add_transaction(event)
    envelope.add_item(
        Item(
            type="span",
            content_type="application/vnd.sentry.items.span.v2+json",
            headers={"item_count": 1},
            payload=PayloadRef(
                json={
                    "version": 2,
                    "items": [
                        {
                            "name": "gen_ai.chat",
                            "span_id": ids["gen_ai"],
                            "parent_span_id": ids["root"],
                            "trace_id": trace["trace_id"],
                            "start_timestamp": datetime.fromisoformat(
                                event["start_timestamp"]
                            ).timestamp(),
                            "end_timestamp": datetime.fromisoformat(
                                event["timestamp"]
                            ).timestamp(),
                            "status": "ok",
                            "is_segment": False,
                            "attributes": {
                                "sentry.op": {"type": "string", "value": "gen_ai.chat"},
                                "sentry.segment.id": {
                                    "type": "string",
                                    "value": ids["root"],
                                },
                            },
                        }
                    ],
                }
            ),
        )
    )
    return event, envelope, ids


@pytest.mark.parametrize(
    "limit",
    [
        pytest.param(0, id="zero-quota"),
        pytest.param(2, id="cached-after-exhaustion"),
    ],
)
def test_python_mixed_envelope_fast_path_accounting(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
    spans_consumer,
    transactions_consumer,
    limit,
):
    """An early span limit must not trigger Managed's accounting assertion."""
    project = mini_sentry.add_full_project_config(42)
    project["config"]["quotas"] = [
        {
            "id": uuid.uuid4().hex,
            "categories": ["span"],
            "limit": limit,
            "window": 3600,
            "reasonCode": "python_mixed_quota",
        }
    ]
    outcomes = outcomes_consumer()
    spans = spans_consumer()
    transactions = transactions_consumer()
    relay = relay_with_processing()
    _, envelope, _ = python_mixed_transaction(project)
    if limit:
        # The first request accepts the GenAI span, rejects the extracted pair,
        # and caches the exhausted span limit for the next request.
        relay.send_envelope(42, envelope)
        assert len(spans.get_spans(n=1)) == 1
        transactions.get_event()
        warmup = Counter()
        for item in outcomes.get_outcomes():
            if item["outcome"] == Outcome.RATE_LIMITED:
                warmup[item["category"]] += item["quantity"]
        assert warmup == {DataCategory.SPAN: 2, DataCategory.SPAN_INDEXED: 2}

        # Cached span limits inherit the transaction category, so the early
        # check now rejects the complete envelope, including all three spans.
        with pytest.raises(HTTPError) as error:
            relay.send_envelope(42, envelope)
        assert error.value.response.status_code == 429
        rejected = outcomes.get_outcomes()
        counts = Counter()
        for item in rejected:
            counts[item["category"]] += item["quantity"]
        assert counts == {
            DataCategory.TRANSACTION: 1,
            DataCategory.TRANSACTION_INDEXED: 1,
            DataCategory.SPAN: 3,
            DataCategory.SPAN_INDEXED: 3,
        }
        spans.assert_empty()
        transactions.assert_empty()
    else:
        relay.send_envelope(42, envelope)
        # One separate span is rejected early, and the transaction's two
        # extracted spans are rejected later. The transaction with embedded spans survives.
        transactions.get_event()
        spans.assert_empty()
        rejected = outcomes.get_outcomes()
        counts = Counter()
        for item in rejected:
            counts[item["category"]] += item["quantity"]
        assert counts == {
            DataCategory.SPAN: 3,
            DataCategory.SPAN_INDEXED: 3,
        }
    assert all(item["outcome"] == Outcome.RATE_LIMITED for item in rejected)
    # The fixture also fails on Relay's internal errors, including accounting panics.
