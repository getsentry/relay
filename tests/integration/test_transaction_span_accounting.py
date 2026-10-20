"""Rate-limit outcomes for envelopes containing transactions and standalone spans."""

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
        pytest.param(
            None, None, {"root", "child", "standalone"}, True, {}, id="no-quota"
        ),
        pytest.param(
            "span", 3, {"root", "child", "standalone"}, True, {}, id="span-fits"
        ),
        pytest.param(
            "span_indexed",
            2,
            {"standalone"},
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
def test_transaction_with_span_container_quotas(
    mixed_envelope,
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
    project, envelope, ids = mixed_envelope
    if category is not None:
        _set_quota(project, category, limit)
    transactions = transactions_consumer()
    spans = spans_consumer()
    outcomes = outcomes_consumer()
    relay = relay_with_processing()

    relay.send_envelope(42, envelope)

    if kept_transaction:
        stored, _ = transactions.get_event()
        assert stored["event_id"] == envelope.headers["event_id"]
    transactions.assert_empty()

    assert {span["span_id"] for span in spans.get_spans(n=len(kept_spans))} == {
        ids[name] for name in kept_spans
    }
    expected = {
        (category, Outcome.RATE_LIMITED): quantity
        for category, quantity in rejected.items()
    }
    if kept_spans:
        expected[DataCategory.SPAN, Outcome.ACCEPTED] = len(kept_spans)
    # Accepted transaction billing outcomes come from segment-span metrics.
    if "root" in kept_spans:
        expected[DataCategory.TRANSACTION, Outcome.ACCEPTED] = 1
    _assert_outcomes(outcomes, expected)


@pytest.fixture
def mixed_envelope(mini_sentry):
    """Match Python's transaction/GenAI split into a transaction and a V2 span container."""
    project = mini_sentry.add_full_project_config(42)
    event = make_transaction({"event_id": uuid.uuid4().hex})
    trace = event["contexts"]["trace"]
    ids = {
        "root": trace["span_id"],
        "child": "bbbbbbbbbbbbbbbb",
        "standalone": "cccccccccccccccc",
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
                            "span_id": ids["standalone"],
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
    return project, envelope, ids


def _set_quota(project, category, limit):
    project["config"]["quotas"] = [
        {
            "id": uuid.uuid4().hex,
            "categories": [category],
            "limit": limit,
            "window": 3600,
            "reasonCode": "mixed_envelope_quota",
        }
    ]


def _assert_outcomes(consumer, expected):
    actual = Counter()
    for outcome in consumer.get_outcomes():
        actual[outcome["category"], outcome["outcome"]] += outcome["quantity"]
        if outcome["outcome"] == Outcome.RATE_LIMITED:
            assert outcome["reason"] == "mixed_envelope_quota"
    assert actual == expected


def test_transaction_with_span_container_zero_span_quota(
    mixed_envelope,
    relay_with_processing,
    outcomes_consumer,
    spans_consumer,
    transactions_consumer,
):
    project, envelope, _ = mixed_envelope
    _set_quota(project, "span", 0)
    outcomes = outcomes_consumer()
    spans = spans_consumer()
    transactions = transactions_consumer()
    relay = relay_with_processing()

    relay.send_envelope(42, envelope)

    # Reject the standalone span early and the two extracted spans during processing.
    outcomes.assert_rate_limited(
        "mixed_envelope_quota",
        categories={DataCategory.SPAN: 3, DataCategory.SPAN_INDEXED: 3},
    )
    stored, _ = transactions.get_event()
    assert stored["event_id"] == envelope.headers["event_id"]
    transactions.assert_empty()
    spans.assert_empty()


def test_transaction_with_span_container_cached_span_quota(
    mixed_envelope,
    relay_with_processing,
    outcomes_consumer,
    spans_consumer,
    transactions_consumer,
):
    project, envelope, ids = mixed_envelope
    _set_quota(project, "span", 2)
    outcomes = outcomes_consumer()
    spans = spans_consumer()
    transactions = transactions_consumer()
    relay = relay_with_processing()

    # Accept the standalone span, reject the extracted pair, and cache the span limit.
    relay.send_envelope(42, envelope)
    assert [span["span_id"] for span in spans.get_spans(n=1)] == [ids["standalone"]]
    stored, _ = transactions.get_event()
    assert stored["event_id"] == envelope.headers["event_id"]
    transactions.assert_empty()
    _assert_outcomes(
        outcomes,
        {
            (DataCategory.SPAN, Outcome.RATE_LIMITED): 2,
            (DataCategory.SPAN_INDEXED, Outcome.RATE_LIMITED): 2,
            (DataCategory.SPAN, Outcome.ACCEPTED): 1,
        },
    )

    # The cached span limit also rejects the transaction on the next request.
    with pytest.raises(HTTPError) as error:
        relay.send_envelope(42, envelope)
    assert error.value.response.status_code == 429
    outcomes.assert_rate_limited(
        "mixed_envelope_quota",
        categories={
            DataCategory.TRANSACTION: 1,
            DataCategory.TRANSACTION_INDEXED: 1,
            DataCategory.SPAN: 3,
            DataCategory.SPAN_INDEXED: 3,
        },
    )
    spans.assert_empty()
    transactions.assert_empty()
