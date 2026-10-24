"""Early rate-limit outcomes for transactions accompanied by standalone spans."""

import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime

import pytest
from sentry_relay.consts import DataCategory
from sentry_sdk.envelope import Envelope, Item, PayloadRef

from .consts import Outcome
from .test_store import make_transaction


@dataclass(frozen=True, kw_only=True)
class Case:
    id: str
    category: str | None
    expected_items: list[str]
    expected_outcomes: dict[DataCategory, int]


@pytest.mark.parametrize(
    "reverse_items", [False, True], ids=["transaction-first", "span-first"]
)
@pytest.mark.parametrize(
    "case",
    [
        Case(
            id="no-quota",
            category=None,
            expected_items=["transaction", "span"],
            expected_outcomes={},
        ),
        Case(
            id="transaction-blocked",
            category="transaction",
            expected_items=[],
            expected_outcomes={
                DataCategory.TRANSACTION: 1,
                DataCategory.TRANSACTION_INDEXED: 1,
                DataCategory.SPAN: 3,
                DataCategory.SPAN_INDEXED: 3,
            },
        ),
        Case(
            id="span-blocked",
            category="span",
            expected_items=["transaction"],
            expected_outcomes={DataCategory.SPAN: 1, DataCategory.SPAN_INDEXED: 1},
        ),
    ],
    ids=lambda case: case.id,
)
def test_transaction_with_span_container_quotas(
    mixed_envelope,
    relay,
    mini_sentry,
    reverse_items,
    case: Case,
):
    project, envelope = mixed_envelope
    if case.category is not None:
        project["config"]["quotas"] = [
            {
                "id": "mixed_envelope_quota",
                "categories": [case.category],
                "limit": 0,
                "window": 3600,
                "reasonCode": "mixed_envelope_quota",
            }
        ]
    if reverse_items:
        envelope.items.reverse()
    relay = relay(mini_sentry)
    relay.send_envelope(42, envelope)

    # Zero quotas without processing isolate early quota enforcement. A span quota
    # removes the standalone container, leaving the transaction for later processing.
    forwarded: list[str] = []
    while len(forwarded) < len(case.expected_items):
        forwarded.extend(
            item.type for item in mini_sentry.get_captured_envelope().items
        )
    assert Counter(forwarded) == Counter(case.expected_items)

    actual: Counter[DataCategory] = Counter()
    for outcome in mini_sentry.get_outcomes(
        timeout=10 if case.expected_outcomes else 1
    ):
        assert outcome["outcome"] == Outcome.RATE_LIMITED
        assert outcome["reason"] == "mixed_envelope_quota"
        actual[outcome["category"]] += outcome["quantity"]
    assert actual == case.expected_outcomes
    assert mini_sentry.captured_envelopes.empty()
    assert mini_sentry.captured_outcomes.empty()


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
    return project, envelope
