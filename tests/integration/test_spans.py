import contextlib
import json
from unittest import mock
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta, timezone

import pytest
from requests import HTTPError
from sentry_relay.consts import DataCategory
from sentry_sdk.envelope import Envelope, Item, PayloadRef

from .asserts import time_within_delta
from .consts import (
    TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION,
)
from .test_store import make_transaction

TEST_CONFIG = {
    "aggregator": {
        "bucket_interval": 1,
        "initial_delay": 0,
        "shift_key": "none",
    }
}


@pytest.mark.parametrize("performance_issues_spans", [False, True])
def test_span_extraction(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    transactions_consumer,
    events_consumer,
    metrics_consumer,
    performance_issues_spans,
):
    spans_consumer = spans_consumer()
    transactions_consumer = transactions_consumer()
    events_consumer = events_consumer()
    metrics_consumer = metrics_consumer()

    relay = relay_with_processing(options=TEST_CONFIG)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION,
    }

    project_config["config"].setdefault("features", [])
    if performance_issues_spans:
        project_config["config"]["features"].append(
            "organizations:performance-issues-spans"
        )

    event = make_transaction({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
    event["contexts"]["trace"]["status"] = "success"
    event["contexts"]["trace"]["origin"] = "manual"
    event["contexts"]["trace"]["links"] = [
        {
            "trace_id": "1f62a8b040f340bda5d830223def1d83",
            "span_id": "dbbbbbbbbbbbbbbd",
            "sampled": True,
            "attributes": {"txn_key": 123},
        },
    ]
    end = datetime.now(timezone.utc) - timedelta(seconds=1)
    duration = timedelta(milliseconds=500)
    start = end - duration
    event["spans"] = [
        {
            "description": "GET /api/0/organizations/?member=1",
            "links": [
                {
                    "trace_id": "0f62a8b040f340bda5d830223def1d82",
                    "span_id": "cbbbbbbbbbbbbbbc",
                    "sampled": True,
                    "attributes": {"span_key": "span_value"},
                },
            ],
            "op": "http",
            "origin": "manual",
            "parent_span_id": "968cff94913ebb07",
            "span_id": "bbbbbbbbbbbbbbbb",
            "start_timestamp": start.isoformat(),
            "status": "success",
            "timestamp": end.isoformat(),
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
    ]

    user_id = str(uuid.uuid4())
    event["user"] = {
        "id": user_id,
        # "email": "john@example.com",
        "ip_address": "192.168.0.1",
        "sentry_user": f"id:{user_id}",
        "geo": {
            "country_code": "AT",
            "city": "Vienna",
            "subdivision": "Vienna",
            "region": "Austria",
        },
    }

    relay.send_event(project_id, event)

    received_event, _ = transactions_consumer.get_event(timeout=2.0)
    assert received_event["event_id"] == event["event_id"]
    assert received_event.get("_performance_issues_spans") == (
        performance_issues_spans or None
    )
    assert {headers[0] for _, headers in metrics_consumer.get_metrics()} == {
        ("namespace", b"spans"),
        ("namespace", b"transactions"),
    }

    child_span = spans_consumer.get_span()

    del child_span["received"]

    expected_child_span = {
        "attributes": {  # Backfilled from `sentry_tags`
            "sentry.category": {"type": "string", "value": "http"},
            "sentry.exclusive_time": {"type": "double", "value": 500.0},
            "sentry.normalized_description": {"type": "string", "value": "GET *"},
            "sentry.group": {"type": "string", "value": "37e3d9fab1ae9162"},
            "sentry.op": {"type": "string", "value": "http"},
            "sentry.origin": {"type": "string", "value": "manual"},
            "sentry.platform": {"type": "string", "value": "other"},
            "sentry.sdk.name": {"type": "string", "value": "raven-node"},
            "sentry.sdk.version": {"type": "string", "value": "2.6.3"},
            "sentry.status": {"type": "string", "value": "ok"},
            "sentry.trace.status": {"type": "string", "value": "ok"},
            "sentry.transaction": {"type": "string", "value": "hi"},
            "sentry.transaction.op": {"type": "string", "value": "hi"},
            "sentry.user": {"type": "string", "value": f"id:{user_id}"},
            "sentry.user.geo.city": {"type": "string", "value": "Vienna"},
            "sentry.user.geo.country_code": {"type": "string", "value": "AT"},
            "sentry.user.geo.region": {"type": "string", "value": "Austria"},
            "sentry.user.geo.subdivision": {"type": "string", "value": "Vienna"},
            "sentry.user.geo.subregion": {"type": "string", "value": "155"},
            "sentry.user.id": {"type": "string", "value": user_id},
            "sentry.user.ip": {"type": "string", "value": "192.168.0.1"},
            "sentry.description": {
                "type": "string",
                "value": "GET /api/0/organizations/?member=1",
            },
            "sentry.is_remote": {"type": "boolean", "value": False},
            "sentry.segment.id": {"type": "string", "value": "968cff94913ebb07"},
        },
        "downsampled_retention_days": 90,
        "end_timestamp": end.timestamp(),
        "event_id": "cbf6960622e14a45abc1f03b2055b186",
        "is_segment": False,
        "links": [
            {
                "trace_id": "0f62a8b040f340bda5d830223def1d82",
                "span_id": "cbbbbbbbbbbbbbbc",
                "sampled": True,
                "attributes": {"span_key": {"type": "string", "value": "span_value"}},
            },
        ],
        "name": "http",
        "organization_id": 1,
        "parent_span_id": "968cff94913ebb07",
        "project_id": 42,
        "key_id": 123,
        "retention_days": 90,
        "span_id": "bbbbbbbbbbbbbbbb",
        "start_timestamp": start.timestamp(),
        "status": "ok",
        "trace_id": "ff62a8b040f340bda5d830223def1d81",
    }
    assert child_span == expected_child_span

    start_timestamp = datetime.fromisoformat(event["start_timestamp"]).replace(
        tzinfo=timezone.utc
    )
    end_timestamp = datetime.fromisoformat(event["timestamp"]).replace(
        tzinfo=timezone.utc
    )
    duration = (end_timestamp - start_timestamp).total_seconds()

    transaction_span = spans_consumer.get_span()

    del transaction_span["received"]

    if performance_issues_spans:
        assert (
            transaction_span["attributes"].pop(
                "sentry._internal.performance_issues_spans"
            )["value"]
            is True
        )

    expected_transaction_span = {
        "attributes": {
            "sentry.description": {"type": "string", "value": "hi"},
            "sentry.exclusive_time": {"type": "double", "value": 1500.0},
            "sentry.is_remote": {"type": "boolean", "value": True},
            "sentry.op": {"type": "string", "value": "hi"},
            "sentry.origin": {"type": "string", "value": "manual"},
            "sentry.platform": {"type": "string", "value": "other"},
            "sentry.sdk.name": {"type": "string", "value": "raven-node"},
            "sentry.sdk.version": {"type": "string", "value": "2.6.3"},
            "sentry.segment.id": {"type": "string", "value": "968cff94913ebb07"},
            "sentry.segment.name": {"type": "string", "value": "hi"},
            "sentry.status": {"type": "string", "value": "ok"},
            "sentry.trace.status": {"type": "string", "value": "ok"},
            "sentry.transaction.op": {"type": "string", "value": "hi"},
            "sentry.transaction": {"type": "string", "value": "hi"},
            "sentry.user.geo.city": {"type": "string", "value": "Vienna"},
            "sentry.user.geo.country_code": {"type": "string", "value": "AT"},
            "sentry.user.geo.region": {"type": "string", "value": "Austria"},
            "sentry.user.geo.subdivision": {"type": "string", "value": "Vienna"},
            "sentry.user.geo.subregion": {"type": "string", "value": "155"},
            "sentry.user.id": {"type": "string", "value": user_id},
            "sentry.user.ip": {"type": "string", "value": "192.168.0.1"},
            "sentry.user": {"type": "string", "value": f"id:{user_id}"},
            "sentry.was_transaction": {"type": "boolean", "value": True},
        },
        "downsampled_retention_days": 90,
        "end_timestamp": end_timestamp.timestamp(),
        "event_id": "cbf6960622e14a45abc1f03b2055b186",
        "is_segment": True,
        "links": [
            {
                "trace_id": "1f62a8b040f340bda5d830223def1d83",
                "span_id": "dbbbbbbbbbbbbbbd",
                "sampled": True,
                "attributes": {"txn_key": {"type": "integer", "value": 123}},
            },
        ],
        "name": "hi",
        "organization_id": 1,
        "project_id": 42,
        "key_id": 123,
        "retention_days": 90,
        "span_id": "968cff94913ebb07",
        "start_timestamp": start_timestamp.timestamp(),
        "status": "ok",
        "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
    }

    assert transaction_span == expected_transaction_span

    spans_consumer.assert_empty()


def test_duplicate_performance_score(mini_sentry, relay):
    relay = relay(mini_sentry, options=TEST_CONFIG)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION,
    }
    project_config["config"]["performanceScore"] = {
        "profiles": [
            {
                "name": "Desktop",
                "scoreComponents": [
                    {"measurement": "cls", "weight": 1.0, "p10": 0.1, "p50": 0.25},
                ],
                "condition": {"op": "and", "inner": []},
            }
        ]
    }
    project_config["config"]["sampling"] = (
        {  # Drop everything, to trigger metrics extractino
            "version": 2,
            "rules": [
                {
                    "id": 1,
                    "samplingValue": {"type": "sampleRate", "value": 0.0},
                    "type": "transaction",
                    "condition": {"op": "and", "inner": []},
                }
            ],
        }
    )
    event = make_transaction({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
    event.setdefault("contexts", {})["browser"] = {"name": "Chrome"}
    event["measurements"] = {"cls": {"value": 0.11}}
    relay.send_event(project_id, event)

    score_total_seen = 0
    for _ in range(3):  # 2 client reports and the actual item we're interested in
        envelope = mini_sentry.get_captured_envelope()
        for item in envelope.items:
            if item.type == "metric_buckets":
                for metric in json.loads(item.payload.get_bytes()):
                    if (
                        metric["name"]
                        == "d:transactions/measurements.score.total@ratio"
                    ):
                        score_total_seen += 1

    assert score_total_seen == 1


def envelope_with_spans(
    start: datetime, end: datetime, metrics_extracted: bool = False
) -> Envelope:
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "description": "https://example.com/p/blah.js",
                        "op": "resource.script",
                        # Span with the same `span_id` and `segment_id`, to make sure it is classified as `is_segment`.
                        "span_id": "b0429c44b67a3eb1",
                        "segment_id": "b0429c44b67a3eb1",
                        "start_timestamp": start.timestamp(),
                        "timestamp": end.timestamp() + 1,
                        "exclusive_time": 345.0,  # The SDK knows that this span has a lower exclusive time
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "measurements": {
                            "score.total": {"unit": "ratio", "value": 0.12121616},
                        },
                        "data": {
                            "browser.name": "Chrome",
                        },
                        "links": [
                            {
                                "trace_id": "99143b0763095bd9c9955e8175d1fb25",
                                "span_id": "e342abb1214ca183",
                                "sampled": True,
                                "attributes": {
                                    "link_bool_key": True,
                                },
                            },
                        ],
                    },
                ).encode()
            ),
        )
    )
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "description": r"test \" with \" escaped \" chars",
                        "op": "default",
                        "span_id": "cd429c44b67a3eb1",
                        "segment_id": "968cff94913ebb07",
                        "start_timestamp": start.timestamp(),
                        "timestamp": end.timestamp() + 1,
                        "exclusive_time": 345.0,  # The SDK knows that this span has a lower exclusive time
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    },
                ).encode()
            ),
        )
    )
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "op": "default",
                        "span_id": "ed429c44b67a3eb1",
                        "segment_id": "968cff94913ebb07",
                        "start_timestamp": start.timestamp(),
                        "timestamp": end.timestamp() + 1,
                        "exclusive_time": 345.0,  # The SDK knows that this span has a lower exclusive time
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    },
                ).encode()
            ),
        )
    )

    return envelope


def envelope_with_transaction_and_spans(start: datetime, end: datetime) -> Envelope:
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="transaction",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "type": "transaction",
                        "timestamp": end.timestamp() + 1,
                        "start_timestamp": start.timestamp(),
                        "spans": [
                            {
                                "op": "default",
                                "span_id": "968cff94913ebb07",
                                "segment_id": "968cff94913ebb07",
                                "start_timestamp": start.timestamp(),
                                "timestamp": end.timestamp() + 1,
                                "exclusive_time": 1000.0,
                                "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
                            },
                        ],
                        "contexts": {
                            "trace": {
                                "op": "hi",
                                "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
                                "span_id": "968cff94913ebb07",
                            }
                        },
                        "transaction": "my_transaction",
                    },
                ).encode()
            ),
        )
    )

    return envelope


def make_otel_span(start, end):
    return {
        "resourceSpans": [
            {
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": "89143b0763095bd9c9955e8175d1fb24",
                                "spanId": "d342abb1214ca182",
                                "name": "my 2nd OTel span",
                                "startTimeUnixNano": str(int(start.timestamp() * 1e9)),
                                "endTimeUnixNano": str(int(end.timestamp() * 1e9)),
                                "kind": 4,
                                "attributes": [
                                    {
                                        "key": "sentry.exclusive_time",
                                        "value": {
                                            "doubleValue": (end - start).total_seconds()
                                            * 1e3,
                                        },
                                    },
                                ],
                                "links": [
                                    {
                                        "traceId": "89143b0763095bd9c9955e8175d1fb24",
                                        "spanId": "e342abb1214ca183",
                                        "attributes": [
                                            {
                                                "key": "link_int_key",
                                                "value": {
                                                    "intValue": "123",
                                                },
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
        ],
    }


def test_otel_endpoint_disabled(mini_sentry, relay):
    relay = relay(
        mini_sentry,
        {
            "outcomes": {
                "emit_outcomes": True,
                "batch_size": 1,
                "batch_interval": 1,
                "source": "relay",
            }
        },
    )
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]
    project_config["features"] = ["organizations:standalone-span-ingestion"]

    end = datetime.now(timezone.utc) - timedelta(seconds=1)
    start = end - timedelta(milliseconds=500)
    relay.send_otel_span(
        project_id,
        json=make_otel_span(start, end),
    )

    assert mini_sentry.get_outcomes(2) == [
        {
            "org_id": 1,
            "key_id": 123,
            "project_id": 42,
            "outcome": 3,
            "reason": "feature_disabled",
            "category": category.value,
            "quantity": 1,
            "source": "relay",
            "timestamp": time_within_delta(),
        }
        for category in [DataCategory.SPAN, DataCategory.SPAN_INDEXED]
    ]

    # Second attempt will cause a 403 response:
    with pytest.raises(HTTPError) as exc_info:
        relay.send_otel_span(
            project_id,
            json=make_otel_span(start, end),
        )
    response = exc_info.value.response
    assert response.status_code == 403
    assert response.json() == {
        "detail": "event submission rejected with_reason: FeatureDisabled(OtelTracesEndpoint)"
    }

    # No envelopes were received:
    assert mini_sentry.captured_envelopes.empty()


def test_span_ingestion_with_performance_scores(
    mini_sentry, relay_with_processing, spans_consumer
):
    spans_consumer = spans_consumer()
    relay = relay_with_processing()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["performanceScore"] = {
        "profiles": [
            {
                "name": "Desktop",
                "scoreComponents": [
                    {"measurement": "fcp", "weight": 0.15, "p10": 900, "p50": 1600},
                    {"measurement": "lcp", "weight": 0.30, "p10": 1200, "p50": 2400},
                    {"measurement": "fid", "weight": 0.30, "p10": 100, "p50": 300},
                    {"measurement": "cls", "weight": 0.25, "p10": 0.1, "p50": 0.25},
                    {"measurement": "ttfb", "weight": 0.0, "p10": 0.2, "p50": 0.4},
                ],
                "condition": {
                    "op": "eq",
                    "name": "event.contexts.browser.name",
                    "value": "Python Requests",
                },
            },
            {
                "name": "Desktop INP",
                "scoreComponents": [
                    {"measurement": "inp", "weight": 1.0, "p10": 200, "p50": 400},
                ],
                "condition": {
                    "op": "eq",
                    "name": "event.contexts.browser.name",
                    "value": "Python Requests",
                },
            },
        ],
    }
    project_config["config"]["features"] = [
        "organizations:performance-calculate-score-relay",
        "organizations:standalone-span-ingestion",
    ]
    project_config["config"]["txNameRules"] = [
        {
            "pattern": "**/interaction/*/**",
            "expiry": "3022-11-30T00:00:00.000000Z",
            "redaction": {"method": "replace", "substitution": "*"},
        }
    ]

    duration = timedelta(milliseconds=500)
    end = datetime.now(timezone.utc) - timedelta(seconds=1)
    start = end - duration

    envelope = Envelope()
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "op": "ui.interaction.click",
                        "span_id": "bd429c44b67a3eb1",
                        "segment_id": "bd429c44b67a3eb1",
                        "start_timestamp": start.timestamp(),
                        "timestamp": end.timestamp() + 1,
                        "exclusive_time": 345.0,  # The SDK knows that this span has a lower exclusive time
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "measurements": {
                            "cls": {"value": 100},
                            "fcp": {"value": 200},
                            "fid": {"value": 300},
                            "lcp": {"value": 400},
                            "ttfb": {"value": 500},
                        },
                    },
                ).encode()
            ),
        )
    )
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "data": {
                            "transaction": "/page/with/click/interaction/jane/123",
                            "replay_id": "8477286c8e5148b386b71ade38374d58",
                            "user": "[email]",
                        },
                        "profile_id": "3d9428087fda4ba0936788b70a7587d0",
                        "op": "ui.interaction.click",
                        "span_id": "cd429c44b67a3eb1",
                        "segment_id": "cd429c44b67a3eb1",
                        "start_timestamp": start.timestamp(),
                        "timestamp": end.timestamp() + 1,
                        "exclusive_time": 345.0,  # The SDK knows that this span has a lower exclusive time
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "measurements": {
                            "inp": {"value": 100},
                        },
                    },
                ).encode()
            ),
        )
    )
    relay.send_envelope(project_id, envelope)

    spans = spans_consumer.get_spans(timeout=10.0, n=2)

    for span in spans:
        span.pop("received", None)

    # endpoint might overtake envelope
    spans.sort(key=lambda msg: msg["span_id"])

    expected_scores = [
        {
            "score.fcp": 0.14999972769539766,
            "score.fid": 0.14999999985,
            "score.lcp": 0.29986141375718806,
            "score.ratio.cls": 0.0,
            "score.ratio.fcp": 0.9999981846359844,
            "score.ratio.fid": 0.4999999995,
            "score.ratio.lcp": 0.9995380458572936,
            "score.ratio.ttfb": 0.0,
            "score.total": 0.5998611413025857,
            "score.ttfb": 0.0,
            "score.weight.cls": 0.25,
            "score.weight.fcp": 0.15,
            "score.weight.fid": 0.3,
            "score.weight.lcp": 0.3,
            "score.weight.ttfb": 0.0,
            "cls": 100.0,
            "fcp": 200.0,
            "fid": 300.0,
            "lcp": 400.0,
            "ttfb": 500.0,
            "score.cls": 0.0,
        },
        {
            "inp": 100.0,
            "score.inp": 0.9948129113413748,
            "score.ratio.inp": 0.9948129113413748,
            "score.total": 0.9948129113413748,
            "score.weight.inp": 1.0,
        },
    ]

    assert len(spans) == len(expected_scores)
    for span, scores in zip(spans, expected_scores):
        for key, score in scores.items():
            assert span["attributes"][key]["value"] == score


def test_rate_limit_indexed_consistent(
    mini_sentry, relay_with_processing, spans_consumer, outcomes_consumer
):
    """Rate limits for indexed are enforced consistently after metrics extraction.

    This test does not cover consistent enforcement of total spans.
    """
    relay = relay_with_processing()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
    ]
    project_config["config"]["quotas"] = [
        {
            "categories": ["span_indexed"],
            "limit": 5,
            "window": int(datetime.now(UTC).timestamp()),
            "id": uuid.uuid4(),
            "reasonCode": "indexed_exceeded",
        },
    ]

    spans_consumer = spans_consumer()
    outcomes_consumer = outcomes_consumer()

    start = datetime.now(timezone.utc)
    end = start + timedelta(seconds=1)

    envelope = envelope_with_spans(start, end)

    def summarize_outcomes():
        counter = Counter()
        for outcome in outcomes_consumer.get_outcomes():
            counter[(outcome["category"], outcome["outcome"])] += outcome["quantity"]
        return counter

    # First batch passes
    relay.send_envelope(project_id, envelope)
    spans = spans_consumer.get_spans(n=3, timeout=10)
    assert len(spans) == 3
    assert summarize_outcomes() == {(16, 0): 3}  # SpanIndexed, Accepted

    # Second batch is limited
    relay.send_envelope(project_id, envelope)
    assert summarize_outcomes() == {(16, 2): 3}  # SpanIndexed, RateLimited

    spans_consumer.assert_empty()
    outcomes_consumer.assert_empty()


def test_rate_limit_consistent_extracted(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    metrics_consumer,
    outcomes_consumer,
):
    """Rate limits for spans that are extracted from transactions"""
    relay = relay_with_processing(options=TEST_CONFIG)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    # Span metrics won't be extracted without a supported transactionMetrics config.
    # Without extraction, the span is treated as `Span`, not `SpanIndexed`.
    project_config["config"]["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION
    }
    project_config["config"]["quotas"] = [
        {
            "categories": ["span"],
            "limit": 2,
            "window": int(datetime.now(UTC).timestamp()),
            "id": uuid.uuid4(),
            "reasonCode": "my_rate_limit",
        },
    ]

    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()
    outcomes_consumer = outcomes_consumer()

    start = datetime.now(timezone.utc)
    end = start + timedelta(seconds=1)

    event = make_transaction({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
    end = datetime.now(timezone.utc) - timedelta(seconds=1)
    duration = timedelta(milliseconds=500)
    start = end - duration
    event["spans"] = [
        {
            "description": "GET /api/0/organizations/?member=1",
            "op": "http",
            "parent_span_id": "968cff94913ebb07",
            "span_id": "bbbbbbbbbbbbbbbb",
            "start_timestamp": start.isoformat(),
            "timestamp": end.isoformat(),
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
    ]

    def summarize_outcomes():
        counter = Counter()
        for outcome in outcomes_consumer.get_outcomes():
            counter[(outcome["category"], outcome["outcome"])] += outcome["quantity"]
        return counter

    # First send should be accepted.
    relay.send_event(project_id, event)
    spans = spans_consumer.get_spans(n=2, timeout=10)
    # one for the transaction, one for the contained span
    assert len(spans) == 2
    assert summarize_outcomes() == {(16, 0): 2}  # SpanIndexed, Accepted
    # A limit only for span_indexed does not affect extracted metrics
    metrics = metrics_consumer.get_metrics(n=8)
    span_count = sum(
        [m[0]["value"] for m in metrics if m[0]["name"] == "c:spans/usage@none"]
    )
    assert span_count == 2

    # Second send should be rejected immediately.
    relay.send_event(project_id, event)
    outcomes = summarize_outcomes()

    expected_outcomes = {
        (12, 2): 2,
        (16, 2): 2,  # SpanIndexed, RateLimited
    }
    assert outcomes == expected_outcomes

    metrics = metrics_consumer.get_metrics(timeout=1)
    assert len(metrics) == 4
    assert all(m[0]["name"][2:14] == "transactions" for m in metrics), metrics

    outcomes_consumer.assert_empty()


def test_rate_limit_spans_in_envelope(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    metrics_consumer,
    outcomes_consumer,
):
    """Rate limits for total spans are enforced and no metrics are emitted."""
    relay = relay_with_processing(options=TEST_CONFIG)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
    ]
    project_config["config"]["quotas"] = [
        {
            "categories": ["span"],
            "limit": 2,
            "window": int(datetime.now(UTC).timestamp()),
            "id": uuid.uuid4(),
            "reasonCode": "total_exceeded",
        },
    ]

    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()
    outcomes_consumer = outcomes_consumer()

    start = datetime.now(UTC)
    end = start + timedelta(seconds=1)

    envelope = envelope_with_spans(start, end)

    def summarize_outcomes():
        counter = Counter()
        for outcome in outcomes_consumer.get_outcomes(timeout=10, n=2):
            counter[(outcome["category"], outcome["outcome"])] += outcome["quantity"]
        return dict(counter)

    relay.send_envelope(project_id, envelope)

    assert summarize_outcomes() == {(12, 2): 3, (16, 2): 3}

    # We emit transaction metrics from spans for legacy reasons. These are not rate limited.
    # (could be a bug)
    ((metric, _),) = metrics_consumer.get_metrics(n=1)
    assert ":spans/" not in metric["name"]

    spans_consumer.assert_empty()
    metrics_consumer.assert_empty()


@pytest.mark.parametrize("category", ["transaction", "transaction_indexed"])
@pytest.mark.parametrize("span_count_header", [None, 666])
def test_rate_limit_is_consistent_between_transaction_and_spans(
    mini_sentry,
    relay_with_processing,
    transactions_consumer,
    spans_consumer,
    metrics_consumer,
    outcomes_consumer,
    category,
    span_count_header,
):
    """
    Rate limits are consistent between transactions and nested spans.
    """
    relay = relay_with_processing(options=TEST_CONFIG)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
    ]
    project_config["config"]["quotas"] = [
        {
            "categories": [category],
            "limit": 1,
            "window": int(datetime.now(UTC).timestamp()),
            "id": uuid.uuid4(),
            "reasonCode": "exceeded",
        },
    ]
    project_config["config"]["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION,
    }

    transactions_consumer = transactions_consumer()
    spans_consumer = spans_consumer()
    outcomes_consumer = outcomes_consumer()
    metrics_consumer = metrics_consumer()

    def usage_metrics():
        metrics = metrics_consumer.get_metrics()
        transaction_count = sum(
            m[0]["value"]
            for m in metrics
            if m[0]["name"] == "c:transactions/usage@none"
        )
        span_count = sum(
            m[0]["value"] for m in metrics if m[0]["name"] == "c:spans/usage@none"
        )
        return (transaction_count, span_count)

    def summarize_outcomes():
        counter = Counter()
        for outcome in outcomes_consumer.get_outcomes(timeout=10):
            counter[(outcome["category"], outcome["outcome"])] += outcome["quantity"]
        return dict(counter)

    start = datetime.now(timezone.utc)
    end = start + timedelta(seconds=1)

    envelope = envelope_with_transaction_and_spans(start, end)
    if span_count_header is not None:
        envelope.items[0].headers["span_count"] = span_count_header

    # First batch passes
    relay.send_envelope(project_id, envelope)

    event, _ = transactions_consumer.get_event(timeout=10)
    assert event["transaction"] == "my_transaction"
    # We have one nested span and the transaction itself becomes a span
    spans = spans_consumer.get_spans(n=2, timeout=10)
    assert len(spans) == 2
    assert summarize_outcomes() == {(16, 0): 2}  # SpanIndexed, Accepted
    assert usage_metrics() == (1, 2)

    # Second batch nothing passes
    relay.send_envelope(project_id, envelope)

    transactions_consumer.assert_empty()
    spans_consumer.assert_empty()
    if category == "transaction":
        assert summarize_outcomes() == {
            (2, 2): 1,  # Transaction, Rate Limited
            (9, 2): 1,  # TransactionIndexed, Rate Limited
            (12, 2): 2,  # Span, Rate Limited
            (16, 2): 2,  # SpanIndexed, Rate Limited
        }
        assert usage_metrics() == (0, 0)
    elif category == "transaction_indexed":
        assert summarize_outcomes() == {
            (9, 2): 1,  # TransactionIndexed, Rate Limited
            (16, 2): 2,  # SpanIndexed, Rate Limited
        }
        assert usage_metrics() == (1, 2)

    # Third batch might raise 429 since it hits the fast path
    maybe_raises = (
        pytest.raises(HTTPError, match="429 Client Error")
        if category == "transaction"
        else contextlib.nullcontext()
    )
    with maybe_raises:
        relay.send_envelope(project_id, envelope)

    # The fast path now trusts the span_count item header
    expected_span_count = 2 if span_count_header is None else 667

    if category == "transaction":
        assert summarize_outcomes() == {
            (2, 2): 1,  # Transaction, Rate Limited
            (9, 2): 1,  # TransactionIndexed, Rate Limited
            (12, 2): expected_span_count,  # Span, Rate Limited
            (16, 2): expected_span_count,  # SpanIndexed, Rate Limited
        }
        assert usage_metrics() == (0, 0)
    elif category == "transaction_indexed":
        # We do not check indexed limits on the fast path,
        # so we count the correct number of spans (ignoring the span_count header):
        assert summarize_outcomes() == {
            (9, 2): 1,  # TransactionIndexed, Rate Limited
            (16, 2): 2,  # SpanIndexed, Rate Limited
        }
        # Metrics are always correct:
        assert usage_metrics() == (1, 2)


def test_span_filtering_with_generic_inbound_filter(
    mini_sentry, relay_with_processing, spans_consumer, outcomes_consumer
):
    mini_sentry.global_config["filters"] = {
        "version": 1,
        "filters": [
            {
                "id": "first-releases",
                "isEnabled": True,
                "condition": {
                    "op": "eq",
                    "name": "span.data.release",
                    "value": "1.0",
                },
            }
        ],
    }

    relay = relay_with_processing(options=TEST_CONFIG)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = ["organizations:standalone-span-ingestion"]

    spans_consumer = spans_consumer()
    outcomes_consumer = outcomes_consumer()

    end = datetime.now(timezone.utc) - timedelta(seconds=1)
    duration = timedelta(milliseconds=500)
    start = end - duration
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "description": "organizations/metrics/data",
                        "op": "default",
                        "span_id": "cd429c44b67a3eb1",
                        "segment_id": "968cff94913ebb07",
                        "start_timestamp": start.timestamp(),
                        "timestamp": end.timestamp() + 1,
                        "exclusive_time": 345.0,  # The SDK knows that this span has a lower exclusive time
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "data": {"sentry.release": "1.0"},
                    },
                ).encode()
            ),
        )
    )

    relay.send_envelope(project_id, envelope)

    def summarize_outcomes():
        counter = Counter()
        for outcome in outcomes_consumer.get_outcomes(timeout=10, n=2):
            counter[(outcome["category"], outcome["outcome"])] += outcome["quantity"]
        return counter

    assert summarize_outcomes() == {(12, 1): 1, (16, 1): 1}
    spans_consumer.assert_empty()
    outcomes_consumer.assert_empty()


@pytest.mark.parametrize("sample_rate", [0.0, 1.0])
def test_dynamic_sampling(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    outcomes_consumer,
    sample_rate,
):
    spans_consumer = spans_consumer()
    outcomes_consumer = outcomes_consumer()

    project_id = 42
    project_config = mini_sentry.add_basic_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
    ]
    project_config["config"]["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION
    }

    sampling_config = mini_sentry.add_basic_project_config(43)
    sampling_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
    ]
    sampling_public_key = sampling_config["publicKeys"][0]["publicKey"]
    sampling_config["config"]["txNameRules"] = [
        {
            "pattern": "/auth/login/*/**",
            "expiry": "3022-11-30T00:00:00.000000Z",
            "redaction": {"method": "replace", "substitution": "*"},
        }
    ]
    sampling_config["config"]["sampling"] = {
        "version": 2,
        "rules": [
            {
                "id": 3001,
                "samplingValue": {"type": "sampleRate", "value": sample_rate},
                "type": "trace",
                "condition": {
                    "op": "and",
                    "inner": [
                        {
                            "op": "eq",
                            "name": "trace.transaction",
                            "value": "/auth/login/*",
                            "options": {
                                "ignoreCase": True,
                            },
                        }
                    ],
                },
            },
        ],
    }

    relay = relay_with_processing(
        options={
            "aggregator": {
                "bucket_interval": 1,
                "initial_delay": 0,
                "max_secs_in_past": 2**64 - 1,
            }
        }
    )

    duration = timedelta(milliseconds=500)
    end = datetime.now(timezone.utc) - timedelta(seconds=1)
    start = end - duration

    # 1 - Send OTel span and sentry span via envelope
    envelope = envelope_with_spans(start, end)
    envelope.headers["trace"] = {
        "public_key": sampling_public_key,
        "trace_id": "89143b0763095bd9c9955e8175d1fb23",
        "segment_name": "/auth/login/my_user_name",
    }

    relay.send_envelope(project_id, envelope)

    def summarize_outcomes(outcomes):
        counter = Counter()
        for outcome in outcomes:
            counter[(outcome["category"], outcome["outcome"])] += outcome["quantity"]
        return counter

    if sample_rate == 1.0:
        spans = spans_consumer.get_spans(timeout=10, n=3)
        assert len(spans) == 3
        outcomes = outcomes_consumer.get_outcomes(timeout=10, n=3)
        assert summarize_outcomes(outcomes) == {(16, 0): 3}  # SpanIndexed, Accepted
    else:
        outcomes = outcomes_consumer.get_outcomes(timeout=10, n=1)
        assert summarize_outcomes(outcomes) == {
            (16, 1): 3,  # SpanIndexed, Filtered
        }
        assert {o["reason"] for o in outcomes} == {
            "Sampled:3000",
        }

    spans_consumer.assert_empty()
    outcomes_consumer.assert_empty()


@pytest.mark.parametrize("scrub_ip_addresses", [False, True])
def test_scrubs_ip_addresses(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    scrub_ip_addresses,
):
    spans_consumer = spans_consumer()

    relay = relay_with_processing()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"].setdefault("datascrubbingSettings", {})[
        "scrubIpAddresses"
    ] = scrub_ip_addresses

    event = make_transaction({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
    event["user"] = {
        "id": "unique_id",
        "username": "my_user",
        "email": "foo@example.com",
        "ip_address": "127.0.0.1",
        "subscription": "basic",
    }
    end = datetime.now(timezone.utc) - timedelta(seconds=1)
    duration = timedelta(milliseconds=500)
    start = end - duration
    event["spans"] = [
        {
            "description": "GET /api/0/organizations/?member=1",
            "op": "http",
            "origin": "manual",
            "parent_span_id": "968cff94913ebb07",
            "span_id": "bbbbbbbbbbbbbbbb",
            "start_timestamp": start.isoformat(),
            "status": "success",
            "tags": {
                "extra_info": "added by user",
            },
            "timestamp": end.isoformat(),
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
    ]

    relay.send_event(project_id, event)

    child_span = spans_consumer.get_span()

    assert child_span["_meta"]["attributes"]["sentry.user.email"] == {
        "": {"len": 15, "rem": [["@email", "s", 0, 7]]}
    }

    if scrub_ip_addresses:
        assert child_span["attributes"]["sentry.user.ip"] is None
        assert child_span["_meta"]["attributes"]["sentry.user.ip"] == {
            "": {
                "len": 9,
                "rem": [["@ip:replace", "s", 0, 4], ["@anything:remove", "x"]],
            }
        }
    else:
        assert child_span["attributes"]["sentry.user.ip"]["value"] == "127.0.0.1"
        assert "sentry.user.ip" not in child_span["_meta"]["attributes"]

    parent_span = spans_consumer.get_span()

    if scrub_ip_addresses:
        assert "sentry.user.ip" not in parent_span["attributes"]
    else:
        assert parent_span["attributes"]["sentry.user.ip"]["value"] == "127.0.0.1"

    spans_consumer.assert_empty()


def test_outcomes_for_trimmed_spans(mini_sentry, relay):
    relay = relay(
        mini_sentry,
        options={
            "limits": {"max_event_size": "20MB"},
            "outcomes": {"emit_outcomes": True},
        },
    )
    project_id = 42
    mini_sentry.add_full_project_config(project_id)

    event = make_transaction({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
    end = datetime.now(timezone.utc) - timedelta(seconds=1)
    duration = timedelta(milliseconds=500)
    start = end - duration
    event["spans"] = 10 * [
        {
            "platform": 1014 * 90 * "a",
            "description": "GET /api/0/organizations/?member=1",
            "op": "http",
            "origin": "manual",
            "parent_span_id": "968cff94913ebb07",
            "span_id": "bbbbbbbbbbbbbbbb",
            "start_timestamp": start.isoformat(),
            "status": "success",
            "tags": {
                "extra_info": "added by user",
            },
            "sentry_tags": {
                "release": 1024 * 100 * "b",
            },
            "timestamp": end.isoformat(),
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
    ]

    relay.send_event(project_id, event)

    outcomes = mini_sentry.get_outcomes(n=2)
    assert outcomes == [
        {
            "category": DataCategory.SPAN,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # invalid
            "project_id": 42,
            "quantity": 1,
            "reason": "too_large:span",
            "timestamp": mock.ANY,
        },
        {
            "category": DataCategory.SPAN_INDEXED,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # invalid
            "project_id": 42,
            "quantity": 1,
            "reason": "too_large:span",
            "timestamp": mock.ANY,
        },
    ]
