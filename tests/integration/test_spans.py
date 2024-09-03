import contextlib
import json
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta, timezone

import pytest
from requests import HTTPError
from sentry_sdk.envelope import Envelope, Item, PayloadRef

from .consts import (
    METRICS_EXTRACTION_MIN_SUPPORTED_VERSION,
    TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION,
)
from .test_metrics import TEST_CONFIG
from .test_store import make_transaction


@pytest.mark.parametrize("discard_transaction", [False, True])
def test_span_extraction(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    transactions_consumer,
    events_consumer,
    metrics_consumer,
    discard_transaction,
):
    spans_consumer = spans_consumer()
    transactions_consumer = transactions_consumer()
    events_consumer = events_consumer()
    metrics_consumer = metrics_consumer()

    relay = relay_with_processing(options=TEST_CONFIG)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "projects:span-metrics-extraction",
        "organizations:indexed-spans-extraction",
    ]
    project_config["config"]["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION,
    }

    if discard_transaction:
        project_config["config"]["features"].append("projects:discard-transaction")

    event = make_transaction({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
    event["contexts"]["trace"]["status"] = "success"
    event["contexts"]["trace"]["origin"] = "manual"
    end = datetime.now(timezone.utc) - timedelta(seconds=1)
    duration = timedelta(milliseconds=500)
    start = end - duration
    event["spans"] = [
        {
            "description": "GET /api/0/organizations/?member=1",
            "op": "http",
            "origin": "manual",
            "parent_span_id": "aaaaaaaaaaaaaaaa",
            "span_id": "bbbbbbbbbbbbbbbb",
            "start_timestamp": start.isoformat(),
            "status": "success",
            "timestamp": end.isoformat(),
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
    ]

    relay.send_event(project_id, event)

    if discard_transaction:
        assert transactions_consumer.poll(timeout=2.0) is None

        # We do not accidentally produce to the events topic:
        assert events_consumer.poll(timeout=2.0) is None

        # We _do_ extract span metrics:
        assert {headers[0] for _, headers in metrics_consumer.get_metrics()} == {
            ("namespace", b"spans")
        }
    else:
        received_event, _ = transactions_consumer.get_event(timeout=2.0)
        assert received_event["event_id"] == event["event_id"]
        assert {headers[0] for _, headers in metrics_consumer.get_metrics()} == {
            ("namespace", b"spans"),
            ("namespace", b"transactions"),
        }

    child_span = spans_consumer.get_span()
    del child_span["received"]
    assert child_span == {
        "description": "GET /api/0/organizations/?member=1",
        "duration_ms": int(duration.total_seconds() * 1e3),
        "event_id": "cbf6960622e14a45abc1f03b2055b186",
        "exclusive_time_ms": 500.0,
        "is_segment": False,
        "organization_id": 1,
        "origin": "manual",
        "parent_span_id": "aaaaaaaaaaaaaaaa",
        "project_id": 42,
        "retention_days": 90,
        "segment_id": "968cff94913ebb07",
        "sentry_tags": {
            "category": "http",
            "description": "GET *",
            "group": "37e3d9fab1ae9162",
            "op": "http",
            "platform": "other",
            "sdk.name": "raven-node",
            "sdk.version": "2.6.3",
            "status": "ok",
            "trace.status": "ok",
            "transaction": "hi",
            "transaction.op": "hi",
        },
        "span_id": "bbbbbbbbbbbbbbbb",
        "start_timestamp_ms": int(start.timestamp() * 1e3),
        "start_timestamp_precise": start.timestamp(),
        "end_timestamp_precise": start.timestamp() + duration.total_seconds(),
        "trace_id": "ff62a8b040f340bda5d830223def1d81",
    }

    start_timestamp = datetime.fromisoformat(event["start_timestamp"]).replace(
        tzinfo=timezone.utc
    )
    end_timestamp = datetime.fromisoformat(event["timestamp"]).replace(
        tzinfo=timezone.utc
    )
    duration = (end_timestamp - start_timestamp).total_seconds()
    duration_ms = int(duration * 1e3)

    transaction_span = spans_consumer.get_span()
    del transaction_span["received"]
    assert transaction_span == {
        "data": {
            "sentry.sdk.name": "raven-node",
            "sentry.sdk.version": "2.6.3",
            "sentry.segment.name": "hi",
        },
        "description": "hi",
        "duration_ms": duration_ms,
        "event_id": "cbf6960622e14a45abc1f03b2055b186",
        "exclusive_time_ms": 2000.0,
        "is_segment": True,
        "organization_id": 1,
        "origin": "manual",
        "project_id": 42,
        "retention_days": 90,
        "segment_id": "968cff94913ebb07",
        "sentry_tags": {
            "op": "hi",
            "platform": "other",
            "sdk.name": "raven-node",
            "sdk.version": "2.6.3",
            "status": "ok",
            "trace.status": "ok",
            "transaction": "hi",
            "transaction.op": "hi",
        },
        "span_id": "968cff94913ebb07",
        "start_timestamp_ms": int(start_timestamp.timestamp() * 1e3),
        "start_timestamp_precise": start_timestamp.timestamp(),
        "end_timestamp_precise": start_timestamp.timestamp() + duration,
        "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
    }

    spans_consumer.assert_empty()


@pytest.mark.parametrize(
    "sample_rate,expected_spans,expected_metrics",
    [
        (None, 2, 6),
        (1.0, 2, 6),
        (0.0, 0, 0),
    ],
)
def test_span_extraction_with_sampling(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    metrics_consumer,
    sample_rate,
    expected_spans,
    expected_metrics,
):
    mini_sentry.global_config["options"] = {
        "relay.span-extraction.sample-rate": sample_rate
    }

    relay = relay_with_processing(options=TEST_CONFIG)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "projects:span-metrics-extraction",
        "organizations:indexed-spans-extraction",
    ]
    project_config["config"]["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION,
    }

    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()

    event = make_transaction({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
    end = datetime.now(timezone.utc) - timedelta(seconds=1)
    duration = timedelta(milliseconds=500)
    start = end - duration
    event["spans"] = [
        {
            "description": "GET /api/0/organizations/?member=1",
            "op": "http",
            "parent_span_id": "aaaaaaaaaaaaaaaa",
            "span_id": "bbbbbbbbbbbbbbbb",
            "start_timestamp": start.isoformat(),
            "timestamp": end.isoformat(),
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
    ]

    relay.send_event(project_id, event)

    if expected_spans > 0:
        spans = spans_consumer.get_spans(n=expected_spans)
        assert len(spans) == expected_spans

    metrics = metrics_consumer.get_metrics()
    span_metrics = [m for (m, _) in metrics if ":spans/" in m["name"]]
    assert len(span_metrics) == expected_metrics

    spans_consumer.assert_empty()
    metrics_consumer.assert_empty()


def test_duplicate_performance_score(mini_sentry, relay):
    relay = relay(mini_sentry, options=TEST_CONFIG)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "projects:span-metrics-extraction",
        "organizations:indexed-spans-extraction",
    ]
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
    for _ in range(2):
        envelope = mini_sentry.captured_events.get()
        for item in envelope.items:
            if item.type == "metric_buckets":
                for metric in item.payload.json:
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
            type="otel_span",
            headers={"metrics_extracted": metrics_extracted},
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "traceId": "89143b0763095bd9c9955e8175d1fb23",
                        "spanId": "a342abb1214ca181",
                        "name": "my 1st OTel span",
                        "startTimeUnixNano": int(start.timestamp() * 1e9),
                        "endTimeUnixNano": int(end.timestamp() * 1e9),
                        "attributes": [
                            {
                                "key": "sentry.op",
                                "value": {
                                    "stringValue": "db.query",
                                },
                            },
                            {
                                "key": "sentry.exclusive_time_nano",
                                "value": {
                                    "intValue": int(
                                        (end - start).total_seconds() * 1e9
                                    ),
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
                                "startTimeUnixNano": int(start.timestamp() * 1e9),
                                "endTimeUnixNano": int(end.timestamp() * 1e9),
                                "attributes": [
                                    {
                                        "key": "sentry.exclusive_time_nano",
                                        "value": {
                                            "intValue": int(
                                                (end - start).total_seconds() * 1e9
                                            ),
                                        },
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
        ],
    }


def test_span_extraction_with_metrics_summary(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    metrics_summaries_consumer,
):
    spans_consumer = spans_consumer()
    metrics_summaries_consumer = metrics_summaries_consumer()

    relay = relay_with_processing()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:custom-metrics",
        "projects:span-metrics-extraction",
        "organizations:indexed-spans-extraction",
    ]

    event = make_transaction({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
    mri = "c:spans/some_metric@none"
    metrics_summary = {
        mri: [
            {
                "min": 1.0,
                "max": 2.0,
                "sum": 3.0,
                "count": 4,
                "tags": {
                    "environment": "test",
                },
            },
        ],
    }
    event["_metrics_summary"] = metrics_summary

    relay.send_event(project_id, event)

    start_timestamp = datetime.fromisoformat(event["start_timestamp"]).replace(
        tzinfo=timezone.utc
    )
    end_timestamp = datetime.fromisoformat(event["timestamp"]).replace(
        tzinfo=timezone.utc
    )
    duration = (end_timestamp - start_timestamp).total_seconds()
    duration_ms = int(duration * 1e3)

    transaction_span = spans_consumer.get_span()
    del transaction_span["received"]
    assert transaction_span == {
        "data": {
            "sentry.sdk.name": "raven-node",
            "sentry.sdk.version": "2.6.3",
            "sentry.segment.name": "hi",
        },
        "description": "hi",
        "duration_ms": duration_ms,
        "event_id": "cbf6960622e14a45abc1f03b2055b186",
        "exclusive_time_ms": 2000.0,
        "is_segment": True,
        "organization_id": 1,
        "project_id": 42,
        "retention_days": 90,
        "segment_id": "968cff94913ebb07",
        "sentry_tags": {
            "op": "hi",
            "platform": "other",
            "sdk.name": "raven-node",
            "sdk.version": "2.6.3",
            "status": "unknown",
            "trace.status": "unknown",
            "transaction": "hi",
            "transaction.op": "hi",
        },
        "span_id": "968cff94913ebb07",
        "start_timestamp_ms": int(
            start_timestamp.timestamp() * 1e3,
        ),
        "start_timestamp_precise": start_timestamp.timestamp(),
        "end_timestamp_precise": end_timestamp.timestamp(),
        "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
    }

    spans_consumer.assert_empty()
    metrics_summary = metrics_summaries_consumer.get_metrics_summary()

    assert metrics_summary["mri"] == mri


def test_span_no_extraction_with_metrics_summary(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    metrics_summaries_consumer,
):
    spans_consumer = spans_consumer()
    metrics_summaries_consumer = metrics_summaries_consumer()

    relay = relay_with_processing()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:custom-metrics",
    ]

    event = make_transaction({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
    mri = "c:spans/some_metric@none"
    metrics_summary = {
        mri: [
            {
                "min": 1.0,
                "max": 2.0,
                "sum": 3.0,
                "count": 4,
                "tags": {
                    "environment": "test",
                },
            },
        ],
    }
    event["_metrics_summary"] = metrics_summary

    relay.send_event(project_id, event)

    spans_consumer.assert_empty()
    metrics_summaries_consumer.assert_empty()


def test_span_extraction_with_ddm_missing_values(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
):
    spans_consumer = spans_consumer()

    relay = relay_with_processing()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:custom-metrics",
        "projects:span-metrics-extraction",
        "organizations:indexed-spans-extraction",
    ]

    event = make_transaction({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
    metrics_summary = {
        "c:spans/some_metric@none": [
            {
                "min": None,
                "max": 2.0,
                "count": 4,
                "tags": {
                    "environment": "test",
                    "release": None,
                },
            },
        ],
    }
    event["_metrics_summary"] = metrics_summary
    event["measurements"] = {
        "somemeasurement": None,
        "anothermeasurement": {
            "value": None,
            "unit": "byte",
        },
    }

    relay.send_event(project_id, event)

    start_timestamp = datetime.fromisoformat(event["start_timestamp"]).replace(
        tzinfo=timezone.utc
    )
    end_timestamp = datetime.fromisoformat(event["timestamp"]).replace(
        tzinfo=timezone.utc
    )
    duration_ms = int((end_timestamp - start_timestamp).total_seconds() * 1e3)

    metrics_summary["c:spans/some_metric@none"][0].pop("min", None)

    transaction_span = spans_consumer.get_span()
    del transaction_span["received"]
    assert transaction_span == {
        "data": {
            "sentry.sdk.name": "raven-node",
            "sentry.sdk.version": "2.6.3",
            "sentry.segment.name": "hi",
        },
        "description": "hi",
        "duration_ms": duration_ms,
        "event_id": "cbf6960622e14a45abc1f03b2055b186",
        "exclusive_time_ms": 2000.0,
        "is_segment": True,
        "measurements": {},
        "organization_id": 1,
        "project_id": 42,
        "retention_days": 90,
        "segment_id": "968cff94913ebb07",
        "sentry_tags": {
            "op": "hi",
            "platform": "other",
            "sdk.name": "raven-node",
            "sdk.version": "2.6.3",
            "status": "unknown",
            "trace.status": "unknown",
            "transaction": "hi",
            "transaction.op": "hi",
        },
        "span_id": "968cff94913ebb07",
        "start_timestamp_ms": int(start_timestamp.timestamp() * 1e3),
        "start_timestamp_precise": start_timestamp.timestamp(),
        "end_timestamp_precise": end_timestamp.timestamp(),
        "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
    }

    spans_consumer.assert_empty()


def test_span_reject_invalid_timestamps(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
):
    spans_consumer = spans_consumer()

    relay = relay_with_processing(
        options={
            "aggregator": {
                "max_secs_in_past": 10,
                "max_secs_in_future": 10,
            }
        }
    )
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
    ]

    duration = timedelta(milliseconds=500)
    yesterday_delta = timedelta(days=1)

    end_yesterday = datetime.now(timezone.utc) - yesterday_delta
    start_yesterday = end_yesterday - duration

    end_today = datetime.now(timezone.utc) - timedelta(seconds=1)
    start_today = end_today - duration

    envelope = Envelope()
    envelope.add_item(
        Item(
            type="otel_span",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "traceId": "89143b0763095bd9c9955e8175d1fb23",
                        "spanId": "a342abb1214ca181",
                        "name": "span with invalid timestamps",
                        "startTimeUnixNano": int(start_yesterday.timestamp() * 1e9),
                        "endTimeUnixNano": int(end_yesterday.timestamp() * 1e9),
                    },
                ).encode()
            ),
        )
    )
    envelope.add_item(
        Item(
            type="otel_span",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "traceId": "89143b0763095bd9c9955e8175d1fb23",
                        "spanId": "a342abb1214ca181",
                        "name": "span with valid timestamps",
                        "startTimeUnixNano": int(start_today.timestamp() * 1e9),
                        "endTimeUnixNano": int(end_today.timestamp() * 1e9),
                    },
                ).encode()
            ),
        )
    )
    relay.send_envelope(project_id, envelope)

    spans = spans_consumer.get_spans(timeout=10.0, n=1)
    assert len(spans) == 1
    assert spans[0]["description"] == "span with valid timestamps"


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
        "projects:span-metrics-extraction",
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
                            "user": "admin@sentry.io",
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

    assert spans == [
        {
            "data": {
                "browser.name": "Python Requests",
                "user_agent.original": "python-requests/2.32.2",
            },
            "duration_ms": 1500,
            "exclusive_time_ms": 345.0,
            "is_segment": False,
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "sentry_tags": {
                "browser.name": "Python Requests",
                "op": "ui.interaction.click",
            },
            "span_id": "bd429c44b67a3eb1",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
            "start_timestamp_precise": start.timestamp(),
            "end_timestamp_precise": end.timestamp() + 1,
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
            "measurements": {
                "score.fcp": {"value": 0.14999972769539766},
                "score.fid": {"value": 0.14999999985},
                "score.lcp": {"value": 0.29986141375718806},
                "score.total": {"value": 0.5998611413025857},
                "score.ttfb": {"value": 0.0},
                "score.weight.cls": {"value": 0.25},
                "score.weight.fcp": {"value": 0.15},
                "score.weight.fid": {"value": 0.3},
                "score.weight.lcp": {"value": 0.3},
                "score.weight.ttfb": {"value": 0.0},
                "cls": {"value": 100.0},
                "fcp": {"value": 200.0},
                "fid": {"value": 300.0},
                "lcp": {"value": 400.0},
                "ttfb": {"value": 500.0},
                "score.cls": {"value": 0.0},
            },
        },
        {
            "data": {
                "browser.name": "Python Requests",
                "sentry.replay.id": "8477286c8e5148b386b71ade38374d58",
                "sentry.segment.name": "/page/with/click/interaction/*/*",
                "user": "admin@sentry.io",
                "user_agent.original": "python-requests/2.32.2",
            },
            "duration_ms": 1500,
            "exclusive_time_ms": 345.0,
            "is_segment": False,
            "profile_id": "3d9428087fda4ba0936788b70a7587d0",
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "sentry_tags": {
                "browser.name": "Python Requests",
                "op": "ui.interaction.click",
                "transaction": "/page/with/click/interaction/*/*",
                "replay_id": "8477286c8e5148b386b71ade38374d58",
                "user": "admin@sentry.io",
            },
            "span_id": "cd429c44b67a3eb1",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
            "start_timestamp_precise": start.timestamp(),
            "end_timestamp_precise": end.timestamp() + 1,
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
            "measurements": {
                "inp": {"value": 100.0},
                "score.inp": {"value": 0.9948129113413748},
                "score.total": {"value": 0.9948129113413748},
                "score.weight.inp": {"value": 1.0},
            },
        },
    ]


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
        "projects:span-metrics-extraction",
        "organizations:standalone-span-ingestion",
    ]
    project_config["config"]["quotas"] = [
        {
            "categories": ["span_indexed"],
            "limit": 4,
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
    spans = spans_consumer.get_spans(n=4, timeout=10)
    assert len(spans) == 4
    assert summarize_outcomes() == {(16, 0): 4}  # SpanIndexed, Accepted

    # Second batch is limited
    relay.send_envelope(project_id, envelope)
    assert summarize_outcomes() == {(16, 2): 4}  # SpanIndexed, RateLimited

    spans_consumer.assert_empty()
    outcomes_consumer.assert_empty()


@pytest.mark.parametrize("category", ["span", "span_indexed"])
def test_rate_limit_consistent_extracted(
    category,
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
    project_config["config"]["features"] = [
        "projects:span-metrics-extraction",
        "organizations:indexed-spans-extraction",
    ]
    project_config["config"]["quotas"] = [
        {
            "categories": [category],
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
            "parent_span_id": "aaaaaaaaaaaaaaaa",
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
    metrics = metrics_consumer.get_metrics(n=10)
    span_count = sum(
        [m[0]["value"] for m in metrics if m[0]["name"] == "c:spans/usage@none"]
    )
    assert span_count == 2

    # Second send should be rejected immediately.
    relay.send_event(project_id, event)
    outcomes = summarize_outcomes()

    expected_outcomes = {
        (16, 2): 2,  # SpanIndexed, RateLimited
    }
    metrics = metrics_consumer.get_metrics(timeout=1)
    if category == "span":
        (expected_outcomes.update({(12, 2): 2}),)  # Span, RateLimited
        assert len(metrics) == 4
        assert all(m[0]["name"][2:14] == "transactions" for m in metrics), metrics
    else:
        span_count = sum(
            [m[0]["value"] for m in metrics if m[0]["name"] == "c:spans/usage@none"]
        )
        assert span_count == 2

    assert outcomes == expected_outcomes

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
        "projects:span-metrics-extraction",
        "organizations:standalone-span-ingestion",
    ]
    project_config["config"]["quotas"] = [
        {
            "categories": ["span"],
            "limit": 3,
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

    assert summarize_outcomes() == {(12, 2): 4, (16, 2): 4}

    # We emit transaction metrics from spans for legacy reasons. These are not rate limited.
    # (could be a bug)
    ((metric, _),) = metrics_consumer.get_metrics(n=1)
    assert ":spans/" not in metric["name"]

    spans_consumer.assert_empty()
    metrics_consumer.assert_empty()


@pytest.mark.parametrize("category", ["transaction", "transaction_indexed"])
def test_rate_limit_is_consistent_between_transaction_and_spans(
    mini_sentry,
    relay_with_processing,
    transactions_consumer,
    spans_consumer,
    metrics_consumer,
    outcomes_consumer,
    category,
):
    """
    Rate limits are consistent between transactions and nested spans.
    """
    relay = relay_with_processing(options=TEST_CONFIG)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "projects:span-metrics-extraction",
        "organizations:standalone-span-ingestion",
        "organizations:indexed-spans-extraction",
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


@pytest.mark.parametrize(
    "tags, expected_tags",
    [
        (
            {
                "some": "tag",
                "other": "value",
            },
            {
                "some": "tag",
                "other": "value",
            },
        ),
        (
            {
                "some": 1,
                "other": True,
            },
            {
                "some": "1",
                "other": "True",
            },
        ),
    ],
)
def test_span_extraction_with_tags(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    tags,
    expected_tags,
):
    spans_consumer = spans_consumer()

    relay = relay_with_processing()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "projects:span-metrics-extraction",
        "organizations:indexed-spans-extraction",
    ]

    event = make_transaction(
        {
            "event_id": "e022a2da91e9495d944c291fe065972d",
            "tags": tags,
        }
    )

    relay.send_event(project_id, event)

    transaction_span = spans_consumer.get_span()

    assert transaction_span["tags"] == expected_tags

    spans_consumer.assert_empty()


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
        spans = spans_consumer.get_spans(timeout=10, n=4)
        assert len(spans) == 4
        outcomes = outcomes_consumer.get_outcomes(timeout=10, n=4)
        assert summarize_outcomes(outcomes) == {(16, 0): 4}  # SpanIndexed, Accepted
    else:
        outcomes = outcomes_consumer.get_outcomes(timeout=10, n=1)
        assert summarize_outcomes(outcomes) == {(16, 1): 4}  # Span, Filtered
        assert {o["reason"] for o in outcomes} == {"Sampled:3000"}

    spans_consumer.assert_empty()
    outcomes_consumer.assert_empty()


def test_metrics_summary_with_extracted_spans(
    mini_sentry,
    relay_with_processing,
    metrics_summaries_consumer,
):
    metrics_summaries_consumer = metrics_summaries_consumer()

    relay = relay_with_processing()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:custom-metrics",
        "projects:span-metrics-extraction",
        "organizations:indexed-spans-extraction",
    ]
    project_config["config"]["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION,
    }
    project_config["config"]["metricExtraction"] = {
        "version": METRICS_EXTRACTION_MIN_SUPPORTED_VERSION,
        "metrics": [
            {
                "category": "span",
                "mri": "d:custom/my_metric@millisecond",
                "field": "span.duration",
            }
        ],
    }

    event = make_transaction({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
    end = datetime.now(timezone.utc) - timedelta(seconds=1)
    duration = timedelta(milliseconds=500)
    start = end - duration
    event["spans"] = [
        {
            "description": "GET /api/0/organizations/?member=1",
            "op": "http",
            "origin": "manual",
            "parent_span_id": "aaaaaaaaaaaaaaaa",
            "span_id": "bbbbbbbbbbbbbbbb",
            "start_timestamp": start.isoformat(),
            "status": "success",
            "timestamp": end.isoformat(),
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
    ]

    mri = "c:spans/some_metric@none"
    metrics_summary = {
        mri: [
            {
                "min": 1.0,
                "max": 2.0,
                "sum": 3.0,
                "count": 4,
                "tags": {
                    "environment": "test",
                },
            },
        ],
    }
    event["_metrics_summary"] = metrics_summary

    relay.send_event(project_id, event)

    metrics_summaries = metrics_summaries_consumer.get_metrics_summaries(
        timeout=10.0, n=3
    )
    expected_mris = ["c:spans/some_metric@none", "d:custom/my_metric@millisecond"]
    for metric_summary in metrics_summaries:
        assert metric_summary["mri"] in expected_mris


def test_metrics_summary_with_standalone_spans(
    mini_sentry,
    relay_with_processing,
    metrics_summaries_consumer,
):
    metrics_summaries_consumer = metrics_summaries_consumer()

    relay = relay_with_processing()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "projects:span-metrics-extraction",
        "organizations:standalone-span-ingestion",
    ]
    project_config["config"]["metricExtraction"] = {
        "version": METRICS_EXTRACTION_MIN_SUPPORTED_VERSION,
        "metrics": [
            {
                "category": "span",
                "mri": "d:custom/my_metric@millisecond",
                "field": "span.duration",
            }
        ],
    }

    duration = timedelta(milliseconds=500)
    now = datetime.now(timezone.utc)
    end = now - timedelta(seconds=1)
    start = end - duration

    envelope = envelope_with_spans(start, end)
    relay.send_envelope(project_id, envelope)

    metrics_summaries = metrics_summaries_consumer.get_metrics_summaries(
        timeout=10.0, n=4
    )
    expected_mris = ["c:spans/some_metric@none", "d:custom/my_metric@millisecond"]
    for metric_summary in metrics_summaries:
        assert metric_summary["mri"] in expected_mris
