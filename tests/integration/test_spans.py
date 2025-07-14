import contextlib
import json
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta, timezone

import pytest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.trace.v1.trace_pb2 import (
    ResourceSpans,
    ScopeSpans,
    Span,
    TracesData,
)
from requests import HTTPError
from sentry_relay.consts import DataCategory
from sentry_sdk.envelope import Envelope, Item, PayloadRef

from .asserts import time_after, time_within_delta
from .consts import (
    TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION,
)
from .test_metrics import TEST_CONFIG
from .test_store import make_transaction


@pytest.mark.parametrize("performance_issues_spans", [False, True])
@pytest.mark.parametrize("discard_transaction", [False, True])
def test_span_extraction(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    transactions_consumer,
    events_consumer,
    metrics_consumer,
    discard_transaction,
    performance_issues_spans,
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
        assert received_event.get("_performance_issues_spans") == (
            performance_issues_spans or None
        )
        assert {headers[0] for _, headers in metrics_consumer.get_metrics()} == {
            ("namespace", b"spans"),
            ("namespace", b"transactions"),
        }

    child_span = spans_consumer.get_span()
    del child_span["received"]
    assert child_span == {
        "data": {  # Backfilled from `sentry_tags`
            "sentry.category": "http",
            "sentry.normalized_description": "GET *",
            "sentry.group": "37e3d9fab1ae9162",
            "sentry.op": "http",
            "sentry.platform": "other",
            "sentry.sdk.name": "raven-node",
            "sentry.sdk.version": "2.6.3",
            "sentry.status": "ok",
            "sentry.trace.status": "ok",
            "sentry.transaction": "hi",
            "sentry.transaction.op": "hi",
        },
        "description": "GET /api/0/organizations/?member=1",
        "duration_ms": int(duration.total_seconds() * 1e3),
        "event_id": "cbf6960622e14a45abc1f03b2055b186",
        "exclusive_time_ms": 500.0,
        "is_segment": False,
        "is_remote": False,
        "links": [
            {
                "trace_id": "0f62a8b040f340bda5d830223def1d82",
                "span_id": "cbbbbbbbbbbbbbbc",
                "sampled": True,
                "attributes": {"span_key": "span_value"},
            },
        ],
        "organization_id": 1,
        "origin": "manual",
        "parent_span_id": "968cff94913ebb07",
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
    if performance_issues_spans:
        assert transaction_span.pop("_performance_issues_spans") is True
    assert transaction_span == {
        "data": {
            "sentry.sdk.name": "raven-node",
            "sentry.sdk.version": "2.6.3",
            "sentry.segment.name": "hi",
            # Backfilled from `sentry_tags`:
            "sentry.op": "hi",
            "sentry.platform": "other",
            "sentry.status": "ok",
            "sentry.trace.status": "ok",
            "sentry.transaction": "hi",
            "sentry.transaction.op": "hi",
        },
        "description": "hi",
        "duration_ms": duration_ms,
        "event_id": "cbf6960622e14a45abc1f03b2055b186",
        "exclusive_time_ms": 1500.0,
        "is_segment": True,
        "is_remote": True,
        "links": [
            {
                "trace_id": "1f62a8b040f340bda5d830223def1d83",
                "span_id": "dbbbbbbbbbbbbbbd",
                "sampled": True,
                "attributes": {"txn_key": 123},
            },
        ],
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
        (None, 2, 7),
        (1.0, 2, 7),
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
            "parent_span_id": "968cff94913ebb07",
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
    for _ in range(3):  # 2 client reports and the actual item we're interested in
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
                        "startTimeUnixNano": str(int(start.timestamp() * 1e9)),
                        "endTimeUnixNano": str(int(end.timestamp() * 1e9)),
                        "attributes": [
                            {
                                "key": "sentry.category",
                                "value": {
                                    "stringValue": "db",
                                },
                            },
                            {
                                "key": "sentry.exclusive_time_nano",
                                "value": {
                                    "intValue": str(
                                        int((end - start).total_seconds() * 1e9)
                                    ),
                                },
                            },
                        ],
                        "links": [
                            {
                                "traceId": "89143b0763095bd9c9955e8175d1fb24",
                                "spanId": "e342abb1214ca183",
                                "attributes": [
                                    {
                                        "key": "link_double_key",
                                        "value": {
                                            "doubleValue": 1.23,
                                        },
                                    },
                                ],
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

    envelope.add_item(
        Item(
            type="span",
            headers={"metrics_extracted": metrics_extracted, "item_count": 2},
            content_type="application/vnd.sentry.items.span.v2+json",
            payload=PayloadRef(
                json={
                    "items": [
                        {
                            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
                            "span_id": "a342abb1214ca182",
                            "name": "my 1st V2 span",
                            "start_timestamp": start.timestamp(),
                            "end_timestamp": end.timestamp(),
                            "attributes": {
                                "sentry.category": {
                                    "type": "string",
                                    "value": "db",
                                },
                                "sentry.exclusive_time_nano": {
                                    "type": "integer",
                                    "value": int((end - start).total_seconds() * 1e9),
                                },
                            },
                            "links": [
                                {
                                    "trace_id": "89143b0763095bd9c9955e8175d1fb24",
                                    "span_id": "e342abb1214ca183",
                                    "sampled": False,
                                    "attributes": {
                                        "link_double_key": {
                                            "type": "double",
                                            "value": 1.23,
                                        },
                                    },
                                },
                            ],
                        },
                        {
                            "trace_id": "ff62a8b040f340bda5d830223def1d81",
                            "span_id": "b0429c44b67a3eb2",
                            "name": "resource.script",
                            "status": "ok",
                            "start_timestamp": start.timestamp(),
                            "end_timestamp": end.timestamp() + 1,
                            "links": [
                                {
                                    "trace_id": "99143b0763095bd9c9955e8175d1fb25",
                                    "span_id": "e342abb1214ca183",
                                    "sampled": True,
                                    "attributes": {
                                        "link_bool_key": {
                                            "type": "boolean",
                                            "value": True,
                                        },
                                    },
                                },
                            ],
                            "attributes": {
                                "browser.name": {"type": "string", "value": "Chrome"},
                                "sentry.description": {
                                    "type": "string",
                                    "value": "https://example.com/p/blah.js",
                                },
                                "sentry.op": {
                                    "type": "string",
                                    "value": "resource.script",
                                },
                                "sentry.exclusive_time_nano": {
                                    "type": "integer",
                                    "value": 161 * 1e6,
                                },
                                # Span with the same `span_id` and `segment_id`, to make sure it is classified as `is_segment`.
                                "sentry.segment.id": {
                                    "type": "string",
                                    "value": "b0429c44b67a3eb2",
                                },
                            },
                        },
                    ]
                }
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
                                        "key": "sentry.exclusive_time_nano",
                                        "value": {
                                            "intValue": str(
                                                int((end - start).total_seconds() * 1e9)
                                            ),
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


def test_span_ingestion(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    metrics_consumer,
):
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()

    relay = relay_with_processing(
        options={
            "aggregator": {
                "bucket_interval": 1,
                "initial_delay": 0,
                "max_secs_in_past": 2**64 - 1,
                "shift_key": "none",
            }
        }
    )
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-metrics-extraction",
        "projects:relay-otel-endpoint",
    ]
    project_config["config"]["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION
    }

    duration = timedelta(milliseconds=500)
    now = datetime.now(timezone.utc)
    end = now - timedelta(seconds=1)
    start = end - duration

    # 1 - Send OTel span and sentry span via envelope
    envelope = envelope_with_spans(start, end)
    relay.send_envelope(
        project_id,
        envelope,
        headers={  # Set browser header to verify that `d:transactions/measurements.score.total@ratio` is extracted only once.
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
        },
    )

    # 2 - Send OTel json span via endpoint
    relay.send_otel_span(
        project_id,
        json=make_otel_span(start, end),
    )

    protobuf_span = Span(
        trace_id=bytes.fromhex("89143b0763095bd9c9955e8175d1fb24"),
        span_id=bytes.fromhex("f0b809703e783d00"),
        parent_span_id=bytes.fromhex("f0f0f0abcdef1234"),
        name="my 3rd protobuf OTel span",
        start_time_unix_nano=int(start.timestamp() * 1e9),
        end_time_unix_nano=int(end.timestamp() * 1e9),
        kind=5,
        attributes=[
            KeyValue(
                key="sentry.exclusive_time_nano",
                value=AnyValue(int_value=int(duration.total_seconds() * 1e9)),
            ),
            # In order to test `category` sentry tag inference.
            KeyValue(
                key="ui.component_name",
                value=AnyValue(string_value="MyComponent"),
            ),
        ],
        links=[
            Span.Link(
                trace_id=bytes.fromhex("89143b0763095bd9c9955e8175d1fb24"),
                span_id=bytes.fromhex("e0b809703e783d01"),
                attributes=[
                    KeyValue(
                        key="link_str_key",
                        value=AnyValue(string_value="link_str_value"),
                    )
                ],
            )
        ],
    )
    scope_spans = ScopeSpans(spans=[protobuf_span])
    resource_spans = ResourceSpans(scope_spans=[scope_spans])
    traces_data = TracesData(resource_spans=[resource_spans])
    protobuf_payload = traces_data.SerializeToString()

    # 3 - Send OTel protobuf span via endpoint
    relay.send_otel_span(
        project_id,
        bytes=protobuf_payload,
        headers={"Content-Type": "application/x-protobuf"},
    )

    spans = spans_consumer.get_spans(timeout=10.0, n=8)

    for span in spans:
        span.pop("received", None)

    # endpoint might overtake envelope
    spans.sort(key=lambda msg: msg["span_id"])

    assert spans == [
        {
            "data": {
                "browser.name": "Chrome",
                "client.address": "127.0.0.1",
                "sentry.category": "db",
                "sentry.name": "my 1st OTel span",
                "user_agent.original": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/111.0.0.0 Safari/537.36",
                # Backfilled from `sentry_tags`:
                "sentry.op": "default",
                "sentry.browser.name": "Chrome",
                "sentry.status": "unknown",
            },
            "description": "my 1st OTel span",
            "duration_ms": 500,
            "exclusive_time_ms": 500.0,
            "is_segment": True,
            "is_remote": False,
            "links": [
                {
                    "trace_id": "89143b0763095bd9c9955e8175d1fb24",
                    "span_id": "e342abb1214ca183",
                    "sampled": False,
                    "attributes": {"link_double_key": 1.23},
                }
            ],
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "segment_id": "a342abb1214ca181",
            "sentry_tags": {
                "browser.name": "Chrome",
                "category": "db",
                "op": "default",
                "status": "unknown",
            },
            "span_id": "a342abb1214ca181",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
            "start_timestamp_precise": start.timestamp(),
            "end_timestamp_precise": end.timestamp(),
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
        },
        {
            "data": {
                "browser.name": "Chrome",
                "client.address": "127.0.0.1",
                "sentry.category": "db",
                "sentry.name": "my 1st V2 span",
                "user_agent.original": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/111.0.0.0 Safari/537.36",
                # Backfilled from `sentry_tags`:
                "sentry.browser.name": "Chrome",
                "sentry.op": "default",
                "sentry.status": "unknown",
            },
            "description": "my 1st V2 span",
            "duration_ms": 500,
            "exclusive_time_ms": 500.0,
            "is_segment": True,
            "is_remote": False,
            "links": [
                {
                    "trace_id": "89143b0763095bd9c9955e8175d1fb24",
                    "span_id": "e342abb1214ca183",
                    "sampled": False,
                    "attributes": {"link_double_key": 1.23},
                }
            ],
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "segment_id": "a342abb1214ca182",
            "sentry_tags": {
                "browser.name": "Chrome",
                "category": "db",
                "op": "default",
                "status": "unknown",
            },
            "span_id": "a342abb1214ca182",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
            "start_timestamp_precise": start.timestamp(),
            "end_timestamp_precise": end.timestamp(),
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
        },
        {
            "data": {
                "browser.name": "Chrome",
                "client.address": "127.0.0.1",
                "user_agent.original": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/111.0.0.0 Safari/537.36",
                # Backfilled from `sentry_tags`:
                "sentry.browser.name": "Chrome",
                "sentry.category": "resource",
                "sentry.normalized_description": "https://example.com/*/blah.js",
                "sentry.domain": "example.com",
                "sentry.file_extension": "js",
                "sentry.group": "8a97a9e43588e2bd",
                "sentry.op": "resource.script",
                # Backfilled from `measurements`:
                "score.total": 0.12121616,
            },
            "description": "https://example.com/p/blah.js",
            "duration_ms": 1500,
            "exclusive_time_ms": 345.0,
            "is_segment": True,
            "is_remote": False,
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
            "measurements": {"score.total": {"value": 0.12121616}},
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "segment_id": "b0429c44b67a3eb1",
            "sentry_tags": {
                "browser.name": "Chrome",
                "category": "resource",
                "description": "https://example.com/*/blah.js",
                "domain": "example.com",
                "file_extension": "js",
                "group": "8a97a9e43588e2bd",
                "op": "resource.script",
            },
            "span_id": "b0429c44b67a3eb1",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
            "start_timestamp_precise": start.timestamp(),
            "end_timestamp_precise": end.timestamp() + 1,
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
        {
            "data": {
                "browser.name": "Chrome",
                "client.address": "127.0.0.1",
                "sentry.name": "resource.script",
                "user_agent.original": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/111.0.0.0 Safari/537.36",
                # Backfilled from `sentry_tags`:
                "sentry.browser.name": "Chrome",
                "sentry.category": "resource",
                "sentry.normalized_description": "https://example.com/*/blah.js",
                "sentry.domain": "example.com",
                "sentry.file_extension": "js",
                "sentry.group": "8a97a9e43588e2bd",
                "sentry.op": "resource.script",
                "sentry.status": "ok",
            },
            "description": "https://example.com/p/blah.js",
            "duration_ms": 1500,
            "exclusive_time_ms": 161.0,
            "is_segment": True,
            "is_remote": False,
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
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "segment_id": "b0429c44b67a3eb2",
            "sentry_tags": {
                "browser.name": "Chrome",
                "category": "resource",
                "description": "https://example.com/*/blah.js",
                "domain": "example.com",
                "file_extension": "js",
                "group": "8a97a9e43588e2bd",
                "op": "resource.script",
                "status": "ok",
            },
            "span_id": "b0429c44b67a3eb2",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
            "start_timestamp_precise": start.timestamp(),
            "end_timestamp_precise": end.timestamp() + 1,
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
        {
            "data": {
                "browser.name": "Chrome",
                "client.address": "127.0.0.1",
                "user_agent.original": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/111.0.0.0 Safari/537.36",
                # Backfilled from `sentry_tags`:
                "sentry.browser.name": "Chrome",
                "sentry.op": "default",
            },
            "description": r"test \" with \" escaped \" chars",
            "duration_ms": 1500,
            "exclusive_time_ms": 345.0,
            "is_segment": False,
            "is_remote": False,
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "segment_id": "968cff94913ebb07",
            "sentry_tags": {"browser.name": "Chrome", "op": "default"},
            "span_id": "cd429c44b67a3eb1",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
            "start_timestamp_precise": start.timestamp(),
            "end_timestamp_precise": end.timestamp() + 1,
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
        {
            "data": {
                "browser.name": "Python Requests",
                "client.address": "127.0.0.1",
                "sentry.name": "my 2nd OTel span",
                "user_agent.original": "python-requests/2.32.2",
                # Backfilled from `sentry_tags`:
                "sentry.browser.name": "Python Requests",
                "sentry.op": "default",
                "sentry.status": "unknown",
            },
            "description": "my 2nd OTel span",
            "duration_ms": 500,
            "exclusive_time_ms": 500.0,
            "is_segment": True,
            "is_remote": False,
            "kind": "producer",
            "links": [
                {
                    "trace_id": "89143b0763095bd9c9955e8175d1fb24",
                    "span_id": "e342abb1214ca183",
                    "sampled": False,
                    "attributes": {
                        "link_int_key": 123,
                    },
                },
            ],
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "segment_id": "d342abb1214ca182",
            "sentry_tags": {
                "browser.name": "Python Requests",
                "op": "default",
                "status": "unknown",
            },
            "span_id": "d342abb1214ca182",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
            "start_timestamp_precise": start.timestamp(),
            "end_timestamp_precise": end.timestamp(),
            "trace_id": "89143b0763095bd9c9955e8175d1fb24",
        },
        {
            "data": {
                "browser.name": "Chrome",
                "client.address": "127.0.0.1",
                "user_agent.original": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/111.0.0.0 Safari/537.36",
                # Backfilled from `sentry_tags`:
                "sentry.browser.name": "Chrome",
                "sentry.op": "default",
            },
            "duration_ms": 1500,
            "exclusive_time_ms": 345.0,
            "is_segment": False,
            "is_remote": False,
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "segment_id": "968cff94913ebb07",
            "sentry_tags": {
                "browser.name": "Chrome",
                "op": "default",
            },
            "span_id": "ed429c44b67a3eb1",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
            "start_timestamp_precise": start.timestamp(),
            "end_timestamp_precise": end.timestamp() + 1,
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
        {
            "data": {
                "browser.name": "Python Requests",
                "client.address": "127.0.0.1",
                "sentry.name": "my 3rd protobuf OTel span",
                "ui.component_name": "MyComponent",
                "user_agent.original": "python-requests/2.32.2",
                # Backfilled from `sentry_tags`:
                "sentry.browser.name": "Python Requests",
                "sentry.op": "default",
                "sentry.category": "ui",
                "sentry.status": "unknown",
            },
            "description": "my 3rd protobuf OTel span",
            "duration_ms": 500,
            "exclusive_time_ms": 500.0,
            "is_segment": False,
            "is_remote": False,
            "kind": "consumer",
            "links": [
                {
                    "trace_id": "89143b0763095bd9c9955e8175d1fb24",
                    "span_id": "e0b809703e783d01",
                    "sampled": False,
                    "attributes": {
                        "link_str_key": "link_str_value",
                    },
                },
            ],
            "organization_id": 1,
            "parent_span_id": "f0f0f0abcdef1234",
            "project_id": 42,
            "retention_days": 90,
            "sentry_tags": {
                "browser.name": "Python Requests",
                "op": "default",
                "category": "ui",
                "status": "unknown",
            },
            "span_id": "f0b809703e783d00",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
            "start_timestamp_precise": start.timestamp(),
            "end_timestamp_precise": end.timestamp(),
            "trace_id": "89143b0763095bd9c9955e8175d1fb24",
        },
    ]

    spans_consumer.assert_empty()

    metrics = [metric for (metric, _headers) in metrics_consumer.get_metrics()]
    metrics_consumer.assert_empty()
    metrics.sort(key=lambda m: (m["name"], sorted(m["tags"].items()), m["timestamp"]))
    for metric in metrics:
        try:
            metric["value"].sort()
        except AttributeError:
            pass

    now_timestamp = int(now.timestamp())
    expected_timestamp = int(end.timestamp())
    expected_span_metrics = [
        {
            "name": "c:spans/count_per_root_project@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_after(now_timestamp),
            "retention_days": 90,
            "tags": {"decision": "keep", "target_project_id": "42"},
            "timestamp": expected_timestamp,
            "type": "c",
            "value": 4.0,
        },
        {
            "name": "c:spans/count_per_root_project@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_after(now_timestamp),
            "retention_days": 90,
            "tags": {"decision": "keep", "target_project_id": "42"},
            "timestamp": expected_timestamp + 1,
            "type": "c",
            "value": 4.0,
        },
        {
            "name": "c:spans/usage@none",
            "org_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "tags": {},
            "timestamp": expected_timestamp,
            "type": "c",
            "value": 4.0,
            "received_at": time_after(now_timestamp),
        },
        {
            "name": "c:spans/usage@none",
            "org_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "tags": {},
            "timestamp": expected_timestamp + 1,
            "type": "c",
            "value": 4.0,
            "received_at": time_after(now_timestamp),
        },
        {
            "name": "d:spans/duration@millisecond",
            "org_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "tags": {
                "file_extension": "js",
                "span.category": "resource",
                "span.description": "https://example.com/*/blah.js",
                "span.domain": "example.com",
                "span.group": "8a97a9e43588e2bd",
                "span.op": "resource.script",
            },
            "timestamp": expected_timestamp + 1,
            "type": "d",
            "value": [1500.0, 1500.0],
            "received_at": time_after(now_timestamp),
        },
        {
            "name": "d:spans/duration@millisecond",
            "org_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "tags": {
                "span.category": "db",
                "span.op": "default",
            },
            "timestamp": expected_timestamp,
            "type": "d",
            "value": [500.0, 500.0],
            "received_at": time_after(now_timestamp),
        },
        {
            "name": "d:spans/duration@millisecond",
            "org_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "tags": {
                "span.op": "default",
            },
            "timestamp": expected_timestamp,
            "type": "d",
            "value": [500.0, 500.0],
            "received_at": time_after(now_timestamp),
        },
        {
            "name": "d:spans/duration@millisecond",
            "org_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "tags": {"span.op": "default"},
            "timestamp": expected_timestamp + 1,
            "type": "d",
            "value": [1500.0, 1500.0],
            "received_at": time_after(now_timestamp),
        },
        {
            "name": "d:spans/duration_light@millisecond",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_after(now_timestamp),
            "retention_days": 90,
            "tags": {
                "file_extension": "js",
                "span.category": "resource",
                "span.description": "https://example.com/*/blah.js",
                "span.domain": "example.com",
                "span.group": "8a97a9e43588e2bd",
                "span.op": "resource.script",
            },
            "timestamp": expected_timestamp + 1,
            "type": "d",
            "value": [1500.0, 1500.0],
        },
        {
            "name": "d:spans/duration_light@millisecond",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_after(now_timestamp),
            "retention_days": 90,
            "tags": {"span.category": "db", "span.op": "default"},
            "timestamp": expected_timestamp,
            "type": "d",
            "value": [500.0, 500.0],
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "d:spans/exclusive_time@millisecond",
            "type": "d",
            "value": [161.0, 345.0],
            "timestamp": expected_timestamp + 1,
            "tags": {
                "file_extension": "js",
                "span.category": "resource",
                "span.description": "https://example.com/*/blah.js",
                "span.domain": "example.com",
                "span.group": "8a97a9e43588e2bd",
                "span.op": "resource.script",
            },
            "retention_days": 90,
            "received_at": time_after(now_timestamp),
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "d:spans/exclusive_time@millisecond",
            "retention_days": 90,
            "tags": {"span.category": "db", "span.op": "default"},
            "timestamp": expected_timestamp,
            "type": "d",
            "value": [500.0, 500.0],
            "received_at": time_after(now_timestamp),
        },
        {
            "name": "d:spans/exclusive_time@millisecond",
            "org_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "tags": {"span.op": "default"},
            "timestamp": expected_timestamp,
            "type": "d",
            "value": [500.0, 500.0],
            "received_at": time_after(now_timestamp),
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "d:spans/exclusive_time@millisecond",
            "retention_days": 90,
            "tags": {"span.op": "default"},
            "timestamp": expected_timestamp + 1,
            "type": "d",
            "value": [345.0, 345.0],
            "received_at": time_after(now_timestamp),
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "d:spans/exclusive_time_light@millisecond",
            "type": "d",
            "value": [161.0, 345.0],
            "timestamp": expected_timestamp + 1,
            "tags": {
                "file_extension": "js",
                "span.category": "resource",
                "span.description": "https://example.com/*/blah.js",
                "span.domain": "example.com",
                "span.group": "8a97a9e43588e2bd",
                "span.op": "resource.script",
            },
            "retention_days": 90,
            "received_at": time_after(now_timestamp),
        },
        {
            "name": "d:spans/exclusive_time_light@millisecond",
            "org_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "tags": {"span.category": "db", "span.op": "default"},
            "timestamp": expected_timestamp,
            "type": "d",
            "value": [500.0, 500.0],
            "received_at": time_after(now_timestamp),
        },
        {
            "name": "d:spans/webvital.score.total@ratio",
            "org_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "tags": {"span.op": "resource.script"},
            "timestamp": expected_timestamp + 1,
            "type": "d",
            "value": [0.12121616],
            "received_at": time_after(now_timestamp),
        },
    ]

    span_metrics = [m for m in metrics if ":spans/" in m["name"]]

    assert len(span_metrics) == len(expected_span_metrics)
    for actual, expected in zip(span_metrics, expected_span_metrics):
        assert actual == expected

    # Regardless of whether transactions are extracted, score.total is only converted to a transaction metric once:
    score_total_metrics = [
        m
        for m in metrics
        if m["name"] == "d:transactions/measurements.score.total@ratio"
    ]
    assert len(score_total_metrics) == 1, score_total_metrics
    assert len(score_total_metrics[0]["value"]) == 1

    metrics_consumer.assert_empty()


def test_standalone_span_ingestion_metric_extraction(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    metrics_consumer,
):
    relay = relay_with_processing(
        options={
            "aggregator": {
                "bucket_interval": 1,
                "initial_delay": 0,
                "max_secs_in_past": 2**64 - 1,
                "shift_key": "none",
            }
        }
    )
    metrics_consumer = metrics_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-metrics-extraction",
        # "projects:relay-otel-endpoint",
    ]

    duration = timedelta(milliseconds=500)
    now = datetime.now(timezone.utc)
    end = now - timedelta(seconds=1)
    start = end - duration

    envelope = Envelope()

    envelope.add_item(
        Item(
            type="span",
            headers={"metrics_extracted": True, "item_count": 1},
            content_type="application/vnd.sentry.items.span.v2+json",
            payload=PayloadRef(
                json={
                    "items": [
                        {
                            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
                            "span_id": "a342abb1214ca182",
                            "name": "SELECT from users",
                            "start_timestamp": start.timestamp(),
                            "end_timestamp": end.timestamp(),
                            "attributes": {
                                "db.system": {
                                    "type": "string",
                                    "value": "mysql",
                                },
                            },
                        },
                    ]
                }
            ),
        )
    )

    relay.send_envelope(
        project_id,
        envelope,
    )

    metrics = [metric for (metric, _headers) in metrics_consumer.get_metrics()]

    metrics.sort(key=lambda m: (m["name"], sorted(m["tags"].items()), m["timestamp"]))

    for metric in metrics:
        try:
            metric["value"].sort()
        except AttributeError:
            pass

    expected_timestamp = int(end.timestamp())
    expected_received = time_after(int(now.timestamp()))

    expected_metrics = [
        {
            "name": "c:spans/count_per_root_project@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": expected_received,
            "retention_days": 90,
            "tags": {"decision": "keep", "target_project_id": "42"},
            "timestamp": expected_timestamp,
            "type": "c",
            "value": 1.0,
        },
        {
            "name": "c:spans/usage@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": expected_received,
            "retention_days": 90,
            "tags": {},
            "timestamp": expected_timestamp,
            "type": "c",
            "value": 1.0,
        },
        {
            "name": "d:spans/duration@millisecond",
            "org_id": 1,
            "project_id": 42,
            "received_at": expected_received,
            "retention_days": 90,
            "tags": {
                "span.action": "SELECT",
                "span.category": "db",
                "span.description": "SELECT from",
                "span.group": "e7ef86adbb98803e",
                "span.op": "db",
            },
            "timestamp": expected_timestamp,
            "type": "d",
            "value": [500.0],
        },
        {
            "name": "d:spans/duration_light@millisecond",
            "org_id": 1,
            "project_id": 42,
            "received_at": expected_received,
            "retention_days": 90,
            "tags": {
                "span.action": "SELECT",
                "span.op": "db",
                "span.category": "db",
                "span.description": "SELECT from",
                "span.group": "e7ef86adbb98803e",
            },
            "timestamp": expected_timestamp,
            "type": "d",
            "value": [500.0],
        },
        {
            "name": "d:spans/exclusive_time@millisecond",
            "org_id": 1,
            "project_id": 42,
            "received_at": expected_received,
            "retention_days": 90,
            "tags": {
                "span.action": "SELECT",
                "span.category": "db",
                "span.description": "SELECT from",
                "span.group": "e7ef86adbb98803e",
                "span.op": "db",
            },
            "timestamp": expected_timestamp,
            "type": "d",
            "value": [500.0],
        },
        {
            "name": "d:spans/exclusive_time_light@millisecond",
            "org_id": 1,
            "project_id": 42,
            "received_at": expected_received,
            "retention_days": 90,
            "tags": {
                "span.action": "SELECT",
                "span.op": "db",
                "span.category": "db",
                "span.description": "SELECT from",
                "span.group": "e7ef86adbb98803e",
            },
            "timestamp": expected_timestamp,
            "type": "d",
            "value": [500.0],
        },
    ]

    assert metrics == expected_metrics

    metrics_consumer.assert_empty()


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

    outcomes = []
    for _ in range(2):
        outcomes.extend(mini_sentry.captured_outcomes.get(timeout=3).get("outcomes"))
    outcomes.sort(key=lambda x: x["category"])

    assert outcomes == [
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
        "detail": "event submission rejected with_reason: FeatureDisabled(OtelEndpoint)"
    }

    # No envelopes were received:
    assert mini_sentry.captured_events.empty()


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
                        "startTimeUnixNano": str(
                            int(start_yesterday.timestamp() * 1e9)
                        ),
                        "endTimeUnixNano": str(int(end_yesterday.timestamp() * 1e9)),
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
                        "startTimeUnixNano": str(int(start_today.timestamp() * 1e9)),
                        "endTimeUnixNano": str(int(end_today.timestamp() * 1e9)),
                    },
                ).encode()
            ),
        )
    )
    relay.send_envelope(project_id, envelope)

    spans = spans_consumer.get_spans(timeout=10.0, n=1)
    assert len(spans) == 1
    assert spans[0]["sentry_tags"]["op"] == "default"


def test_span_filter_empty_measurements(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
):
    spans_consumer = spans_consumer()

    relay = relay_with_processing()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
    ]

    start = datetime.now(UTC)
    end = start + timedelta(seconds=1)

    envelope = Envelope()
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "description": "https://example.com/p/blah.js",
                        "op": "resource.script",
                        "span_id": "b0429c44b67a3eb1",
                        "segment_id": "b0429c44b67a3eb1",
                        "start_timestamp": start.timestamp(),
                        "timestamp": end.timestamp() + 1,
                        "exclusive_time": 345.0,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "measurements": {
                            "score.total": {"unit": "ratio", "value": 0.12121616},
                            "missing": {"unit": "ratio", "value": None},
                            "other_missing": {"unit": "ratio"},
                        },
                    },
                ).encode()
            ),
        )
    )
    relay.send_envelope(project_id, envelope)

    spans = spans_consumer.get_spans(timeout=10.0, n=1)
    assert len(spans) == 1
    assert spans[0]["measurements"] == {"score.total": {"value": 0.12121616}}


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

    assert spans == [
        {
            "data": {
                "browser.name": "Python Requests",
                "client.address": "127.0.0.1",
                "user_agent.original": "python-requests/2.32.2",
                # Backfilled from `sentry_tags`:
                "sentry.browser.name": "Python Requests",
                "sentry.op": "ui.interaction.click",
                # Backfilled from `measurements`:
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
            "duration_ms": 1500,
            "exclusive_time_ms": 345.0,
            "is_segment": False,
            "is_remote": False,
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
                "score.ratio.cls": {"value": 0.0},
                "score.ratio.fcp": {"value": 0.9999981846359844},
                "score.ratio.fid": {"value": 0.4999999995},
                "score.ratio.lcp": {"value": 0.9995380458572936},
                "score.ratio.ttfb": {"value": 0.0},
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
            "_meta": {
                "data": {
                    "sentry.segment.name": {
                        "": {
                            "rem": [
                                [
                                    "int",
                                    "s",
                                    34,
                                    37,
                                ],
                                ["**/interaction/*/**", "s"],
                            ],
                            "val": "/page/with/click/interaction/jane/123",
                        }
                    }
                }
            },
            "data": {
                "browser.name": "Python Requests",
                "client.address": "127.0.0.1",
                "sentry.replay.id": "8477286c8e5148b386b71ade38374d58",
                "sentry.segment.name": "/page/with/click/interaction/*/*",
                "user": "[email]",
                "user_agent.original": "python-requests/2.32.2",
                # Backfilled from `sentry_tags`:
                "sentry.browser.name": "Python Requests",
                "sentry.op": "ui.interaction.click",
                "sentry.transaction": "/page/with/click/interaction/*/*",
                "sentry.replay_id": "8477286c8e5148b386b71ade38374d58",
                "sentry.user": "[email]",
                # Backfilled from `measurements`:
                "inp": 100.0,
                "score.inp": 0.9948129113413748,
                "score.ratio.inp": 0.9948129113413748,
                "score.total": 0.9948129113413748,
                "score.weight.inp": 1.0,
            },
            "duration_ms": 1500,
            "exclusive_time_ms": 345.0,
            "is_segment": False,
            "is_remote": False,
            "profile_id": "3d9428087fda4ba0936788b70a7587d0",
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "sentry_tags": {
                "browser.name": "Python Requests",
                "op": "ui.interaction.click",
                "transaction": "/page/with/click/interaction/*/*",
                "replay_id": "8477286c8e5148b386b71ade38374d58",
                "user": "[email]",
            },
            "span_id": "cd429c44b67a3eb1",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
            "start_timestamp_precise": start.timestamp(),
            "end_timestamp_precise": end.timestamp() + 1,
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
            "measurements": {
                "inp": {"value": 100.0},
                "score.inp": {"value": 0.9948129113413748},
                "score.ratio.inp": {"value": 0.9948129113413748},
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
            "limit": 6,
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
    spans = spans_consumer.get_spans(n=6, timeout=10)
    assert len(spans) == 6
    assert summarize_outcomes() == {(16, 0): 6}  # SpanIndexed, Accepted

    # Second batch is limited
    relay.send_envelope(project_id, envelope)
    assert summarize_outcomes() == {(16, 2): 6}  # SpanIndexed, RateLimited

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
    metrics = metrics_consumer.get_metrics(n=11)
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

    assert summarize_outcomes() == {(12, 2): 6, (16, 2): 6}

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
        spans = spans_consumer.get_spans(timeout=10, n=6)
        assert len(spans) == 6
        outcomes = outcomes_consumer.get_outcomes(timeout=10, n=6)
        assert summarize_outcomes(outcomes) == {(16, 0): 6}  # SpanIndexed, Accepted
    else:
        outcomes = outcomes_consumer.get_outcomes(timeout=10, n=1)
        assert summarize_outcomes(outcomes) == {(16, 1): 6}  # Span, Filtered
        assert {o["reason"] for o in outcomes} == {"Sampled:3000"}

    spans_consumer.assert_empty()
    outcomes_consumer.assert_empty()


@pytest.mark.parametrize("ingest_in_eap", [True, False])
def test_ingest_in_eap_for_organization(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    ingest_in_eap,
):
    spans_consumer = spans_consumer()

    relay = relay_with_processing(options=TEST_CONFIG)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:indexed-spans-extraction",
    ]

    if ingest_in_eap:
        project_config["config"]["features"] += ["organizations:ingest-spans-in-eap"]

    event = make_transaction({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
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
            "timestamp": end.isoformat(),
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
    ]

    relay.send_event(project_id, event)

    if ingest_in_eap:
        spans_consumer.get_span()
        spans_consumer.get_span()

    spans_consumer.assert_empty()


@pytest.mark.parametrize("ingest_in_eap", [True, False])
def test_ingest_in_eap_for_project(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    ingest_in_eap,
):
    spans_consumer = spans_consumer()

    relay = relay_with_processing(options=TEST_CONFIG)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:indexed-spans-extraction",
    ]

    if ingest_in_eap:
        project_config["config"]["features"] += ["projects:ingest-spans-in-eap"]

    event = make_transaction({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
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
            "timestamp": end.isoformat(),
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
    ]

    relay.send_event(project_id, event)

    if ingest_in_eap:
        spans_consumer.get_span()
        spans_consumer.get_span()

    spans_consumer.assert_empty()


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
    project_config["config"]["features"] = [
        "projects:span-metrics-extraction",
        "organizations:indexed-spans-extraction",
    ]
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
    del child_span["received"]

    expected = {
        "_meta": {
            "sentry_tags": {
                "user.email": {"": {"len": 15, "rem": [["@email", "s", 0, 7]]}},
                "user.ip": {
                    "": {
                        "len": 9,
                        "rem": [["@ip:replace", "s", 0, 4], ["@anything:remove", "x"]],
                    }
                },
            }
        },
        "data": {
            # Backfilled from `sentry_tags`
            "sentry.category": "http",
            "sentry.normalized_description": "GET *",
            "sentry.group": "37e3d9fab1ae9162",
            "sentry.op": "http",
            "sentry.platform": "other",
            "sentry.sdk.name": "raven-node",
            "sentry.sdk.version": "2.6.3",
            "sentry.status": "ok",
            "sentry.trace.status": "unknown",
            "sentry.transaction": "hi",
            "sentry.transaction.op": "hi",
            "sentry.user": "id:unique_id",
            "sentry.user.email": "[email]",
            "sentry.user.id": "unique_id",
            "sentry.user.ip": "127.0.0.1",
            "sentry.user.username": "my_user",
            # Backfilled from `tags`
            "extra_info": "added by user",
        },
        "description": "GET /api/0/organizations/?member=1",
        "duration_ms": int(duration.total_seconds() * 1e3),
        "event_id": "cbf6960622e14a45abc1f03b2055b186",
        "exclusive_time_ms": 500.0,
        "is_segment": False,
        "is_remote": False,
        "organization_id": 1,
        "origin": "manual",
        "parent_span_id": "968cff94913ebb07",
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
            "trace.status": "unknown",
            "transaction": "hi",
            "transaction.op": "hi",
            "user": "id:unique_id",
            "user.email": "[email]",
            "user.id": "unique_id",
            "user.ip": "127.0.0.1",
            "user.username": "my_user",
        },
        "tags": {
            "extra_info": "added by user",
        },
        "span_id": "bbbbbbbbbbbbbbbb",
        "start_timestamp_ms": int(start.timestamp() * 1e3),
        "start_timestamp_precise": start.timestamp(),
        "end_timestamp_precise": start.timestamp() + duration.total_seconds(),
        "trace_id": "ff62a8b040f340bda5d830223def1d81",
    }
    if scrub_ip_addresses:
        del expected["sentry_tags"]["user.ip"]
        del expected["data"]["sentry.user.ip"]
    else:
        del expected["_meta"]["sentry_tags"]["user.ip"]
    assert child_span == expected

    start_timestamp = datetime.fromisoformat(event["start_timestamp"]).replace(
        tzinfo=timezone.utc
    )
    end_timestamp = datetime.fromisoformat(event["timestamp"]).replace(
        tzinfo=timezone.utc
    )
    duration = (end_timestamp - start_timestamp).total_seconds()
    duration_ms = int(duration * 1e3)

    child_span = spans_consumer.get_span()
    del child_span["received"]

    expected = {
        "data": {
            "sentry.sdk.name": "raven-node",
            "sentry.sdk.version": "2.6.3",
            "sentry.segment.name": "hi",
            # Backfilled from `sentry_tags`:
            "sentry.op": "hi",
            "sentry.platform": "other",
            "sentry.status": "unknown",
            "sentry.trace.status": "unknown",
            "sentry.transaction": "hi",
            "sentry.transaction.op": "hi",
            "sentry.user": "id:unique_id",
            "sentry.user.email": "[email]",
            "sentry.user.id": "unique_id",
            "sentry.user.ip": "127.0.0.1",
            "sentry.user.username": "my_user",
        },
        "description": "hi",
        "duration_ms": duration_ms,
        "event_id": "cbf6960622e14a45abc1f03b2055b186",
        "exclusive_time_ms": 1500.0,
        "is_segment": True,
        "is_remote": True,
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
            "user": "id:unique_id",
            "user.email": "[email]",
            "user.id": "unique_id",
            "user.ip": "127.0.0.1",
            "user.username": "my_user",
        },
        "span_id": "968cff94913ebb07",
        "start_timestamp_ms": int(start_timestamp.timestamp() * 1e3),
        "start_timestamp_precise": start_timestamp.timestamp(),
        "end_timestamp_precise": start_timestamp.timestamp() + duration,
        "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
    }
    if scrub_ip_addresses:
        del expected["sentry_tags"]["user.ip"]
        del expected["data"]["sentry.user.ip"]
    assert child_span == expected

    spans_consumer.assert_empty()


def test_spans_v2_multiple_containers_not_allowed(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    outcomes_consumer,
):
    spans_consumer = spans_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
    ]

    relay = relay_with_processing(options=TEST_CONFIG)
    start = datetime.now(timezone.utc)
    envelope = Envelope()

    payload = {
        "start_timestamp": start.timestamp(),
        "end_timestamp": start.timestamp() + 0.500,
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "span_id": "eee19b7ec3c1b175",
        "name": "some op",
    }
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(json={"items": [payload]}),
            content_type="application/vnd.sentry.items.span.v2+json",
            headers={"item_count": 1},
        )
    )
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(json={"items": [payload, payload]}),
            content_type="application/vnd.sentry.items.span.v2+json",
            headers={"item_count": 2},
        )
    )

    relay.send_envelope(project_id, envelope)

    spans_consumer.assert_empty()

    outcomes = outcomes_consumer.get_outcomes()

    outcomes.sort(key=lambda o: sorted(o.items()))

    assert outcomes == [
        {
            "category": DataCategory.SPAN.value,
            "timestamp": time_within_delta(),
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 3,
            "reason": "duplicate_item",
        },
        {
            "category": DataCategory.SPAN_INDEXED.value,
            "timestamp": time_within_delta(),
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 3,
            "reason": "duplicate_item",
        },
    ]
