from collections import Counter
import json
from datetime import datetime, timedelta, timezone
import uuid

from opentelemetry.proto.trace.v1.trace_pb2 import (
    Span,
    ScopeSpans,
    ResourceSpans,
    TracesData,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue

import pytest

from sentry_sdk.envelope import Envelope, Item, PayloadRef

from .test_store import make_transaction
from .test_metrics import TEST_CONFIG


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
        "projects:span-metrics-extraction-all-modules",
    ]
    project_config["config"]["transactionMetrics"] = {
        "version": 3,
    }

    if discard_transaction:
        project_config["config"]["features"].append("projects:discard-transaction")

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

    if discard_transaction:
        assert transactions_consumer.poll(timeout=2.0) is None

        # We do not accidentally produce to the events topic:
        assert events_consumer.poll(timeout=2.0) is None

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
            "transaction": "hi",
            "transaction.op": "hi",
        },
        "span_id": "bbbbbbbbbbbbbbbb",
        "start_timestamp_ms": int(start.timestamp() * 1e3),
        "trace_id": "ff62a8b040f340bda5d830223def1d81",
    }

    start_timestamp = datetime.fromisoformat(event["start_timestamp"])
    end_timestamp = datetime.fromisoformat(event["timestamp"])
    duration_ms = int((end_timestamp - start_timestamp).total_seconds() * 1e3)

    transaction_span = spans_consumer.get_span()
    del transaction_span["received"]
    assert transaction_span == {
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
            "transaction": "hi",
            "transaction.op": "hi",
        },
        "span_id": "968cff94913ebb07",
        "start_timestamp_ms": int(
            start_timestamp.replace(tzinfo=timezone.utc).timestamp() * 1e3
        ),
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
    ]
    project_config["config"]["transactionMetrics"] = {
        "version": 3,
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

    spans = list(spans_consumer.get_spans(max_attempts=2))
    assert len(spans) == expected_spans

    metrics = list(metrics_consumer.get_metrics())
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
    ]
    project_config["config"]["transactionMetrics"] = {
        "version": 1,
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
                                "key": "sentry.exclusive_time_ns",
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
                        "span_id": "bd429c44b67a3eb1",
                        "segment_id": "968cff94913ebb07",
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
                                        "key": "sentry.exclusive_time_ns",
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


@pytest.mark.parametrize("extract_transaction", [False, True])
def test_span_ingestion(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    metrics_consumer,
    transactions_consumer,
    extract_transaction,
):
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()
    transactions_consumer = transactions_consumer()

    relay = relay_with_processing(
        options={
            "aggregator": {
                "bucket_interval": 1,
                "initial_delay": 0,
                "debounce_delay": 0,
                "max_secs_in_past": 2**64 - 1,
            }
        }
    )
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-metrics-extraction",
    ]
    project_config["config"]["transactionMetrics"] = {"version": 1}
    if extract_transaction:
        project_config["config"]["features"].append(
            "projects:extract-transaction-from-segment-span"
        )

    duration = timedelta(milliseconds=500)
    end = datetime.now(timezone.utc) - timedelta(seconds=1)
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
        attributes=[
            KeyValue(
                key="sentry.exclusive_time_ns",
                value=AnyValue(int_value=int(duration.total_seconds() * 1e9)),
            ),
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

    spans = list(spans_consumer.get_spans(timeout=10.0, max_attempts=6))

    for span in spans:
        span.pop("received", None)

    # endpoint might overtake envelope
    spans.sort(key=lambda msg: msg["span_id"])

    assert spans == [
        {
            "description": "my 1st OTel span",
            "duration_ms": 500,
            "exclusive_time_ms": 500.0,
            "is_segment": True,
            "organization_id": 1,
            "parent_span_id": "",
            "project_id": 42,
            "retention_days": 90,
            "segment_id": "a342abb1214ca181",
            "sentry_tags": {
                "browser.name": "Chrome",
                "category": "db",
                "op": "db.query",
            },
            "span_id": "a342abb1214ca181",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
            "trace_id": "89143b0763095bd9c9955e8175d1fb23",
        },
        {
            "description": "https://example.com/p/blah.js",
            "duration_ms": 1500,
            "exclusive_time_ms": 345.0,
            "is_segment": True,
            "measurements": {"score.total": {"value": 0.12121616}},
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "segment_id": "bd429c44b67a3eb1",
            "sentry_tags": {
                "browser.name": "Chrome",
                "category": "resource",
                "description": "https://example.com/*/blah.js",
                "domain": "example.com",
                "file_extension": "js",
                "group": "8a97a9e43588e2bd",
                "op": "resource.script",
            },
            "span_id": "bd429c44b67a3eb1",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
        {
            "description": r"test \" with \" escaped \" chars",
            "duration_ms": 1500,
            "exclusive_time_ms": 345.0,
            "is_segment": True,
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "segment_id": "cd429c44b67a3eb1",
            "sentry_tags": {"browser.name": "Chrome", "op": "default"},
            "span_id": "cd429c44b67a3eb1",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
        {
            "description": "my 2nd OTel span",
            "duration_ms": 500,
            "exclusive_time_ms": 500.0,
            "is_segment": True,
            "organization_id": 1,
            "parent_span_id": "",
            "project_id": 42,
            "retention_days": 90,
            "segment_id": "d342abb1214ca182",
            "sentry_tags": {
                "browser.name": "Python Requests",
                "op": "default",
            },
            "span_id": "d342abb1214ca182",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
            "trace_id": "89143b0763095bd9c9955e8175d1fb24",
        },
        {
            "duration_ms": 1500,
            "exclusive_time_ms": 345.0,
            "is_segment": True,
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "segment_id": "ed429c44b67a3eb1",
            "sentry_tags": {
                "browser.name": "Chrome",
                "op": "default",
            },
            "span_id": "ed429c44b67a3eb1",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
        {
            "description": "my 3rd protobuf OTel span",
            "duration_ms": 500,
            "exclusive_time_ms": 500.0,
            "is_segment": False,
            "organization_id": 1,
            "parent_span_id": "f0f0f0abcdef1234",
            "project_id": 42,
            "retention_days": 90,
            "sentry_tags": {"browser.name": "Python Requests", "op": "default"},
            "span_id": "f0b809703e783d00",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
            "trace_id": "89143b0763095bd9c9955e8175d1fb24",
        },
    ]

    spans_consumer.assert_empty()

    # If transaction extraction is enabled, expect transactions:
    if extract_transaction:
        expected_transactions = 5

        transactions = [
            transactions_consumer.get_event()[0] for _ in range(expected_transactions)
        ]

        assert len(transactions) == expected_transactions
        for transaction in transactions:
            # Not checking all individual fields here, most should be tested in convert.rs

            # SDK gets taken from the header:
            if sdk := transaction.get("sdk"):
                assert sdk == {"name": "raven-node", "version": "2.6.3"}

            # No errors during normalization:
            assert not transaction.get("errors")

    transactions_consumer.assert_empty()

    metrics = [metric for (metric, _headers) in metrics_consumer.get_metrics()]
    metrics.sort(key=lambda m: (m["name"], sorted(m["tags"].items()), m["timestamp"]))
    for metric in metrics:
        try:
            metric["value"].sort()
        except AttributeError:
            pass

    expected_timestamp = int(end.timestamp())
    expected_span_metrics = [
        {
            "name": "c:spans/usage@none",
            "org_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "tags": {},
            "timestamp": expected_timestamp,
            "type": "c",
            "value": 3.0,
        },
        {
            "name": "c:spans/usage@none",
            "org_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "tags": {},
            "timestamp": expected_timestamp + 1,
            "type": "c",
            "value": 3.0,
        },
        {
            "name": "d:spans/duration@millisecond",
            "org_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "tags": {
                "span.op": "resource.script",
                "span.group": "8a97a9e43588e2bd",
            },
            "timestamp": expected_timestamp + 1,
            "type": "d",
            "value": [1500.0],
        },
        {
            "name": "d:spans/duration@millisecond",
            "org_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "tags": {"span.op": "db.query"},
            "timestamp": expected_timestamp,
            "type": "d",
            "value": [500.0],
        },
        {
            "name": "d:spans/duration@millisecond",
            "org_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "tags": {"span.op": "default"},
            "timestamp": expected_timestamp,
            "type": "d",
            "value": [500.0, 500.0],
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
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "d:spans/exclusive_time@millisecond",
            "type": "d",
            "value": [345.0],
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
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "d:spans/exclusive_time@millisecond",
            "retention_days": 90,
            "tags": {"span.category": "db", "span.op": "db.query"},
            "timestamp": expected_timestamp,
            "type": "d",
            "value": [500.0],
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
        },
        {
            "name": "d:spans/exclusive_time@millisecond",
            "org_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "tags": {"span.op": "default"},
            "timestamp": expected_timestamp + 1,
            "type": "d",
            "value": [345.0, 345.0],
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "d:spans/exclusive_time_light@millisecond",
            "type": "d",
            "value": [345.0],
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
        },
        {
            "name": "d:spans/exclusive_time_light@millisecond",
            "org_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "tags": {"span.category": "db", "span.op": "db.query"},
            "timestamp": expected_timestamp,
            "type": "d",
            "value": [500.0],
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
        },
    ]
    assert [m for m in metrics if ":spans/" in m["name"]] == expected_span_metrics

    transaction_duration_metrics = [
        m for m in metrics if m["name"] == "d:transactions/duration@millisecond"
    ]

    if extract_transaction:
        assert {
            (m["name"], m["tags"]["transaction"]) for m in transaction_duration_metrics
        } == {
            ("d:transactions/duration@millisecond", "<unlabeled transaction>"),
            ("d:transactions/duration@millisecond", "https://example.com/p/blah.js"),
            ("d:transactions/duration@millisecond", "my 1st OTel span"),
            ("d:transactions/duration@millisecond", "my 2nd OTel span"),
            (
                "d:transactions/duration@millisecond",
                'test \\" with \\" escaped \\" chars',
            ),
        }
        # Make sure we're not double-reporting:
        for m in transaction_duration_metrics:
            assert len(m["value"]) == 1
    else:
        assert len(transaction_duration_metrics) == 0

    # Regardless of whether transactions are extracted, score.total is only converted to a transaction metric once:
    score_total_metrics = [
        m
        for m in metrics
        if m["name"] == "d:transactions/measurements.score.total@ratio"
    ]
    assert len(score_total_metrics) == 1, score_total_metrics
    assert len(score_total_metrics[0]["value"]) == 1

    metrics_consumer.assert_empty()


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

    start_timestamp = datetime.fromisoformat(event["start_timestamp"])
    end_timestamp = datetime.fromisoformat(event["timestamp"])
    duration_ms = int((end_timestamp - start_timestamp).total_seconds() * 1e3)

    transaction_span = spans_consumer.get_span()
    del transaction_span["received"]
    assert transaction_span == {
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
            "transaction": "hi",
            "transaction.op": "hi",
        },
        "span_id": "968cff94913ebb07",
        "start_timestamp_ms": int(
            start_timestamp.replace(tzinfo=timezone.utc).timestamp() * 1e3
        ),
        "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
    }

    spans_consumer.assert_empty()
    metrics_summary = metrics_summaries_consumer.get_metrics_summary()

    assert metrics_summary["mri"] == mri


def test_extracted_transaction_gets_normalized(
    mini_sentry, transactions_consumer, relay_with_processing, relay, relay_credentials
):
    """When normalization in processing relays has been disabled, an extracted
    transaction still gets normalized.

    This test was copied and adapted from test_store::test_relay_chain_normalization

    """
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:extract-transaction-from-segment-span",
    ]

    transactions_consumer = transactions_consumer()

    credentials = relay_credentials()
    processing = relay_with_processing(
        static_relays={
            credentials["id"]: {
                "public_key": credentials["public_key"],
                "internal": True,
            },
        },
        options={"processing": {"normalize": "disabled"}},
    )
    relay = relay(
        processing,
        credentials=credentials,
        options={
            "processing": {
                "normalize": "full",
            }
        },
    )

    duration = timedelta(milliseconds=500)
    end = datetime.now(timezone.utc) - timedelta(seconds=1)
    start = end - duration
    otel_payload = make_otel_span(start, end)

    # Unset name to validate transaction normalization
    del otel_payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["name"]

    relay.send_otel_span(project_id, json=otel_payload)

    ingested, _ = transactions_consumer.get_event(timeout=10)

    # "<unlabeled transaction>" was set by normalization:
    assert ingested["transaction"] == "<unlabeled transaction>"


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

    start_timestamp = datetime.fromisoformat(event["start_timestamp"])
    end_timestamp = datetime.fromisoformat(event["timestamp"])
    duration_ms = int((end_timestamp - start_timestamp).total_seconds() * 1e3)

    metrics_summary["c:spans/some_metric@none"][0].pop("min", None)

    transaction_span = spans_consumer.get_span()
    del transaction_span["received"]
    assert transaction_span == {
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
            "transaction": "hi",
            "transaction.op": "hi",
        },
        "span_id": "968cff94913ebb07",
        "start_timestamp_ms": int(
            start_timestamp.replace(tzinfo=timezone.utc).timestamp() * 1e3
        ),
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

    spans = list(spans_consumer.get_spans(timeout=10.0, max_attempts=1))

    assert len(spans) == 1
    assert spans[0]["description"] == "span with valid timestamps"


def test_span_ingestion_with_performance_scores(
    mini_sentry, relay_with_processing, spans_consumer, metrics_consumer
):
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()
    relay = relay_with_processing(
        options={
            "aggregator": {
                "bucket_interval": 1,
                "initial_delay": 0,
                "debounce_delay": 0,
                "shift_key": "none",
            }
        }
    )

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
                        "segment_id": "968cff94913ebb07",
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
                        "span_id": "bd429c44b67a3eb1",
                        "segment_id": "968cff94913ebb07",
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

    spans = list(spans_consumer.get_spans(timeout=10.0, max_attempts=2))

    for span in spans:
        span.pop("received", None)

    # endpoint might overtake envelope
    spans.sort(key=lambda msg: msg["span_id"])

    assert spans == [
        {
            "duration_ms": 1500,
            "exclusive_time_ms": 345.0,
            "is_segment": True,
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "segment_id": "bd429c44b67a3eb1",
            "sentry_tags": {
                "browser.name": "Python Requests",
                "op": "ui.interaction.click",
            },
            "span_id": "bd429c44b67a3eb1",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
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
            "duration_ms": 1500,
            "exclusive_time_ms": 345.0,
            "is_segment": True,
            "profile_id": "3d9428087fda4ba0936788b70a7587d0",
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "segment_id": "bd429c44b67a3eb1",
            "sentry_tags": {
                "browser.name": "Python Requests",
                "op": "ui.interaction.click",
                "transaction": "/page/with/click/interaction/*/*",
                "replay_id": "8477286c8e5148b386b71ade38374d58",
                "user": "admin@sentry.io",
            },
            "span_id": "bd429c44b67a3eb1",
            "start_timestamp_ms": int(start.timestamp() * 1e3),
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
            "window": 1000,
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
    spans = list(spans_consumer.get_spans(max_attempts=4, timeout=10))
    assert len(spans) == 4
    assert summarize_outcomes() == {(16, 0): 4}  # SpanIndexed, Accepted

    # Second batch is limited
    relay.send_envelope(project_id, envelope)
    assert summarize_outcomes() == {(16, 2): 4}  # SpanIndexed, RateLimited

    spans_consumer.assert_empty()
    outcomes_consumer.assert_empty()


def test_rate_limit_indexed_consistent_extracted(
    mini_sentry, relay_with_processing, spans_consumer, outcomes_consumer
):
    """Rate limits for indexed spans that are extracted from transactions"""
    relay = relay_with_processing()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "projects:span-metrics-extraction",
    ]
    project_config["config"]["quotas"] = [
        {
            "categories": ["span_indexed"],
            "limit": 3,
            "window": 1000,
            "id": uuid.uuid4(),
            "reasonCode": "indexed_exceeded",
        },
    ]

    spans_consumer = spans_consumer()
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
    spans = list(spans_consumer.get_spans(max_attempts=2, timeout=10))
    # one for the transaction, one for the contained span
    assert len(spans) == 2
    assert summarize_outcomes() == {(16, 0): 2}  # SpanIndexed, Accepted

    # Second send should be rejected immediately.
    relay.send_event(project_id, event)
    spans = list(spans_consumer.get_spans(max_attempts=1, timeout=2))
    assert len(spans) == 0  # all rejected
    assert summarize_outcomes() == {(16, 2): 2}  # SpanIndexed, RateLimited

    spans_consumer.assert_empty()
    outcomes_consumer.assert_empty()


def test_rate_limit_metrics_consistent(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    metrics_consumer,
    outcomes_consumer,
):
    """Rate limits for total spans (i.e. metrics) are enforced consistently after metrics extraction."""
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
            "window": 1000,
            "id": uuid.uuid4(),
            "reasonCode": "total_exceeded",
        },
    ]

    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()
    outcomes_consumer = outcomes_consumer()

    start = datetime.now(timezone.utc)
    end = start + timedelta(seconds=1)

    envelope = envelope_with_spans(start, end)

    def summarize_outcomes():
        counter = Counter()
        for outcome in outcomes_consumer.get_outcomes():
            counter[(outcome["category"], outcome["outcome"])] += outcome["quantity"]
        return counter

    # First batch passes (we over-accept once)
    relay.send_envelope(project_id, envelope)
    spans = list(spans_consumer.get_spans(max_attempts=4, timeout=10))
    assert len(spans) == 4
    metrics = list(metrics_consumer.get_metrics())
    assert len(metrics) > 0
    assert all(headers == [("namespace", b"spans")] for _, headers in metrics), metrics

    # Accepted outcomes for main category are logged in sentry.
    assert summarize_outcomes() == {(16, 0): 4}  # SpanIndexed, Accepted

    # Second batch is limited
    relay.send_envelope(project_id, envelope)
    spans = list(spans_consumer.get_spans(max_attempts=1, timeout=2))
    assert len(spans) == 0
    metrics = list(metrics_consumer.get_metrics())
    assert len(metrics) == 0
    assert summarize_outcomes() == {
        (16, 2): 4,  # SpanIndexed, RateLimited
        (12, 2): 4,  # Span, RateLimited
    }

    spans_consumer.assert_empty()
    outcomes_consumer.assert_empty()


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
