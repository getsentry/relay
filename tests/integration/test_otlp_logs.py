import json
from datetime import datetime, timezone, timedelta

from sentry_relay.consts import DataCategory

from .asserts import time_within_delta, time_within


TEST_CONFIG = {
    "outcomes": {
        "emit_outcomes": True,
        "batch_size": 1,
        "batch_interval": 1,
        "aggregator": {
            "bucket_interval": 1,
            "flush_interval": 1,
        },
    },
    "aggregator": {
        "bucket_interval": 1,
        "initial_delay": 0,
    },
}


def test_otlp_logs_conversion(mini_sentry, relay):
    """Test OTLP logs conversion including basic and complex attributes."""
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
        "organizations:relay-otel-logs-endpoint",
    ]
    relay = relay(mini_sentry)

    ts = datetime.now(timezone.utc)
    ts_nanos = str(int(ts.timestamp() * 1e6) * 1000)

    otel_logs_payload = {
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {"stringValue": "test-service"},
                        }
                    ]
                },
                "scopeLogs": [
                    {
                        "scope": {"name": "test-library"},
                        "logRecords": [
                            {
                                "timeUnixNano": ts_nanos,
                                "observedTimeUnixNano": ts_nanos,
                                "severityNumber": 10,
                                "severityText": "Information",
                                "traceId": "5B8EFFF798038103D269B633813FC60C",
                                "spanId": "EEE19B7EC3C1B174",
                                "body": {"stringValue": "Example log record"},
                                "attributes": [
                                    {
                                        "key": "string.attribute",
                                        "value": {"stringValue": "some string"},
                                    },
                                    {
                                        "key": "boolean.attribute",
                                        "value": {"boolValue": True},
                                    },
                                    {
                                        "key": "int.attribute",
                                        "value": {"intValue": "10"},
                                    },
                                    {
                                        "key": "double.attribute",
                                        "value": {"doubleValue": 637.704},
                                    },
                                    {
                                        "key": "array.attribute",
                                        "value": {
                                            "arrayValue": {
                                                "values": [
                                                    {"stringValue": "first"},
                                                    {"stringValue": "second"},
                                                ]
                                            }
                                        },
                                    },
                                    {
                                        "key": "map.attribute",
                                        "value": {
                                            "kvlistValue": {
                                                "values": [
                                                    {
                                                        "key": "nested.key",
                                                        "value": {
                                                            "stringValue": "nested value"
                                                        },
                                                    }
                                                ]
                                            }
                                        },
                                    },
                                ],
                            }
                        ],
                    }
                ],
            }
        ]
    }

    relay.send_otel_logs(project_id, json=otel_logs_payload)

    # Add some debugging to see if any envelope is captured
    envelope = mini_sentry.captured_events.get(timeout=5)

    assert [item.type for item in envelope.items] == ["log"]
    log_item = json.loads(envelope.items[0].payload.bytes)

    assert log_item["items"] == [
        {
            "__header": {"byte_size": 248},
            "attributes": {
                "array.attribute": {"type": "string", "value": '["first","second"]'},
                "boolean.attribute": {"type": "boolean", "value": True},
                "double.attribute": {"type": "double", "value": 637.704},
                "instrumentation.name": {"type": "string", "value": "test-library"},
                "int.attribute": {"type": "integer", "value": 10},
                "map.attribute": {
                    "type": "string",
                    "value": '{"nested.key":"nested value"}',
                },
                "resource.service.name": {"type": "string", "value": "test-service"},
                "sentry.browser.name": {"type": "string", "value": "Python Requests"},
                "sentry.browser.version": {"type": "string", "value": "2.32"},
                "sentry.observed_timestamp_nanos": {
                    "type": "string",
                    "value": time_within(ts, expect_resolution="ns"),
                },
                "string.attribute": {"type": "string", "value": "some string"},
            },
            "body": "Example log record",
            "level": "info",
            "span_id": "eee19b7ec3c1b174",
            "timestamp": time_within_delta(ts, delta=timedelta(seconds=1)),
            "trace_id": "5b8efff798038103d269b633813fc60c",
        }
    ]

    assert mini_sentry.captured_events.empty()


def test_otlp_logs_multiple_records(mini_sentry, relay):
    """Test multiple log records in a single payload."""
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
        "organizations:relay-otel-logs-endpoint",
    ]
    relay = relay(mini_sentry)

    ts = datetime.now(timezone.utc)
    ts_nanos = str(int(ts.timestamp() * 1e6) * 1000)

    otel_logs_payload = {
        "resourceLogs": [
            {
                "scopeLogs": [
                    {
                        "logRecords": [
                            {
                                "timeUnixNano": ts_nanos,
                                "severityNumber": 18,
                                "severityText": "Error",
                                "traceId": "5B8EFFF798038103D269B633813FC60C",
                                "spanId": "EEE19B7EC3C1B174",
                                "body": {"stringValue": "First log entry"},
                            },
                            {
                                "timeUnixNano": ts_nanos,
                                "severityNumber": 6,
                                "severityText": "Debug",
                                "traceId": "5B8EFFF798038103D269B633813FC60C",
                                "spanId": "EEE19B7EC3C1B175",
                                "body": {"stringValue": "Second log entry"},
                            },
                        ]
                    }
                ]
            }
        ]
    }

    relay.send_otel_logs(project_id, json=otel_logs_payload)

    envelope = mini_sentry.captured_events.get(timeout=5)

    assert [item.type for item in envelope.items] == ["log"]
    log_item = json.loads(envelope.items[0].payload.bytes)

    assert log_item["items"] == [
        {
            "__header": {"byte_size": 15},
            "timestamp": time_within_delta(ts, delta=timedelta(seconds=1)),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "error",
            "body": "First log entry",
            "attributes": {
                "sentry.browser.name": {"type": "string", "value": "Python Requests"},
                "sentry.browser.version": {"type": "string", "value": "2.32"},
                "sentry.observed_timestamp_nanos": {
                    "type": "string",
                    "value": time_within(ts, expect_resolution="ns"),
                },
            },
        },
        {
            "__header": {"byte_size": 16},
            "timestamp": time_within_delta(ts, delta=timedelta(seconds=1)),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "level": "debug",
            "body": "Second log entry",
            "attributes": {
                "sentry.browser.name": {"type": "string", "value": "Python Requests"},
                "sentry.browser.version": {"type": "string", "value": "2.32"},
                "sentry.observed_timestamp_nanos": {
                    "type": "string",
                    "value": time_within(ts, expect_resolution="ns"),
                },
            },
        },
    ]

    assert mini_sentry.captured_events.empty()


def test_otlp_logs_outcomes(
    mini_sentry,
    relay,
    relay_with_processing,
    outcomes_consumer,
):
    """Test that OTLP logs produce proper outcomes."""
    outcomes_consumer = outcomes_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
        "organizations:relay-otel-logs-endpoint",
    ]

    relay = relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    ts_nanos = str(int(ts.timestamp() * 1e6) * 1000)

    otel_logs_payload = {
        "resourceLogs": [
            {
                "scopeLogs": [
                    {
                        "logRecords": [
                            {
                                "timeUnixNano": ts_nanos,
                                "severityNumber": 18,
                                "severityText": "Error",
                                "traceId": "5B8EFFF798038103D269B633813FC60C",
                                "spanId": "EEE19B7EC3C1B174",
                                "body": {"stringValue": "Test log entry"},
                            },
                        ]
                    }
                ]
            }
        ]
    }

    relay.send_otel_logs(project_id, json=otel_logs_payload)

    outcomes = outcomes_consumer.get_aggregated_outcomes(n=2)
    assert outcomes == [
        {
            "category": DataCategory.LOG_ITEM.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 1,
        },
        {
            "category": DataCategory.LOG_BYTE.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 124,
        },
    ]
