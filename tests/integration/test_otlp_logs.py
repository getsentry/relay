import pytest

from datetime import datetime, timezone, timedelta
from unittest import mock

from sentry_relay.consts import DataCategory

from .asserts import time_within_delta, time_within, only_items


TEST_CONFIG = {
    "outcomes": {
        "emit_outcomes": True,
    },
}


@pytest.mark.parametrize("retention_config_field", ("retentions", "item_configs"))
def test_otlp_logs_conversion(
    mini_sentry,
    relay,
    relay_with_processing,
    outcomes_consumer,
    items_consumer,
    retention_config_field,
):
    """Test OTLP logs conversion including basic and complex attributes."""
    items_consumer = items_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
        "organizations:relay-otel-logs-endpoint",
    ]

    if retention_config_field == "retentions":
        project_config["config"]["retentions"] = {
            "log": {"standard": 30, "downsampled": 13 * 30},
        }
    elif retention_config_field == "item_configs":
        project_config["config"]["itemConfigs"] = {
            "log": {"retention": {"standard": 30, "downsampled": 13 * 30}}
        }
        # Put a bogus value in `"retentions"`. The one in `"item_configs"` should
        # take precedence.
        project_config["config"]["retentions"] = {
            "log": {"standard": 1, "downsampled": 1},
        }

    relay = relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG)

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

    # Check that the items are properly processed via items_consumer
    items = items_consumer.get_items(n=1)
    assert items == [
        {
            "attributes": {
                "array.attribute": {
                    "arrayValue": {
                        "values": [{"stringValue": "first"}, {"stringValue": "second"}]
                    }
                },
                "boolean.attribute": {"boolValue": True},
                "double.attribute": {"doubleValue": 637.704},
                "instrumentation.name": {"stringValue": "test-library"},
                "int.attribute": {"intValue": "10"},
                "map.attribute": {"stringValue": '{"nested.key":"nested value"}'},
                "resource.service.name": {"stringValue": "test-service"},
                "sentry.body": {"stringValue": "Example log record"},
                "sentry.browser.name": {"stringValue": "Python Requests"},
                "sentry.browser.version": {"stringValue": "2.32"},
                "sentry.observed_timestamp_nanos": {
                    "stringValue": time_within(ts, expect_resolution="ns")
                },
                "sentry.origin": {"stringValue": "auto.otlp.logs"},
                "sentry.payload_size_bytes": {"intValue": "378"},
                "sentry.severity_text": {"stringValue": "info"},
                "sentry.span_id": {"stringValue": "eee19b7ec3c1b174"},
                "sentry.timestamp_precise": {
                    "intValue": time_within_delta(
                        ts,
                        delta=timedelta(seconds=0),
                        expect_resolution="ns",
                        precision="us",
                    )
                },
                "string.attribute": {"stringValue": "some string"},
            },
            "clientSampleRate": 1.0,
            "itemId": mock.ANY,
            "itemType": "TRACE_ITEM_TYPE_LOG",
            "organizationId": "1",
            "projectId": "42",
            "received": time_within_delta(),
            "retentionDays": 30,
            "downsampledRetentionDays": 390,
            "serverSampleRate": 1.0,
            "timestamp": time_within_delta(ts, delta=timedelta(seconds=1)),
            "traceId": "5b8efff798038103d269b633813fc60c",
        }
    ]

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
            "quantity": 378,
        },
    ]


@pytest.mark.parametrize("retention_config_field", ("retentions", "item_configs"))
def test_otlp_logs_multiple_records(
    mini_sentry,
    relay,
    relay_with_processing,
    outcomes_consumer,
    items_consumer,
    retention_config_field,
):
    """Test multiple log records in a single payload."""
    items_consumer = items_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
        "organizations:relay-otel-logs-endpoint",
    ]

    if retention_config_field == "retentions":
        project_config["config"]["retentions"] = {
            "log": {"standard": 30, "downsampled": 13 * 30},
        }
    elif retention_config_field == "item_configs":
        project_config["config"]["itemConfigs"] = {
            "log": {"retention": {"standard": 30, "downsampled": 13 * 30}}
        }
        # Put a bogus value in `"retentions"`. The one in `"item_configs"` should
        # take precedence.
        project_config["config"]["retentions"] = {
            "log": {"standard": 1, "downsampled": 1},
        }

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

    # Check that the items are properly processed via items_consumer
    items = items_consumer.get_items(n=2)
    assert items == [
        {
            "attributes": {
                "sentry.body": {"stringValue": "First log entry"},
                "sentry.browser.name": {"stringValue": "Python Requests"},
                "sentry.browser.version": {"stringValue": "2.32"},
                "sentry.observed_timestamp_nanos": {
                    "stringValue": time_within(ts, expect_resolution="ns")
                },
                "sentry.origin": {"stringValue": "auto.otlp.logs"},
                "sentry.payload_size_bytes": {"intValue": mock.ANY},
                "sentry.severity_text": {"stringValue": "error"},
                "sentry.span_id": {"stringValue": "eee19b7ec3c1b174"},
                "sentry.timestamp_precise": {
                    "intValue": time_within_delta(
                        ts,
                        delta=timedelta(seconds=0),
                        expect_resolution="ns",
                        precision="us",
                    )
                },
            },
            "clientSampleRate": 1.0,
            "itemId": mock.ANY,
            "itemType": "TRACE_ITEM_TYPE_LOG",
            "organizationId": "1",
            "projectId": "42",
            "received": time_within_delta(),
            "retentionDays": 30,
            "downsampledRetentionDays": 390,
            "serverSampleRate": 1.0,
            "timestamp": time_within_delta(ts, delta=timedelta(seconds=1)),
            "traceId": "5b8efff798038103d269b633813fc60c",
        },
        {
            "attributes": {
                "sentry.body": {"stringValue": "Second log entry"},
                "sentry.browser.name": {"stringValue": "Python Requests"},
                "sentry.browser.version": {"stringValue": "2.32"},
                "sentry.observed_timestamp_nanos": {
                    "stringValue": time_within(ts, expect_resolution="ns")
                },
                "sentry.origin": {"stringValue": "auto.otlp.logs"},
                "sentry.payload_size_bytes": {"intValue": mock.ANY},
                "sentry.severity_text": {"stringValue": "debug"},
                "sentry.span_id": {"stringValue": "eee19b7ec3c1b175"},
                "sentry.timestamp_precise": {
                    "intValue": time_within_delta(
                        ts,
                        delta=timedelta(seconds=0),
                        expect_resolution="ns",
                        precision="us",
                    )
                },
            },
            "clientSampleRate": 1.0,
            "itemId": mock.ANY,
            "itemType": "TRACE_ITEM_TYPE_LOG",
            "organizationId": "1",
            "projectId": "42",
            "received": time_within_delta(),
            "retentionDays": 30,
            "downsampledRetentionDays": 390,
            "serverSampleRate": 1.0,
            "timestamp": time_within_delta(ts, delta=timedelta(seconds=1)),
            "traceId": "5b8efff798038103d269b633813fc60c",
        },
    ]

    outcomes = outcomes_consumer.get_aggregated_outcomes(n=4)
    assert outcomes == [
        {
            "category": DataCategory.LOG_ITEM.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 2,
        },
        {
            "category": DataCategory.LOG_BYTE.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 305,
        },
    ]


def test_otlp_logs_size_limits(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
        "organizations:relay-otel-logs-endpoint",
    ]

    relay = relay(mini_sentry, options={"limits": {"max_log_size": 50}, **TEST_CONFIG})

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
                                "body": {"stringValue": "123"},
                            },
                            {
                                "timeUnixNano": ts_nanos,
                                "severityNumber": 6,
                                "severityText": "Debug",
                                "traceId": "5B8EFFF798038103D269B633813FC60C",
                                "spanId": "EEE19B7EC3C1B175",
                                "body": {"stringValue": "a" * 100},
                            },
                        ]
                    }
                ]
            }
        ]
    }

    relay.send_otel_logs(project_id, json=otel_logs_payload)

    assert mini_sentry.get_captured_envelope() == only_items("log")
    assert mini_sentry.get_aggregated_outcomes() == [
        {
            "category": DataCategory.LOG_ITEM,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,
            "project_id": project_id,
            "quantity": 1,
            "reason": "too_large:log",
        },
        {
            "category": DataCategory.LOG_BYTE,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,
            "project_id": project_id,
            "quantity": 127,
            "reason": "too_large:log",
        },
    ]
