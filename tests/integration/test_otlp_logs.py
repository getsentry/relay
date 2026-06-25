from datetime import datetime, timezone, timedelta

from sentry_relay.consts import DataCategory

from .asserts import matches_any, time_within_delta, time_within, only_items

TEST_CONFIG = {
    "outcomes": {
        "emit_outcomes": True,
    },
}


def test_otlp_logs_conversion(
    mini_sentry, relay, relay_with_processing, outcomes_consumer, items_consumer
):
    """Test OTLP logs conversion including basic and complex attributes."""
    items_consumer = items_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]
    project_config["config"]["retentions"] = {
        "log": {"standard": 30, "downsampled": 13 * 30},
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
                "sentry.observed_timestamp_nanos": {
                    "stringValue": time_within(ts, expect_resolution="ns")
                },
                "sentry.origin": {"stringValue": "auto.otlp.logs"},
                "sentry.payload_size_bytes": {"intValue": matches_any()},
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
            "itemId": matches_any(),
            "itemType": "TRACE_ITEM_TYPE_LOG",
            "organizationId": "1",
            "projectId": "42",
            "received": time_within_delta(),
            "retentionDays": 30,
            "downsampledRetentionDays": 390,
            "serverSampleRate": 1.0,
            "timestamp": time_within_delta(ts, delta=timedelta(seconds=1)),
            "traceId": "5b8efff798038103d269b633813fc60c",
            "outcomes": {
                "categoryCount": [
                    {
                        "dataCategory": DataCategory.LOG_ITEM.value,
                        "quantity": "1",
                    },
                    {
                        "dataCategory": DataCategory.LOG_BYTE.value,
                        "quantity": "318",
                    },
                ],
                "keyId": "123",
            },
        }
    ]


def test_otlp_logs_multiple_records(
    mini_sentry, relay, relay_with_processing, outcomes_consumer, items_consumer
):
    """Test multiple log records in a single payload."""
    items_consumer = items_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
        "organizations:relay-generate-billing-outcome",
    ]
    project_config["config"]["retentions"] = {
        "log": {"standard": 30, "downsampled": 13 * 30},
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
                "sentry.observed_timestamp_nanos": {
                    "stringValue": time_within(ts, expect_resolution="ns")
                },
                "sentry.origin": {"stringValue": "auto.otlp.logs"},
                "sentry.payload_size_bytes": {"intValue": matches_any()},
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
            "itemId": matches_any(),
            "itemType": "TRACE_ITEM_TYPE_LOG",
            "organizationId": "1",
            "projectId": "42",
            "received": time_within_delta(),
            "retentionDays": 30,
            "downsampledRetentionDays": 390,
            "serverSampleRate": 1.0,
            "timestamp": time_within_delta(ts, delta=timedelta(seconds=1)),
            "traceId": "5b8efff798038103d269b633813fc60c",
            "outcomes": {
                "categoryCount": [
                    {
                        "dataCategory": DataCategory.LOG_ITEM.value,
                        "quantity": "1",
                    },
                    {
                        "dataCategory": DataCategory.LOG_BYTE.value,
                        "quantity": "92",
                    },
                ],
                "keyId": "123",
            },
        },
        {
            "attributes": {
                "sentry.body": {"stringValue": "Second log entry"},
                "sentry.observed_timestamp_nanos": {
                    "stringValue": time_within(ts, expect_resolution="ns")
                },
                "sentry.origin": {"stringValue": "auto.otlp.logs"},
                "sentry.payload_size_bytes": {"intValue": matches_any()},
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
            "itemId": matches_any(),
            "itemType": "TRACE_ITEM_TYPE_LOG",
            "organizationId": "1",
            "projectId": "42",
            "received": time_within_delta(),
            "retentionDays": 30,
            "downsampledRetentionDays": 390,
            "serverSampleRate": 1.0,
            "timestamp": time_within_delta(ts, delta=timedelta(seconds=1)),
            "traceId": "5b8efff798038103d269b633813fc60c",
            "outcomes": {
                "categoryCount": [
                    {
                        "dataCategory": DataCategory.LOG_ITEM.value,
                        "quantity": "1",
                    },
                    {
                        "dataCategory": DataCategory.LOG_BYTE.value,
                        "quantity": "93",
                    },
                ],
                "keyId": "123",
            },
        },
    ]


def test_otlp_logs_size_limits(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
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
            "outcome": 3,
            "quantity": 1,
            "reason": "too_large:log",
        },
        {
            "category": DataCategory.LOG_BYTE,
            "outcome": 3,
            "quantity": 127,
            "reason": "too_large:log",
        },
    ]
