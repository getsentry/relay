import json
from datetime import datetime, timezone
from unittest import mock

from .asserts import time_within_delta


def test_otlp_logs_conversion(mini_sentry, relay):
    """Test OTLP logs conversion including basic and complex attributes."""
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
        "organizations:relay-otel-logs-endpoint",
    ]
    relay = relay(mini_sentry)

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
                                "timeUnixNano": "1544712660300000000",
                                "observedTimeUnixNano": "1544712660300000000",
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

    # Verify the structure contains our log entry
    assert "items" in log_item
    assert len(log_item["items"]) == 1

    log_entry = log_item["items"][0]

    # Verify basic structure
    assert "timestamp" in log_entry
    assert "trace_id" in log_entry
    assert "level" in log_entry
    assert "body" in log_entry
    assert "attributes" in log_entry

    # Verify specific values
    assert log_entry["body"] == "Example log record"
    assert log_entry["level"] == "info"
    assert log_entry["trace_id"] == "5b8efff798038103d269b633813fc60c"

    # Verify attributes were preserved
    attributes = log_entry["attributes"]
    assert "string.attribute" in attributes
    assert attributes["string.attribute"]["value"] == "some string"
    assert attributes["string.attribute"]["type"] == "string"

    assert "boolean.attribute" in attributes
    assert attributes["boolean.attribute"]["value"] is True
    assert attributes["boolean.attribute"]["type"] == "boolean"

    assert "int.attribute" in attributes
    assert attributes["int.attribute"]["value"] == 10
    assert attributes["int.attribute"]["type"] == "integer"

    assert "double.attribute" in attributes
    assert attributes["double.attribute"]["value"] == 637.704
    assert attributes["double.attribute"]["type"] == "double"

    # Test array attribute (should be serialized as JSON string)
    assert "array.attribute" in attributes
    assert attributes["array.attribute"]["type"] == "string"
    assert attributes["array.attribute"]["value"] == '["first","second"]'

    # Test map attribute (should be serialized as JSON string)
    assert "map.attribute" in attributes
    assert attributes["map.attribute"]["type"] == "string"
    assert attributes["map.attribute"]["value"] == '{"nested.key":"nested value"}'

    # Verify timestamp conversion (from unix nano to seconds)
    expected_timestamp = datetime.fromtimestamp(1544712660.3, tz=timezone.utc)
    assert log_entry["timestamp"] == time_within_delta(expected_timestamp)

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

    otel_logs_payload = {
        "resourceLogs": [
            {
                "scopeLogs": [
                    {
                        "logRecords": [
                            {
                                "timeUnixNano": "1544712660300000000",
                                "severityNumber": 18,
                                "severityText": "Error",
                                "traceId": "5B8EFFF798038103D269B633813FC60C",
                                "spanId": "EEE19B7EC3C1B174",
                                "body": {"stringValue": "First log entry"},
                            },
                            {
                                "timeUnixNano": "1544712661300000000",
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
    envelope = mini_sentry.captured_events.get(timeout=3)
    log_item = json.loads(envelope.items[0].payload.bytes)

    # Should have both log entries
    assert len(log_item["items"]) == 2

    # Verify first log entry
    first_log = log_item["items"][0]
    assert first_log["body"] == "First log entry"
    assert first_log["level"] == "error"
    assert first_log["span_id"] == "eee19b7ec3c1b174"

    # Verify second log entry
    second_log = log_item["items"][1]
    assert second_log["body"] == "Second log entry"
    assert second_log["level"] == "debug"
    assert second_log["span_id"] == "eee19b7ec3c1b175"

    assert mini_sentry.captured_events.empty()
