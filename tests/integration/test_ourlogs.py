import json

from datetime import datetime, timezone, timedelta
from unittest import mock

from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_relay.consts import DataCategory

from google.protobuf.json_format import MessageToDict

from .asserts import time_within_delta, time_within, matches

import pytest


TEST_CONFIG = {
    "aggregator": {
        "bucket_interval": 1,
        "initial_delay": 0,
    }
}


def envelope_with_sentry_logs(*payloads: dict) -> Envelope:
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="log",
            payload=PayloadRef(json={"items": payloads}),
            content_type="application/vnd.sentry.items.log+json",
            headers={"item_count": len(payloads)},
        )
    )
    return envelope


def envelope_with_otel_logs(timestamp_nanos: str) -> Envelope:
    envelope = Envelope()

    envelope.add_item(
        Item(
            type="otel_log",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "timeUnixNano": timestamp_nanos,
                        "observedTimeUnixNano": timestamp_nanos,
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
                            {"key": "boolean.attribute", "value": {"boolValue": True}},
                            {"key": "int.attribute", "value": {"intValue": "10"}},
                            {
                                "key": "double.attribute",
                                "value": {"doubleValue": 637.704},
                            },
                        ],
                    }
                ).encode()
            ),
        )
    )

    return envelope


def test_ourlog_extraction_with_otel_logs(
    mini_sentry,
    relay_with_processing,
    ourlogs_consumer,
):
    ourlogs_consumer = ourlogs_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]
    relay = relay_with_processing(options=TEST_CONFIG)
    start = datetime.now(timezone.utc)
    timestamp = start.timestamp()
    timestamp_nanos = int(timestamp * 1e9)
    envelope = envelope_with_otel_logs(str(timestamp_nanos))

    relay.send_envelope(project_id, envelope)

    logs = [MessageToDict(log) for log in ourlogs_consumer.get_ourlogs()]

    assert logs == [
        {
            "attributes": {
                "boolean.attribute": {"boolValue": True},
                "double.attribute": {"doubleValue": 637.704},
                "int.attribute": {"intValue": "10"},
                "sentry.body": {"stringValue": "Example log record"},
                "sentry.browser.name": {"stringValue": "Python Requests"},
                "sentry.browser.version": {"stringValue": "2.32"},
                "sentry.observed_timestamp_nanos": {
                    "stringValue": time_within(
                        start, expect_resolution="ns", precision="s"
                    )
                },
                "sentry.severity_number": {"intValue": "10"},
                "sentry.severity_text": {"stringValue": "Information"},
                "sentry.span_id": {"stringValue": "eee19b7ec3c1b174"},
                "sentry.timestamp_nanos": {"stringValue": str(timestamp_nanos)},
                "sentry.timestamp_precise": {"intValue": str(timestamp_nanos)},
                "sentry.trace_flags": {"intValue": "0"},
                "string.attribute": {"stringValue": "some string"},
            },
            "clientSampleRate": 1.0,
            "itemId": mock.ANY,
            "itemType": "TRACE_ITEM_TYPE_LOG",
            "organizationId": "1",
            "projectId": "42",
            "received": time_within_delta(),
            "retentionDays": 90,
            "serverSampleRate": 1.0,
            "timestamp": time_within_delta(
                start, delta=timedelta(seconds=1), expect_resolution="ns"
            ),
            "traceId": "5b8efff798038103d269b633813fc60c",
        },
    ]

    ourlogs_consumer.assert_empty()


def test_ourlog_multiple_containers_not_allowed(
    mini_sentry,
    relay_with_processing,
    ourlogs_consumer,
    outcomes_consumer,
):
    ourlogs_consumer = ourlogs_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]

    relay = relay_with_processing(options=TEST_CONFIG)
    start = datetime.now(timezone.utc)
    envelope = Envelope()

    for _ in range(2):
        payload = {
            "timestamp": start.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "level": "error",
            "body": "oops, not again",
        }
        envelope.add_item(
            Item(
                type="log",
                payload=PayloadRef(json={"items": [payload]}),
                content_type="application/vnd.sentry.items.log+json",
                headers={"item_count": 1},
            )
        )

    relay.send_envelope(project_id, envelope)

    outcomes = outcomes_consumer.get_outcomes()

    outcomes.sort(key=lambda o: sorted(o.items()))

    assert 300 < outcomes[1].pop("quantity") < 400
    assert outcomes == [
        {
            "category": DataCategory.LOG_ITEM.value,
            "timestamp": time_within_delta(),
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 2,
            "reason": "duplicate_item",
        },
        {
            "category": DataCategory.LOG_BYTE.value,
            "timestamp": time_within_delta(),
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "reason": "duplicate_item",
        },
    ]

    ourlogs_consumer.assert_empty()


@pytest.mark.parametrize("calculated_byte_count", [False, True])
def test_ourlog_extraction_with_sentry_logs(
    mini_sentry,
    relay,
    relay_with_processing,
    ourlogs_consumer,
    outcomes_consumer,
    calculated_byte_count,
):
    ourlogs_consumer = ourlogs_consumer()
    outcomes_consumer = outcomes_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]
    if calculated_byte_count:
        project_config["config"]["features"].append(
            "organizations:ourlogs-calculated-byte-count"
        )
    relay = relay(relay_with_processing(options=TEST_CONFIG))
    start = datetime.now(timezone.utc)

    timestamp = start.timestamp()
    timestamp_nanos = int(timestamp * 1e6) * 1000

    envelope = envelope_with_sentry_logs(
        {
            "timestamp": timestamp,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "level": "error",
            "body": "This is really bad",
        },
        {
            "timestamp": timestamp,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Example log record",
            "severity_number": 10,
            "attributes": {
                "boolean.attribute": {"value": True, "type": "boolean"},
                "integer.attribute": {"value": 42, "type": "integer"},
                "double.attribute": {"value": 1.23, "type": "double"},
                "string.attribute": {"value": "some string", "type": "string"},
                "pii": {"value": "4242 4242 4242 4242", "type": "string"},
                "sentry.severity_text": {"value": "info", "type": "string"},
                "unknown_type": {"value": "info", "type": "unknown"},
                "broken_type": {"value": "info", "type": "not_a_real_type"},
                "mismatched_type": {"value": "some string", "type": "boolean"},
                "valid_string_with_other": {
                    "value": "test",
                    "type": "string",
                    "some_other_field": "some_other_value",
                },
            },
        },
    )

    relay.send_envelope(project_id, envelope)

    logs = [MessageToDict(log) for log in ourlogs_consumer.get_ourlogs()]

    assert logs == [
        {
            "attributes": {
                "sentry.body": {"stringValue": "This is really bad"},
                "sentry.browser.name": {"stringValue": "Python Requests"},
                "sentry.browser.version": {"stringValue": "2.32"},
                "sentry.observed_timestamp_nanos": {
                    "stringValue": time_within(
                        start, expect_resolution="ns", precision="s"
                    )
                },
                "sentry.severity_number": {"intValue": "17"},
                "sentry.severity_text": {"stringValue": "error"},
                "sentry.span_id": {"stringValue": "eee19b7ec3c1b175"},
                "sentry.timestamp_nanos": {"stringValue": str(timestamp_nanos)},
                "sentry.timestamp_precise": {"intValue": str(timestamp_nanos)},
                "sentry.trace_flags": {"intValue": "0"},
            },
            "clientSampleRate": 1.0,
            "itemId": mock.ANY,
            "itemType": "TRACE_ITEM_TYPE_LOG",
            "organizationId": "1",
            "projectId": "42",
            "received": time_within_delta(),
            "retentionDays": 90,
            "serverSampleRate": 1.0,
            "timestamp": time_within_delta(
                start, delta=timedelta(seconds=1), expect_resolution="ns"
            ),
            "traceId": "5b8efff798038103d269b633813fc60c",
        },
        {
            "attributes": {
                "boolean.attribute": {"boolValue": True},
                "double.attribute": {"doubleValue": 1.23},
                "integer.attribute": {"intValue": "42"},
                "pii": {"stringValue": "[creditcard]"},
                "sentry.body": {"stringValue": "Example log record"},
                "sentry.browser.name": {"stringValue": "Python Requests"},
                "sentry.browser.version": {"stringValue": "2.32"},
                "sentry.observed_timestamp_nanos": {
                    "stringValue": time_within(
                        start, expect_resolution="ns", precision="s"
                    )
                },
                "sentry.severity_number": {"intValue": "9"},
                "sentry.severity_text": {"stringValue": "info"},
                "sentry.span_id": {"stringValue": "eee19b7ec3c1b174"},
                "sentry.timestamp_nanos": {"stringValue": str(timestamp_nanos)},
                "sentry.timestamp_precise": {"intValue": str(timestamp_nanos)},
                "sentry.trace_flags": {"intValue": "0"},
                "string.attribute": {"stringValue": "some string"},
                "valid_string_with_other": {"stringValue": "test"},
            },
            "clientSampleRate": 1.0,
            "itemId": mock.ANY,
            "itemType": "TRACE_ITEM_TYPE_LOG",
            "organizationId": "1",
            "projectId": "42",
            "received": time_within_delta(),
            "retentionDays": 90,
            "serverSampleRate": 1.0,
            "timestamp": time_within_delta(
                start, delta=timedelta(seconds=1), expect_resolution="ns"
            ),
            "traceId": "5b8efff798038103d269b633813fc60c",
        },
    ]

    outcomes = outcomes_consumer.get_aggregated_outcomes(
        n=4 if calculated_byte_count else 2
    )
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
            # This is a billing relevant number, do not just adjust this because it changed.
            #
            # This is 'fuzzy' for the non-calculated outcome, as timestamps do not have a constant size.
            "quantity": (
                260 if calculated_byte_count else matches(lambda x: 2470 <= x <= 2480)
            ),
        },
    ]

    ourlogs_consumer.assert_empty()


def test_ourlog_extraction_with_sentry_logs_with_missing_fields(
    mini_sentry,
    relay_with_processing,
    ourlogs_consumer,
):
    ourlogs_consumer = ourlogs_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]
    relay = relay_with_processing(options=TEST_CONFIG)
    start = datetime.now(timezone.utc)
    timestamp = start.timestamp()
    timestamp_nanos = int(timestamp * 1e6) * 1000
    envelope = envelope_with_sentry_logs(
        {
            "timestamp": timestamp,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "level": "warn",
            "body": "Example log record 2",
        }
    )

    relay.send_envelope(project_id, envelope)

    logs = [MessageToDict(log) for log in ourlogs_consumer.get_ourlogs()]
    assert logs == [
        {
            "attributes": {
                "sentry.body": {"stringValue": "Example log record 2"},
                "sentry.browser.name": {"stringValue": "Python Requests"},
                "sentry.browser.version": {"stringValue": "2.32"},
                "sentry.observed_timestamp_nanos": {
                    "stringValue": time_within(
                        start, expect_resolution="ns", precision="s"
                    )
                },
                "sentry.severity_number": {"intValue": "13"},
                "sentry.severity_text": {"stringValue": "warn"},
                "sentry.timestamp_nanos": {"stringValue": str(timestamp_nanos)},
                "sentry.timestamp_precise": {"intValue": str(timestamp_nanos)},
                "sentry.trace_flags": {"intValue": "0"},
            },
            "clientSampleRate": 1.0,
            "itemId": mock.ANY,
            "itemType": "TRACE_ITEM_TYPE_LOG",
            "organizationId": "1",
            "projectId": "42",
            "received": time_within_delta(),
            "retentionDays": 90,
            "serverSampleRate": 1.0,
            "timestamp": time_within_delta(
                start, delta=timedelta(seconds=1), expect_resolution="ns"
            ),
            "traceId": "5b8efff798038103d269b633813fc60c",
        },
    ]

    ourlogs_consumer.assert_empty()


def test_ourlog_extraction_is_disabled_without_feature(
    mini_sentry,
    relay_with_processing,
    ourlogs_consumer,
):
    ourlogs_consumer = ourlogs_consumer()
    relay = relay_with_processing(options=TEST_CONFIG)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = []

    start = datetime.now(timezone.utc)
    timestamp_nanos = str(int(start.timestamp() * 1e9))
    envelope = envelope_with_otel_logs(str(timestamp_nanos))

    relay.send_envelope(project_id, envelope)

    ourlogs = ourlogs_consumer.get_ourlogs()

    assert len(ourlogs) == 0
    ourlogs_consumer.assert_empty()


@pytest.mark.parametrize(
    "user_agent,expected_browser_name,expected_browser_version",
    [
        # Chrome desktop
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Chrome",
            "131.0.0",
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Chrome",
            "120.0.0",
        ),
        # Firefox desktop
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Firefox",
            "121.0",
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Firefox",
            "120.0",
        ),
        # Safari desktop
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Safari",
            "17.1",
        ),
        # Edge desktop
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
            "Edge",
            "120.0.0",
        ),
        # Chrome mobile
        (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/119.0.6045.169 Mobile/15E148 Safari/604.1",
            "Chrome Mobile iOS",
            "119.0.6045",
        ),
        (
            "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
            "Chrome Mobile",
            "119.0.0",
        ),
        # Safari mobile
        (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
            "Mobile Safari",
            "17.1",
        ),
    ],
)
def test_browser_name_version_extraction(
    mini_sentry,
    relay_with_processing,
    ourlogs_consumer,
    user_agent,
    expected_browser_name,
    expected_browser_version,
):
    ourlogs_consumer = ourlogs_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]
    relay = relay_with_processing(options=TEST_CONFIG)
    start = datetime.now(timezone.utc)
    timestamp = start.timestamp()
    timestamp_nanos = int(timestamp * 1e6) * 1000
    envelope = envelope_with_sentry_logs(
        {
            "timestamp": timestamp,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "level": "error",
            "body": "This is really bad",
        },
    )

    relay.send_envelope(project_id, envelope, headers={"User-Agent": user_agent})

    logs = [MessageToDict(log) for log in ourlogs_consumer.get_ourlogs()]

    assert logs == [
        {
            "attributes": {
                "sentry.body": {"stringValue": "This is really bad"},
                "sentry.browser.name": {"stringValue": expected_browser_name},
                "sentry.browser.version": {"stringValue": expected_browser_version},
                "sentry.observed_timestamp_nanos": {
                    "stringValue": time_within(
                        start, expect_resolution="ns", precision="s"
                    )
                },
                "sentry.severity_number": {"intValue": "17"},
                "sentry.severity_text": {"stringValue": "error"},
                "sentry.span_id": {"stringValue": "eee19b7ec3c1b175"},
                "sentry.timestamp_nanos": {"stringValue": str(timestamp_nanos)},
                "sentry.timestamp_precise": {"intValue": str(timestamp_nanos)},
                "sentry.trace_flags": {"intValue": "0"},
            },
            "clientSampleRate": 1.0,
            "itemId": mock.ANY,
            "itemType": "TRACE_ITEM_TYPE_LOG",
            "organizationId": "1",
            "projectId": "42",
            "received": time_within_delta(),
            "retentionDays": 90,
            "serverSampleRate": 1.0,
            "timestamp": time_within_delta(
                start, delta=timedelta(seconds=1), expect_resolution="ns"
            ),
            "traceId": "5b8efff798038103d269b633813fc60c",
        }
    ]

    ourlogs_consumer.assert_empty()
