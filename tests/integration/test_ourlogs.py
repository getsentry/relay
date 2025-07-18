import json

from datetime import datetime, timezone, timedelta
from unittest import mock

from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_relay.consts import DataCategory

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


def envelope_with_otel_logs(ts: datetime) -> Envelope:
    envelope = Envelope()

    timestamp_nanos = int(ts.timestamp() * 1_000_000_000)
    envelope.add_item(
        Item(
            type="otel_log",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "timeUnixNano": str(timestamp_nanos),
                        "observedTimeUnixNano": str(timestamp_nanos),
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


def timestamps(ts: datetime):
    return {
        "sentry.observed_timestamp_nanos": {
            "stringValue": time_within(ts, expect_resolution="ns", precision="s")
        },
        "sentry.timestamp_nanos": {
            "stringValue": time_within_delta(
                ts, delta=timedelta(seconds=0), expect_resolution="ns", precision="us"
            )
        },
        "sentry.timestamp_precise": {
            "intValue": time_within_delta(
                ts, delta=timedelta(seconds=0), expect_resolution="ns", precision="us"
            )
        },
    }


def test_ourlog_extraction_with_otel_logs(
    mini_sentry,
    relay_with_processing,
    items_consumer,
):
    items_consumer = items_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]
    relay = relay_with_processing(options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    envelope = envelope_with_otel_logs(ts)

    relay.send_envelope(project_id, envelope)

    assert items_consumer.get_item() == {
        "attributes": {
            "boolean.attribute": {"boolValue": True},
            "double.attribute": {"doubleValue": 637.704},
            "int.attribute": {"intValue": "10"},
            "sentry.body": {"stringValue": "Example log record"},
            "sentry.browser.name": {"stringValue": "Python Requests"},
            "sentry.browser.version": {"stringValue": "2.32"},
            "sentry.severity_number": {"intValue": "10"},
            "sentry.severity_text": {"stringValue": "Information"},
            "sentry.span_id": {"stringValue": "eee19b7ec3c1b174"},
            "sentry.trace_flags": {"intValue": "0"},
            "string.attribute": {"stringValue": "some string"},
            **timestamps(ts),
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
            ts, delta=timedelta(seconds=1), expect_resolution="ns"
        ),
        "traceId": "5b8efff798038103d269b633813fc60c",
    }


def test_ourlog_multiple_containers_not_allowed(
    mini_sentry,
    relay_with_processing,
    items_consumer,
    outcomes_consumer,
):
    items_consumer = items_consumer()
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
            "quantity": matches(lambda x: 300 < x < 400),
            "reason": "duplicate_item",
        },
    ]


@pytest.mark.parametrize("meta_enabled", [False, True])
def test_ourlog_meta_attributes(
    mini_sentry,
    relay_with_processing,
    items_consumer,
    meta_enabled,
):
    items_consumer = items_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["datascrubbingSettings"] = {
        "scrubData": True,
    }
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
        "organizations:ourlogs-calculated-byte-count",
        "organizations:ourlogs-meta-attributes" if meta_enabled else "",
    ]

    relay = relay_with_processing(options=TEST_CONFIG)
    ts = datetime.now(timezone.utc)

    envelope = envelope_with_sentry_logs(
        {
            "timestamp": ts.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "level": "error",
            "body": "oops, not again",
            "attributes": {
                "creditcard": {
                    "value": "4242424242424242",
                    "type": "string",
                }
            },
        }
    )

    relay.send_envelope(project_id, envelope)

    assert items_consumer.get_item() == {
        "attributes": {
            "creditcard": {"stringValue": "[creditcard]"},
            "sentry.body": {"stringValue": "oops, not again"},
            "sentry.browser.name": {"stringValue": "Python Requests"},
            "sentry.browser.version": {"stringValue": "2.32"},
            "sentry.severity_number": {"intValue": "17"},
            "sentry.severity_text": {"stringValue": "error"},
            "sentry.span_id": {"stringValue": "eee19b7ec3c1b175"},
            "sentry.trace_flags": {"intValue": "0"},
            **timestamps(ts),
            **(
                {
                    "sentry._meta.fields.attributes.creditcard": {
                        "stringValue": '{"meta":{"value":{"":{"rem":[["@creditcard","s",0,12]],"len":16}}}}'
                    }
                }
                if meta_enabled
                else {}
            ),
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
            ts, delta=timedelta(seconds=1), expect_resolution="ns"
        ),
        "traceId": "5b8efff798038103d269b633813fc60c",
    }


@pytest.mark.parametrize("calculated_byte_count", [False, True])
def test_ourlog_extraction_with_sentry_logs(
    mini_sentry,
    relay,
    relay_with_processing,
    items_consumer,
    outcomes_consumer,
    calculated_byte_count,
):
    items_consumer = items_consumer()
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
    ts = datetime.now(timezone.utc)

    envelope = envelope_with_sentry_logs(
        {
            "timestamp": ts.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "level": "error",
            "body": "This is really bad",
        },
        {
            "timestamp": ts.timestamp(),
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

    assert items_consumer.get_items(n=2) == [
        {
            "attributes": {
                "sentry.body": {"stringValue": "This is really bad"},
                "sentry.browser.name": {"stringValue": "Python Requests"},
                "sentry.browser.version": {"stringValue": "2.32"},
                "sentry.severity_number": {"intValue": "17"},
                "sentry.severity_text": {"stringValue": "error"},
                "sentry.span_id": {"stringValue": "eee19b7ec3c1b175"},
                "sentry.trace_flags": {"intValue": "0"},
                **timestamps(ts),
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
                ts, delta=timedelta(seconds=1), expect_resolution="ns"
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
                "sentry.severity_number": {"intValue": "9"},
                "sentry.severity_text": {"stringValue": "info"},
                "sentry.span_id": {"stringValue": "eee19b7ec3c1b174"},
                "sentry.trace_flags": {"intValue": "0"},
                "string.attribute": {"stringValue": "some string"},
                "valid_string_with_other": {"stringValue": "test"},
                **timestamps(ts),
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
                ts, delta=timedelta(seconds=1), expect_resolution="ns"
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


def test_ourlog_extraction_with_sentry_logs_with_missing_fields(
    mini_sentry,
    relay_with_processing,
    items_consumer,
):
    items_consumer = items_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]
    relay = relay_with_processing(options=TEST_CONFIG)
    ts = datetime.now(timezone.utc)

    envelope = envelope_with_sentry_logs(
        {
            "timestamp": ts.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "level": "warn",
            "body": "Example log record 2",
        }
    )

    relay.send_envelope(project_id, envelope)

    assert items_consumer.get_item() == {
        "attributes": {
            "sentry.body": {"stringValue": "Example log record 2"},
            "sentry.browser.name": {"stringValue": "Python Requests"},
            "sentry.browser.version": {"stringValue": "2.32"},
            "sentry.severity_number": {"intValue": "13"},
            "sentry.severity_text": {"stringValue": "warn"},
            "sentry.trace_flags": {"intValue": "0"},
            **timestamps(ts),
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
            ts, delta=timedelta(seconds=1), expect_resolution="ns"
        ),
        "traceId": "5b8efff798038103d269b633813fc60c",
    }


def test_ourlog_extraction_is_disabled_without_feature(
    mini_sentry,
    relay_with_processing,
    items_consumer,
):
    items_consumer = items_consumer()
    relay = relay_with_processing(options=TEST_CONFIG)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = []

    envelope = envelope_with_otel_logs(datetime.now(timezone.utc))
    relay.send_envelope(project_id, envelope)

    items_consumer.assert_empty()


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
    items_consumer,
    user_agent,
    expected_browser_name,
    expected_browser_version,
):
    items_consumer = items_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]
    relay = relay_with_processing(options=TEST_CONFIG)
    ts = datetime.now(timezone.utc)

    envelope = envelope_with_sentry_logs(
        {
            "timestamp": ts.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "level": "error",
            "body": "This is really bad",
        },
    )

    relay.send_envelope(project_id, envelope, headers={"User-Agent": user_agent})

    assert items_consumer.get_item() == {
        "attributes": {
            "sentry.body": {"stringValue": "This is really bad"},
            "sentry.browser.name": {"stringValue": expected_browser_name},
            "sentry.browser.version": {"stringValue": expected_browser_version},
            "sentry.severity_number": {"intValue": "17"},
            "sentry.severity_text": {"stringValue": "error"},
            "sentry.span_id": {"stringValue": "eee19b7ec3c1b175"},
            "sentry.trace_flags": {"intValue": "0"},
            **timestamps(ts),
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
            ts, delta=timedelta(seconds=1), expect_resolution="ns"
        ),
        "traceId": "5b8efff798038103d269b633813fc60c",
    }
