from datetime import datetime, timezone, timedelta
from unittest import mock

from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_relay.consts import DataCategory

from .asserts import time_within_delta


TEST_CONFIG = {
    "outcomes": {
        "emit_outcomes": True,
    },
}


def envelope_with_trace_metrics(*payloads: dict) -> Envelope:
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="trace_metric",
            payload=PayloadRef(json={"items": payloads}),
            content_type="application/vnd.sentry.items.trace-metric+json",
            headers={"item_count": len(payloads)},
        )
    )
    return envelope


def test_trace_metric_extraction(
    mini_sentry,
    relay,
    relay_with_processing,
    items_consumer,
    outcomes_consumer,
):
    items_consumer = items_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:tracemetrics-ingestion",
    ]
    project_config["config"]["retentions"] = {
        "trace_metric": {"standard": 30, "downsampled": 13 * 30},
    }

    relay = relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG)
    start = datetime.now(timezone.utc)

    payload = {
        "timestamp": start.timestamp(),
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "span_id": "eee19b7ec3c1b175",
        "name": "http.request.duration",
        "type": "distribution",
        "value": 123.45,
        "attributes": {
            "http.method": {"value": "GET", "type": "string"},
            "http.status_code": {"value": 200, "type": "integer"},
        },
    }

    envelope = envelope_with_trace_metrics(payload)
    relay.send_envelope(project_id, envelope)

    item = items_consumer.get_item()
    assert item == {
        "attributes": {
            "sentry.metric_name": {"stringValue": "http.request.duration"},
            "sentry.metric_type": {"stringValue": "distribution"},
            "sentry.value": {"doubleValue": 123.45},
            "sentry.timestamp_precise": {
                "intValue": time_within_delta(
                    start,
                    delta=timedelta(seconds=0),
                    expect_resolution="ns",
                    precision="us",
                )
            },
            "sentry.observed_timestamp_nanos": {
                "stringValue": time_within_delta(
                    start,
                    delta=timedelta(seconds=2),
                    expect_resolution="ns",
                    precision="us",
                )
            },
            "sentry.span_id": {"stringValue": "eee19b7ec3c1b175"},
            "sentry.browser.name": {"stringValue": mock.ANY},
            "sentry.browser.version": {"stringValue": mock.ANY},
            "http.method": {"stringValue": "GET"},
            "http.status_code": {"intValue": "200"},
            "sentry._internal.cooccuring.name.http.request.duration": {
                "boolValue": True
            },
            "sentry._internal.cooccuring.type.distribution": {"boolValue": True},
        },
        "clientSampleRate": 1.0,
        "downsampledRetentionDays": 390,
        "itemId": mock.ANY,
        "itemType": "TRACE_ITEM_TYPE_METRIC",
        "organizationId": "1",
        "projectId": "42",
        "received": time_within_delta(),
        "retentionDays": 30,
        "serverSampleRate": 1.0,
        "timestamp": time_within_delta(start, expect_resolution="ns"),
        "traceId": "5b8efff798038103d269b633813fc60c",
    }

    outcomes = outcomes_consumer.get_aggregated_outcomes(n=1)
    assert outcomes == [
        {
            "category": DataCategory.TRACE_METRIC.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 1,
        }
    ]


def test_trace_metric_validation(
    mini_sentry,
    relay,
    relay_with_processing,
    items_consumer,
    outcomes_consumer,
):
    items_consumer = items_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:tracemetrics-ingestion",
    ]

    relay = relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG)
    start = datetime.now(timezone.utc)

    # Missing required field type
    invalid_payload = {
        "timestamp": start.timestamp(),
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "name": "test.metric",
        "value": 1.0,
    }

    envelope = envelope_with_trace_metrics(invalid_payload)
    relay.send_envelope(project_id, envelope)

    outcomes = outcomes_consumer.get_aggregated_outcomes(n=1)
    assert outcomes == [
        {
            "category": DataCategory.TRACE_METRIC.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 1,
            "reason": "invalid_trace_metric",
        }
    ]


def test_trace_metric_pii_scrubbing(
    mini_sentry,
    relay,
    relay_with_processing,
    items_consumer,
    outcomes_consumer,
):
    items_consumer = items_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:tracemetrics-ingestion",
    ]
    project_config["config"]["piiConfig"] = {
        "rules": {"strip_ips": {"type": "ip", "redaction": {"method": "remove"}}},
        "applications": {"**": ["strip_ips"]},
    }

    relay = relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG)
    start = datetime.now(timezone.utc)

    payload = {
        "timestamp": start.timestamp(),
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "name": "test.metric",
        "type": "counter",
        "value": 1.0,
        "attributes": {
            "user.ip": {"value": "192.168.1.1", "type": "string"},
            "safe.attribute": {"value": "keep this", "type": "string"},
        },
    }

    envelope = envelope_with_trace_metrics(payload)
    relay.send_envelope(project_id, envelope)

    item = items_consumer.get_item()
    assert item == {
        "attributes": {
            "sentry.metric_name": {"stringValue": "test.metric"},
            "sentry.metric_type": {"stringValue": "counter"},
            "sentry.value": {"doubleValue": 1.0},
            "sentry.timestamp_precise": {
                "intValue": time_within_delta(
                    start,
                    delta=timedelta(seconds=0),
                    expect_resolution="ns",
                    precision="us",
                )
            },
            "sentry.observed_timestamp_nanos": {
                "stringValue": time_within_delta(
                    start,
                    delta=timedelta(seconds=2),
                    expect_resolution="ns",
                    precision="us",
                )
            },
            "sentry.browser.name": {"stringValue": mock.ANY},
            "sentry.browser.version": {"stringValue": mock.ANY},
            "safe.attribute": {"stringValue": "keep this"},
            "user.ip": {"stringValue": ""},
            "sentry._meta.fields.attributes.user.ip": {
                "stringValue": '{"meta":{"value":{"":{"rem":[["strip_ips","x",0,0]],"len":11}}}}'
            },
            "sentry._internal.cooccuring.name.test.metric": {"boolValue": True},
            "sentry._internal.cooccuring.type.counter": {"boolValue": True},
        },
        "clientSampleRate": 1.0,
        "downsampledRetentionDays": 90,
        "itemId": mock.ANY,
        "itemType": "TRACE_ITEM_TYPE_METRIC",
        "organizationId": "1",
        "projectId": "42",
        "received": time_within_delta(),
        "retentionDays": 90,
        "serverSampleRate": 1.0,
        "timestamp": time_within_delta(start, expect_resolution="ns"),
        "traceId": "5b8efff798038103d269b633813fc60c",
    }

    outcomes = outcomes_consumer.get_aggregated_outcomes(n=1)
    assert outcomes == [
        {
            "category": DataCategory.TRACE_METRIC.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 1,
        }
    ]
