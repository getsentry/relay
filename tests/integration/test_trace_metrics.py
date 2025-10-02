import json

from datetime import datetime, timezone, timedelta
from unittest import mock

from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_relay.consts import DataCategory

from .asserts import time_within_delta, time_within, matches

import pytest


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


def envelope_with_trace_metrics(*payloads: dict) -> Envelope:
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="metric",
            payload=PayloadRef(json={"items": payloads}),
            content_type="application/vnd.sentry.items.metric+json",
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
            "http.status_code": {"value": "200", "type": "integer"},
        },
    }

    envelope = envelope_with_trace_metrics(payload)
    relay.send_envelope(project_id, envelope)

    item = items_consumer.get_item()
    assert item["itemType"] == 8  # TRACE_ITEM_TYPE_METRIC

    attributes = item["attributes"]
    assert attributes["sentry.metric_name"]["stringValue"] == "http.request.duration"
    assert attributes["sentry.metric_type"]["stringValue"] == "distribution"
    assert attributes["sentry.value"]["doubleValue"] == 123.45
    assert attributes["http.method"]["stringValue"] == "GET"
    assert attributes["http.status_code"]["stringValue"] == "200"

    outcomes = outcomes_consumer.get_outcomes()
    assert len(outcomes) == 1
    assert outcomes[0]["category"] == DataCategory.TRACE_METRIC.value


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

    # Should get an outcome for invalid metric
    outcomes = outcomes_consumer.get_outcomes()
    assert len(outcomes) == 1
    assert outcomes[0]["category"] == DataCategory.TRACE_METRIC.value
    assert outcomes[0]["outcome"] == 3  # Invalid
    assert outcomes[0]["reason"] == "invalid_trace_metric"


def test_trace_metric_pii_scrubbing(
    mini_sentry,
    relay,
    relay_with_processing,
    items_consumer,
):
    items_consumer = items_consumer()
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
    assert item["itemType"] == 8  # TRACE_ITEM_TYPE_METRIC

    attributes = item["attributes"]
    assert (
        "user.ip" not in attributes
        or attributes.get("user.ip", {}).get("stringValue") == ""
    )
    assert attributes["safe.attribute"]["stringValue"] == "keep this"
