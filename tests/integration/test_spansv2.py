import json

from datetime import datetime, timezone, timedelta
from unittest import mock

from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_relay.consts import DataCategory

from .asserts import time_within_delta, time_within, matches

from .test_dynamic_sampling import _add_sampling_config

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


def envelope_with_spans(*payloads: dict, trace_info=None) -> Envelope:
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(json={"items": payloads}),
            content_type="application/vnd.sentry.items.span.v2+json",
            headers={"item_count": len(payloads)},
        )
    )
    if trace_info:
        envelope.headers["trace"] = trace_info
    return envelope


def test_spansv2_basic(spans_consumer, mini_sentry, relay, relay_with_processing):
    spans_consumer = spans_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
    ]

    relay = relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.500,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "name": "some op",
            "attributes": {"foo": {"value": "bar", "type": "string"}},
        }
    )

    relay.send_envelope(project_id, envelope)

    assert spans_consumer.get_span() == {
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "span_id": "eee19b7ec3c1b175",
        "data": {"foo": "bar", "sentry.name": "some op"},
        "description": "some op",
        "received": time_within(ts),
        "start_timestamp_ms": time_within(ts, precision="ms", expect_resolution="ms"),
        "start_timestamp_precise": time_within(ts),
        "end_timestamp_precise": time_within(ts),
        "duration_ms": 500,
        "exclusive_time_ms": 500.0,
        "is_remote": False,
        "is_segment": False,
        "retention_days": 90,
        "downsampled_retention_days": 90,
        "key_id": 123,
        "organization_id": 1,
        "project_id": 42,
    }


@pytest.mark.parametrize(
    "rule_type",
    ["transaction", "trace"],
)
def test_spansv2_ds_drop(mini_sentry, relay, rule_type):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
    ]
    _add_sampling_config(project_config, sample_rate=0, rule_type=rule_type)

    relay = relay(mini_sentry, options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.500,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "name": "some op",
            "attributes": {"foo": {"value": "bar", "type": "string"}},
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )

    relay.send_envelope(project_id, envelope)

    assert mini_sentry.captured_outcomes.get(timeout=3).get("outcomes") == [
        {
            "category": DataCategory.SPAN_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 1,
            "project_id": 42,
            "quantity": 1,
            "reason": "Sampled:0",
            "timestamp": time_within_delta(),
        },
    ]
    # TODO: assert span metrics when implemented (usage and count per root)

    assert mini_sentry.captured_events.empty()
    assert mini_sentry.captured_outcomes.empty()
