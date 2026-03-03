from datetime import datetime, timezone, timedelta
from unittest import mock

from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_relay.consts import DataCategory

from .asserts import time_within_delta, time_within, time_is

from .test_dynamic_sampling import add_sampling_config

import uuid
import json
import pytest

TEST_CONFIG = {
    "outcomes": {
        "emit_outcomes": True,
    }
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
    envelope.headers["trace"] = trace_info
    return envelope


@pytest.mark.parametrize(
    "eap_span_outcomes_rollout_rate",
    [
        pytest.param(0.0, id="relay_emits_accepted_outcome"),
        pytest.param(1.0, id="eap_emits_accepted_outcome"),
    ],
)
def test_spansv2_basic(
    mini_sentry,
    relay,
    relay_with_processing,
    spans_consumer,
    metrics_consumer,
    outcomes_consumer,
    eap_span_outcomes_rollout_rate,
):
    """
    A basic test making sure spans can be ingested and have basic normalizations applied.
    """
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()
    outcomes_consumer = outcomes_consumer()

    mini_sentry.global_config["options"][
        "relay.eap-span-outcomes.rollout-rate"
    ] = eap_span_outcomes_rollout_rate
    relay_emits_accepted_outcome = eap_span_outcomes_rollout_rate == 0.0

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"].update(
        {
            "features": [
                "organizations:standalone-span-ingestion",
                "projects:span-v2-experimental-processing",
            ],
            "retentions": {"span": {"standard": 42, "downsampled": 1337}},
        }
    )

    relay = relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "is_segment": True,
            "name": "some op",
            "status": "ok",
            "attributes": {
                "foo": {"value": "bar", "type": "string"},
                "array": {"value": ["foo", "bar"], "type": "array"},
                "valid_int": {"value": 9223372036854775807, "type": "integer"},
                "invalid_int": {"value": 9223372036854775808, "type": "integer"},
                "invalid": {"value": True, "type": "string"},
                "http.response_content_length": {"value": 17, "type": "integer"},
            },
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": project_config["publicKeys"][0]["publicKey"],
            "release": "foo@1.0",
            "environment": "prod",
            "transaction": "/my/fancy/endpoint",
        },
    )

    relay.send_envelope(project_id, envelope)

    assert spans_consumer.get_span() == {
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "span_id": "eee19b7ec3c1b175",
        "attributes": {
            "array": {"type": "array", "value": ["foo", "bar"]},
            "foo": {"type": "string", "value": "bar"},
            "http.response_content_length": {"value": 17, "type": "integer"},
            "http.response.body.size": {"value": 17, "type": "integer"},
            "valid_int": {"value": 9223372036854775807, "type": "integer"},
            "invalid_int": None,
            "invalid": None,
            "sentry.browser.name": {"type": "string", "value": "Python Requests"},
            "sentry.browser.version": {"type": "string", "value": "2.32"},
            "sentry.dsc.environment": {"type": "string", "value": "prod"},
            "sentry.dsc.public_key": {
                "type": "string",
                "value": project_config["publicKeys"][0]["publicKey"],
            },
            "sentry.dsc.release": {"type": "string", "value": "foo@1.0"},
            "sentry.dsc.transaction": {"type": "string", "value": "/my/fancy/endpoint"},
            "sentry.dsc.trace_id": {
                "type": "string",
                "value": "5b8efff798038103d269b633813fc60c",
            },
            "sentry.observed_timestamp_nanos": {
                "type": "string",
                "value": time_within(ts, expect_resolution="ns"),
            },
            "sentry.op": {"type": "string", "value": "default"},
        },
        "_meta": {
            "attributes": {
                "invalid_int": {
                    "": {
                        "err": ["invalid_data"],
                        "val": {"type": "integer", "value": 9223372036854775808},
                    }
                },
                "invalid": {
                    "": {
                        "err": ["invalid_data"],
                        "val": {"type": "string", "value": True},
                    }
                },
            }
        },
        "name": "some op",
        "received": time_within(ts),
        "start_timestamp": time_is(ts),
        "end_timestamp": time_is(ts.timestamp() + 0.5),
        "is_segment": True,
        "status": "ok",
        "retention_days": 42,
        "accepted_outcome_emitted": relay_emits_accepted_outcome,
        "downsampled_retention_days": 1337,
        "key_id": 123,
        "organization_id": 1,
        "project_id": 42,
    }

    assert metrics_consumer.get_metrics(n=2, with_headers=False) == [
        {
            "name": "c:spans/count_per_root_project@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within(ts, precision="s"),
            "retention_days": 90,
            "tags": {
                "decision": "keep",
                "is_segment": "true",
                "target_project_id": "42",
                "transaction": "/my/fancy/endpoint",
            },
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
        {
            "name": "c:spans/usage@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within(ts, precision="s"),
            "retention_days": 90,
            "tags": {
                "was_transaction": "false",
                "is_segment": "true",
            },
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
    ]

    if relay_emits_accepted_outcome:
        assert outcomes_consumer.get_aggregated_outcomes() == [
            {
                "category": DataCategory.SPAN_INDEXED.value,
                "key_id": 123,
                "org_id": 1,
                "outcome": 0,
                "project_id": 42,
                "quantity": 1,
            }
        ]


def test_spansv2_trimming_basic(
    mini_sentry,
    relay,
    relay_with_processing,
    spans_consumer,
    metrics_consumer,
):
    """
    An adaptation of `test_spansv2_basic` that has a size limit for spans and attributes large enough
    to demonstrate that trimming works.
    """
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"].update(
        {
            "features": [
                "organizations:standalone-span-ingestion",
                "projects:span-v2-experimental-processing",
            ],
            "retentions": {"span": {"standard": 42, "downsampled": 1337}},
            # This is sufficient for all builtin attributes not
            # to be trimmed. The span fields that aren't trimmed
            # also still count for the size limit.
            "trimming": {"span": {"maxSize": 453}},
        }
    )

    config = {
        "limits": {
            "max_removed_attribute_key_size": 30,
        },
        **TEST_CONFIG,
    }

    relay = relay(relay_with_processing(options=config), options=config)

    ts = datetime.now(timezone.utc)
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "is_segment": True,
            "name": "some op",
            "status": "ok",
            "attributes": {
                "custom.string.attribute": {
                    "value": "This is actually a pretty long string",
                    "type": "string",
                },
                # This attribute will get trimmed in the middle of the third string.
                "custom.array.attribute": {
                    "value": [
                        "A string",
                        "Another longer string",
                        "Yet another string",
                    ],
                    "type": "array",
                },
                "custom.invalid.attribute": {"value": True, "type": "string"},
                # This attribute will be removed because the `max_removed_attribute_key_bytes` (30B)
                # is already consumed by the previous invalid attribute
                "second.custom.invalid.attribute": {"value": None, "type": "integer"},
            },
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": project_config["publicKeys"][0]["publicKey"],
            "release": "foo@1.0",
            "environment": "prod",
            "transaction": "/my/fancy/endpoint",
        },
    )

    relay.send_envelope(project_id, envelope)

    span = spans_consumer.get_span()

    assert span == {
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "span_id": "eee19b7ec3c1b175",
        "attributes": {
            "custom.array.attribute": {
                "type": "array",
                "value": ["A string", "Another longer string", "Yet anothe..."],
            },
            "custom.string.attribute": {
                "type": "string",
                "value": "This is actually a pretty long string",
            },
            "custom.invalid.attribute": None,
            "sentry.browser.name": {"type": "string", "value": "Python Requests"},
            "sentry.browser.version": {"type": "string", "value": "2.32"},
            "sentry.dsc.environment": {"type": "string", "value": "prod"},
            "sentry.dsc.public_key": {
                "type": "string",
                "value": project_config["publicKeys"][0]["publicKey"],
            },
            "sentry.dsc.release": {"type": "string", "value": "foo@1.0"},
            "sentry.dsc.transaction": {"type": "string", "value": "/my/fancy/endpoint"},
            "sentry.dsc.trace_id": {
                "type": "string",
                "value": "5b8efff798038103d269b633813fc60c",
            },
            "sentry.observed_timestamp_nanos": {
                "type": "string",
                "value": time_within(ts, expect_resolution="ns"),
            },
            "sentry.op": {"type": "string", "value": "default"},
        },
        "_meta": {
            "attributes": {
                "": {"len": 505},
                "custom.array.attribute": {
                    "value": {
                        "2": {
                            "": {
                                "len": 18,
                                "rem": [
                                    [
                                        "!limit",
                                        "s",
                                        10,
                                        13,
                                    ],
                                ],
                            },
                        },
                    },
                },
                "custom.invalid.attribute": {
                    "": {
                        "err": ["invalid_data"],
                        "val": {"type": "string", "value": True},
                    }
                },
            }
        },
        "name": "some op",
        "received": time_within(ts),
        "start_timestamp": time_is(ts),
        "end_timestamp": time_is(ts.timestamp() + 0.5),
        "is_segment": True,
        "status": "ok",
        "retention_days": 42,
        "accepted_outcome_emitted": True,
        "downsampled_retention_days": 1337,
        "key_id": 123,
        "organization_id": 1,
        "project_id": 42,
    }

    assert metrics_consumer.get_metrics(n=2, with_headers=False) == [
        {
            "name": "c:spans/count_per_root_project@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within(ts, precision="s"),
            "retention_days": 90,
            "tags": {
                "decision": "keep",
                "is_segment": "true",
                "target_project_id": "42",
                "transaction": "/my/fancy/endpoint",
            },
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
        {
            "name": "c:spans/usage@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within(ts, precision="s"),
            "retention_days": 90,
            "tags": {
                "was_transaction": "false",
                "is_segment": "true",
            },
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
    ]


@pytest.mark.parametrize(
    "rule_type",
    ["project", "trace"],
)
def test_spansv2_ds_drop(mini_sentry, relay, rule_type):
    """
    The test asserts that dynamic sampling correctly drops items, based on different rule types
    and makes sure the correct outcomes and metrics are emitted.
    """
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
    ]
    # A transaction rule should never apply.
    add_sampling_config(project_config, sample_rate=1, rule_type="transaction")
    # Setup the actual rule we want to test against.
    add_sampling_config(project_config, sample_rate=0, rule_type=rule_type)

    relay = relay(mini_sentry, options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "is_segment": False,
            "name": "some op",
            "attributes": {"foo": {"value": "bar", "type": "string"}},
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": project_config["publicKeys"][0]["publicKey"],
            "transaction": "tx_from_root",
        },
    )

    # Add legacy span to ensure that the v2 sampling deals with them correctly.
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(
                json={
                    "start_timestamp": ts.timestamp(),
                    "timestamp": ts.timestamp() + 0.5,
                    "trace_id": "5b8efff798038103d269b633813fc60c",
                    "span_id": "eee19b7ec3c1b176",
                    "op": "some op",
                    "description": "some description",
                    "data": {"foo": "bar"},
                }
            ),
            content_type="application/json",
        )
    )

    relay.send_envelope(project_id, envelope)

    assert mini_sentry.captured_outcomes.get(timeout=5).get("outcomes") == [
        {
            "category": DataCategory.SPAN_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 1,
            "project_id": 42,
            "quantity": 2,
            "reason": "Sampled:0",
            "timestamp": time_within_delta(),
        },
    ]

    assert mini_sentry.get_metrics() == [
        {
            "metadata": mock.ANY,
            "name": "c:spans/count_per_root_project@none",
            "tags": {
                "decision": "drop",
                "is_segment": "false",
                "target_project_id": "42",
                "transaction": "tx_from_root",
            },
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 2.0,
            "width": 1,
        },
        {
            "metadata": mock.ANY,
            "name": "c:spans/usage@none",
            "tags": {
                "is_segment": "false",
            },
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 2.0,
            "width": 1,
        },
    ]

    assert mini_sentry.captured_envelopes.empty()
    assert mini_sentry.captured_outcomes.empty()


@pytest.mark.parametrize("rate_limit", [DataCategory.SPAN, DataCategory.SPAN_INDEXED])
def test_spansv2_rate_limits(mini_sentry, relay, rate_limit):
    """
    The test asserts that dynamic sampling correctly drops items, based on different rule types
    and makes sure the correct outcomes and metrics are emitted.
    """
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
    ]

    ts = datetime.now(timezone.utc)

    project_config["config"]["quotas"] = [
        {
            "categories": [rate_limit.name.lower()],
            "limit": 0,
            "window": int(ts.timestamp()),
            "id": uuid.uuid4(),
            "reasonCode": "rate_limit_exceeded",
        }
    ]

    relay = relay(mini_sentry, options=TEST_CONFIG)

    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "is_segment": True,
            "name": "some op",
            "status": "ok",
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )

    relay.send_envelope(project_id, envelope)

    assert mini_sentry.get_aggregated_outcomes() == [
        *(
            [
                {
                    "category": 12,
                    "key_id": 123,
                    "org_id": 1,
                    "outcome": 2,
                    "project_id": 42,
                    "quantity": 1,
                    "reason": "rate_limit_exceeded",
                }
            ]
            if rate_limit == DataCategory.SPAN
            else []
        ),
        {
            "category": DataCategory.SPAN_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 2,
            "project_id": 42,
            "quantity": 1,
            "reason": "rate_limit_exceeded",
        },
    ]

    if rate_limit == DataCategory.SPAN_INDEXED:
        assert mini_sentry.get_metrics() == [
            {
                "metadata": mock.ANY,
                "name": "c:spans/count_per_root_project@none",
                "tags": {
                    "decision": "keep",
                    "is_segment": "true",
                    "target_project_id": "42",
                },
                "timestamp": time_within_delta(),
                "type": "c",
                "value": 1.0,
                "width": 1,
            },
            {
                "metadata": mock.ANY,
                "name": "c:spans/usage@none",
                "tags": {
                    "was_transaction": "false",
                    "is_segment": "true",
                },
                "timestamp": time_within_delta(),
                "type": "c",
                "value": 1.0,
                "width": 1,
            },
        ]

    assert mini_sentry.captured_envelopes.empty()
    assert mini_sentry.captured_outcomes.empty()


def test_spansv2_ds_sampled(
    mini_sentry,
    relay,
    relay_with_processing,
    outcomes_consumer,
    spans_consumer,
    metrics_consumer,
):
    """
    The test asserts, that dynamic sampling correctly samples spans with the trace rules
    of the trace root's project.
    """
    outcomes_consumer = outcomes_consumer()
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
    ]
    add_sampling_config(project_config, sample_rate=0.0, rule_type="trace")

    sampling_project_id = 43
    sampling_config = mini_sentry.add_basic_project_config(sampling_project_id)
    sampling_config["organizationId"] = project_config["organizationId"]
    add_sampling_config(sampling_config, sample_rate=0.9, rule_type="trace")

    relay = relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "aaaaaaaaaaaaaaaa",
            "is_segment": False,
            "name": "some op",
            "attributes": {"foo": {"value": "bar", "type": "string"}},
            "status": "ok",
        },
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "bbbbbbbbbbbbbbbb",
            "is_segment": True,
            "name": "some other op",
            "status": "ok",
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": sampling_config["publicKeys"][0]["publicKey"],
            "transaction": "tx_from_root",
        },
    )

    relay.send_envelope(project_id, envelope)

    for span in spans_consumer.get_spans(n=2):
        assert span["span_id"] in ("aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb")
        assert span["attributes"]["sentry.server_sample_rate"]["value"] == 0.9

    assert metrics_consumer.get_metrics(n=4, with_headers=False) == [
        {
            "name": "c:spans/count_per_root_project@none",
            "org_id": 1,
            "project_id": 43,
            "received_at": time_within(ts, precision="s"),
            "retention_days": 90,
            "tags": {
                "decision": "keep",
                "is_segment": "false",
                "target_project_id": "42",
                "transaction": "tx_from_root",
            },
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
        {
            "name": "c:spans/count_per_root_project@none",
            "org_id": 1,
            "project_id": 43,
            "received_at": time_within(ts, precision="s"),
            "retention_days": 90,
            "tags": {
                "decision": "keep",
                "is_segment": "true",
                "target_project_id": "42",
                "transaction": "tx_from_root",
            },
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
        {
            "name": "c:spans/usage@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within(ts, precision="s"),
            "retention_days": 90,
            "tags": {
                "is_segment": "false",
            },
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
        {
            "name": "c:spans/usage@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within(ts, precision="s"),
            "retention_days": 90,
            "tags": {
                "was_transaction": "false",
                "is_segment": "true",
            },
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
    ]

    assert outcomes_consumer.get_aggregated_outcomes(n=2) == [
        {
            "category": DataCategory.SPAN_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 2,
        }
    ]


def test_spansv2_ds_root_in_different_org(
    mini_sentry,
    relay,
    relay_with_processing,
    outcomes_consumer,
    spans_consumer,
    metrics_consumer,
):
    """
    The test asserts that traces where the root originates from a different Sentry organization,
    correctly uses the dynamic sampling rules of the current project and emits the count_per_root metric
    into the current project.
    """
    outcomes_consumer = outcomes_consumer()
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
    ]
    add_sampling_config(project_config, sample_rate=0.0, rule_type="trace")

    sampling_project_id = 43
    sampling_config = mini_sentry.add_basic_project_config(sampling_project_id)
    sampling_config["organizationId"] = 99
    add_sampling_config(sampling_config, sample_rate=1.0, rule_type="trace")

    relay = relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "is_segment": False,
            "name": "some op",
            "attributes": {"foo": {"value": "bar", "type": "string"}},
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": sampling_config["publicKeys"][0]["publicKey"],
        },
    )

    relay.send_envelope(project_id, envelope)

    assert metrics_consumer.get_metrics(n=2, with_headers=False) == [
        {
            "name": "c:spans/count_per_root_project@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within(ts, precision="s"),
            "retention_days": 90,
            "tags": {
                "decision": "drop",
                "is_segment": "false",
                "target_project_id": "42",
            },
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
        {
            "name": "c:spans/usage@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within(ts, precision="s"),
            "retention_days": 90,
            "tags": {
                "is_segment": "false",
            },
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
    ]

    assert outcomes_consumer.get_outcome() == {
        "category": DataCategory.SPAN_INDEXED.value,
        "key_id": 123,
        "org_id": 1,
        "outcome": 1,
        "project_id": 42,
        "quantity": 1,
        "reason": "Sampled:0",
        "timestamp": time_within_delta(),
    }

    spans_consumer.assert_empty()


@pytest.mark.parametrize(
    "filter_name,filter_config,args",
    [
        pytest.param(
            "release-version",
            {"releases": {"releases": ["foobar@1.0"]}},
            {},
            id="release",
        ),
        pytest.param(
            "filtered-transaction",
            {"ignoreTransactions": {"isEnabled": True, "patterns": ["*health*"]}},
            {},
            id="transaction",
        ),
        pytest.param(
            "legacy-browsers",
            {"legacyBrowsers": {"isEnabled": True, "options": ["ie9"]}},
            {
                "user-agent": "Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)"
            },
            id="legacy-browsers",
        ),
        pytest.param(
            "web-crawlers",
            {"webCrawlers": {"isEnabled": True}},
            {
                "user-agent": "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; PerplexityBot/1.0; +https://perplexity.ai/perplexitybot)"
            },
            id="web-crawlers",
        ),
        pytest.param(
            "gen_name",
            {
                "op": "glob",
                "name": "span.name",
                "value": ["so*op"],
            },
            {},
            id="gen_name",
        ),
        pytest.param(
            "gen_attr",
            {
                "op": "gte",
                "name": "span.attributes.some_integer.value",
                "value": 123,
            },
            {},
            id="gen_attr",
        ),
    ],
)
def test_spanv2_inbound_filters(
    mini_sentry,
    relay,
    filter_name,
    filter_config,
    args,
):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
    ]

    if filter_name.startswith("gen_"):
        filter_config = {
            "generic": {
                "version": 1,
                "filters": [
                    {
                        "id": filter_name,
                        "isEnabled": True,
                        "condition": filter_config,
                    }
                ],
            }
        }

    project_config["config"]["filterSettings"] = filter_config

    relay = relay(mini_sentry, options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)

    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "is_segment": False,
            "name": "some op",
            "status": "ok",
            "attributes": {
                "some_integer": {"value": 123, "type": "integer"},
                "sentry.release": {"value": "foobar@1.0", "type": "string"},
                "sentry.segment.name": {"value": "/foo/healthz", "type": "string"},
            },
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )

    headers = None
    if user_agent := args.get("user-agent"):
        headers = {"User-Agent": user_agent}

    relay.send_envelope(project_id, envelope, headers=headers)

    assert mini_sentry.get_outcomes(2) == [
        {
            "category": DataCategory.SPAN.value,
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,  # Filtered
            "reason": filter_name,
            "quantity": 1,
            "timestamp": time_within_delta(ts),
        },
        {
            "category": DataCategory.SPAN_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 1,
            "project_id": 42,
            "quantity": 1,
            "reason": filter_name,
            "timestamp": time_within_delta(ts),
        },
    ]

    assert mini_sentry.captured_envelopes.empty()


def test_spans_v2_multiple_containers_not_allowed(
    mini_sentry,
    relay,
):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
    ]

    relay = relay(mini_sentry, options=TEST_CONFIG)
    start = datetime.now(timezone.utc)
    envelope = Envelope()

    payload = {
        "start_timestamp": start.timestamp(),
        "end_timestamp": start.timestamp() + 0.500,
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "span_id": "eee19b7ec3c1b175",
        "name": "some op",
        "is_segment": False,
        "status": "ok",
    }
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(json={"items": [payload]}),
            content_type="application/vnd.sentry.items.span.v2+json",
            headers={"item_count": 1},
        )
    )
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(json={"items": [payload, payload]}),
            content_type="application/vnd.sentry.items.span.v2+json",
            headers={"item_count": 2},
        )
    )

    relay.send_envelope(project_id, envelope)

    assert mini_sentry.get_outcomes(2) == [
        {
            "category": DataCategory.SPAN.value,
            "timestamp": time_within_delta(),
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 3,
            "reason": "duplicate_item",
        },
        {
            "category": DataCategory.SPAN_INDEXED.value,
            "timestamp": time_within_delta(),
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 3,
            "reason": "duplicate_item",
        },
    ]

    assert mini_sentry.captured_envelopes.empty()
    assert mini_sentry.captured_outcomes.empty()


@pytest.mark.parametrize("validation", ["missing_dsc", "invalid_dsc"])
def test_spans_v2_dsc_validations(
    mini_sentry,
    relay,
    validation,
):
    """
    Test verifies envelopes with invalid or misconfigured DSCs
    are rejected by Relay with an appropriate reason.
    """
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
    ]

    relay = relay(mini_sentry, options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "is_segment": False,
            "name": "some op",
            "status": "ok",
        },
        # Note: even this 'correct' span is rejected, as the entire envelope
        # is considered invalid.
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "33333333333333333333333333333333",
            "span_id": "eee19b7ec3c1b176",
            "is_segment": False,
            "name": "some op",
            "status": "ok",
        },
        trace_info=(
            None
            if validation == "missing_dsc"
            else {
                "trace_id": "33333333333333333333333333333333",
                "public_key": project_config["publicKeys"][0]["publicKey"],
            }
        ),
    )

    relay.send_envelope(project_id, envelope)

    assert mini_sentry.get_outcomes(2) == [
        {
            "category": DataCategory.SPAN.value,
            "timestamp": time_within_delta(),
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 2,
            "reason": validation,
        },
        {
            "category": DataCategory.SPAN_INDEXED.value,
            "timestamp": time_within_delta(),
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 2,
            "reason": validation,
        },
    ]


def test_spanv2_with_string_pii_scrubbing(
    mini_sentry,
    relay,
    scrubbing_rule,
):
    rule_type, test_value, expected_scrubbed = scrubbing_rule
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
    ]

    project_config["config"]["piiConfig"]["applications"] = {"$string": [rule_type]}

    relay = relay(mini_sentry, options=TEST_CONFIG)
    ts = datetime.now(timezone.utc)

    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "name": "Test span",
            "status": "ok",
            "is_segment": False,
            "attributes": {
                "test_pii": {"value": test_value, "type": "string"},
            },
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )

    relay.send_envelope(project_id, envelope)

    envelope = mini_sentry.get_captured_envelope()
    item_payload = json.loads(envelope.items[0].payload.bytes.decode())
    item = item_payload["items"][0]

    assert item == {
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "span_id": "eee19b7ec3c1b174",
        "attributes": {
            "test_pii": {"type": "string", "value": expected_scrubbed},
            "sentry.browser.name": {"type": "string", "value": "Python Requests"},
            "sentry.browser.version": {"type": "string", "value": "2.32"},
            "sentry.observed_timestamp_nanos": {
                "type": "string",
                "value": time_within(ts, expect_resolution="ns"),
            },
            "sentry.op": {"type": "string", "value": "default"},
        },
        "_meta": {
            "attributes": {
                "test_pii": {
                    "value": {
                        "": {
                            "len": mock.ANY,
                            "rem": [[rule_type, mock.ANY, mock.ANY, mock.ANY]],
                        }
                    }
                }
            },
        },
        "name": "Test span",
        "start_timestamp": time_is(ts),
        "end_timestamp": time_is(ts.timestamp() + 0.5),
        "is_segment": False,
        "status": "ok",
    }


def test_spanv2_default_pii_scrubbing_attributes(
    mini_sentry,
    relay,
    secret_attribute,
):
    attribute_key, attribute_value, expected_value, rule_type = secret_attribute
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
    ]
    project_config["config"].setdefault(
        "datascrubbingSettings",
        {
            "scrubData": True,
            "scrubDefaults": True,
            "scrubIpAddresses": True,
        },
    )

    relay_instance = relay(mini_sentry, options=TEST_CONFIG)
    ts = datetime.now(timezone.utc)

    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "name": "Test span",
            "status": "ok",
            "is_segment": False,
            "attributes": {
                attribute_key: {"value": attribute_value, "type": "string"},
            },
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )

    relay_instance.send_envelope(project_id, envelope)

    envelope = mini_sentry.get_captured_envelope()
    item_payload = json.loads(envelope.items[0].payload.bytes.decode())
    item = item_payload["items"][0]
    attributes = item["attributes"]

    assert attribute_key in attributes
    assert attributes[attribute_key]["value"] == expected_value
    assert "_meta" in item
    meta = item["_meta"]["attributes"][attribute_key]["value"][""]
    assert "rem" in meta
    rem_info = meta["rem"]
    assert len(rem_info) == 1
    assert rem_info[0][0] == rule_type


def test_spanv2_meta_pii_scrubbing_complex_attribute(mini_sentry, relay):
    """
    Tests PII scrubbing works as expected for arrays and in the future objects.
    """
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
    ]
    project_config["config"]["datascrubbingSettings"] = {
        "scrubData": True,
        "scrubDefaults": True,
    }

    relay = relay(mini_sentry, options=TEST_CONFIG)
    ts = datetime.now(timezone.utc)

    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "name": "Test span",
            "status": "ok",
            "is_segment": False,
            "attributes": {
                "pii_array": {
                    "value": ["normal", "4242424242424242", "other"],
                    "type": "array",
                },
            },
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )

    relay.send_envelope(project_id, envelope)

    envelope = mini_sentry.get_captured_envelope()
    item_payload = json.loads(envelope.items[0].payload.bytes.decode())
    item = item_payload["items"][0]

    assert item == {
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "span_id": "eee19b7ec3c1b174",
        "attributes": {
            "pii_array": {
                "type": "array",
                "value": ["normal", "[creditcard]", "other"],
            },
            "sentry.browser.name": {"type": "string", "value": "Python Requests"},
            "sentry.browser.version": {"type": "string", "value": "2.32"},
            "sentry.observed_timestamp_nanos": {
                "type": "string",
                "value": time_within(ts, expect_resolution="ns"),
            },
            "sentry.op": {"type": "string", "value": "default"},
        },
        "_meta": {
            "attributes": {
                "pii_array": {
                    "value": {
                        "1": {
                            "": {
                                "len": mock.ANY,
                                "rem": [["@creditcard", mock.ANY, mock.ANY, mock.ANY]],
                            }
                        }
                    }
                }
            },
        },
        "name": "Test span",
        "start_timestamp": time_is(ts),
        "end_timestamp": time_is(ts.timestamp() + 0.5),
        "is_segment": False,
        "status": "ok",
    }


def test_spansv2_attribute_normalization(
    mini_sentry,
    relay,
    relay_with_processing,
    spans_consumer,
):
    """
    A test making sure spans undergo attribute normalization after ingestion.
    """
    spans_consumer = spans_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"].update(
        {
            "features": [
                "organizations:standalone-span-ingestion",
                "projects:span-v2-experimental-processing",
            ],
            "retentions": {"span": {"standard": 42, "downsampled": 1337}},
        }
    )

    relay = relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)

    db_span_id = "eee19b7ec3c1b174"
    db_span = {
        "start_timestamp": ts.timestamp(),
        "end_timestamp": ts.timestamp() + 0.5,
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "span_id": db_span_id,
        "is_segment": False,
        "name": "Test span",
        "status": "ok",
        "attributes": {
            "db.system.name": {"value": "mysql", "type": "string"},
            "db.operation.name": {"value": "SELECT", "type": "string"},
            "db.query.text": {
                "value": "SELECT id FROM users WHERE id = 1 AND name = 'Test'",
                "type": "string",
            },
            "db.collection.name": {"value": "users", "type": "string"},
        },
    }

    http_span_id = "eee19b7ec3c1b175"
    http_span = {
        "start_timestamp": ts.timestamp(),
        "end_timestamp": ts.timestamp() + 0.5,
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "span_id": http_span_id,
        "is_segment": False,
        "name": "Test span",
        "status": "ok",
        "attributes": {
            "sentry.op": {"value": "http.client", "type": "string"},
            "http.request.method": {"value": "get", "type": "string"},
            "url.full": {
                "value": "https://www.service.io/users/01234-qwerty/settings/98765-adfghj",
                "type": "string",
            },
        },
    }

    envelope = envelope_with_spans(
        db_span,
        http_span,
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": project_config["publicKeys"][0]["publicKey"],
            "release": "foo@1.0",
            "environment": "prod",
            "transaction": "/my/fancy/endpoint",
        },
    )

    relay.send_envelope(project_id, envelope)

    spans = [spans_consumer.get_span(), spans_consumer.get_span()]
    spans_by_id = {s["span_id"]: s for s in spans}

    common = {
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "name": "Test span",
        "is_segment": False,
        "received": time_within(ts),
        "start_timestamp": time_is(ts),
        "end_timestamp": time_is(ts.timestamp() + 0.5),
        "status": "ok",
        "retention_days": 42,
        "accepted_outcome_emitted": True,
        "downsampled_retention_days": 1337,
        "key_id": 123,
        "organization_id": 1,
        "project_id": 42,
    }

    # Verify DB attribute normalization
    db_result = spans_by_id[db_span_id]
    assert db_result == {
        **common,
        "span_id": db_span_id,
        "attributes": {
            "sentry.browser.name": {"type": "string", "value": "Python Requests"},
            "sentry.browser.version": {"type": "string", "value": "2.32"},
            "sentry.category": {"type": "string", "value": "db"},
            "sentry.op": {"type": "string", "value": "db"},
            "db.system": {"type": "string", "value": "mysql"},
            "db.system.name": {"type": "string", "value": "mysql"},
            "db.operation.name": {"type": "string", "value": "SELECT"},
            "sentry.action": {"type": "string", "value": "SELECT"},
            "db.query.text": {
                "type": "string",
                "value": "SELECT id FROM users WHERE id = 1 AND name = 'Test'",
            },
            "db.collection.name": {"type": "string", "value": "users"},
            "sentry.description": {
                "type": "string",
                "value": "SELECT id FROM users WHERE id = 1 AND name = 'Test'",
            },
            "sentry.domain": {"type": "string", "value": ",users,"},
            "sentry.normalized_db_query": {
                "type": "string",
                "value": "SELECT id FROM users WHERE id = %s AND name = %s",
            },
            "sentry.normalized_description": {
                "type": "string",
                "value": "SELECT id FROM users WHERE id = %s AND name = %s",
            },
            "sentry.normalized_db_query.hash": {
                "type": "string",
                "value": "f79af0ba3d26284c",
            },
            "sentry.observed_timestamp_nanos": {
                "type": "string",
                "value": time_within(ts, expect_resolution="ns"),
            },
        },
    }

    # Verify HTTP attribute normalization
    http_result = spans_by_id[http_span_id]
    assert http_result == {
        **common,
        "span_id": http_span_id,
        "attributes": {
            "sentry.browser.name": {"type": "string", "value": "Python Requests"},
            "sentry.browser.version": {"type": "string", "value": "2.32"},
            "sentry.category": {"type": "string", "value": "http"},
            "sentry.description": {
                "type": "string",
                "value": "GET https://www.service.io/users/01234-qwerty/settings/98765-adfghj",
            },
            "sentry.op": {"type": "string", "value": "http.client"},
            "sentry.observed_timestamp_nanos": {
                "type": "string",
                "value": time_within(ts, expect_resolution="ns"),
            },
            "http.request.method": {"type": "string", "value": "GET"},
            "sentry.action": {"type": "string", "value": "GET"},
            "server.address": {"type": "string", "value": "*.service.io"},
            "sentry.domain": {"type": "string", "value": "*.service.io"},
            "url.full": {
                "type": "string",
                "value": "https://www.service.io/users/01234-qwerty/settings/98765-adfghj",
            },
        },
    }


def test_invalid_spans(mini_sentry, relay):

    def span_id():
        counter = 1
        while True:
            yield f"{counter:016x}"
            counter += 1

    span_id = span_id()

    """
    A test asserting proper outcomes are emitted for invalid spans missing required attributes.
    """
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
    ]

    relay = relay(mini_sentry, options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)

    valid_span = {
        "end_timestamp": ts.timestamp() + 0.5,
        "name": "some op",
        "span_id": "eee19b7ec3c1b174",
        "start_timestamp": ts.timestamp(),
        "status": "ok",
        "trace_id": "5b8efff798038103d269b633813fc60c",
    }

    # Need to exclude the `trace_id`, since this one is fundamentally required
    # for DSC validations. Envelopes with mismatching DSCs are entirely rejected.
    required_keys = valid_span.keys() - {"trace_id"}
    nonempty_keys = valid_span.keys() - {"trace_id", "name", "status"}

    invalid_spans = []
    for key in required_keys:
        invalid_span = valid_span.copy()
        invalid_span["span_id"] = next(span_id)
        del invalid_span[key]
        invalid_spans.append(invalid_span)

    for key in required_keys:
        invalid_span = valid_span.copy()
        invalid_span["span_id"] = next(span_id)
        invalid_span[key] = None
        invalid_spans.append(invalid_span)

    for key in nonempty_keys:
        invalid_span = valid_span.copy()
        invalid_span["span_id"] = next(span_id)
        invalid_span[key] = ""
        invalid_spans.append(invalid_span)

    envelope = envelope_with_spans(
        *(invalid_spans + [valid_span]),
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )
    relay.send_envelope(project_id, envelope)

    # Wait here till the event arrives at which point we know that all the outcomes should also be
    # available as well to check afterwards.
    envelope = mini_sentry.get_captured_envelope()
    spans = json.loads(envelope.items[0].payload.bytes.decode())["items"]

    assert len(spans) == 1
    spans[0].pop("attributes")  # irrelevant for this test
    assert spans[0] == valid_span

    outcomes = mini_sentry.get_aggregated_outcomes(timeout=5)
    assert outcomes == [
        {
            "category": DataCategory.SPAN.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,
            "project_id": 42,
            "quantity": 3,
            "reason": "invalid_span",
        },
        {
            "category": DataCategory.SPAN.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,
            "project_id": 42,
            "reason": "no_data",
            "quantity": 4,
        },
        {
            "category": DataCategory.SPAN.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,
            "project_id": 42,
            "reason": "timestamp",
            "quantity": 6,
        },
        {
            "category": DataCategory.SPAN_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,
            "project_id": 42,
            "quantity": 3,
            "reason": "invalid_span",
        },
        {
            "category": DataCategory.SPAN_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,
            "project_id": 42,
            "reason": "no_data",
            "quantity": 4,
        },
        {
            "category": DataCategory.SPAN_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,
            "project_id": 42,
            "reason": "timestamp",
            "quantity": 6,
        },
    ]

    assert mini_sentry.captured_envelopes.empty()


@pytest.mark.parametrize(
    "delta,error",
    [
        (-timedelta(days=2), "past_timestamp"),
        (timedelta(days=2), "future_timestamp"),
    ],
)
def test_time_corrections(mini_sentry, relay, delta, error):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
    ]
    project_config["config"]["retentions"] = {
        "span": {"standard": 1, "downsampled": 100},
    }

    relay = relay(mini_sentry, options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)

    envelope = envelope_with_spans(
        {
            "start_timestamp": (ts + delta).timestamp(),
            "end_timestamp": (ts + delta).timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "is_segment": True,
            "name": "some op",
            "status": "ok",
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )

    relay.send_envelope(project_id, envelope)

    envelope = mini_sentry.get_captured_envelope()
    item_payload = json.loads(envelope.items[0].payload.bytes.decode())
    assert item_payload["items"][0] == {
        "_meta": {
            "start_timestamp": {
                "": {
                    "err": [
                        [
                            error,
                            {
                                "sdk_time": time_within_delta(ts + delta),
                                "server_time": time_within_delta(ts),
                            },
                        ]
                    ]
                }
            }
        },
        "attributes": mock.ANY,
        "status": "ok",
        "is_segment": True,
        "name": "some op",
        "start_timestamp": time_within_delta(ts),
        "end_timestamp": time_within_delta(ts),
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "span_id": "eee19b7ec3c1b175",
    }
