from datetime import datetime, timezone
from unittest import mock

from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_relay.consts import DataCategory

from .asserts import time_within_delta, time_within

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


def test_spansv2_basic(
    mini_sentry,
    relay,
    relay_with_processing,
    spans_consumer,
    metrics_consumer,
):
    """
    A basic test making sure spans can be ingested and have basic normalizations applied.
    """
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()

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
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "is_remote": False,
            "name": "some op",
            "attributes": {"foo": {"value": "bar", "type": "string"}},
        }
    )

    relay.send_envelope(project_id, envelope)

    assert spans_consumer.get_span() == {
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "span_id": "eee19b7ec3c1b175",
        "data": {
            "foo": "bar",
            "sentry.name": "some op",
            "sentry.browser.name": "Python Requests",
            "sentry.browser.version": "2.32",
            "sentry.observed_timestamp_nanos": time_within(ts, expect_resolution="ns"),
        },
        "description": "some op",
        "received": time_within(ts),
        "start_timestamp_ms": time_within(ts, precision="ms", expect_resolution="ms"),
        "start_timestamp_precise": time_within(ts),
        "end_timestamp_precise": time_within(ts.timestamp() + 0.5),
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

    assert metrics_consumer.get_metrics(n=2, with_headers=False) == [
        {
            "name": "c:spans/count_per_root_project@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {"decision": "keep", "target_project_id": "42"},
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
        {
            "name": "c:spans/usage@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {},
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
    _add_sampling_config(project_config, sample_rate=1, rule_type="transaction")
    # Setup the actual rule we want to test against.
    _add_sampling_config(project_config, sample_rate=0, rule_type=rule_type)

    relay = relay(mini_sentry, options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "is_remote": False,
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

    assert mini_sentry.get_metrics() == [
        {
            "metadata": mock.ANY,
            "name": "c:spans/count_per_root_project@none",
            "tags": {"decision": "drop", "target_project_id": "42"},
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
            "width": 1,
        },
        {
            "metadata": mock.ANY,
            "name": "c:spans/usage@none",
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
            "width": 1,
        },
    ]

    assert mini_sentry.captured_events.empty()
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
    _add_sampling_config(project_config, sample_rate=0.0, rule_type="trace")

    sampling_project_id = 43
    sampling_config = mini_sentry.add_basic_project_config(sampling_project_id)
    sampling_config["organizationId"] = project_config["organizationId"]
    _add_sampling_config(sampling_config, sample_rate=0.9, rule_type="trace")

    relay = relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "is_remote": False,
            "name": "some op",
            "attributes": {"foo": {"value": "bar", "type": "string"}},
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": sampling_config["publicKeys"][0]["publicKey"],
        },
    )

    relay.send_envelope(project_id, envelope)

    assert spans_consumer.get_span() == {
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "span_id": "eee19b7ec3c1b175",
        "description": "some op",
        "data": {
            "foo": "bar",
            "sentry.name": "some op",
            "sentry.server_sample_rate": 0.9,
            "sentry.browser.name": "Python Requests",
            "sentry.browser.version": "2.32",
            "sentry.observed_timestamp_nanos": time_within(ts, expect_resolution="ns"),
        },
        "measurements": {"server_sample_rate": {"value": 0.9}},
        "server_sample_rate": 0.9,
        "received": time_within_delta(ts),
        "start_timestamp_ms": time_within(ts, precision="ms", expect_resolution="ms"),
        "start_timestamp_precise": time_within(ts),
        "end_timestamp_precise": time_within(ts.timestamp() + 0.5),
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

    assert metrics_consumer.get_metrics(n=2, with_headers=False) == [
        {
            "name": "c:spans/count_per_root_project@none",
            "org_id": 1,
            "project_id": 43,
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {"decision": "keep", "target_project_id": "42"},
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
        {
            "name": "c:spans/usage@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {},
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
    ]

    outcomes_consumer.assert_empty()


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
    _add_sampling_config(project_config, sample_rate=0.0, rule_type="trace")

    sampling_project_id = 43
    sampling_config = mini_sentry.add_basic_project_config(sampling_project_id)
    sampling_config["organizationId"] = 99
    _add_sampling_config(sampling_config, sample_rate=1.0, rule_type="trace")

    relay = relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "is_remote": False,
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
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {"decision": "drop", "target_project_id": "42"},
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
        {
            "name": "c:spans/usage@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {},
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
            "is_remote": False,
            "name": "some op",
            "attributes": {
                "some_integer": {"value": 123, "type": "integer"},
                "sentry.release": {"value": "foobar@1.0", "type": "string"},
                "sentry.segment.name": {"value": "/foo/healthz", "type": "string"},
            },
        }
    )

    headers = None
    if user_agent := args.get("user-agent"):
        headers = {"User-Agent": user_agent}

    relay.send_envelope(project_id, envelope, headers=headers)

    outcomes = []
    for _ in range(2):
        outcomes.extend(mini_sentry.captured_outcomes.get(timeout=3).get("outcomes"))
    outcomes.sort(key=lambda x: x["category"])

    assert outcomes == [
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

    assert mini_sentry.captured_events.empty()
