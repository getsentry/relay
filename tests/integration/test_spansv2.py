from datetime import datetime, timezone
from unittest import mock

from sentry_sdk import envelope
from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_relay.consts import DataCategory

from .asserts import time_within_delta, time_within

from .test_dynamic_sampling import _add_sampling_config

import json
import uuid
import pytest

TEST_CONFIG = {
    "outcomes": {
        "emit_outcomes": True,
    }
}


# Simple test to check the behavior of the attachment, before treating the span attachment as
# something special this test passed.
def test_span_attachment(
    mini_sentry, relay_with_processing, attachments_consumer, outcomes_consumer
):
    project_id = 42
    event_id = "515539018c9b4260a6f999572f1661ee"
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config = project_config["config"].update(
        {
            "features": [
                "organizations:standalone-span-ingestion",
                "projects:span-v2-experimental-processing",
            ],
            "retentions": {"span": {"standard": 42, "downsampled": 1337}},
        }
    )
    relay = relay_with_processing()
    attachments_consumer = attachments_consumer()
    outcomes_consumer = outcomes_consumer()

    metadata = {
        "attachment_id": str(uuid.uuid4()),
        "timestamp": 1760520026.781239,
        "filename": "myfile.txt",
        "content_type": "text/plain",
        "attributes": {
            "foo": {"type": "string", "value": "bar"},
            "custom_tag": {"type": "string", "value": "test"},
        },
    }

    attachment_body = b"This is some mock attachment content"
    metadata_bytes = json.dumps(metadata).encode("utf-8")
    combined_payload = metadata_bytes + attachment_body

    envelope = Envelope(headers={"event_id": event_id})
    attachment_item = Item(
        payload=PayloadRef(bytes=combined_payload),
        type="attachment",
    )

    attachment_item.headers["content_type"] = "application/vnd.sentry.attachment.v2"
    attachment_item.headers["meta_length"] = len(metadata_bytes)
    attachment_item.headers["span_id"] = "EEE19B7EC3C1B174"

    envelope.add_item(attachment_item)
    relay.send_envelope(project_id, envelope)

    attachment = attachments_consumer.get_individual_attachment()
    assert attachment == {
        "type": "attachment",
        "attachment": {
            "name": "Unnamed Attachment",
            "rate_limited": False,
            "attachment_type": "event.attachment",
            "size": len(combined_payload),
            "data": combined_payload,
            "content_type": "application/vnd.sentry.attachment.v2",
            "id": mock.ANY,
        },
        "event_id": event_id,
        "project_id": project_id,
    }

    outcomes_consumer.assert_empty()


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
            "foo": {"type": "string", "value": "bar"},
            "http.response_content_length": {"value": 17, "type": "integer"},
            "http.response.body.size": {"value": 17, "type": "integer"},
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
        },
        "_meta": {
            "attributes": {
                "invalid": {
                    "": {
                        "err": ["invalid_data"],
                        "val": {"type": "string", "value": True},
                    }
                }
            }
        },
        "name": "some op",
        "received": time_within(ts),
        "start_timestamp": time_within(ts),
        "end_timestamp": time_within(ts.timestamp() + 0.5),
        "is_segment": True,
        "status": "ok",
        "retention_days": 42,
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
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {
                "decision": "keep",
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

    relay.send_envelope(project_id, envelope)

    assert mini_sentry.captured_outcomes.get(timeout=5).get("outcomes") == [
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
            "tags": {
                "decision": "drop",
                "target_project_id": "42",
                "transaction": "tx_from_root",
            },
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
            "is_segment": False,
            "name": "some op",
            "attributes": {"foo": {"value": "bar", "type": "string"}},
            "status": "ok",
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": sampling_config["publicKeys"][0]["publicKey"],
            "transaction": "tx_from_root",
        },
    )

    relay.send_envelope(project_id, envelope)

    span = spans_consumer.get_span()
    assert span["span_id"] == "eee19b7ec3c1b175"
    assert span["attributes"]["sentry.server_sample_rate"]["value"] == 0.9

    assert metrics_consumer.get_metrics(n=2, with_headers=False) == [
        {
            "name": "c:spans/count_per_root_project@none",
            "org_id": 1,
            "project_id": 43,
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {
                "decision": "keep",
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
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {},
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
    ]

    outcomes = outcomes_consumer.get_outcomes(n=1)
    outcomes.sort(key=lambda o: sorted(o.items()))

    assert outcomes == [
        {
            "category": DataCategory.SPAN_INDEXED.value,
            "timestamp": time_within_delta(),
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 1,
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

    assert mini_sentry.captured_events.empty()


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

    assert mini_sentry.captured_events.empty()
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
            "span_id": "eee19b7ec3c1b175",
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

    envelope = mini_sentry.captured_events.get(timeout=5)
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
        "start_timestamp": time_within(ts),
        "end_timestamp": time_within(ts.timestamp() + 0.5),
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

    envelope = mini_sentry.captured_events.get(timeout=5)
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


def test_invalid_spans(mini_sentry, relay):
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
        del invalid_span[key]
        invalid_spans.append(invalid_span)

    for key in required_keys:
        invalid_span = valid_span.copy()
        invalid_span[key] = None
        invalid_spans.append(invalid_span)

    for key in nonempty_keys:
        invalid_span = valid_span.copy()
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

    outcomes = mini_sentry.get_aggregated_outcomes(timeout=5)
    assert outcomes == [
        {
            "category": 12,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,
            "project_id": 42,
            "reason": "no_data",
            "timestamp": time_within_delta(),
            "quantity": len(invalid_spans),
        },
        {
            "category": 16,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,
            "project_id": 42,
            "reason": "no_data",
            "timestamp": time_within_delta(),
            "quantity": len(invalid_spans),
        },
    ]

    envelope = mini_sentry.captured_events.get(timeout=0.1)
    spans = json.loads(envelope.items[0].payload.bytes.decode())["items"]

    assert len(spans) == 1
    spans[0].pop("attributes")  # irrelevant for this test
    assert spans[0] == valid_span

    assert mini_sentry.captured_events.empty()
