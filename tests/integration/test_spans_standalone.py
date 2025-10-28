from datetime import datetime, timezone

from sentry_sdk.envelope import Envelope, Item, PayloadRef

from .asserts import time_within_delta, time_within

import pytest


def envelope_with_spans(*payloads: dict, trace_info=None) -> Envelope:
    envelope = Envelope()
    for payload in payloads:
        item = Item(
            type="span",
            payload=PayloadRef(json=payload),
            content_type="application/json",
        )
        envelope.add_item(item)
    envelope.headers["trace"] = trace_info
    return envelope


def lcp_cls_inp_differences(mode):
    """
    Differences in processing between 'legacy' and 'v2' for LCP, CLS, INP standalone spans.

    All these differences should be resolved.
    """
    if mode == "legacy":
        attributes = {
            "browser.name": {"type": "string", "value": "Chrome"},
            "client.address": {"type": "string", "value": "127.0.0.1"},
            # Legacy behaviour, new field is `sentry.segment.name`
            # Maybe this shouldn't exist since parent and segment information is also removed
            "sentry.transaction": {"type": "string", "value": "/insights/projects/"},
        }
        fields = {}
    else:
        attributes = {
            # Not implemented
            "client.address": {"type": "string", "value": "{{auto}}"},
            # We additionally extract the browser version for EAP items
            "sentry.browser.version": {"type": "string", "value": "141.0.0"},
            # New for EAP items
            "sentry.observed_timestamp_nanos": {
                "type": "string",
                "value": time_within_delta(expect_resolution="ns"),
            },
            # Maybe should not exist. Segment information in legacy processing is removed.
            "sentry.segment.id": {"type": "string", "value": "8a6626cc9bdd5d9b"},
        }
        fields = {
            # See `set_segment_attributes` which removes segment information on LCP and CLS spans.
            # Introduced in https://github.com/getsentry/relay/pull/3522.
            # Not fully clear what the intention of that change is and if we still need it.
            "parent_span_id": "8a6626cc9bdd5d9b",
        }

    return (attributes, fields)


@pytest.mark.parametrize("mode", ["legacy", "v2"])
def test_lcp_span(
    mini_sentry, relay, relay_with_processing, spans_consumer, metrics_consumer, mode
):
    """
    Test verifies LCP spans processed via the SpanV2 and legacy standalone processing pipeline are equally processed.

    Some differences between the pipelines exist and are noted in the test.
    """
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = ["organizations:standalone-span-ingestion"]
    if mode == "v2":
        project_config["config"]["features"].append(
            "projects:span-v2-experimental-processing"
        )

    relay = relay(relay_with_processing())

    ts = datetime.now(timezone.utc)

    envelope = envelope_with_spans(
        {
            "data": {
                "sentry.origin": "auto.http.browser.lcp",
                "sentry.op": "ui.webvital.lcp",
                "release": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
                "environment": "prod",
                "replay_id": "3d76a6311de149b9b3f560827ea0ecf9",
                "transaction": "/insights/projects/",
                "user_agent.original": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
                "client.address": "{{auto}}",
                "sentry.exclusive_time": 0,
                "sentry.pageload.span_id": "8a6626cc9bdd5d9b",
                "sentry.report_event": "navigation",
                "lcp.url": "https://s1.sentry-cdn.com/../sentry-loader.svg",
                "lcp.loadTime": 527.5,
                "lcp.renderTime": 548,
                "lcp.size": 8100,
            },
            "description": "<unknown>",
            "op": "ui.webvital.lcp",
            "parent_span_id": "8a6626cc9bdd5d9b",
            "span_id": "9fd17741416e8e4e",
            "start_timestamp": ts.timestamp() - 0.5,
            "timestamp": ts.timestamp(),
            "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
            "origin": "auto.http.browser.lcp",
            "exclusive_time": 0,
            "measurements": {"lcp": {"value": 548, "unit": "millisecond"}},
            "segment_id": "8a6626cc9bdd5d9b",
        },
        trace_info={
            "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
            "public_key": project_config["publicKeys"][0]["publicKey"],
            "transaction": "/insights/projects/",
        },
    )

    relay.send_envelope(project_id, envelope)

    (attributes, fields) = lcp_cls_inp_differences(mode)

    assert spans_consumer.get_span() == {
        "attributes": {
            "lcp": {"type": "double", "value": 548.0},
            "lcp.loadTime": {"type": "double", "value": 527.5},
            "lcp.renderTime": {"type": "integer", "value": 548},
            "lcp.size": {"type": "integer", "value": 8100},
            "lcp.url": {
                "type": "string",
                "value": "https://s1.sentry-cdn.com/../sentry-loader.svg",
            },
            "sentry.browser.name": {"type": "string", "value": "Chrome"},
            "sentry.description": {"type": "string", "value": "<unknown>"},
            "sentry.environment": {"type": "string", "value": "prod"},
            "sentry.exclusive_time": {"type": "double", "value": 0.0},
            "sentry.op": {"type": "string", "value": "ui.webvital.lcp"},
            "sentry.origin": {"type": "string", "value": "auto.http.browser.lcp"},
            "sentry.pageload.span_id": {"type": "string", "value": "8a6626cc9bdd5d9b"},
            "sentry.release": {
                "type": "string",
                "value": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
            },
            "sentry.replay_id": {
                "type": "string",
                "value": "3d76a6311de149b9b3f560827ea0ecf9",
            },
            "sentry.report_event": {"type": "string", "value": "navigation"},
            "sentry.segment.name": {"type": "string", "value": "/insights/projects/"},
            "user_agent.original": {
                "type": "string",
                "value": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 "
                "Safari/537.36",
            },
            **attributes,
        },
        "downsampled_retention_days": 90,
        "end_timestamp": time_within(ts),
        "is_remote": False,
        "key_id": 123,
        "name": "ui.webvital.lcp",
        "organization_id": 1,
        "project_id": 42,
        "received": time_within(ts),
        "retention_days": 90,
        "span_id": "9fd17741416e8e4e",
        "start_timestamp": time_within(ts.timestamp() - 0.5),
        "status": "ok",
        "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
        **fields,
    }

    assert metrics_consumer.get_metrics(with_headers=False) == [
        {
            "org_id": 1,
            "project_id": 42,
            "name": "c:spans/count_per_root_project@none",
            "type": "c",
            "value": 1.0,
            "timestamp": time_within_delta(ts),
            "tags": {
                "decision": "keep",
                "target_project_id": "42",
                "transaction": "/insights/projects/",
            },
            "retention_days": 90,
            "received_at": time_within(ts),
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "c:spans/usage@none",
            "type": "c",
            "value": 1.0,
            "timestamp": time_within_delta(ts),
            "tags": {},
            "retention_days": 90,
            "received_at": time_within(ts),
        },
        # No metric extraction in the SpanV2 pipeline.
        *(
            [
                {
                    "org_id": 1,
                    "project_id": 42,
                    "name": "d:transactions/measurements.lcp@millisecond",
                    "type": "d",
                    "value": [548.0],
                    "timestamp": time_within_delta(ts),
                    "tags": {
                        "browser.name": "Chrome",
                        "environment": "prod",
                        "release": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
                        "span.op": "ui.webvital.lcp",
                        "transaction": "/insights/projects/",
                    },
                    "retention_days": 90,
                    "received_at": time_within(ts),
                }
            ]
            if mode == "legacy"
            else []
        ),
    ]


@pytest.mark.parametrize("mode", ["legacy", "v2"])
def test_cls_span(
    mini_sentry, relay, relay_with_processing, spans_consumer, metrics_consumer, mode
):
    """
    Test verifies CLS spans processed via the SpanV2 and legacy standalone processing pipeline are equally processed.

    Some differences between the pipelines exist and are noted in the test.
    """
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = ["organizations:standalone-span-ingestion"]
    if mode == "v2":
        project_config["config"]["features"].append(
            "projects:span-v2-experimental-processing"
        )

    relay = relay(relay_with_processing())

    ts = datetime.now(timezone.utc)

    envelope = envelope_with_spans(
        {
            "data": {
                "sentry.origin": "auto.http.browser.cls",
                "sentry.op": "ui.webvital.cls",
                "release": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
                "environment": "prod",
                "replay_id": "3d76a6311de149b9b3f560827ea0ecf9",
                "transaction": "/insights/projects/",
                "user_agent.original": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
                "client.address": "{{auto}}",
                "sentry.exclusive_time": 0,
                "sentry.pageload.span_id": "8a6626cc9bdd5d9b",
                "sentry.report_event": "navigation",
                "cls.source.1": "AppContainer > NavContent > MobileTopbar > StyledButton",
                "cls.source.2": "div.app-1azrk9k.etjky0h0 > AppContainer > BodyContainer > BaseFooter",
                "cls.source.3": "<unknown>",
            },
            "description": "AppContainer > NavContent > MobileTopbar > StyledButton",
            "op": "ui.webvital.cls",
            "parent_span_id": "8a6626cc9bdd5d9b",
            "span_id": "be6fa380c55f2fcb",
            "start_timestamp": ts.timestamp() - 0.5,
            "timestamp": ts.timestamp(),
            "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
            "origin": "auto.http.browser.cls",
            "exclusive_time": 0,
            "measurements": {"cls": {"value": 0.1, "unit": ""}},
            "segment_id": "8a6626cc9bdd5d9b",
        },
        trace_info={
            "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
            "public_key": project_config["publicKeys"][0]["publicKey"],
            "transaction": "/insights/projects/",
        },
    )

    relay.send_envelope(project_id, envelope)

    (attributes, fields) = lcp_cls_inp_differences(mode)

    assert spans_consumer.get_span() == {
        "attributes": {
            "cls": {"type": "double", "value": 0.1},
            "cls.source.1": {
                "type": "string",
                "value": "AppContainer > NavContent > MobileTopbar > StyledButton",
            },
            "cls.source.2": {
                "type": "string",
                "value": "div.app-1azrk9k.etjky0h0 > AppContainer > BodyContainer > BaseFooter",
            },
            "cls.source.3": {"type": "string", "value": "<unknown>"},
            "sentry.browser.name": {"type": "string", "value": "Chrome"},
            "sentry.description": {
                "type": "string",
                "value": "AppContainer > NavContent > MobileTopbar > StyledButton",
            },
            "sentry.environment": {"type": "string", "value": "prod"},
            "sentry.exclusive_time": {"type": "double", "value": 0.0},
            "sentry.op": {"type": "string", "value": "ui.webvital.cls"},
            "sentry.origin": {"type": "string", "value": "auto.http.browser.cls"},
            "sentry.pageload.span_id": {"type": "string", "value": "8a6626cc9bdd5d9b"},
            "sentry.release": {
                "type": "string",
                "value": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
            },
            "sentry.replay_id": {
                "type": "string",
                "value": "3d76a6311de149b9b3f560827ea0ecf9",
            },
            "sentry.report_event": {"type": "string", "value": "navigation"},
            "sentry.segment.name": {"type": "string", "value": "/insights/projects/"},
            "user_agent.original": {
                "type": "string",
                "value": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 "
                "Safari/537.36",
            },
            **attributes,
        },
        "downsampled_retention_days": 90,
        "end_timestamp": time_within(ts),
        "is_remote": False,
        "key_id": 123,
        "name": "ui.webvital.cls",
        "organization_id": 1,
        "project_id": 42,
        "received": time_within(ts),
        "retention_days": 90,
        "span_id": "be6fa380c55f2fcb",
        "start_timestamp": time_within(ts.timestamp() - 0.5),
        "status": "ok",
        "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
        **fields,
    }

    assert metrics_consumer.get_metrics(with_headers=False) == [
        {
            "org_id": 1,
            "project_id": 42,
            "name": "c:spans/count_per_root_project@none",
            "type": "c",
            "value": 1.0,
            "timestamp": time_within_delta(ts),
            "tags": {
                "decision": "keep",
                "target_project_id": "42",
                "transaction": "/insights/projects/",
            },
            "retention_days": 90,
            "received_at": time_within(ts),
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "c:spans/usage@none",
            "type": "c",
            "value": 1.0,
            "timestamp": time_within_delta(ts),
            "tags": {},
            "retention_days": 90,
            "received_at": time_within(ts),
        },
        # No metric extraction in the SpanV2 pipeline.
        *(
            [
                {
                    "org_id": 1,
                    "project_id": 42,
                    "name": "d:transactions/measurements.cls@none",
                    "type": "d",
                    "value": [0.1],
                    "timestamp": time_within_delta(ts),
                    "tags": {
                        "browser.name": "Chrome",
                        "environment": "prod",
                        "release": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
                        "span.op": "ui.webvital.cls",
                        "transaction": "/insights/projects/",
                    },
                    "retention_days": 90,
                    "received_at": time_within(ts),
                }
            ]
            if mode == "legacy"
            else []
        ),
    ]


@pytest.mark.parametrize("mode", ["legacy", "v2"])
def test_inp_span(
    mini_sentry, relay, relay_with_processing, spans_consumer, metrics_consumer, mode
):
    """
    Test verifies CLS spans processed via the SpanV2 and legacy standalone processing pipeline are equally processed.

    Some differences between the pipelines exist and are noted in the test.
    """
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = ["organizations:standalone-span-ingestion"]
    if mode == "v2":
        project_config["config"]["features"].append(
            "projects:span-v2-experimental-processing"
        )

    relay = relay(relay_with_processing())

    ts = datetime.now(timezone.utc)

    envelope = envelope_with_spans(
        {
            "data": {
                "sentry.origin": "auto.http.browser.inp",
                "sentry.op": "ui.interaction.click",
                "release": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
                "environment": "prod",
                "replay_id": "3d76a6311de149b9b3f560827ea0ecf9",
                "transaction": "/insights/projects/",
                "user_agent.original": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
                "client.address": "{{auto}}",
                "sentry.exclusive_time": 104,
            },
            "description": "<unknown>",
            "op": "ui.interaction.click",
            "parent_span_id": "8a6626cc9bdd5d9b",
            "span_id": "a6f029fbe0e2389a",
            "start_timestamp": ts.timestamp() - 0.5,
            "timestamp": ts.timestamp(),
            "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
            "origin": "auto.http.browser.inp",
            "exclusive_time": 104,
            "measurements": {"inp": {"value": 104, "unit": "millisecond"}},
            "segment_id": "8a6626cc9bdd5d9b",
        },
        trace_info={
            "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
            "public_key": project_config["publicKeys"][0]["publicKey"],
            "transaction": "/insights/projects/",
        },
    )

    relay.send_envelope(project_id, envelope)

    (attributes, fields) = lcp_cls_inp_differences(mode)

    assert spans_consumer.get_span() == {
        "attributes": {
            "inp": {"type": "double", "value": 104.0},
            "sentry.browser.name": {"type": "string", "value": "Chrome"},
            "sentry.description": {
                "type": "string",
                "value": "<unknown>",
            },
            "sentry.environment": {"type": "string", "value": "prod"},
            "sentry.exclusive_time": {"type": "double", "value": 104.0},
            "sentry.op": {"type": "string", "value": "ui.interaction.click"},
            "sentry.origin": {"type": "string", "value": "auto.http.browser.inp"},
            "sentry.release": {
                "type": "string",
                "value": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
            },
            "sentry.replay_id": {
                "type": "string",
                "value": "3d76a6311de149b9b3f560827ea0ecf9",
            },
            "sentry.segment.name": {"type": "string", "value": "/insights/projects/"},
            "user_agent.original": {
                "type": "string",
                "value": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 "
                "Safari/537.36",
            },
            **attributes,
        },
        "downsampled_retention_days": 90,
        "end_timestamp": time_within(ts),
        "is_remote": False,
        "key_id": 123,
        "name": "ui.interaction.click",
        "organization_id": 1,
        "project_id": 42,
        "received": time_within(ts),
        "retention_days": 90,
        "span_id": "a6f029fbe0e2389a",
        "start_timestamp": time_within(ts.timestamp() - 0.5),
        "status": "ok",
        "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
        **fields,
    }

    assert metrics_consumer.get_metrics(with_headers=False) == [
        {
            "org_id": 1,
            "project_id": 42,
            "name": "c:spans/count_per_root_project@none",
            "type": "c",
            "value": 1.0,
            "timestamp": time_within_delta(ts),
            "tags": {
                "decision": "keep",
                "target_project_id": "42",
                "transaction": "/insights/projects/",
            },
            "retention_days": 90,
            "received_at": time_within(ts),
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "c:spans/usage@none",
            "type": "c",
            "value": 1.0,
            "timestamp": time_within_delta(ts),
            "tags": {},
            "retention_days": 90,
            "received_at": time_within(ts),
        },
        # No metric extraction in the SpanV2 pipeline.
        *(
            [
                {
                    "org_id": 1,
                    "project_id": 42,
                    "name": "d:spans/webvital.inp@millisecond",
                    "type": "d",
                    "value": [104.0],
                    "timestamp": time_within_delta(ts),
                    "tags": {
                        "browser.name": "Chrome",
                        "environment": "prod",
                        "release": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
                        "span.op": "ui.interaction.click",
                        "transaction": "/insights/projects/",
                    },
                    "retention_days": 90,
                    "received_at": time_within(ts),
                }
            ]
            if mode == "legacy"
            else []
        ),
    ]
