from datetime import datetime, timezone

from sentry_sdk.envelope import Envelope, Item, PayloadRef

from .asserts import matches_any, time_within_delta, time_within

import pytest

# Some profiles from Sentry
performance_score_profiles = [
    {
        "name": "Firefox",
        "scoreComponents": [
            {
                "measurement": "fcp",
                "weight": 0.15,
                "p10": 900.0,
                "p50": 1600.0,
                "optional": True,
            },
            {
                "measurement": "lcp",
                "weight": 0.30,
                "p10": 1200.0,
                "p50": 2400.0,
                "optional": True,
            },
            {
                "measurement": "cls",
                "weight": 0.15,
                "p10": 0.1,
                "p50": 0.25,
                "optional": True,
            },
            {
                "measurement": "ttfb",
                "weight": 0.10,
                "p10": 200.0,
                "p50": 400.0,
                "optional": True,
            },
        ],
        "condition": {
            "op": "or",
            "inner": [
                {
                    "op": "eq",
                    "name": "event.contexts.browser.name",
                    "value": "Firefox",
                },
                {
                    "op": "eq",
                    "name": "span.attributes.browser.name.value",
                    "value": "Firefox",
                },
            ],
        },
    },
    {
        "name": "Firefox INP",
        "scoreComponents": [
            {
                "measurement": "inp",
                "weight": 1.0,
                "p10": 200.0,
                "p50": 500.0,
                "optional": False,
            },
        ],
        "condition": {
            "op": "or",
            "inner": [
                {
                    "op": "eq",
                    "name": "event.contexts.browser.name",
                    "value": "Firefox",
                },
                {
                    "op": "eq",
                    "name": "span.attributes.browser.name.value",
                    "value": "Firefox",
                },
            ],
        },
    },
]


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
        return {
            # The legacy pipeline extracts this attribute from `sentry_tags`.
            "sentry.browser.name": {"type": "string", "value": "Firefox"},
            "sentry.relay.pipeline": {"type": "string", "value": "span_legacy"},
        }
    else:
        return {
            # We additionally extract the browser version for EAP items
            "browser.version": {"type": "string", "value": "42.0"},
            "sentry.relay.pipeline": {"type": "string", "value": "span_v2"},
            # New for EAP items
            "sentry.observed_timestamp_nanos": {
                "type": "string",
                "value": time_within_delta(expect_resolution="ns"),
            },
        }


@pytest.mark.parametrize("is_segment", [False, True])
@pytest.mark.parametrize("mode", ["legacy", "v2"])
def test_lcp_span(
    mini_sentry,
    relay,
    relay_with_processing,
    spans_consumer,
    metrics_consumer,
    mode,
    is_segment,
):
    """
    Test verifies LCP spans processed via the SpanV2 and legacy standalone processing pipeline are equally processed.

    Some differences between the pipelines exist and are noted in the test.
    """
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["performanceScore"] = {
        "profiles": performance_score_profiles
    }
    project_config["config"].setdefault(
        "features", ["organizations:relay-generate-billing-outcome"]
    )
    if mode == "v2":
        project_config["config"]["features"].append(
            "projects:span-v2-experimental-processing"
        )

    relay = relay(relay_with_processing())

    ts = datetime.now(timezone.utc)

    envelope = envelope_with_spans(
        {
            "data": {
                "sentry.op": "ui.webvital.lcp",
                "release": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
                "environment": "prod",
                "replay_id": "3d76a6311de149b9b3f560827ea0ecf9",
                "transaction": "/insights/projects/b8686628-95f0-4be9-af6b-a98164504d8f",
                "user_agent.original": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/42.0",
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
            "is_segment": is_segment,
        },
        trace_info={
            "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
            "public_key": project_config["publicKeys"][0]["publicKey"],
            "transaction": "/insights/projects/",
        },
    )

    relay.send_envelope(project_id, envelope)

    assert spans_consumer.get_span() == {
        "_meta": matches_any(),
        "attributes": {
            "client.address": {"type": "string", "value": "127.0.0.1"},
            "browser.web_vital.lcp.value": {"type": "double", "value": 548.0},
            "browser.web_vital.lcp.load_time": {"type": "double", "value": 527.5},
            "browser.web_vital.lcp.render_time": {"type": "integer", "value": 548},
            "browser.web_vital.lcp.size": {"type": "integer", "value": 8100},
            "browser.web_vital.lcp.url": {
                "type": "string",
                "value": "https://s1.sentry-cdn.com/../sentry-loader.svg",
            },
            "lcp.loadTime": {"type": "double", "value": 527.5},
            "lcp.renderTime": {"type": "integer", "value": 548},
            "lcp.size": {"type": "integer", "value": 8100},
            "lcp.url": {
                "type": "string",
                "value": "https://s1.sentry-cdn.com/../sentry-loader.svg",
            },
            "browser.name": {"type": "string", "value": "Firefox"},
            "sentry.description": {"type": "string", "value": "<unknown>"},
            "sentry.dsc.transaction": {
                "type": "string",
                "value": "/insights/projects/",
            },
            "sentry.dsc.project_id": {"type": "string", "value": "42"},
            "sentry.dsc.trace_id": {
                "type": "string",
                "value": "d3d20f000885466b8c8f947c9b92b8d3",
            },
            "sentry.environment": {"type": "string", "value": "prod"},
            "environment": None,
            "sentry.exclusive_time": {"type": "double", "value": 0.0},
            "sentry.op": {"type": "string", "value": "ui.webvital.lcp"},
            "sentry.origin": {"type": "string", "value": "auto.http.browser.lcp"},
            "sentry.pageload.span_id": {"type": "string", "value": "8a6626cc9bdd5d9b"},
            "sentry.relay.ingress": {"type": "string", "value": "legacy"},
            "sentry.release": {
                "type": "string",
                "value": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
            },
            "release": None,
            "sentry.replay_id": {
                "type": "string",
                "value": "3d76a6311de149b9b3f560827ea0ecf9",
            },
            "replay_id": None,
            "sentry.report_event": {"type": "string", "value": "navigation"},
            "sentry.segment.name": {"type": "string", "value": "/insights/projects/*"},
            "transaction": None,
            "user_agent.original": {
                "type": "string",
                "value": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Firefox/42.0",
            },
            # Attributes computed by performace score normalization
            "score.lcp": {
                "type": "double",
                "value": 0.9968400718909384,
            },
            "score.ratio.lcp": {
                "type": "double",
                "value": 0.9968400718909384,
            },
            "score.total": {
                "type": "double",
                "value": 0.9968400718909384,
            },
            "score.weight.cls": {
                "type": "double",
                "value": 0.0,
            },
            "score.weight.fcp": {
                "type": "double",
                "value": 0.0,
            },
            "score.weight.lcp": {
                "type": "double",
                "value": 1.0,
            },
            "score.weight.ttfb": {
                "type": "double",
                "value": 0.0,
            },
            **lcp_cls_inp_differences(mode),
        },
        "downsampled_retention_days": 90,
        "end_timestamp": time_within(ts),
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
                "is_segment": "false",
                "target_project_id": "42",
                "transaction": "/insights/projects/",
            },
            "retention_days": 90,
            "received_at": time_within(ts, precision="s"),
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "c:spans/usage@none",
            "type": "c",
            "value": 1.0,
            "timestamp": time_within_delta(ts),
            "tags": {"is_segment": "false", "billing_outcome_emitted": "true"},
            "retention_days": 90,
            "received_at": time_within(ts, precision="s"),
        },
    ]


@pytest.mark.parametrize("is_segment", [False, True])
@pytest.mark.parametrize("mode", ["legacy", "v2"])
def test_cls_span(
    mini_sentry,
    relay,
    relay_with_processing,
    spans_consumer,
    metrics_consumer,
    mode,
    is_segment,
):
    """
    Test verifies CLS spans processed via the SpanV2 and legacy standalone processing pipeline are equally processed.

    Some differences between the pipelines exist and are noted in the test.
    """
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["performanceScore"] = {
        "profiles": performance_score_profiles
    }
    project_config["config"].setdefault("features", []).append(
        "organizations:relay-generate-billing-outcome"
    )

    if mode == "v2":
        project_config["config"]["features"].append(
            "projects:span-v2-experimental-processing"
        )

    relay = relay(relay_with_processing())

    ts = datetime.now(timezone.utc)

    envelope = envelope_with_spans(
        {
            "data": {
                "sentry.op": "ui.webvital.cls",
                "release": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
                "environment": "prod",
                "replay_id": "3d76a6311de149b9b3f560827ea0ecf9",
                "transaction": "/insights/projects/b8686628-95f0-4be9-af6b-a98164504d8f",
                "user_agent.original": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/42.0",
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
            "is_segment": is_segment,
        },
        trace_info={
            "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
            "public_key": project_config["publicKeys"][0]["publicKey"],
            "transaction": "/insights/projects/",
        },
    )

    relay.send_envelope(project_id, envelope)

    assert spans_consumer.get_span() == {
        "_meta": matches_any(),
        "attributes": {
            "client.address": {"type": "string", "value": "127.0.0.1"},
            "browser.web_vital.cls.value": {"type": "double", "value": 0.1},
            "browser.web_vital.cls.source.1": {
                "type": "string",
                "value": "AppContainer > NavContent > MobileTopbar > StyledButton",
            },
            "browser.web_vital.cls.source.2": {
                "type": "string",
                "value": "div.app-1azrk9k.etjky0h0 > AppContainer > BodyContainer > BaseFooter",
            },
            "browser.web_vital.cls.source.3": {"type": "string", "value": "<unknown>"},
            "cls.source.1": {
                "type": "string",
                "value": "AppContainer > NavContent > MobileTopbar > StyledButton",
            },
            "cls.source.2": {
                "type": "string",
                "value": "div.app-1azrk9k.etjky0h0 > AppContainer > BodyContainer > BaseFooter",
            },
            "cls.source.3": {"type": "string", "value": "<unknown>"},
            "browser.name": {"type": "string", "value": "Firefox"},
            "sentry.description": {
                "type": "string",
                "value": "AppContainer > NavContent > MobileTopbar > StyledButton",
            },
            "sentry.dsc.transaction": {
                "type": "string",
                "value": "/insights/projects/",
            },
            "sentry.dsc.project_id": {"type": "string", "value": "42"},
            "sentry.dsc.trace_id": {
                "type": "string",
                "value": "d3d20f000885466b8c8f947c9b92b8d3",
            },
            "sentry.environment": {"type": "string", "value": "prod"},
            "environment": None,
            "sentry.exclusive_time": {"type": "double", "value": 0.0},
            "sentry.op": {"type": "string", "value": "ui.webvital.cls"},
            "sentry.origin": {"type": "string", "value": "auto.http.browser.cls"},
            "sentry.pageload.span_id": {"type": "string", "value": "8a6626cc9bdd5d9b"},
            "sentry.relay.ingress": {"type": "string", "value": "legacy"},
            "sentry.release": {
                "type": "string",
                "value": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
            },
            "release": None,
            "sentry.replay_id": {
                "type": "string",
                "value": "3d76a6311de149b9b3f560827ea0ecf9",
            },
            "replay_id": None,
            "sentry.report_event": {"type": "string", "value": "navigation"},
            "sentry.segment.name": {"type": "string", "value": "/insights/projects/*"},
            "transaction": None,
            "user_agent.original": {
                "type": "string",
                "value": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Firefox/42.0",
            },
            # Attributes computed by performace score normalization
            "score.cls": {
                "type": "double",
                "value": 0.8999999314038525,
            },
            "score.ratio.cls": {
                "type": "double",
                "value": 0.8999999314038525,
            },
            "score.total": {
                "type": "double",
                "value": 0.8999999314038525,
            },
            "score.weight.cls": {
                "type": "double",
                "value": 1.0,
            },
            "score.weight.fcp": {
                "type": "double",
                "value": 0.0,
            },
            "score.weight.lcp": {
                "type": "double",
                "value": 0.0,
            },
            "score.weight.ttfb": {
                "type": "double",
                "value": 0.0,
            },
            **lcp_cls_inp_differences(mode),
        },
        "downsampled_retention_days": 90,
        "end_timestamp": time_within(ts),
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
                "is_segment": "false",
                "target_project_id": "42",
                "transaction": "/insights/projects/",
            },
            "retention_days": 90,
            "received_at": time_within(ts, precision="s"),
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "c:spans/usage@none",
            "type": "c",
            "value": 1.0,
            "timestamp": time_within_delta(ts),
            "tags": {"is_segment": "false", "billing_outcome_emitted": "true"},
            "retention_days": 90,
            "received_at": time_within(ts, precision="s"),
        },
    ]


@pytest.mark.parametrize("is_segment", [False, True])
@pytest.mark.parametrize("mode", ["legacy", "v2"])
def test_inp_span(
    mini_sentry,
    relay,
    relay_with_processing,
    spans_consumer,
    metrics_consumer,
    mode,
    is_segment,
):
    """
    Test verifies INP spans processed via the SpanV2 and legacy standalone processing pipeline are equally processed.

    Some differences between the pipelines exist and are noted in the test.
    """
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["performanceScore"] = {
        "profiles": performance_score_profiles
    }
    project_config["config"].setdefault("features", []).append(
        "organizations:relay-generate-billing-outcome"
    )
    if mode == "v2":
        project_config["config"].setdefault("features", []).append(
            "projects:span-v2-experimental-processing"
        )

    relay = relay(relay_with_processing())

    ts = datetime.now(timezone.utc)

    envelope = envelope_with_spans(
        {
            "data": {
                "sentry.op": "ui.interaction.click",
                "release": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
                "environment": "prod",
                "replay_id": "3d76a6311de149b9b3f560827ea0ecf9",
                "transaction": "/insights/projects/b8686628-95f0-4be9-af6b-a98164504d8f",
                "user_agent.original": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/42.0",
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
            "is_segment": is_segment,
        },
        trace_info={
            "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
            "public_key": project_config["publicKeys"][0]["publicKey"],
            "transaction": "/insights/projects/",
        },
    )

    relay.send_envelope(project_id, envelope)

    assert spans_consumer.get_span() == {
        "_meta": matches_any(),
        "attributes": {
            "client.address": {"type": "string", "value": "127.0.0.1"},
            "browser.web_vital.inp.value": {"type": "double", "value": 104.0},
            "browser.name": {"type": "string", "value": "Firefox"},
            "sentry.description": {
                "type": "string",
                "value": "<unknown>",
            },
            "sentry.dsc.transaction": {
                "type": "string",
                "value": "/insights/projects/",
            },
            "sentry.dsc.project_id": {"type": "string", "value": "42"},
            "sentry.dsc.trace_id": {
                "type": "string",
                "value": "d3d20f000885466b8c8f947c9b92b8d3",
            },
            "sentry.environment": {"type": "string", "value": "prod"},
            "environment": None,
            "sentry.exclusive_time": {"type": "double", "value": 104.0},
            "sentry.op": {"type": "string", "value": "ui.interaction.click"},
            "sentry.origin": {"type": "string", "value": "auto.http.browser.inp"},
            "sentry.relay.ingress": {"type": "string", "value": "legacy"},
            "sentry.release": {
                "type": "string",
                "value": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
            },
            "release": None,
            "sentry.replay_id": {
                "type": "string",
                "value": "3d76a6311de149b9b3f560827ea0ecf9",
            },
            "replay_id": None,
            "sentry.segment.name": {"type": "string", "value": "/insights/projects/*"},
            "transaction": None,
            "user_agent.original": {
                "type": "string",
                "value": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Firefox/42.0",
            },
            # Attributes computed by performace score normalization
            "score.inp": {
                "type": "double",
                "value": 0.9859595387898855,
            },
            "score.ratio.inp": {
                "type": "double",
                "value": 0.9859595387898855,
            },
            "score.total": {
                "type": "double",
                "value": 0.9859595387898855,
            },
            "score.weight.inp": {
                "type": "double",
                "value": 1.0,
            },
            **lcp_cls_inp_differences(mode),
        },
        "downsampled_retention_days": 90,
        "end_timestamp": time_within(ts),
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
                "is_segment": "false",
                "target_project_id": "42",
                "transaction": "/insights/projects/",
            },
            "retention_days": 90,
            "received_at": time_within(ts, precision="s"),
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "c:spans/usage@none",
            "type": "c",
            "value": 1.0,
            "timestamp": time_within_delta(ts),
            "tags": {"is_segment": "false", "billing_outcome_emitted": "true"},
            "retention_days": 90,
            "received_at": time_within(ts, precision="s"),
        },
    ]


def test_spans_standalone_dsc_normalization(
    mini_sentry, relay, relay_with_processing, spans_consumer
):
    spans_consumer = spans_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    relay = relay(relay_with_processing())
    ts = datetime.now(timezone.utc)
    envelope = envelope_with_spans(
        {
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "aaaaaaaaaaaaaaaa",
            "is_segment": True,
            "start_timestamp": ts.timestamp(),
            "timestamp": ts.timestamp() + 0.5,
            "exclusive_time": 500,
        },
        {
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "bbbbbbbbbbbbbbbb",
            "parent_span_id": "aaaaaaaaaaaaaaaa",
            "is_segment": False,
            "start_timestamp": ts.timestamp(),
            "timestamp": ts.timestamp() + 0.3,
            "exclusive_time": 300,
        },
        {
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "cccccccccccccccc",
            "parent_span_id": "aaaaaaaaaaaaaaaa",
            "is_segment": False,
            "start_timestamp": ts.timestamp(),
            "timestamp": ts.timestamp() + 0.3,
            "exclusive_time": 300,
            "data": {
                "sentry.dsc.trace_id": "5b8efff798038103d269b633813fc60c",
                "sentry.dsc.transaction": "/transaction/already/exists",
                "sentry.dsc.project_id": "41",
            },
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": project_config["publicKeys"][0]["publicKey"],
            "transaction": "/my/fancy/endpoint",
        },
    )

    relay.send_envelope(project_id, envelope)
    spans = {s["span_id"]: s for s in spans_consumer.get_spans()}

    def get_transaction(span_id: str):
        return spans[span_id]["attributes"]["sentry.dsc.transaction"]["value"]

    def get_project_id(span_id: str):
        return spans[span_id]["attributes"]["sentry.dsc.project_id"]["value"]

    def get_trace_id(span_id: str):
        return spans[span_id]["attributes"]["sentry.dsc.trace_id"]["value"]

    assert spans["aaaaaaaaaaaaaaaa"]["is_segment"] is True
    assert spans["bbbbbbbbbbbbbbbb"]["is_segment"] is False
    assert spans["cccccccccccccccc"]["is_segment"] is False
    assert get_transaction("aaaaaaaaaaaaaaaa") == "/my/fancy/endpoint"
    assert get_transaction("bbbbbbbbbbbbbbbb") == "/my/fancy/endpoint"
    assert get_transaction("cccccccccccccccc") == "/transaction/already/exists"
    assert get_project_id("aaaaaaaaaaaaaaaa") == "42"
    assert get_project_id("bbbbbbbbbbbbbbbb") == "42"
    assert get_project_id("cccccccccccccccc") == "41"
    assert get_trace_id("aaaaaaaaaaaaaaaa") == "5b8efff798038103d269b633813fc60c"
    assert get_trace_id("bbbbbbbbbbbbbbbb") == "5b8efff798038103d269b633813fc60c"
    assert get_trace_id("cccccccccccccccc") == "5b8efff798038103d269b633813fc60c"


@pytest.mark.parametrize("mode", ["legacy", "v2"])
def test_mobile_measurements(
    mini_sentry,
    relay,
    relay_with_processing,
    spans_consumer,
    mode,
):
    """
    Verify mobile measurements calculations (slow/frozen frames, stalls).
    """
    spans_consumer = spans_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    if mode == "v2":
        project_config["config"].setdefault("features", []).append(
            "projects:span-v2-experimental-processing"
        )

    relay = relay(relay_with_processing())

    ts = datetime.now(timezone.utc)

    envelope = envelope_with_spans(
        {
            "data": {
                "sentry.op": "ui.interaction.click",
                "release": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
                "environment": "prod",
                "replay_id": "3d76a6311de149b9b3f560827ea0ecf9",
                "user_agent.original": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/42.0",
                "client.address": "{{auto}}",
                "sentry.exclusive_time": 104,
            },
            "description": "<unknown>",
            "op": "ui.interaction.click",
            "parent_span_id": "8a6626cc9bdd5d9b",
            "span_id": "a6f029fbe0e2389a",
            "start_timestamp": ts.timestamp() - 5,
            "timestamp": ts.timestamp(),
            "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
            "origin": "mobile",
            "exclusive_time": 104,
            "measurements": {
                "app_start_cold": {"value": 0.123, "unit": "millisecond"},
                "frames_slow": {"value": 1},
                "frames_frozen": {"value": 2},
                "frames_total": {"value": 4},
                "stall_total_time": {"value": 4000, "unit": "millisecond"},
            },
            "segment_id": "8a6626cc9bdd5d9b",
        },
        trace_info={
            "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
            "public_key": project_config["publicKeys"][0]["publicKey"],
            "transaction": "/insights/projects/",
        },
    )

    relay.send_envelope(project_id, envelope)

    assert spans_consumer.get_span() == {
        "_meta": matches_any(),
        "attributes": {
            "client.address": {"type": "string", "value": "127.0.0.1"},
            "browser.name": {"type": "string", "value": "Firefox"},
            "sentry.description": {
                "type": "string",
                "value": "<unknown>",
            },
            "sentry.dsc.transaction": {
                "type": "string",
                "value": "/insights/projects/",
            },
            "sentry.dsc.project_id": {"type": "string", "value": "42"},
            "sentry.dsc.trace_id": {
                "type": "string",
                "value": "d3d20f000885466b8c8f947c9b92b8d3",
            },
            "sentry.environment": {"type": "string", "value": "prod"},
            "environment": None,
            "sentry.exclusive_time": {"type": "double", "value": 104.0},
            "sentry.op": {"type": "string", "value": "ui.interaction.click"},
            "sentry.origin": {"type": "string", "value": "mobile"},
            "sentry.relay.ingress": {"type": "string", "value": "legacy"},
            "sentry.release": {
                "type": "string",
                "value": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
            },
            "release": None,
            "sentry.replay_id": {
                "type": "string",
                "value": "3d76a6311de149b9b3f560827ea0ecf9",
            },
            "replay_id": None,
            "user_agent.original": {
                "type": "string",
                "value": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Firefox/42.0",
            },
            "stall_total_time": {"value": 4000.0, "type": "double"},
            "stall_percentage": {"value": 0.8, "type": "double"},
            "app.vitals.frames.slow.count": {"value": 1.0, "type": "double"},
            "app.vitals.frames.frozen.count": {"value": 2.0, "type": "double"},
            "app.vitals.frames.total.count": {"value": 4.0, "type": "double"},
            "frames_frozen_rate": {"value": 0.5, "type": "double"},
            "frames_slow_rate": {"value": 0.25, "type": "double"},
            "app.vitals.start.cold.value": {"value": 0.123, "type": "double"},
            **_if_dict(
                mode == "v2",
                {
                    "app.vitals.start.value": {"value": 0.123, "type": "double"},
                    "app.vitals.start.type": {"value": "cold", "type": "string"},
                },
            ),
            **lcp_cls_inp_differences(mode),
        },
        "downsampled_retention_days": 90,
        "end_timestamp": time_within(ts),
        "key_id": 123,
        "name": "ui.interaction.click",
        "organization_id": 1,
        "project_id": 42,
        "received": time_within(ts),
        "retention_days": 90,
        "span_id": "a6f029fbe0e2389a",
        "start_timestamp": time_within(ts.timestamp() - 5),
        "status": "ok",
        "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
    }


@pytest.mark.parametrize("mode", ["legacy", "v2"])
@pytest.mark.parametrize("client_address_auto", [True, False])
def test_ua_ip_inference(
    mini_sentry, relay, relay_with_processing, spans_consumer, client_address_auto, mode
):
    """
    Tests that IP addresses and user agent attributes are inferred.
    """
    spans_consumer = spans_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    if mode == "v2":
        project_config["config"].setdefault("features", []).append(
            "projects:span-v2-experimental-processing"
        )

    relay = relay(relay_with_processing())

    ts = datetime.now(timezone.utc)

    envelope = envelope_with_spans(
        {
            "data": {
                "sentry.op": "ui.webvital.lcp",
                "release": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
                "environment": "prod",
                "replay_id": "3d76a6311de149b9b3f560827ea0ecf9",
                "transaction": "/insights/projects/",
                **_if_dict(client_address_auto, {"client.address": "{{auto}}"}),
                "sentry.exclusive_time": 0,
                "sentry.pageload.span_id": "8a6626cc9bdd5d9b",
                "sentry.report_event": "navigation",
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
            "measurements": {},
            "segment_id": "8a6626cc9bdd5d9b",
        },
        trace_info={
            "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
            "public_key": project_config["publicKeys"][0]["publicKey"],
            "transaction": "/insights/projects/",
        },
    )

    relay.send_envelope(project_id, envelope)

    assert spans_consumer.get_span() == {
        "_meta": matches_any(),
        "attributes": {
            "client.address": {"type": "string", "value": "127.0.0.1"},
            "browser.name": {"type": "string", "value": "Firefox"},
            "sentry.description": {"type": "string", "value": "<unknown>"},
            "sentry.dsc.transaction": {
                "type": "string",
                "value": "/insights/projects/",
            },
            "sentry.dsc.project_id": {"type": "string", "value": "42"},
            "sentry.dsc.trace_id": {
                "type": "string",
                "value": "d3d20f000885466b8c8f947c9b92b8d3",
            },
            "sentry.environment": {"type": "string", "value": "prod"},
            "environment": None,
            "sentry.exclusive_time": {"type": "double", "value": 0.0},
            "sentry.op": {"type": "string", "value": "ui.webvital.lcp"},
            "sentry.origin": {"type": "string", "value": "auto.http.browser.lcp"},
            "sentry.pageload.span_id": {"type": "string", "value": "8a6626cc9bdd5d9b"},
            "sentry.relay.ingress": {"type": "string", "value": "legacy"},
            "sentry.release": {
                "type": "string",
                "value": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
            },
            "release": None,
            "sentry.replay_id": {
                "type": "string",
                "value": "3d76a6311de149b9b3f560827ea0ecf9",
            },
            "replay_id": None,
            "sentry.report_event": {"type": "string", "value": "navigation"},
            "sentry.segment.name": {"type": "string", "value": "/insights/projects/"},
            "transaction": None,
            "user_agent.original": {
                "type": "string",
                "value": "RelayIntegrationTests/1.0.0 Firefox/42.0",
            },
            **lcp_cls_inp_differences(mode),
        },
        "downsampled_retention_days": 90,
        "end_timestamp": time_within(ts),
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
    }


@pytest.mark.parametrize("mode", ["legacy", "v2"])
@pytest.mark.parametrize("origin", ["manual", "auto.http.browser.lcp"])
def test_name_inference(
    mini_sentry, relay, relay_with_processing, spans_consumer, mode, origin
):
    """
    Tests that span names are inferred.
    """
    spans_consumer = spans_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    if mode == "v2":
        project_config["config"].setdefault("features", []).append(
            "projects:span-v2-experimental-processing"
        )
    project_config["config"]["piiConfig"]["applications"]["data.'http.route'"] = [
        "@anything:mask"
    ]
    project_config["config"]["piiConfig"]["applications"][
        "$span.attributes.'http.route'.value"
    ] = ["@anything:mask"]

    relay = relay(relay_with_processing())

    ts = datetime.now(timezone.utc)

    envelope = envelope_with_spans(
        {
            # The combination of op, `http.request.method`, and
            # `http.route` means this span has its name synthesized as
            # `GET https://example.com`.
            "op": "http.client",
            "data": {
                "http.request.method": "GET",
                "http.route": "https://example.com",
            },
            "description": "Test span",
            "parent_span_id": "8a6626cc9bdd5d9b",
            "span_id": "9fd17741416e8e4e",
            "start_timestamp": ts.timestamp() - 0.5,
            "timestamp": ts.timestamp(),
            "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
            "origin": origin,
            "exclusive_time": 0,
            "measurements": {},
            "segment_id": "8a6626cc9bdd5d9b",
            "is_segment": False,
        },
        trace_info={
            "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
            "public_key": project_config["publicKeys"][0]["publicKey"],
            "transaction": "/insights/projects/",
        },
    )

    relay.send_envelope(project_id, envelope)

    # If the span's origin is "manual", the name should be the same as the description.
    # Otherwise it should be backfilled according to conventions, which includes
    # a redacted attribute.
    if origin == "manual":
        expected_name = "Test span"
    else:
        expected_name = "GET *******************"

    # _meta unfortunately differs slightly between the pipelines
    if mode == "v2":
        meta = {
            "attributes": {
                "http.route": {
                    "value": {
                        "": {
                            "len": 19,
                            "rem": [
                                [
                                    "@anything:mask",
                                    "m",
                                    0,
                                    19,
                                ],
                            ],
                        },
                    },
                },
            },
        }
    else:
        meta = {
            "attributes": {
                "http.route": {
                    "": {
                        "len": 19,
                        "rem": [
                            [
                                "@anything:mask",
                                "m",
                                0,
                                19,
                            ],
                        ],
                    },
                },
            }
        }

    assert spans_consumer.get_span() == {
        "attributes": {
            "client.address": {"type": "string", "value": "127.0.0.1"},
            "browser.name": {"type": "string", "value": "Firefox"},
            "http.request.method": {"type": "string", "value": "GET"},
            "http.route": {"type": "string", "value": "*******************"},
            "sentry.action": {"type": "string", "value": "GET"},
            "sentry.category": {"type": "string", "value": "http"},
            "sentry.description": {"type": "string", "value": "Test span"},
            "sentry.dsc.transaction": {
                "type": "string",
                "value": "/insights/projects/",
            },
            "sentry.dsc.project_id": {"type": "string", "value": "42"},
            "sentry.dsc.trace_id": {
                "type": "string",
                "value": "d3d20f000885466b8c8f947c9b92b8d3",
            },
            "sentry.exclusive_time": {"type": "double", "value": 0.0},
            "sentry.op": {"type": "string", "value": "http.client"},
            "sentry.origin": {"type": "string", "value": origin},
            "sentry.relay.ingress": {"type": "string", "value": "legacy"},
            "sentry.segment.id": {"type": "string", "value": "8a6626cc9bdd5d9b"},
            "user_agent.original": {
                "type": "string",
                "value": "RelayIntegrationTests/1.0.0 Firefox/42.0",
            },
            **lcp_cls_inp_differences(mode),
        },
        "downsampled_retention_days": 90,
        "end_timestamp": time_within(ts),
        "key_id": 123,
        "name": expected_name,
        "organization_id": 1,
        "project_id": 42,
        "received": time_within(ts),
        "retention_days": 90,
        "span_id": "9fd17741416e8e4e",
        "start_timestamp": time_within(ts.timestamp() - 0.5),
        "status": "ok",
        "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
        "is_segment": False,
        "parent_span_id": "8a6626cc9bdd5d9b",
        "_meta": meta,
    }


def _if_dict(cond, then):
    return then if cond else {}
