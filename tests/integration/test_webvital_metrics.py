import json
from datetime import datetime, timedelta, timezone

from sentry_sdk.envelope import Envelope, Item, PayloadRef

from .asserts import matches_any, time_within_delta


def v1_transaction_envelope(*payloads: dict, data: dict) -> Envelope:
    envelope = Envelope()

    spans = [payload for payload in payloads]

    envelope.add_item(
        Item(
            type="transaction",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "type": "transaction",
                        "timestamp": spans[0]["timestamp"],
                        "start_timestamp": spans[0]["start_timestamp"],
                        "spans": spans,
                        "contexts": {
                            "trace": {
                                "op": "pageload",
                                "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
                                "span_id": "968cff94913ebb07",
                                "sentry.origin": "manual",
                                "data": data,
                            }
                        },
                        "transaction": "pageload",
                        "environment": "production",
                        "platform": "node",
                    },
                ).encode()
            ),
        )
    )

    return envelope


def v1_envelope_with_spans(*payloads: dict, trace_info=None) -> Envelope:
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


def v2_envelope_with_spans(*payloads: dict, trace_info=None, metadata=None) -> Envelope:
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(json={"items": payloads, **(metadata or {})}),
            content_type="application/vnd.sentry.items.span.v2+json",
            headers={"item_count": len(payloads)},
        )
    )
    envelope.headers["trace"] = trace_info
    return envelope


# Test v1 legacy web vitals, with most sent as data on the transaction, and inp sent as a span.
def test_v1_transaction(
    mini_sentry,
    relay_with_processing,
    items_consumer,
):
    relay = relay_with_processing()
    items_consumer = items_consumer()

    project_id = 42
    mini_sentry.add_full_project_config(project_id)

    duration = timedelta(milliseconds=500)
    end = datetime.now(timezone.utc) - timedelta(seconds=1)
    start = end - duration

    envelope = v1_transaction_envelope(
        {
            "description": "<unknown>",
            "op": "ui.interaction.click",
            "parent_span_id": "bd429c44b67a3eb1",
            "span_id": "a6f029fbe0e2389a",
            "start_timestamp": start.timestamp(),
            "timestamp": end.timestamp() + 1,
            "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
            "origin": "auto.http.browser.inp",
            "exclusive_time": 104,
            "measurements": {"inp": {"value": 104}},
            "segment_id": "bd429c44b67a3eb1",
        },
        data={
            "browser.web_vital.cls.value": 100,
            "browser.web_vital.fcp.value": 200,
            "browser.web_vital.lcp.value": 400,
            "browser.web_vital.ttfb.value": 500,
        },
    )

    relay.send_envelope(project_id, envelope)

    expected_metrics = [
        {
            "organizationId": "1",
            "projectId": "42",
            "traceId": "a0fa8803753e40fd8124b21eeb2986b5",
            "itemId": matches_any(),
            "itemType": "TRACE_ITEM_TYPE_METRIC",
            "timestamp": time_within_delta(),
            "attributes": {
                "sentry.metric_name": {"stringValue": "browser.web_vital.cls"},
                "sentry.sdk.name": {"stringValue": "raven-node"},
                "sentry._internal.cooccuring.unit.none": {"boolValue": True},
                "sentry.metric_unit": {"stringValue": "none"},
                "sentry.sdk.version": {"stringValue": "2.6.3"},
                "sentry._internal.cooccuring.name.browser.web_vital.cls": {
                    "boolValue": True
                },
                "sentry.value": {"doubleValue": 100.0},
                "sentry.metric.source": {"stringValue": "span"},
                "sentry.timestamp_precise": {
                    "intValue": time_within_delta(expect_resolution="ns")
                },
                "sentry.span_id": {"stringValue": "968cff94913ebb07"},
                "sentry.payload_size_bytes": {"intValue": "175"},
                "sentry._internal.cooccuring.type.distribution": {"boolValue": True},
                "sentry.metric_type": {"stringValue": "distribution"},
                "sentry.platform": {"stringValue": "node"},
                "sentry.segment.name": {"stringValue": "pageload"},
                "sentry.environment": {"stringValue": "production"},
            },
            "clientSampleRate": 1.0,
            "serverSampleRate": 1.0,
            "retentionDays": 90,
            "received": time_within_delta(),
            "downsampledRetentionDays": 90,
        },
        {
            "organizationId": "1",
            "projectId": "42",
            "traceId": "a0fa8803753e40fd8124b21eeb2986b5",
            "itemId": matches_any(),
            "itemType": "TRACE_ITEM_TYPE_METRIC",
            "timestamp": time_within_delta(),
            "attributes": {
                "sentry.metric_name": {"stringValue": "browser.web_vital.fcp"},
                "sentry.sdk.name": {"stringValue": "raven-node"},
                "sentry._internal.cooccuring.name.browser.web_vital.fcp": {
                    "boolValue": True
                },
                "sentry.metric_unit": {"stringValue": "millisecond"},
                "sentry.sdk.version": {"stringValue": "2.6.3"},
                "sentry.value": {"doubleValue": 200.0},
                "sentry.metric.source": {"stringValue": "span"},
                "sentry.timestamp_precise": {
                    "intValue": time_within_delta(expect_resolution="ns")
                },
                "sentry.span_id": {"stringValue": "968cff94913ebb07"},
                "sentry.payload_size_bytes": {"intValue": "175"},
                "sentry.metric_type": {"stringValue": "distribution"},
                "sentry.platform": {"stringValue": "node"},
                "sentry._internal.cooccuring.type.distribution": {"boolValue": True},
                "sentry._internal.cooccuring.unit.millisecond": {"boolValue": True},
                "sentry.segment.name": {"stringValue": "pageload"},
                "sentry.environment": {"stringValue": "production"},
            },
            "clientSampleRate": 1.0,
            "serverSampleRate": 1.0,
            "retentionDays": 90,
            "received": time_within_delta(),
            "downsampledRetentionDays": 90,
        },
        {
            "organizationId": "1",
            "projectId": "42",
            "traceId": "d3d20f000885466b8c8f947c9b92b8d3",
            "itemId": matches_any(),
            "itemType": "TRACE_ITEM_TYPE_METRIC",
            "timestamp": time_within_delta(),
            "attributes": {
                "sentry.metric_name": {"stringValue": "browser.web_vital.inp"},
                "sentry.sdk.name": {"stringValue": "raven-node"},
                "sentry.origin": {"stringValue": "auto.http.browser.inp"},
                "sentry.metric_unit": {"stringValue": "millisecond"},
                "sentry.sdk.version": {"stringValue": "2.6.3"},
                "sentry.value": {"doubleValue": 104.0},
                "sentry._internal.cooccuring.name.browser.web_vital.inp": {
                    "boolValue": True
                },
                "sentry.metric.source": {"stringValue": "span"},
                "sentry.timestamp_precise": {
                    "intValue": time_within_delta(expect_resolution="ns")
                },
                "sentry.span_id": {"stringValue": "a6f029fbe0e2389a"},
                "sentry.payload_size_bytes": {"intValue": "209"},
                "sentry._internal.cooccuring.type.distribution": {"boolValue": True},
                "sentry.platform": {"stringValue": "node"},
                "sentry.metric_type": {"stringValue": "distribution"},
                "sentry._internal.cooccuring.unit.millisecond": {"boolValue": True},
                "sentry.segment.name": {"stringValue": "pageload"},
                "sentry.environment": {"stringValue": "production"},
            },
            "clientSampleRate": 1.0,
            "serverSampleRate": 1.0,
            "retentionDays": 90,
            "received": time_within_delta(),
            "downsampledRetentionDays": 90,
        },
        {
            "organizationId": "1",
            "projectId": "42",
            "traceId": "a0fa8803753e40fd8124b21eeb2986b5",
            "itemId": matches_any(),
            "itemType": "TRACE_ITEM_TYPE_METRIC",
            "timestamp": time_within_delta(),
            "attributes": {
                "sentry.metric_name": {"stringValue": "browser.web_vital.lcp"},
                "sentry.sdk.name": {"stringValue": "raven-node"},
                "sentry._internal.cooccuring.name.browser.web_vital.lcp": {
                    "boolValue": True
                },
                "sentry.metric_unit": {"stringValue": "millisecond"},
                "sentry.sdk.version": {"stringValue": "2.6.3"},
                "sentry.value": {"doubleValue": 400.0},
                "sentry.metric.source": {"stringValue": "span"},
                "sentry.metric_type": {"stringValue": "distribution"},
                "sentry.span_id": {"stringValue": "968cff94913ebb07"},
                "sentry.payload_size_bytes": {"intValue": "175"},
                "sentry._internal.cooccuring.type.distribution": {"boolValue": True},
                "sentry.timestamp_precise": {
                    "intValue": time_within_delta(expect_resolution="ns")
                },
                "sentry.platform": {"stringValue": "node"},
                "sentry._internal.cooccuring.unit.millisecond": {"boolValue": True},
                "sentry.segment.name": {"stringValue": "pageload"},
                "sentry.environment": {"stringValue": "production"},
            },
            "clientSampleRate": 1.0,
            "serverSampleRate": 1.0,
            "retentionDays": 90,
            "received": time_within_delta(),
            "downsampledRetentionDays": 90,
        },
        {
            "organizationId": "1",
            "projectId": "42",
            "traceId": "a0fa8803753e40fd8124b21eeb2986b5",
            "itemId": matches_any(),
            "itemType": "TRACE_ITEM_TYPE_METRIC",
            "timestamp": time_within_delta(),
            "attributes": {
                "sentry.metric_name": {"stringValue": "browser.web_vital.ttfb"},
                "sentry.sdk.name": {"stringValue": "raven-node"},
                "sentry._internal.cooccuring.name.browser.web_vital.ttfb": {
                    "boolValue": True
                },
                "sentry.metric_unit": {"stringValue": "millisecond"},
                "sentry.sdk.version": {"stringValue": "2.6.3"},
                "sentry.value": {"doubleValue": 500.0},
                "sentry.metric.source": {"stringValue": "span"},
                "sentry._internal.cooccuring.type.distribution": {"boolValue": True},
                "sentry.span_id": {"stringValue": "968cff94913ebb07"},
                "sentry.payload_size_bytes": {"intValue": "176"},
                "sentry.platform": {"stringValue": "node"},
                "sentry.metric_type": {"stringValue": "distribution"},
                "sentry.timestamp_precise": {
                    "intValue": time_within_delta(expect_resolution="ns")
                },
                "sentry._internal.cooccuring.unit.millisecond": {"boolValue": True},
                "sentry.segment.name": {"stringValue": "pageload"},
                "sentry.environment": {"stringValue": "production"},
            },
            "clientSampleRate": 1.0,
            "serverSampleRate": 1.0,
            "retentionDays": 90,
            "received": time_within_delta(),
            "downsampledRetentionDays": 90,
        },
    ]

    expected_metrics = [dict(sorted(item.items())) for item in expected_metrics]

    items = [dict(sorted(item.items())) for item in items_consumer.get_items()]
    items.sort(key=lambda item: item["attributes"]["sentry.metric_name"]["stringValue"])

    assert expected_metrics == items


# Test v1 legacy metrics, with all values sent on spans.  Includes performance score
# generation, as well as legacy metric span generation.
def test_v1_spans(mini_sentry, relay_with_processing, items_consumer, spans_consumer):

    spans_consumer = spans_consumer()
    items_consumer = items_consumer()
    relay = relay_with_processing()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["performanceScore"] = {
        "profiles": [
            {
                "name": "Desktop",
                "scoreComponents": [
                    {"measurement": "fcp", "weight": 0.15, "p10": 900, "p50": 1600},
                    {"measurement": "lcp", "weight": 0.30, "p10": 1200, "p50": 2400},
                    {"measurement": "cls", "weight": 0.25, "p10": 0.1, "p50": 0.25},
                    {"measurement": "ttfb", "weight": 0.30, "p10": 0.2, "p50": 0.4},
                ],
                "condition": {
                    "op": "eq",
                    "name": "event.contexts.browser.name",
                    "value": "Firefox",
                },
            },
            {
                "name": "Desktop INP",
                "scoreComponents": [
                    {"measurement": "inp", "weight": 1.0, "p10": 200, "p50": 400},
                ],
                "condition": {
                    "op": "eq",
                    "name": "event.contexts.browser.name",
                    "value": "Firefox",
                },
            },
        ],
    }
    project_config["config"]["txNameRules"] = [
        {
            "pattern": "**/interaction/*/**",
            "expiry": "3022-11-30T00:00:00.000000Z",
            "redaction": {"method": "replace", "substitution": "*"},
        }
    ]

    duration = timedelta(milliseconds=500)
    end = datetime.now(timezone.utc) - timedelta(seconds=1)
    start = end - duration

    envelope = v1_envelope_with_spans(
        {
            "op": "ui.interaction.click",
            "span_id": "bd429c44b67a3eb1",
            "segment_id": "bd429c44b67a3eb1",
            "start_timestamp": start.timestamp(),
            "timestamp": end.timestamp() + 1,
            "exclusive_time": 345.0,  # The SDK knows that this span has a lower exclusive time
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
            "measurements": {
                "cls": {"value": 100},
                "fcp": {"value": 200},
                "lcp": {"value": 400},
                "ttfb": {"value": 500},
            },
        },
        {
            "data": {
                "transaction": "/page/with/click/interaction/jane/123",
                "replay_id": "8477286c8e5148b386b71ade38374d58",
                "user": "[email]",
            },
            "profile_id": "3d9428087fda4ba0936788b70a7587d0",
            "op": "ui.interaction.click",
            "span_id": "cd429c44b67a3eb1",
            "segment_id": "cd429c44b67a3eb1",
            "start_timestamp": start.timestamp(),
            "timestamp": end.timestamp() + 1,
            "exclusive_time": 345.0,  # The SDK knows that this span has a lower exclusive time
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
            "measurements": {
                "inp": {"value": 100},
            },
        },
    )

    relay.send_envelope(project_id, envelope)

    spans = spans_consumer.get_spans(timeout=10.0, n=2)

    for span in spans:
        span.pop("received", None)

    spans.sort(key=lambda msg: msg["span_id"])

    expected_scores = [
        {
            "score.fcp": 0.14999972769539766,
            "score.lcp": 0.29986141375718806,
            "score.ratio.cls": 0.0,
            "score.ratio.fcp": 0.9999981846359844,
            "score.ratio.lcp": 0.9995380458572936,
            "score.ratio.ttfb": 0.0,
            "score.total": 0.4498611414525857,
            "score.ttfb": 0.0,
            "score.weight.cls": 0.25,
            "score.weight.fcp": 0.15,
            "score.weight.lcp": 0.3,
            "score.weight.ttfb": 0.3,
            "browser.web_vital.cls.value": 100.0,
            "browser.web_vital.fcp.value": 200.0,
            "browser.web_vital.lcp.value": 400.0,
            "browser.web_vital.ttfb.value": 500.0,
            "score.cls": 0.0,
        },
        {
            "browser.web_vital.inp.value": 100.0,
            "score.inp": 0.9948129113413748,
            "score.ratio.inp": 0.9948129113413748,
            "score.total": 0.9948129113413748,
            "score.weight.inp": 1.0,
        },
    ]

    # Confirm that we're emitting spans that contain the expected scores
    assert len(spans) == len(expected_scores)
    for span, scores in zip(spans, expected_scores):
        for key, score in scores.items():
            assert span["attributes"][key]["value"] == score

    expected_metrics = [
        {
            "organizationId": "1",
            "projectId": "42",
            "traceId": "ff62a8b040f340bda5d830223def1d81",
            "itemId": matches_any(),
            "itemType": "TRACE_ITEM_TYPE_METRIC",
            "timestamp": time_within_delta(),
            "attributes": {
                "sentry.metric_name": {"stringValue": "browser.web_vital.cls"},
                "sentry._internal.cooccuring.unit.none": {"boolValue": True},
                "sentry.metric_unit": {"stringValue": "none"},
                "score.cls": {"doubleValue": 0.0},
                "sentry._internal.cooccuring.name.browser.web_vital.cls": {
                    "boolValue": True
                },
                "sentry.value": {"doubleValue": 100.0},
                "user_agent.original": {
                    "stringValue": "RelayIntegrationTests/1.0.0 Firefox/42.0"
                },
                "score.weight.cls": {"doubleValue": 0.25},
                "sentry.metric.source": {"stringValue": "span"},
                "sentry._internal.cooccuring.type.distribution": {"boolValue": True},
                "sentry.span_id": {"stringValue": "bd429c44b67a3eb1"},
                "sentry.payload_size_bytes": {"intValue": "176"},
                "sentry.timestamp_precise": {
                    "intValue": time_within_delta(expect_resolution="ns")
                },
                "sentry.metric_type": {"stringValue": "distribution"},
                "score.ratio.cls": {"doubleValue": 0.0},
            },
            "clientSampleRate": 1.0,
            "serverSampleRate": 1.0,
            "retentionDays": 90,
            "received": time_within_delta(),
            "downsampledRetentionDays": 90,
        },
        {
            "organizationId": "1",
            "projectId": "42",
            "traceId": "ff62a8b040f340bda5d830223def1d81",
            "itemId": matches_any(),
            "itemType": "TRACE_ITEM_TYPE_METRIC",
            "timestamp": time_within_delta(),
            "attributes": {
                "sentry.metric_name": {"stringValue": "browser.web_vital.fcp"},
                "sentry._internal.cooccuring.name.browser.web_vital.fcp": {
                    "boolValue": True
                },
                "score.ratio.fcp": {"doubleValue": 0.9999981846359844},
                "sentry.metric_unit": {"stringValue": "millisecond"},
                "score.fcp": {"doubleValue": 0.14999972769539766},
                "sentry.value": {"doubleValue": 200.0},
                "score.weight.fcp": {"doubleValue": 0.15},
                "user_agent.original": {
                    "stringValue": "RelayIntegrationTests/1.0.0 Firefox/42.0"
                },
                "sentry.metric.source": {"stringValue": "span"},
                "sentry._internal.cooccuring.type.distribution": {"boolValue": True},
                "sentry.span_id": {"stringValue": "bd429c44b67a3eb1"},
                "sentry.payload_size_bytes": {"intValue": "176"},
                "sentry.timestamp_precise": {
                    "intValue": time_within_delta(expect_resolution="ns")
                },
                "sentry.metric_type": {"stringValue": "distribution"},
                "sentry._internal.cooccuring.unit.millisecond": {"boolValue": True},
            },
            "clientSampleRate": 1.0,
            "serverSampleRate": 1.0,
            "retentionDays": 90,
            "received": time_within_delta(),
            "downsampledRetentionDays": 90,
        },
        {
            "organizationId": "1",
            "projectId": "42",
            "traceId": "ff62a8b040f340bda5d830223def1d81",
            "itemId": matches_any(),
            "itemType": "TRACE_ITEM_TYPE_METRIC",
            "timestamp": time_within_delta(),
            "attributes": {
                "sentry.metric_name": {"stringValue": "browser.web_vital.inp"},
                "score.inp": {"doubleValue": 0.9948129113413748},
                "score.ratio.inp": {"doubleValue": 0.9948129113413748},
                "sentry.metric_unit": {"stringValue": "millisecond"},
                "sentry.value": {"doubleValue": 100.0},
                "user_agent.original": {
                    "stringValue": "RelayIntegrationTests/1.0.0 Firefox/42.0"
                },
                "sentry._internal.cooccuring.name.browser.web_vital.inp": {
                    "boolValue": True
                },
                "sentry.metric.source": {"stringValue": "span"},
                "sentry.timestamp_precise": {
                    "intValue": time_within_delta(expect_resolution="ns")
                },
                "sentry.span_id": {"stringValue": "cd429c44b67a3eb1"},
                "sentry.payload_size_bytes": {"intValue": "227"},
                "sentry.metric_type": {"stringValue": "distribution"},
                "sentry._internal.cooccuring.type.distribution": {"boolValue": True},
                "score.weight.inp": {"doubleValue": 1.0},
                "sentry._internal.cooccuring.unit.millisecond": {"boolValue": True},
                "sentry.segment.name": {
                    "stringValue": "/page/with/click/interaction/*/*"
                },
            },
            "clientSampleRate": 1.0,
            "serverSampleRate": 1.0,
            "retentionDays": 90,
            "received": time_within_delta(),
            "downsampledRetentionDays": 90,
        },
        {
            "organizationId": "1",
            "projectId": "42",
            "traceId": "ff62a8b040f340bda5d830223def1d81",
            "itemId": matches_any(),
            "itemType": "TRACE_ITEM_TYPE_METRIC",
            "timestamp": time_within_delta(),
            "attributes": {
                "score.ratio.lcp": {"doubleValue": 0.9995380458572936},
                "sentry.metric_name": {"stringValue": "browser.web_vital.lcp"},
                "sentry._internal.cooccuring.name.browser.web_vital.lcp": {
                    "boolValue": True
                },
                "sentry.metric_unit": {"stringValue": "millisecond"},
                "score.weight.lcp": {"doubleValue": 0.3},
                "user_agent.original": {
                    "stringValue": "RelayIntegrationTests/1.0.0 Firefox/42.0"
                },
                "sentry.value": {"doubleValue": 400.0},
                "score.lcp": {"doubleValue": 0.29986141375718806},
                "sentry.metric.source": {"stringValue": "span"},
                "sentry.timestamp_precise": {
                    "intValue": time_within_delta(expect_resolution="ns")
                },
                "sentry.span_id": {"stringValue": "bd429c44b67a3eb1"},
                "sentry.payload_size_bytes": {"intValue": "176"},
                "sentry._internal.cooccuring.type.distribution": {"boolValue": True},
                "sentry.metric_type": {"stringValue": "distribution"},
                "sentry._internal.cooccuring.unit.millisecond": {"boolValue": True},
            },
            "clientSampleRate": 1.0,
            "serverSampleRate": 1.0,
            "retentionDays": 90,
            "received": time_within_delta(),
            "downsampledRetentionDays": 90,
        },
        {
            "organizationId": "1",
            "projectId": "42",
            "traceId": "ff62a8b040f340bda5d830223def1d81",
            "itemId": matches_any(),
            "itemType": "TRACE_ITEM_TYPE_METRIC",
            "timestamp": time_within_delta(),
            "attributes": {
                "sentry.metric_name": {"stringValue": "browser.web_vital.ttfb"},
                "sentry._internal.cooccuring.name.browser.web_vital.ttfb": {
                    "boolValue": True
                },
                "score.ttfb": {"doubleValue": 0.0},
                "sentry.metric_unit": {"stringValue": "millisecond"},
                "score.weight.ttfb": {"doubleValue": 0.3},
                "score.ratio.ttfb": {"doubleValue": 0.0},
                "sentry.value": {"doubleValue": 500.0},
                "user_agent.original": {
                    "stringValue": "RelayIntegrationTests/1.0.0 Firefox/42.0"
                },
                "sentry.metric.source": {"stringValue": "span"},
                "sentry.timestamp_precise": {
                    "intValue": time_within_delta(expect_resolution="ns")
                },
                "sentry.span_id": {"stringValue": "bd429c44b67a3eb1"},
                "sentry.payload_size_bytes": {"intValue": "180"},
                "sentry._internal.cooccuring.type.distribution": {"boolValue": True},
                "sentry.metric_type": {"stringValue": "distribution"},
                "sentry._internal.cooccuring.unit.millisecond": {"boolValue": True},
            },
            "clientSampleRate": 1.0,
            "serverSampleRate": 1.0,
            "retentionDays": 90,
            "received": time_within_delta(),
            "downsampledRetentionDays": 90,
        },
    ]
    expected_metrics = [dict(sorted(item.items())) for item in expected_metrics]

    items = [dict(sorted(item.items())) for item in items_consumer.get_items()]
    items.sort(key=lambda item: item["attributes"]["sentry.metric_name"]["stringValue"])

    assert expected_metrics == items


# Test standalone spans (just lcp)
def test_v1_standalone_span(mini_sentry, relay_with_processing, items_consumer):
    items_consumer = items_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)

    relay = relay_with_processing()

    ts = datetime.now(timezone.utc)

    envelope = v1_envelope_with_spans(
        {
            "data": {
                "sentry.op": "ui.webvital.lcp",
                "release": "frontend@488531b11e6401fa530ac25554d44426e6ef0f0b",
                "environment": "prod",
                "replay_id": "3d76a6311de149b9b3f560827ea0ecf9",
                "transaction": "/insights/projects/",
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
        },
        trace_info={
            "trace_id": "d3d20f000885466b8c8f947c9b92b8d3",
            "public_key": project_config["publicKeys"][0]["publicKey"],
            "transaction": "/insights/projects/",
        },
    )

    relay.send_envelope(project_id, envelope)

    items = items_consumer.get_items()

    assert len(items) == 1
    assert (
        items[0]["attributes"]["sentry.metric_name"]["stringValue"]
        == "browser.web_vital.lcp"
    )


def test_v2(
    mini_sentry,
    relay_with_processing,
    items_consumer,
):
    relay = relay_with_processing()
    items_consumer = items_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)

    ts = datetime.now(timezone.utc)
    envelope = v2_envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "is_segment": True,
            "name": "pageload",
            "status": "ok",
            "attributes": {
                "sentry.op": {"value": "pageload", "type": "string"},
                "sentry.segment.id": {"value": "bd429c44b67a3eb1", "type": "string"},
                "cls": {"value": 100.0, "type": "double"},
                "fcp": {"value": 200.0, "type": "double"},
                "lcp": {"value": 400.0, "type": "double"},
                "ttfb": {"value": 500.0, "type": "double"},
            },
        },
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b176",
            "is_segment": True,
            "name": "ui.interaction.drag",
            "status": "ok",
            "attributes": {
                "sentry.op": {"value": "ui.interaction.drag", "type": "string"},
                "sentry.profile_id": {
                    "value": "3d9428087fda4ba0936788b70a7587d0",
                    "type": "string",
                },
                "sentry.segment.id": {"value": "cd429c44b67a3eb1", "type": "string"},
                "inp": {"value": 100.0, "type": "double"},
            },
        },
        metadata={
            "version": 2,
            "ingest_settings": {
                "infer_user_agent": "auto",
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

    items = items_consumer.get_items()
    items.sort(key=lambda item: item["attributes"]["sentry.metric_name"]["stringValue"])
    assert len(items) == 5
    assert (
        items[0]["attributes"]["sentry.metric_name"]["stringValue"]
        == "browser.web_vital.cls"
    )
    assert (
        items[1]["attributes"]["sentry.metric_name"]["stringValue"]
        == "browser.web_vital.fcp"
    )
    assert (
        items[2]["attributes"]["sentry.metric_name"]["stringValue"]
        == "browser.web_vital.inp"
    )
    assert (
        items[3]["attributes"]["sentry.metric_name"]["stringValue"]
        == "browser.web_vital.lcp"
    )
    assert (
        items[4]["attributes"]["sentry.metric_name"]["stringValue"]
        == "browser.web_vital.ttfb"
    )
