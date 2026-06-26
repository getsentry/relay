import json
from datetime import datetime, timedelta, timezone

from sentry_sdk.envelope import Envelope, Item, PayloadRef


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

    envelope = Envelope()
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "op": "pageload",
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
                ).encode()
            ),
        )
    )
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(
                bytes=json.dumps(
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
                        "measurements": {"inp": {"value": 104, "unit": "millisecond"}},
                        "segment_id": "bd429c44b67a3eb1",
                    },
                ).encode()
            ),
        )
    )

    relay.send_envelope(project_id, envelope)

    items = items_consumer.get_items()
    items.sort(key=lambda item: item["attributes"]["sentry.metric_name"]["stringValue"])
    assert len(items) == 5
    assert (
        items[0]["attributes"]["sentry.metric_name"]["stringValue"]
        == "browser.web_vital.cls.value"
    )
    assert (
        items[1]["attributes"]["sentry.metric_name"]["stringValue"]
        == "browser.web_vital.fcp.value"
    )
    assert (
        items[2]["attributes"]["sentry.metric_name"]["stringValue"]
        == "browser.web_vital.inp.value"
    )
    assert (
        items[3]["attributes"]["sentry.metric_name"]["stringValue"]
        == "browser.web_vital.lcp.value"
    )
    assert (
        items[4]["attributes"]["sentry.metric_name"]["stringValue"]
        == "browser.web_vital.ttfb.value"
    )


def test_v1_standalone_span(mini_sentry, relay_with_processing, items_consumer):
    """
    Test verifies LCP spans processed via the SpanV2 and legacy standalone processing pipeline are equally processed.

    Some differences between the pipelines exist and are noted in the test.
    """
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
        == "browser.web_vital.lcp.value"
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
        == "browser.web_vital.cls.value"
    )
    assert (
        items[1]["attributes"]["sentry.metric_name"]["stringValue"]
        == "browser.web_vital.fcp.value"
    )
    assert (
        items[2]["attributes"]["sentry.metric_name"]["stringValue"]
        == "browser.web_vital.inp.value"
    )
    assert (
        items[3]["attributes"]["sentry.metric_name"]["stringValue"]
        == "browser.web_vital.lcp.value"
    )
    assert (
        items[4]["attributes"]["sentry.metric_name"]["stringValue"]
        == "browser.web_vital.ttfb.value"
    )
