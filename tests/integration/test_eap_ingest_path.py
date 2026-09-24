import re
from datetime import datetime, timezone

from sentry_sdk.envelope import Envelope, Item, PayloadRef

from .asserts import matches
from .test_store import make_transaction


def envelope_with_spans(*payloads: dict, metadata=None) -> Envelope:
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(json={"items": payloads, **(metadata or {})}),
            content_type="application/vnd.sentry.items.span.v2+json",
            headers={"item_count": len(payloads)},
        )
    )
    return envelope


def envelope_with_logs(*payloads: dict, metadata=None) -> Envelope:
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="log",
            payload=PayloadRef(json={"items": payloads, **(metadata or {})}),
            content_type="application/vnd.sentry.items.log+json",
            headers={"item_count": len(payloads)},
        )
    )
    return envelope


def envelope_with_trace_metrics(*payloads: dict, metadata=None) -> Envelope:
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="trace_metric",
            payload=PayloadRef(json={"items": payloads, **(metadata or {})}),
            content_type="application/vnd.sentry.items.trace-metric+json",
            headers={"item_count": len(payloads)},
        )
    )
    return envelope


def matches_relay_hop(version: str, public_key: str):
    pattern = re.compile(
        rf"^{re.escape(version)}(?:@\[[0-9a-f]{{8}}\])?:{re.escape(public_key)}$"
    )
    return matches(
        lambda value: isinstance(value, str) and pattern.fullmatch(value) is not None
    )


def span_ingest_path(relay, latest_relay_version):
    return {
        "type": "array",
        "value": [
            matches_relay_hop(latest_relay_version, key)
            for key in relay.iter_public_keys()
        ],
    }


def item_ingest_path(relay, latest_relay_version):
    return {
        "arrayValue": {
            "values": [
                {"stringValue": matches_relay_hop(latest_relay_version, key)}
                for key in relay.iter_public_keys()
            ]
        }
    }


def test_span_v2_ingest_path(
    mini_sentry,
    relay,
    relay_with_processing,
    spans_consumer,
    latest_relay_version,
):
    spans_consumer = spans_consumer()
    relay = relay(relay_with_processing())

    project_id = 42
    mini_sentry.add_full_project_config(project_id)

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
        },
        metadata={"version": 2},
    )

    relay.send_envelope(project_id, envelope)

    span = spans_consumer.get_span()
    assert span["attributes"]["sentry.relay.ingest_path"] == span_ingest_path(
        relay, latest_relay_version
    )


def test_legacy_transaction_spans_ingest_path(
    mini_sentry,
    relay,
    relay_with_processing,
    spans_consumer,
    latest_relay_version,
):
    spans_consumer = spans_consumer()
    relay = relay(relay_with_processing())

    project_id = 42
    mini_sentry.add_full_project_config(project_id)

    event = make_transaction({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
    event["spans"] = [
        {
            "description": "GET /api/0/organizations/?member=1",
            "op": "http",
            "origin": "manual",
            "parent_span_id": "968cff94913ebb07",
            "span_id": "bbbbbbbbbbbbbbbb",
            "start_timestamp": event["start_timestamp"],
            "status": "success",
            "timestamp": event["timestamp"],
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        }
    ]

    relay.send_event(project_id, event)

    spans = spans_consumer.get_spans(n=2)
    assert len(spans) == 2
    expected_ingest_path = span_ingest_path(relay, latest_relay_version)
    assert all(
        span["attributes"]["sentry.relay.ingest_path"] == expected_ingest_path
        for span in spans
    )


def test_log_ingest_path(
    mini_sentry,
    relay,
    relay_with_processing,
    items_consumer,
    latest_relay_version,
):
    items_consumer = items_consumer()
    relay = relay(relay_with_processing())

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]
    project_config["config"]["retentions"] = {
        "log": {"standard": 30, "downsampled": 13 * 30},
    }

    ts = datetime.now(timezone.utc)
    envelope = envelope_with_logs(
        {
            "timestamp": ts.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "level": "info",
            "body": "Example log record",
        }
    )

    relay.send_envelope(project_id, envelope)

    item = items_consumer.get_item()
    assert item["attributes"]["sentry.relay.ingest_path"] == item_ingest_path(
        relay, latest_relay_version
    )


def test_trace_metric_ingest_path(
    mini_sentry,
    relay,
    relay_with_processing,
    items_consumer,
    latest_relay_version,
):
    items_consumer = items_consumer()
    relay = relay(relay_with_processing())

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:tracemetrics-ingestion",
    ]
    project_config["config"]["retentions"] = {
        "traceMetric": {"standard": 30, "downsampled": 13 * 30},
    }

    ts = datetime.now(timezone.utc)
    envelope = envelope_with_trace_metrics(
        {
            "timestamp": ts.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "name": "http.request.duration seconds",
            "type": "distribution",
            "value": 123.45,
            "unit": "millisecond",
        }
    )

    relay.send_envelope(project_id, envelope)

    item = items_consumer.get_item()
    assert item["attributes"]["sentry.relay.ingest_path"] == item_ingest_path(
        relay, latest_relay_version
    )
