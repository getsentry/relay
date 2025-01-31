import json
from datetime import datetime, timedelta, timezone

import pytest
from sentry_sdk.envelope import Envelope, Item, PayloadRef
from .test_store import make_transaction


TEST_CONFIG = {
    "aggregator": {
        "bucket_interval": 1,
        "initial_delay": 0,
    }
}


def envelope_with_ourlogs(start: datetime, end: datetime) -> Envelope:
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="otel_log",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "timeUnixNano": str(int(start.timestamp() * 1e9)),
                        "observedTimeUnixNano": str(int(end.timestamp() * 1e9)),
                        "severityNumber": 10,
                        "severityText": "Information",
                        "traceId": "5B8EFFF798038103D269B633813FC60C",
                        "spanId": "EEE19B7EC3C1B174",
                        "body": {"stringValue": "Example log record"},
                        "attributes": [
                            {
                                "key": "string.attribute",
                                "value": {"stringValue": "some string"},
                            },
                            {"key": "boolean.attribute", "value": {"boolValue": True}},
                            {"key": "int.attribute", "value": {"intValue": "10"}},
                            {
                                "key": "double.attribute",
                                "value": {"doubleValue": 637.704},
                            },
                        ],
                    }
                ).encode()
            ),
        )
    )
    return envelope


def test_ourlog_extraction(
    mini_sentry,
    relay_with_processing,
    ourlogs_consumer,
):
    ourlogs_consumer = ourlogs_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]

    relay = relay_with_processing(options=TEST_CONFIG)

    duration = timedelta(milliseconds=500)
    now = datetime.now(timezone.utc)
    end = now - timedelta(seconds=1)
    start = end - duration

    # Send OTel log and sentry log via envelope
    envelope = envelope_with_ourlogs(start, end)
    relay.send_envelope(project_id, envelope)

    ourlogs = ourlogs_consumer.get_ourlogs()
    assert len(ourlogs) == 1
    expected = {
        "organization_id": 1,
        "project_id": 42,
        "retention_days": 90,
        "timestamp_nanos": int(start.timestamp() * 1e9),
        "observed_timestamp_nanos": int(end.timestamp() * 1e9),
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "body": "Example log record",
        "trace_flags": 0,
        "span_id": "eee19b7ec3c1b174",
        "severity_text": "Information",
        "severity_number": 10,
        "attributes": {
            "string.attribute": {"string_value": "some string"},
            "boolean.attribute": {"bool_value": True},
            "int.attribute": {"int_value": 10},
            "double.attribute": {"double_value": 637.704},
        },
    }

    del ourlogs[0]["received"]
    assert ourlogs[0] == expected

    ourlogs_consumer.assert_empty()


def test_ourlog_extraction_is_disabled_without_feature(
    mini_sentry,
    relay_with_processing,
    ourlogs_consumer,
):
    ourlogs_consumer = ourlogs_consumer()
    relay = relay_with_processing(options=TEST_CONFIG)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = []

    duration = timedelta(milliseconds=500)
    now = datetime.now(timezone.utc)
    end = now - timedelta(seconds=1)
    start = end - duration

    envelope = envelope_with_ourlogs(start, end)
    relay.send_envelope(project_id, envelope)

    ourlogs = ourlogs_consumer.get_ourlogs()
    assert len(ourlogs) == 0

    ourlogs_consumer.assert_empty()


@pytest.mark.parametrize(
    "sample_rate,expected_ourlogs",
    [
        (None, 0),
        (1.0, 1),
        (0.0, 0),
    ],
)
def test_ourlog_breadcrumb_extraction_sample_rate(
    mini_sentry,
    relay_with_processing,
    ourlogs_consumer,
    sample_rate,
    expected_ourlogs,
):
    ourlogs_consumer = ourlogs_consumer()
    project_id = 42

    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "projects:ourlogs-breadcrumb-extraction",
        "organizations:ourlogs-ingestion",
    ]

    mini_sentry.global_config["options"] = {
        "relay.ourlogs-breadcrumb-extraction.sample-rate": sample_rate,
        "relay.ourlogs-breadcrumb-extraction.max-breadcrumbs-converted": 100,
    }

    def send_transaction_with_breadcrumb(upstream):
        transaction = make_transaction({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
        transaction["breadcrumbs"] = [
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message": "Test breadcrumb",
                "category": "test",
                "level": "info",
            }
        ]
        transaction["contexts"]["sentry_logs"] = {
            "deduplicated_breadcrumbs": True,
            "type": "sentry_logs",
        }
        envelope = Envelope()
        envelope.add_transaction(transaction)
        upstream.send_envelope(project_id, envelope)

    relay = relay_with_processing(options=TEST_CONFIG)
    send_transaction_with_breadcrumb(relay)
    assert len(ourlogs_consumer.get_ourlogs()) == expected_ourlogs
    ourlogs_consumer.assert_empty()
