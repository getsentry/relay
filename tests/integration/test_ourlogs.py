import json

from datetime import datetime, timezone

from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_relay.consts import DataCategory

from sentry_protos.snuba.v1.trace_item_pb2 import TraceItem, AnyValue
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import MessageToDict

from .asserts.time import time_within_delta

import pytest


TEST_CONFIG = {
    "aggregator": {
        "bucket_interval": 1,
        "initial_delay": 0,
    }
}


def envelope_with_sentry_logs(*payloads: dict) -> Envelope:
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="log",
            payload=PayloadRef(json={"items": payloads}),
            content_type="application/vnd.sentry.items.log+json",
            headers={"item_count": len(payloads)},
        )
    )
    return envelope


def envelope_with_otel_logs(timestamp_nanos: str) -> Envelope:
    envelope = Envelope()

    envelope.add_item(
        Item(
            type="otel_log",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "timeUnixNano": timestamp_nanos,
                        "observedTimeUnixNano": timestamp_nanos,
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


def test_ourlog_extraction_with_otel_logs(
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
    start = datetime.now(timezone.utc)
    timestamp = start.timestamp()
    timestamp_nanos = int(timestamp * 1e9)
    envelope = envelope_with_otel_logs(str(timestamp_nanos))

    relay.send_envelope(project_id, envelope)

    timestamp_proto = Timestamp()

    timestamp_proto.FromSeconds(int(timestamp))

    expected_logs = [
        MessageToDict(
            TraceItem(
                organization_id=1,
                project_id=project_id,
                timestamp=timestamp_proto,
                trace_id="5b8efff798038103d269b633813fc60c",
                item_id=timestamp_nanos.to_bytes(
                    length=16,
                    byteorder="little",
                    signed=False,
                ),
                item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                attributes={
                    "boolean.attribute": AnyValue(bool_value=True),
                    "browser.name": AnyValue(string_value="Python Requests"),
                    "browser.version": AnyValue(string_value="2.32"),
                    "double.attribute": AnyValue(double_value=637.704),
                    "int.attribute": AnyValue(int_value=10),
                    "sentry.body": AnyValue(string_value="Example log record"),
                    "sentry.severity_number": AnyValue(int_value=10),
                    "sentry.severity_text": AnyValue(string_value="Information"),
                    "sentry.span_id": AnyValue(string_value="eee19b7ec3c1b174"),
                    "sentry.timestamp_nanos": AnyValue(
                        string_value=str(timestamp_nanos)
                    ),
                    "sentry.timestamp_precise": AnyValue(int_value=timestamp_nanos),
                    "sentry.trace_flags": AnyValue(int_value=0),
                    "string.attribute": AnyValue(string_value="some string"),
                },
                retention_days=90,
                client_sample_rate=1.0,
                server_sample_rate=1.0,
            )
        )
    ]

    logs = [MessageToDict(log) for log in ourlogs_consumer.get_ourlogs()]

    for log, expected_log in zip(logs, expected_logs):
        # we can't generate uuid7 with a specific timestamp
        # in Python just yet so we're overriding it
        expected_log["itemId"] = log["itemId"]
        expected_log["received"] = time_within_delta()

        # This field is set by Relay so we need to remove it
        del log["attributes"]["sentry.observed_timestamp_nanos"]

    assert logs == expected_logs

    ourlogs_consumer.assert_empty()


def test_ourlog_multiple_containers_not_allowed(
    mini_sentry,
    relay_with_processing,
    ourlogs_consumer,
    outcomes_consumer,
):
    ourlogs_consumer = ourlogs_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]

    relay = relay_with_processing(options=TEST_CONFIG)
    start = datetime.now(timezone.utc)
    envelope = Envelope()

    for _ in range(2):
        payload = {
            "timestamp": start.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "level": "error",
            "body": "oops, not again",
        }
        envelope.add_item(
            Item(
                type="log",
                payload=PayloadRef(json={"items": [payload]}),
                content_type="application/vnd.sentry.items.log+json",
                headers={"item_count": 1},
            )
        )

    relay.send_envelope(project_id, envelope)

    outcomes = outcomes_consumer.get_outcomes()

    outcomes.sort(key=lambda o: sorted(o.items()))

    assert 300 < outcomes[1].pop("quantity") < 400
    assert outcomes == [
        {
            "category": DataCategory.LOG_ITEM.value,
            "timestamp": time_within_delta(),
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 2,
            "reason": "duplicate_item",
        },
        {
            "category": DataCategory.LOG_BYTE.value,
            "timestamp": time_within_delta(),
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "reason": "duplicate_item",
        },
    ]

    ourlogs_consumer.assert_empty()


def test_ourlog_extraction_with_sentry_logs(
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
    start = datetime.now(timezone.utc)
    timestamp = start.timestamp()
    envelope = envelope_with_sentry_logs(
        {
            "timestamp": timestamp,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "level": "error",
            "body": "This is really bad",
        },
        {
            "timestamp": timestamp,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Example log record",
            "severity_number": 10,
            "attributes": {
                "boolean.attribute": {"value": True, "type": "boolean"},
                "integer.attribute": {"value": 42, "type": "integer"},
                "double.attribute": {"value": 1.23, "type": "double"},
                "string.attribute": {"value": "some string", "type": "string"},
                "pii": {"value": "4242 4242 4242 4242", "type": "string"},
                "sentry.severity_text": {"value": "info", "type": "string"},
                "unknown_type": {"value": "info", "type": "unknown"},
                "broken_type": {"value": "info", "type": "not_a_real_type"},
                "mismatched_type": {"value": "some string", "type": "boolean"},
                "valid_string_with_other": {
                    "value": "test",
                    "type": "string",
                    "some_other_field": "some_other_value",
                },
            },
        },
    )

    relay.send_envelope(project_id, envelope)

    timestamp_nanos = int(timestamp * 1e6) * 1000
    timestamp_proto = Timestamp()

    timestamp_proto.FromSeconds(int(timestamp))

    expected_logs = [
        MessageToDict(log)
        for log in [
            TraceItem(
                organization_id=1,
                project_id=project_id,
                timestamp=timestamp_proto,
                trace_id="5b8efff798038103d269b633813fc60c",
                item_id=timestamp_nanos.to_bytes(
                    length=16,
                    byteorder="little",
                    signed=False,
                ),
                item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                attributes={
                    "browser.name": AnyValue(string_value="Python Requests"),
                    "browser.version": AnyValue(string_value="2.32"),
                    "sentry.body": AnyValue(string_value="This is really bad"),
                    "sentry.severity_number": AnyValue(int_value=17),
                    "sentry.severity_text": AnyValue(string_value="error"),
                    "sentry.span_id": AnyValue(string_value="eee19b7ec3c1b175"),
                    "sentry.trace_flags": AnyValue(int_value=0),
                    "sentry.observed_timestamp_nanos": AnyValue(
                        string_value=str(timestamp_nanos)
                    ),
                    "sentry.timestamp_precise": AnyValue(int_value=timestamp_nanos),
                    "sentry.timestamp_nanos": AnyValue(
                        string_value=str(timestamp_nanos)
                    ),
                },
                retention_days=90,
                client_sample_rate=1.0,
                server_sample_rate=1.0,
            ),
            TraceItem(
                organization_id=1,
                project_id=project_id,
                timestamp=timestamp_proto,
                trace_id="5b8efff798038103d269b633813fc60c",
                item_id=timestamp_nanos.to_bytes(
                    length=16,
                    byteorder="little",
                    signed=False,
                ),
                item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                attributes={
                    "boolean.attribute": AnyValue(bool_value=True),
                    "browser.name": AnyValue(string_value="Python Requests"),
                    "browser.version": AnyValue(string_value="2.32"),
                    "double.attribute": AnyValue(double_value=1.23),
                    "integer.attribute": AnyValue(int_value=42),
                    "pii": AnyValue(string_value="[creditcard]"),
                    "sentry.body": AnyValue(string_value="Example log record"),
                    "sentry.severity_number": AnyValue(int_value=9),
                    "sentry.severity_text": AnyValue(string_value="info"),
                    "sentry.trace_flags": AnyValue(int_value=0),
                    "sentry.span_id": AnyValue(string_value="eee19b7ec3c1b174"),
                    "sentry.observed_timestamp_nanos": AnyValue(
                        string_value=str(timestamp_nanos)
                    ),
                    "string.attribute": AnyValue(string_value="some string"),
                    "valid_string_with_other": AnyValue(string_value="test"),
                    "sentry.timestamp_precise": AnyValue(int_value=timestamp_nanos),
                    "sentry.timestamp_nanos": AnyValue(
                        string_value=str(timestamp_nanos)
                    ),
                },
                retention_days=90,
                client_sample_rate=1.0,
                server_sample_rate=1.0,
            ),
        ]
    ]

    logs = [MessageToDict(log) for log in ourlogs_consumer.get_ourlogs()]

    for log, expected_log in zip(logs, expected_logs):
        # we can't generate uuid7 with a specific timestamp
        # in Python just yet so we're overriding it
        expected_log["itemId"] = log["itemId"]
        expected_log["received"] = time_within_delta()

    assert logs == expected_logs

    ourlogs_consumer.assert_empty()


def test_ourlog_extraction_with_sentry_logs_with_missing_fields(
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
    start = datetime.now(timezone.utc)
    timestamp = start.timestamp()
    envelope = envelope_with_sentry_logs(
        {
            "timestamp": timestamp,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "level": "warn",
            "body": "Example log record 2",
        }
    )

    relay.send_envelope(project_id, envelope)

    timestamp_nanos = int(timestamp * 1e6) * 1000
    timestamp_proto = Timestamp()

    timestamp_proto.FromSeconds(int(timestamp))

    expected_logs = [
        MessageToDict(
            TraceItem(
                organization_id=1,
                project_id=project_id,
                timestamp=timestamp_proto,
                trace_id="5b8efff798038103d269b633813fc60c",
                item_id=timestamp_nanos.to_bytes(
                    length=16,
                    byteorder="little",
                    signed=False,
                ),
                item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                attributes={
                    "browser.name": AnyValue(string_value="Python Requests"),
                    "browser.version": AnyValue(string_value="2.32"),
                    "sentry.body": AnyValue(string_value="Example log record 2"),
                    "sentry.observed_timestamp_nanos": AnyValue(
                        string_value=str(timestamp_nanos)
                    ),
                    "sentry.severity_number": AnyValue(int_value=13),
                    "sentry.severity_text": AnyValue(string_value="warn"),
                    "sentry.timestamp_nanos": AnyValue(
                        string_value=str(timestamp_nanos)
                    ),
                    "sentry.timestamp_precise": AnyValue(int_value=timestamp_nanos),
                    "sentry.trace_flags": AnyValue(int_value=0),
                },
                retention_days=90,
                client_sample_rate=1.0,
                server_sample_rate=1.0,
            ),
        ),
    ]

    logs = [MessageToDict(log) for log in ourlogs_consumer.get_ourlogs()]

    for log, expected_log in zip(logs, expected_logs):
        # we can't generate uuid7 with a specific timestamp
        # in Python just yet so we're overriding it
        expected_log["itemId"] = log["itemId"]
        expected_log["received"] = time_within_delta()

    assert logs == expected_logs

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

    start = datetime.now(timezone.utc)
    timestamp_nanos = str(int(start.timestamp() * 1e9))
    envelope = envelope_with_otel_logs(str(timestamp_nanos))

    relay.send_envelope(project_id, envelope)

    ourlogs = ourlogs_consumer.get_ourlogs()

    assert len(ourlogs) == 0
    ourlogs_consumer.assert_empty()


@pytest.mark.parametrize(
    "user_agent,expected_browser_name,expected_browser_version",
    [
        # Chrome desktop
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Chrome",
            "131.0.0",
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Chrome",
            "120.0.0",
        ),
        # Firefox desktop
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Firefox",
            "121.0",
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Firefox",
            "120.0",
        ),
        # Safari desktop
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Safari",
            "17.1",
        ),
        # Edge desktop
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
            "Edge",
            "120.0.0",
        ),
        # Chrome mobile
        (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/119.0.6045.169 Mobile/15E148 Safari/604.1",
            "Chrome Mobile iOS",
            "119.0.6045",
        ),
        (
            "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
            "Chrome Mobile",
            "119.0.0",
        ),
        # Safari mobile
        (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
            "Mobile Safari",
            "17.1",
        ),
    ],
)
def test_browser_name_version_extraction(
    mini_sentry,
    relay_with_processing,
    ourlogs_consumer,
    user_agent,
    expected_browser_name,
    expected_browser_version,
):
    ourlogs_consumer = ourlogs_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]
    relay = relay_with_processing(options=TEST_CONFIG)
    start = datetime.now(timezone.utc)
    timestamp = start.timestamp()
    envelope = envelope_with_sentry_logs(
        {
            "timestamp": timestamp,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "level": "error",
            "body": "This is really bad",
        },
    )

    relay.send_envelope(project_id, envelope, headers={"User-Agent": user_agent})

    timestamp_nanos = int(timestamp * 1e6) * 1000
    timestamp_proto = Timestamp()

    timestamp_proto.FromSeconds(int(timestamp))

    expected_log = MessageToDict(
        TraceItem(
            organization_id=1,
            project_id=project_id,
            timestamp=timestamp_proto,
            trace_id="5b8efff798038103d269b633813fc60c",
            item_id=timestamp_nanos.to_bytes(
                length=16,
                byteorder="little",
                signed=False,
            ),
            item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
            attributes={
                "browser.name": AnyValue(string_value=expected_browser_name),
                "browser.version": AnyValue(string_value=expected_browser_version),
                "sentry.body": AnyValue(string_value="This is really bad"),
                "sentry.severity_number": AnyValue(int_value=17),
                "sentry.severity_text": AnyValue(string_value="error"),
                "sentry.span_id": AnyValue(string_value="eee19b7ec3c1b175"),
                "sentry.trace_flags": AnyValue(int_value=0),
                "sentry.observed_timestamp_nanos": AnyValue(
                    string_value=str(timestamp_nanos)
                ),
                "sentry.timestamp_precise": AnyValue(int_value=timestamp_nanos),
                "sentry.timestamp_nanos": AnyValue(string_value=str(timestamp_nanos)),
            },
            retention_days=90,
            client_sample_rate=1.0,
            server_sample_rate=1.0,
        ),
    )

    logs = [MessageToDict(log) for log in ourlogs_consumer.get_ourlogs()]

    assert len(logs) == 1
    log = logs[0]

    # we can't generate uuid7 with a specific timestamp
    # in Python just yet so we're overriding it
    expected_log["itemId"] = log["itemId"]
    expected_log["received"] = time_within_delta()

    assert log == expected_log

    ourlogs_consumer.assert_empty()
