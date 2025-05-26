import json

from datetime import datetime, timezone

from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_relay.consts import DataCategory

from sentry_protos.snuba.v1.trace_item_pb2 import TraceItem, AnyValue
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import MessageToDict

from .asserts.time import time_within_delta


TEST_CONFIG = {
    "aggregator": {
        "bucket_interval": 1,
        "initial_delay": 0,
    }
}


def envelope_with_v2_spans(*payloads: dict) -> Envelope:
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(json={"items": payloads}),
            content_type="application/vnd.sentry.items.span.v2+json",
            headers={"item_count": len(payloads)},
        )
    )
    return envelope


# def test_ourlog_extraction_with_otel_logs(
#     mini_sentry,
#     relay_with_processing,
#     spans_consumer
# ):
#     ourlogs_consumer = spans_consumer()
#     project_id = 42
#     relay = relay_with_processing(options=TEST_CONFIG)
#     start = datetime.now(timezone.utc)
#     timestamp = start.timestamp()
#     timestamp_nanos = int(timestamp * 1e9)
#     envelope = envelope_with_(str(timestamp_nanos))

#     relay.send_envelope(project_id, envelope)

#     timestamp_proto = Timestamp()

#     timestamp_proto.FromSeconds(int(timestamp))

#     expected_logs = [
#         MessageToDict(
#             TraceItem(
#                 organization_id=1,
#                 project_id=project_id,
#                 timestamp=timestamp_proto,
#                 trace_id="5b8efff798038103d269b633813fc60c",
#                 item_id=timestamp_nanos.to_bytes(
#                     length=16,
#                     byteorder="little",
#                     signed=False,
#                 ),
#                 item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
#                 attributes={
#                     "boolean.attribute": AnyValue(bool_value=True),
#                     "double.attribute": AnyValue(double_value=637.704),
#                     "int.attribute": AnyValue(int_value=10),
#                     "sentry.body": AnyValue(string_value="Example log record"),
#                     "sentry.severity_number": AnyValue(int_value=10),
#                     "sentry.severity_text": AnyValue(string_value="Information"),
#                     "sentry.span_id": AnyValue(string_value="eee19b7ec3c1b174"),
#                     "sentry.timestamp_nanos": AnyValue(
#                         string_value=str(timestamp_nanos)
#                     ),
#                     "sentry.timestamp_precise": AnyValue(int_value=timestamp_nanos),
#                     "sentry.trace_flags": AnyValue(int_value=0),
#                     "string.attribute": AnyValue(string_value="some string"),
#                 },
#                 retention_days=90,
#                 client_sample_rate=1.0,
#                 server_sample_rate=1.0,
#             )
#         )
#     ]

#     logs = [MessageToDict(log) for log in ourlogs_consumer.get_ourlogs()]

#     for log, expected_log in zip(logs, expected_logs):
#         # we can't generate uuid7 with a specific timestamp
#         # in Python just yet so we're overriding it
#         expected_log["itemId"] = log["itemId"]
#         expected_log["received"] = time_within_delta()

#         # This field is set by Relay so we need to remove it
#         del log["attributes"]["sentry.observed_timestamp_nanos"]

#     assert logs == expected_logs

#     ourlogs_consumer.assert_empty()


def test_spans_v2_multiple_containers_not_allowed(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    outcomes_consumer,
):
    spans_consumer = spans_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
    ]

    relay = relay_with_processing(options=TEST_CONFIG)
    start = datetime.now(timezone.utc)
    envelope = Envelope()

    payload = {
        "start_timestamp": start.timestamp(),
        "end_timestamp": start.timestamp() + 0.500,
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "span_id": "eee19b7ec3c1b175",
        "name": "some op",
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

    spans_consumer.assert_empty()

    outcomes = outcomes_consumer.get_outcomes()

    outcomes.sort(key=lambda o: sorted(o.items()))

    # assert 300 < outcomes[1].pop("quantity") < 400
    assert outcomes == [
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


# def test_ourlog_extraction_with_sentry_logs(
#     mini_sentry,
#     relay_with_processing,
#     ourlogs_consumer,
# ):
#     ourlogs_consumer = ourlogs_consumer()
#     project_id = 42
#     project_config = mini_sentry.add_full_project_config(project_id)
#     project_config["config"]["features"] = [
#         "organizations:ourlogs-ingestion",
#     ]
#     relay = relay_with_processing(options=TEST_CONFIG)
#     start = datetime.now(timezone.utc)
#     timestamp = start.timestamp()
#     envelope = envelope_with_sentry_logs(
#         {
#             "timestamp": timestamp,
#             "trace_id": "5b8efff798038103d269b633813fc60c",
#             "span_id": "eee19b7ec3c1b175",
#             "level": "error",
#             "body": "This is really bad",
#         },
#         {
#             "timestamp": timestamp,
#             "trace_id": "5b8efff798038103d269b633813fc60c",
#             "span_id": "eee19b7ec3c1b174",
#             "level": "info",
#             "body": "Example log record",
#             "severity_number": 10,
#             "attributes": {
#                 "boolean.attribute": {"value": True, "type": "boolean"},
#                 "integer.attribute": {"value": 42, "type": "integer"},
#                 "double.attribute": {"value": 1.23, "type": "double"},
#                 "string.attribute": {"value": "some string", "type": "string"},
#                 "pii": {"value": "4242 4242 4242 4242", "type": "string"},
#                 "sentry.severity_text": {"value": "info", "type": "string"},
#                 "unknown_type": {"value": "info", "type": "unknown"},
#                 "broken_type": {"value": "info", "type": "not_a_real_type"},
#                 "mismatched_type": {"value": "some string", "type": "boolean"},
#                 "valid_string_with_other": {
#                     "value": "test",
#                     "type": "string",
#                     "some_other_field": "some_other_value",
#                 },
#             },
#         },
#     )

#     relay.send_envelope(project_id, envelope)

#     timestamp_nanos = int(timestamp * 1e6) * 1000
#     timestamp_proto = Timestamp()

#     timestamp_proto.FromSeconds(int(timestamp))

#     expected_logs = [
#         MessageToDict(log)
#         for log in [
#             TraceItem(
#                 organization_id=1,
#                 project_id=project_id,
#                 timestamp=timestamp_proto,
#                 trace_id="5b8efff798038103d269b633813fc60c",
#                 item_id=timestamp_nanos.to_bytes(
#                     length=16,
#                     byteorder="little",
#                     signed=False,
#                 ),
#                 item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
#                 attributes={
#                     "sentry.body": AnyValue(string_value="This is really bad"),
#                     "sentry.severity_number": AnyValue(int_value=17),
#                     "sentry.severity_text": AnyValue(string_value="error"),
#                     "sentry.span_id": AnyValue(string_value="eee19b7ec3c1b175"),
#                     "sentry.trace_flags": AnyValue(int_value=0),
#                     "sentry.observed_timestamp_nanos": AnyValue(
#                         string_value=str(timestamp_nanos)
#                     ),
#                     "sentry.timestamp_precise": AnyValue(int_value=timestamp_nanos),
#                     "sentry.timestamp_nanos": AnyValue(
#                         string_value=str(timestamp_nanos)
#                     ),
#                 },
#                 retention_days=90,
#                 client_sample_rate=1.0,
#                 server_sample_rate=1.0,
#             ),
#             TraceItem(
#                 organization_id=1,
#                 project_id=project_id,
#                 timestamp=timestamp_proto,
#                 trace_id="5b8efff798038103d269b633813fc60c",
#                 item_id=timestamp_nanos.to_bytes(
#                     length=16,
#                     byteorder="little",
#                     signed=False,
#                 ),
#                 item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
#                 attributes={
#                     "boolean.attribute": AnyValue(bool_value=True),
#                     "double.attribute": AnyValue(double_value=1.23),
#                     "integer.attribute": AnyValue(int_value=42),
#                     "pii": AnyValue(string_value="[creditcard]"),
#                     "sentry.body": AnyValue(string_value="Example log record"),
#                     "sentry.severity_number": AnyValue(int_value=9),
#                     "sentry.severity_text": AnyValue(string_value="info"),
#                     "sentry.trace_flags": AnyValue(int_value=0),
#                     "sentry.span_id": AnyValue(string_value="eee19b7ec3c1b174"),
#                     "sentry.observed_timestamp_nanos": AnyValue(
#                         string_value=str(timestamp_nanos)
#                     ),
#                     "string.attribute": AnyValue(string_value="some string"),
#                     "valid_string_with_other": AnyValue(string_value="test"),
#                     "sentry.timestamp_precise": AnyValue(int_value=timestamp_nanos),
#                     "sentry.timestamp_nanos": AnyValue(
#                         string_value=str(timestamp_nanos)
#                     ),
#                 },
#                 retention_days=90,
#                 client_sample_rate=1.0,
#                 server_sample_rate=1.0,
#             ),
#         ]
#     ]

#     logs = [MessageToDict(log) for log in ourlogs_consumer.get_ourlogs()]

#     for log, expected_log in zip(logs, expected_logs):
#         # we can't generate uuid7 with a specific timestamp
#         # in Python just yet so we're overriding it
#         expected_log["itemId"] = log["itemId"]
#         expected_log["received"] = time_within_delta()

#     assert logs == expected_logs

#     ourlogs_consumer.assert_empty()


# def test_ourlog_extraction_with_sentry_logs_with_missing_fields(
#     mini_sentry,
#     relay_with_processing,
#     ourlogs_consumer,
# ):
#     ourlogs_consumer = ourlogs_consumer()
#     project_id = 42
#     project_config = mini_sentry.add_full_project_config(project_id)
#     project_config["config"]["features"] = [
#         "organizations:ourlogs-ingestion",
#     ]
#     relay = relay_with_processing(options=TEST_CONFIG)
#     start = datetime.now(timezone.utc)
#     timestamp = start.timestamp()
#     envelope = envelope_with_sentry_logs(
#         {
#             "timestamp": timestamp,
#             "trace_id": "5b8efff798038103d269b633813fc60c",
#             "level": "warn",
#             "body": "Example log record 2",
#         }
#     )

#     relay.send_envelope(project_id, envelope)

#     timestamp_nanos = int(timestamp * 1e6) * 1000
#     timestamp_proto = Timestamp()

#     timestamp_proto.FromSeconds(int(timestamp))

#     expected_logs = [
#         MessageToDict(
#             TraceItem(
#                 organization_id=1,
#                 project_id=project_id,
#                 timestamp=timestamp_proto,
#                 trace_id="5b8efff798038103d269b633813fc60c",
#                 item_id=timestamp_nanos.to_bytes(
#                     length=16,
#                     byteorder="little",
#                     signed=False,
#                 ),
#                 item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
#                 attributes={
#                     "sentry.body": AnyValue(string_value="Example log record 2"),
#                     "sentry.observed_timestamp_nanos": AnyValue(
#                         string_value=str(timestamp_nanos)
#                     ),
#                     "sentry.severity_number": AnyValue(int_value=13),
#                     "sentry.severity_text": AnyValue(string_value="warn"),
#                     "sentry.timestamp_nanos": AnyValue(
#                         string_value=str(timestamp_nanos)
#                     ),
#                     "sentry.timestamp_precise": AnyValue(int_value=timestamp_nanos),
#                     "sentry.trace_flags": AnyValue(int_value=0),
#                 },
#                 retention_days=90,
#                 client_sample_rate=1.0,
#                 server_sample_rate=1.0,
#             ),
#         ),
#     ]

#     logs = [MessageToDict(log) for log in ourlogs_consumer.get_ourlogs()]

#     for log, expected_log in zip(logs, expected_logs):
#         # we can't generate uuid7 with a specific timestamp
#         # in Python just yet so we're overriding it
#         expected_log["itemId"] = log["itemId"]
#         expected_log["received"] = time_within_delta()

#     assert logs == expected_logs

#     ourlogs_consumer.assert_empty()


# def test_ourlog_extraction_is_disabled_without_feature(
#     mini_sentry,
#     relay_with_processing,
#     ourlogs_consumer,
# ):
#     ourlogs_consumer = ourlogs_consumer()
#     relay = relay_with_processing(options=TEST_CONFIG)
#     project_id = 42
#     project_config = mini_sentry.add_full_project_config(project_id)
#     project_config["config"]["features"] = []

#     start = datetime.now(timezone.utc)
#     timestamp_nanos = str(int(start.timestamp() * 1e9))
#     envelope = envelope_with_otel_logs(str(timestamp_nanos))

#     relay.send_envelope(project_id, envelope)

#     ourlogs = ourlogs_consumer.get_ourlogs()

#     assert len(ourlogs) == 0
#     ourlogs_consumer.assert_empty()
