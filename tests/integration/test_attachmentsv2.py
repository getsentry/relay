import base64
from datetime import datetime, timezone
from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_relay.consts import DataCategory
from unittest import mock

from .asserts import time_within_delta, time_is
from .test_spansv2 import envelope_with_spans

from .test_dynamic_sampling import _add_sampling_config

import json
import uuid
import pytest

TEST_CONFIG = {
    "outcomes": {
        "emit_outcomes": True,
    }
}


def create_attachment_metadata():
    return {
        "trace_id": uuid.uuid4().hex,
        "attachment_id": str(uuid.uuid4()),
        "timestamp": 1760520026.781239,
        "filename": "myfile.txt",
        "content_type": "text/plain",
        "attributes": {
            "foo": {"type": "string", "value": "bar"},
        },
    }


def create_attachment_envelope(project_config):
    return Envelope(
        headers={
            "event_id": "515539018c9b4260a6f999572f1661ee",
            "trace": {
                "trace_id": "5b8efff798038103d269b633813fc60c",
                "public_key": project_config["publicKeys"][0]["publicKey"],
            },
        }
    )


def test_standalone_attachment_forwarding(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
        "projects:span-v2-attachment-processing",
    ]
    relay = relay(mini_sentry, options=TEST_CONFIG)

    attachment_metadata = create_attachment_metadata()
    attachment_body = b"This is some mock attachment content"
    metadata_bytes = json.dumps(attachment_metadata, separators=(",", ":")).encode(
        "utf-8"
    )
    combined_payload = metadata_bytes + attachment_body

    envelope = create_attachment_envelope(project_config)
    headers = {
        "content_type": "application/vnd.sentry.attachment.v2",
        "meta_length": len(metadata_bytes),
        "span_id": None,
        "length": len(combined_payload),
        "type": "attachment",
    }

    attachment_item = Item(payload=PayloadRef(bytes=combined_payload), headers=headers)
    envelope.add_item(attachment_item)
    relay.send_envelope(project_id, envelope)

    forwarded_envelope = mini_sentry.get_captured_event()
    attachment_item = forwarded_envelope.items[0]
    assert attachment_item.type == "attachment"

    meta_length = attachment_item.headers.get("meta_length")
    payload = attachment_item.payload.bytes

    metadata_part = json.loads(payload[:meta_length].decode("utf-8"))
    body_part = payload[meta_length:]

    # Things send in should match the things coming out
    assert metadata_part == attachment_metadata
    assert body_part == attachment_body
    assert attachment_item.headers == headers


def test_standalone_attachment_store(
    mini_sentry, relay_with_processing, items_consumer, objectstore
):
    items_consumer = items_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
        "projects:span-v2-attachment-processing",
    ]
    relay = relay_with_processing(
        {"processing": {"upload": {"objectstore_url": "http://127.0.0.1:8888/"}}}
    )

    attachment_metadata = create_attachment_metadata()
    attachment_body = b"This is some mock attachment content"
    metadata_bytes = json.dumps(attachment_metadata, separators=(",", ":")).encode(
        "utf-8"
    )
    combined_payload = metadata_bytes + attachment_body

    envelope = create_attachment_envelope(project_config)
    headers = {
        "content_type": "application/vnd.sentry.attachment.v2",
        "meta_length": len(metadata_bytes),
        "span_id": None,
        "length": len(combined_payload),
        "type": "attachment",
    }

    attachment_item = Item(payload=PayloadRef(bytes=combined_payload), headers=headers)
    envelope.add_item(attachment_item)
    relay.send_envelope(project_id, envelope)

    produced_item = items_consumer.get_item()
    expected_item_id = base64.b64encode(
        uuid.UUID(hex=attachment_metadata["attachment_id"]).bytes
    ).decode("utf-8")
    assert produced_item == {
        "attributes": {
            "file.name": {"stringValue": "myfile.txt"},
            "foo": {"stringValue": "bar"},
            "sentry.content-type": {"stringValue": "text/plain"},
        },
        "clientSampleRate": 1.0,
        "downsampledRetentionDays": 90,
        "itemId": expected_item_id,
        "itemType": 10,
        "organizationId": "1",
        "projectId": "42",
        "received": mock.ANY,
        "retentionDays": 90,
        "serverSampleRate": 1.0,
        "timestamp": mock.ANY,
        "traceId": attachment_metadata["trace_id"].replace("-", ""),
    }

    objectstore = objectstore(usecase="trace_attachments", project_id=project_id)
    assert (
        objectstore.get(attachment_metadata["attachment_id"]).payload.read()
        == attachment_body
    )


@pytest.mark.parametrize(
    "invalid_headers,quantity",
    [
        # Invalid since there is no span with that id in the envelope, also the quantity here is
        # lower since only the body is already counted at this point and not the meta.
        pytest.param({"span_id": "ABCDFDEAD5F74052"}, 36, id="invalid_span_id"),
        pytest.param({"meta_length": None}, 273, id="missing_meta_length"),
        pytest.param({"meta_length": 999}, 1, id="meta_length_exceeds_payload"),
    ],
)
def test_invalid_item_headers(mini_sentry, relay, invalid_headers, quantity):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
        "projects:span-v2-attachment-processing",
    ]
    relay = relay(mini_sentry, options=TEST_CONFIG)

    attachment_metadata = create_attachment_metadata()
    attachment_body = b"This is some mock attachment content"
    metadata_bytes = json.dumps(attachment_metadata, separators=(",", ":")).encode(
        "utf-8"
    )
    combined_payload = metadata_bytes + attachment_body

    envelope = create_attachment_envelope(project_config)
    headers = {
        "content_type": "application/vnd.sentry.attachment.v2",
        "meta_length": len(metadata_bytes),
        "span_id": None,
        "length": len(combined_payload),
        "type": "attachment",
    }
    headers.update(invalid_headers)  # Apply invalid values

    envelope.add_item(Item(payload=PayloadRef(bytes=combined_payload), headers=headers))
    relay.send_envelope(project_id, envelope)

    assert mini_sentry.get_outcomes(n=2) == [
        {
            "category": DataCategory.ATTACHMENT.value,
            "org_id": 1,
            "outcome": 3,
            "key_id": 123,
            "project_id": 42,
            "reason": "invalid_span_attachment",
            "quantity": quantity,
            "timestamp": time_within_delta(),
        },
        {
            "category": DataCategory.ATTACHMENT_ITEM.value,
            "org_id": 1,
            "outcome": 3,
            "key_id": 123,
            "project_id": 42,
            "reason": "invalid_span_attachment",
            "quantity": 1,
            "timestamp": time_within_delta(),
        },
    ]

    assert mini_sentry.captured_events.empty()


# Tests taken from test_spansv2.py but modified to include span attachments
def test_attachment_with_matching_span(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
        "projects:span-v2-attachment-processing",
    ]
    relay = relay(mini_sentry, options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    span_id = "eee19b7ec3c1b174"
    trace_id = "5b8efff798038103d269b633813fc60c"
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": trace_id,
            "span_id": span_id,
            "is_segment": True,
            "name": "test span",
            "status": "ok",
        },
        trace_info={
            "trace_id": trace_id,
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )

    metadata = create_attachment_metadata()
    body = b"span attachment content"
    metadata_bytes = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    combined_payload = metadata_bytes + body

    envelope.add_item(
        Item(
            payload=PayloadRef(bytes=combined_payload),
            type="attachment",
            headers={
                "content_type": "application/vnd.sentry.attachment.v2",
                "meta_length": len(metadata_bytes),
                "span_id": span_id,
                "length": len(combined_payload),
            },
        )
    )

    relay.send_envelope(project_id, envelope)
    forwarded = mini_sentry.get_captured_event()

    span_item = next(i for i in forwarded.items if i.type == "span")
    spans = json.loads(span_item.payload.bytes.decode())["items"]
    assert spans == [
        {
            "trace_id": trace_id,
            "span_id": span_id,
            "name": "test span",
            "status": "ok",
            "is_segment": True,
            "start_timestamp": time_is(ts),
            "end_timestamp": time_is(ts.timestamp() + 0.5),
            "attributes": mock.ANY,
        }
    ]

    attachment = next(i for i in forwarded.items if i.type == "attachment")
    assert attachment.payload.bytes == combined_payload
    assert attachment.headers == {
        "type": "attachment",
        "length": 260,
        "content_type": "application/vnd.sentry.attachment.v2",
        "meta_length": 237,
        "span_id": span_id,
    }


def test_attachment_with_matching_span_store(
    mini_sentry, relay_with_processing, items_consumer, objectstore
):
    items_consumer = items_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
        "projects:span-v2-attachment-processing",
    ]
    relay = relay_with_processing(
        {"processing": {"upload": {"objectstore_url": "http://127.0.0.1:8888/"}}}
    )

    ts = datetime.now(timezone.utc)
    span_id = "eee19b7ec3c1b174"
    trace_id = "5b8efff798038103d269b633813fc60c"
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": trace_id,
            "span_id": span_id,
            "is_segment": True,
            "name": "test span",
            "status": "ok",
        },
        trace_info={
            "trace_id": trace_id,
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )

    metadata = create_attachment_metadata()
    body = b"span attachment content"
    metadata_bytes = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    combined_payload = metadata_bytes + body

    envelope.add_item(
        Item(
            payload=PayloadRef(bytes=combined_payload),
            type="attachment",
            headers={
                "content_type": "application/vnd.sentry.attachment.v2",
                "meta_length": len(metadata_bytes),
                "span_id": span_id,
                "length": len(combined_payload),
            },
        )
    )

    relay.send_envelope(project_id, envelope)

    attachment_item = items_consumer.get_item()
    expected_item_id = base64.b64encode(
        uuid.UUID(hex=metadata["attachment_id"]).bytes
    ).decode("utf-8")
    assert attachment_item == {
        "attributes": {
            "file.name": {"stringValue": "myfile.txt"},
            "foo": {"stringValue": "bar"},
            "sentry.content-type": {"stringValue": "text/plain"},
            "sentry.span_id": {"stringValue": span_id},
        },
        "clientSampleRate": 1.0,
        "downsampledRetentionDays": 90,
        "itemId": expected_item_id,
        "itemType": 10,
        "organizationId": "1",
        "projectId": "42",
        "received": mock.ANY,
        "retentionDays": 90,
        "serverSampleRate": 1.0,
        "timestamp": mock.ANY,
        "traceId": metadata["trace_id"],
    }

    objectstore = objectstore(usecase="trace_attachments", project_id=project_id)
    assert objectstore.get(metadata["attachment_id"]).payload.read() == body


def test_two_attachments_mapping_to_same_span(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
        "projects:span-v2-attachment-processing",
    ]
    relay = relay(mini_sentry, options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    span_id = "eee19b7ec3c1b174"
    trace_id = "5b8efff798038103d269b633813fc60c"
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": trace_id,
            "span_id": span_id,
            "is_segment": True,
            "name": "test span",
            "status": "ok",
        },
        trace_info={
            "trace_id": trace_id,
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )

    metadata = create_attachment_metadata()
    body = b"span attachment content"
    metadata_bytes = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    combined_payload = metadata_bytes + body

    # Two attachments both pointing to the same span (one should be dropped)
    envelope.add_item(
        Item(
            payload=PayloadRef(bytes=combined_payload),
            type="attachment",
            headers={
                "content_type": "application/vnd.sentry.attachment.v2",
                "meta_length": len(metadata_bytes),
                "span_id": span_id,
                "length": len(combined_payload),
            },
        )
    )
    envelope.add_item(
        Item(
            payload=PayloadRef(bytes=combined_payload),
            type="attachment",
            headers={
                "content_type": "application/vnd.sentry.attachment.v2",
                "meta_length": len(metadata_bytes),
                "span_id": span_id,
                "length": len(combined_payload),
            },
        )
    )

    relay.send_envelope(project_id, envelope)
    forwarded = mini_sentry.get_captured_event()

    span_item = next(i for i in forwarded.items if i.type == "span")
    spans = json.loads(span_item.payload.bytes.decode())["items"]
    assert spans == [
        {
            "trace_id": trace_id,
            "span_id": span_id,
            "name": "test span",
            "status": "ok",
            "is_segment": True,
            "start_timestamp": time_is(ts),
            "end_timestamp": time_is(ts.timestamp() + 0.5),
            "attributes": mock.ANY,
        }
    ]

    attachment_items = [item for item in forwarded.items if item.type == "attachment"]
    assert len(attachment_items) == 2
    for item in attachment_items:
        assert item.payload.bytes == combined_payload
        assert item.headers == {
            "type": "attachment",
            "length": 260,
            "content_type": "application/vnd.sentry.attachment.v2",
            "meta_length": 237,
            "span_id": span_id,
        }

    assert mini_sentry.captured_outcomes.empty()


@pytest.mark.parametrize(
    "rule_type",
    ["project", "trace"],
)
def test_span_attachment_ds_drop(mini_sentry, relay, rule_type):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
        "projects:span-v2-attachment-processing",
    ]
    # A transaction rule should never apply.
    _add_sampling_config(project_config, sample_rate=1, rule_type="transaction")
    # Setup the actual rule we want to test against.
    _add_sampling_config(project_config, sample_rate=0, rule_type=rule_type)

    relay = relay(mini_sentry, options=TEST_CONFIG)

    span_id = "eee19b7ec3c1b174"
    ts = datetime.now(timezone.utc)
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": span_id,
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

    metadata = create_attachment_metadata()
    body = b"span attachment content"
    metadata_bytes = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    combined_payload = metadata_bytes + body

    envelope.add_item(
        Item(
            payload=PayloadRef(bytes=combined_payload),
            type="attachment",
            headers={
                "content_type": "application/vnd.sentry.attachment.v2",
                "meta_length": len(metadata_bytes),
                "span_id": span_id,
                "length": len(combined_payload),
            },
        )
    )

    relay.send_envelope(project_id, envelope)

    assert mini_sentry.get_outcomes(n=3) == [
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,
            "reason": "Sampled:0",
            "category": DataCategory.ATTACHMENT.value,
            "quantity": len(body),
        },
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,
            "reason": "Sampled:0",
            "category": DataCategory.SPAN_INDEXED.value,
            "quantity": 1,
        },
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,
            "reason": "Sampled:0",
            "category": DataCategory.ATTACHMENT_ITEM.value,
            "quantity": 1,
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


def test_attachments_dropped_with_span_inbound_filters(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
        "projects:span-v2-attachment-processing",
    ]

    project_config["config"]["filterSettings"] = {
        "releases": {"releases": ["foobar@1.0"]}
    }

    relay = relay(mini_sentry, options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    span_id = "eee19b7ec3c1b175"
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": span_id,
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
    metadata = create_attachment_metadata()
    body = b"span attachment content"
    metadata_bytes = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    combined_payload = metadata_bytes + body

    envelope.add_item(
        Item(
            payload=PayloadRef(bytes=combined_payload),
            type="attachment",
            headers={
                "content_type": "application/vnd.sentry.attachment.v2",
                "meta_length": len(metadata_bytes),
                "span_id": span_id,
                "length": len(combined_payload),
            },
        )
    )

    relay.send_envelope(project_id, envelope, headers=headers)
    assert mini_sentry.get_outcomes(n=4) == [
        {
            "timestamp": time_within_delta(ts),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,
            "reason": "release-version",
            "category": DataCategory.ATTACHMENT.value,
            "quantity": 23,
        },
        {
            "timestamp": time_within_delta(ts),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,
            "reason": "release-version",
            "category": DataCategory.SPAN.value,
            "quantity": 1,
        },
        {
            "timestamp": time_within_delta(ts),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,
            "reason": "release-version",
            "category": DataCategory.SPAN_INDEXED.value,
            "quantity": 1,
        },
        {
            "timestamp": time_within_delta(ts),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,
            "reason": "release-version",
            "category": DataCategory.ATTACHMENT_ITEM.value,
            "quantity": 1,
        },
    ]

    assert mini_sentry.captured_events.empty()


def test_attachment_dropped_with_invalid_spans(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
        "projects:span-v2-attachment-processing",
    ]
    relay = relay(mini_sentry, options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    span_id = "eee19b7ec3c1b174"
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": span_id,
            "is_segment": True,
            "name": None,  # Should be none-empty hence invalid
            "status": "ok",
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )

    metadata = create_attachment_metadata()
    body = b"span attachment content"
    metadata_bytes = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    combined_payload = metadata_bytes + body

    envelope.add_item(
        Item(
            payload=PayloadRef(bytes=combined_payload),
            type="attachment",
            headers={
                "content_type": "application/vnd.sentry.attachment.v2",
                "meta_length": len(metadata_bytes),
                "span_id": span_id,
                "length": len(combined_payload),
            },
        )
    )

    relay.send_envelope(project_id, envelope)
    assert mini_sentry.get_outcomes(n=4) == [
        {
            "timestamp": time_within_delta(ts),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 3,
            "reason": "no_data",
            "category": DataCategory.ATTACHMENT.value,
            "quantity": 23,
        },
        {
            "timestamp": time_within_delta(ts),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 3,
            "reason": "no_data",
            "category": DataCategory.SPAN.value,
            "quantity": 1,
        },
        {
            "timestamp": time_within_delta(ts),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 3,
            "reason": "no_data",
            "category": DataCategory.SPAN_INDEXED.value,
            "quantity": 1,
        },
        {
            "timestamp": time_within_delta(ts),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 3,
            "reason": "no_data",
            "category": DataCategory.ATTACHMENT_ITEM.value,
            "quantity": 1,
        },
    ]

    assert mini_sentry.captured_events.empty()


@pytest.mark.parametrize(
    "quota_config,expected_outcomes",
    [
        pytest.param(
            [
                {
                    "categories": ["span_indexed"],
                    "limit": 0,
                    "window": 3600,
                    "id": "span_limit",
                    "reasonCode": "span_quota_exceeded",
                }
            ],
            {
                # Rate limit spans
                (DataCategory.SPAN.value, 2): 1,
                (DataCategory.SPAN_INDEXED.value, 2): 1,
                # Rate limit associated span attachments
                (DataCategory.ATTACHMENT.value, 2): 64,
                (DataCategory.ATTACHMENT_ITEM.value, 2): 2,
            },
            id="span_quota_exceeded",
        ),
        pytest.param(
            [
                {
                    "categories": ["attachment"],
                    "limit": 0,
                    "window": 3600,
                    "id": "attachment_limit",
                    "reasonCode": "attachment_quota_exceeded",
                }
            ],
            {
                # Attachments don't make it through
                (DataCategory.ATTACHMENT.value, 2): 64,
                (DataCategory.ATTACHMENT_ITEM.value, 2): 2,
            },
            id="attachment_quota_exceeded",
        ),
        pytest.param(
            [
                {
                    "categories": ["span_indexed"],
                    "limit": 0,
                    "window": 3600,
                    "id": "span_limit",
                    "reasonCode": "span_quota_exceeded",
                },
                {
                    "categories": ["attachment"],
                    "limit": 0,
                    "window": 3600,
                    "id": "attachment_limit",
                    "reasonCode": "attachment_quota_exceeded",
                },
            ],
            {
                # Nothing makes it through
                (DataCategory.SPAN.value, 2): 1,
                (DataCategory.SPAN_INDEXED.value, 2): 1,
                (DataCategory.ATTACHMENT.value, 2): 64,
                (DataCategory.ATTACHMENT_ITEM.value, 2): 2,
            },
            id="both_quotas_exceeded",
        ),
    ],
)
def test_span_attachment_independent_rate_limiting(
    mini_sentry,
    relay,
    outcomes_consumer,
    quota_config,
    expected_outcomes,
):

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
        "projects:span-v2-attachment-processing",
    ]
    project_config["config"]["quotas"] = quota_config

    relay = relay(mini_sentry, options=TEST_CONFIG)
    outcomes_consumer = outcomes_consumer()

    ts = datetime.now(timezone.utc)
    span_id = "eee19b7ec3c1b174"
    trace_id = "5b8efff798038103d269b633813fc60c"

    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": trace_id,
            "span_id": span_id,
            "is_segment": True,
            "name": "test span",
            "status": "ok",
        },
        trace_info={
            "trace_id": trace_id,
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )

    per_span_metadata = create_attachment_metadata()
    per_span_body = b"per-span attachment"
    per_span_metadata_bytes = json.dumps(
        per_span_metadata, separators=(",", ":")
    ).encode("utf-8")
    per_span_payload = per_span_metadata_bytes + per_span_body

    envelope.add_item(
        Item(
            payload=PayloadRef(bytes=per_span_payload),
            type="attachment",
            headers={
                "content_type": "application/vnd.sentry.attachment.v2",
                "meta_length": len(per_span_metadata_bytes),
                "span_id": span_id,
                "length": len(per_span_payload),
            },
        )
    )

    standalone_metadata = create_attachment_metadata()
    standalone_body = b"standalone attachment - should be independent"
    standalone_metadata_bytes = json.dumps(
        standalone_metadata, separators=(",", ":")
    ).encode("utf-8")
    standalone_payload = standalone_metadata_bytes + standalone_body

    envelope.add_item(
        Item(
            payload=PayloadRef(bytes=standalone_payload),
            type="attachment",
            headers={
                "content_type": "application/vnd.sentry.attachment.v2",
                "meta_length": len(standalone_metadata_bytes),
                "span_id": None,  # Not attached to any span
                "length": len(standalone_payload),
            },
        )
    )

    relay.send_envelope(project_id, envelope)

    outcomes = mini_sentry.get_outcomes(n=len(expected_outcomes))
    outcome_counter = {}
    for outcome in outcomes:
        key = (outcome["category"], outcome["outcome"])
        outcome_counter[key] = outcome_counter.get(key, 0) + outcome["quantity"]

    assert outcome_counter == expected_outcomes

    outcomes_consumer.assert_empty()


def test_attachment_default_pii_scrubbing_meta(
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
        "projects:span-v2-attachment-processing",
    ]
    project_config["config"].setdefault(
        "datascrubbingSettings",
        {
            "scrubData": True,
            "scrubDefaults": True,
            "scrubIpAddresses": True,
        },
    )

    relay = relay(mini_sentry, options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    span_id = "eee19b7ec3c1b174"
    trace_id = "5b8efff798038103d269b633813fc60c"
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": trace_id,
            "span_id": span_id,
            "is_segment": True,
            "name": "test span",
            "status": "ok",
        },
        trace_info={
            "trace_id": trace_id,
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )

    metadata = {
        "attachment_id": str(uuid.uuid4()),
        "timestamp": ts.timestamp(),
        "filename": "data.txt",
        "content_type": "text/plain",
        "attributes": {
            attribute_key: {"type": "string", "value": attribute_value},
        },
    }
    body = b"Some content"
    metadata_bytes = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    combined_payload = metadata_bytes + body

    envelope.add_item(
        Item(
            payload=PayloadRef(bytes=combined_payload),
            type="attachment",
            headers={
                "content_type": "application/vnd.sentry.attachment.v2",
                "meta_length": len(metadata_bytes),
                "span_id": span_id,
                "length": len(combined_payload),
            },
        )
    )

    relay.send_envelope(project_id, envelope)
    forwarded = mini_sentry.get_captured_event()

    attachment = next(i for i in forwarded.items if i.type == "attachment")
    meta_length = attachment.headers.get("meta_length")
    payload = attachment.payload.bytes
    metadata_part = json.loads(payload[:meta_length].decode("utf-8"))

    assert metadata_part == {
        "attachment_id": mock.ANY,
        "timestamp": time_within_delta(ts),
        "filename": "data.txt",
        "content_type": "text/plain",
        "attributes": {
            attribute_key: {"type": "string", "value": expected_value},
        },
        "_meta": {
            "attributes": {
                attribute_key: {
                    "value": {
                        "": {
                            "len": mock.ANY,
                            "rem": [[rule_type, mock.ANY, mock.ANY, mock.ANY]],
                        }
                    }
                }
            }
        },
    }


def test_attachment_pii_scrubbing_meta_attribute(
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
        "projects:span-v2-attachment-processing",
    ]
    project_config["config"]["piiConfig"]["applications"] = {"$string": [rule_type]}

    relay = relay(mini_sentry, options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    span_id = "eee19b7ec3c1b174"
    trace_id = "5b8efff798038103d269b633813fc60c"
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": trace_id,
            "span_id": span_id,
            "is_segment": True,
            "name": "test span",
            "status": "ok",
        },
        trace_info={
            "trace_id": trace_id,
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )

    metadata = {
        "attachment_id": str(uuid.uuid4()),
        "timestamp": ts.timestamp(),
        "filename": "data.txt",
        "content_type": "text/plain",
        "attributes": {
            "test_pii": {"type": "string", "value": test_value},
        },
    }
    body = b"Some attachment content"
    metadata_bytes = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    combined_payload = metadata_bytes + body

    envelope.add_item(
        Item(
            payload=PayloadRef(bytes=combined_payload),
            type="attachment",
            headers={
                "content_type": "application/vnd.sentry.attachment.v2",
                "meta_length": len(metadata_bytes),
                "span_id": span_id,
                "length": len(combined_payload),
            },
        )
    )

    relay.send_envelope(project_id, envelope)
    forwarded = mini_sentry.get_captured_event()

    attachment = next(i for i in forwarded.items if i.type == "attachment")
    meta_length = attachment.headers.get("meta_length")
    payload = attachment.payload.bytes
    metadata_part = json.loads(payload[:meta_length].decode("utf-8"))

    assert metadata_part == {
        "attachment_id": mock.ANY,
        "timestamp": time_within_delta(ts),
        "filename": "data.txt",
        "content_type": "text/plain",
        "attributes": {
            "test_pii": {"type": "string", "value": expected_scrubbed},
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
            }
        },
    }


def test_attachment_pii_scrubbing_body(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
        "projects:span-v2-attachment-processing",
    ]
    project_config["config"]["piiConfig"] = {
        "rules": {"0": {"type": "ip", "redaction": {"method": "remove"}}},
        "applications": {"$attachments.'log.txt'": ["0"]},
    }

    relay = relay(mini_sentry, options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)
    span_id = "eee19b7ec3c1b174"
    trace_id = "5b8efff798038103d269b633813fc60c"
    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": trace_id,
            "span_id": span_id,
            "is_segment": True,
            "name": "test span",
            "status": "ok",
        },
        trace_info={
            "trace_id": trace_id,
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )

    metadata = {
        "attachment_id": str(uuid.uuid4()),
        "timestamp": ts.timestamp(),
        "filename": "log.txt",
        "content_type": "text/plain",
    }
    body = b"before 127.0.0.1 after"
    metadata_bytes = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    combined_payload = metadata_bytes + body

    envelope.add_item(
        Item(
            payload=PayloadRef(bytes=combined_payload),
            type="attachment",
            headers={
                "content_type": "application/vnd.sentry.attachment.v2",
                "meta_length": len(metadata_bytes),
                "span_id": span_id,
                "length": len(combined_payload),
            },
        )
    )

    relay.send_envelope(project_id, envelope)
    forwarded = mini_sentry.get_captured_event()

    attachment = next(i for i in forwarded.items if i.type == "attachment")
    meta_length = attachment.headers.get("meta_length")
    payload = attachment.payload.bytes
    body_part = payload[meta_length:]

    assert body_part == b"before ********* after"
