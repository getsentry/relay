from datetime import datetime, timezone
from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_relay.consts import DataCategory

from .asserts import time_within_delta, time_within
from .test_spansv2 import envelope_with_spans

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

    forwarded_envelope = mini_sentry.captured_events.get(timeout=1)
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


@pytest.mark.parametrize(
    "invalid_headers",
    [
        # Invalid sine there is no span with that id in the envelope
        pytest.param({"span_id": "ABCDFDEAD5F74052"}, id="invalid_span_id"),
        pytest.param({"meta_length": None}, id="missing_meta_length"),
        pytest.param({"meta_length": 999}, id="meta_length_exceeds_payload"),
    ],
)
def test_invalid_item_headers(mini_sentry, relay, invalid_headers):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-v2-experimental-processing",
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

    assert mini_sentry.get_outcomes(n=2, timeout=1) == [
        {
            "category": DataCategory.ATTACHMENT.value,
            "org_id": 1,
            "outcome": 3,
            "key_id": 123,
            "project_id": 42,
            "reason": "invalid_json",
            "quantity": len(combined_payload),
            "timestamp": time_within_delta(),
        },
        {
            "category": DataCategory.ATTACHMENT_ITEM.value,
            "org_id": 1,
            "outcome": 3,
            "key_id": 123,
            "project_id": 42,
            "reason": "invalid_json",
            "quantity": 1,
            "timestamp": time_within_delta(),
        },
    ]

    assert mini_sentry.captured_events.empty()
