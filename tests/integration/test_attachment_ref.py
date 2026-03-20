import json
import pytest
import uuid

from unittest import mock
from requests.exceptions import HTTPError
from sentry_sdk.envelope import Envelope, Item, PayloadRef

from .test_store import make_transaction


def make_envelope(event_id):
    """Create an envelope with a single AttachmentRef item."""
    envelope = Envelope(headers=[["event_id", event_id]])
    envelope.add_item(
        Item(
            payload=PayloadRef(bytes=b""),
            headers={
                "type": "attachment",
                "content_type": "application/vnd.sentry.attachment-ref",
                "attachment_length": 1,
            },
        )
    )
    return envelope


def upload_and_make_ref(
    relay,
    project_id,
    project_key,
    data,
    filename="test.txt",
    content_type="text/plain",
    attachment_type="event.attachment",
):
    """Upload data via TUS, then construct an AttachmentRef item pointing at it."""
    # TODO: We want to use the new (POST and PATCH) approach here as well or is the old way fine?
    response = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}",
        headers={
            "Tus-Resumable": "1.0.0",
            "Content-Type": "application/offset+octet-stream",
            "Upload-Length": str(len(data)),
        },
        data=data,
    )
    assert response.status_code == 201
    location = response.headers["Location"]

    payload = json.dumps({"location": location, "content_type": content_type})
    return Item(
        payload=PayloadRef(bytes=payload.encode()),
        headers={
            "type": "attachment",
            "content_type": "application/vnd.sentry.attachment-ref",
            "length": len(payload),
            "attachment_length": len(data),
            "filename": filename,
            "attachment_type": attachment_type,
        },
    )


@pytest.mark.parametrize("data_category", ["attachment", "attachment_item"])
def test_attachment_ref_ratelimit(
    mini_sentry,
    relay_with_processing,
    attachments_consumer,
    outcomes_consumer,
    data_category,
):
    event_id = "515539018c9b4260a6f999572f1661ee"
    project_id = 42

    project_config = mini_sentry.add_full_project_config(project_id)

    project_config["config"]["quotas"] = [
        {
            "id": f"test_rate_limiting_{uuid.uuid4().hex}",
            "categories": [data_category],
            "window": 3600,
            "limit": 1,
            "reasonCode": "attachment_ref_exceeded",
        }
    ]

    relay = relay_with_processing()
    attachments_consumer = attachments_consumer()
    outcomes_consumer = outcomes_consumer()

    # First envelope: should go through (200 response)
    envelope = make_envelope(event_id)
    relay.send_envelope(project_id, envelope)
    attachments_consumer.get_individual_attachment()

    # Second envelope: rate limited but returns 200
    envelope = make_envelope(event_id)
    relay.send_envelope(project_id, envelope)
    outcomes_consumer.assert_rate_limited(
        "attachment_ref_exceeded",
        categories={"attachment": 1, "attachment_item": 1},
    )

    # Third envelope: returns 429
    envelope = make_envelope(event_id)
    with pytest.raises(HTTPError) as excinfo:
        relay.send_envelope(project_id, envelope)
    assert excinfo.value.response.status_code == 429
    outcomes_consumer.assert_rate_limited(
        "attachment_ref_exceeded",
        categories={"attachment": 1, "attachment_item": 1},
    )


@pytest.mark.parametrize(
    "event_type",
    [
        pytest.param("none", id="standalone"),
        pytest.param("event", id="with_event"),
        pytest.param("transaction", id="with_transaction"),
    ],
)
def test_attachment_ref(
    mini_sentry,
    relay_with_processing,
    attachments_consumer,
    objectstore,
    event_type,
):
    event_id = "515539018c9b4260a6f999572f1661ee"
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"].setdefault("features", []).append(
        "projects:relay-upload-endpoint"
    )
    mini_sentry.global_config["options"][
        "relay.objectstore-attachments.sample-rate"
    ] = 1.0

    relay = relay_with_processing()
    attachments_consumer = attachments_consumer()
    project_key = mini_sentry.get_dsn_public_key(project_id)

    attachment_data = b"<some very large attachment payload>"
    envelope = Envelope(headers=[["event_id", event_id]])
    envelope.add_item(
        upload_and_make_ref(
            relay,
            project_id,
            project_key,
            attachment_data,
        )
    )

    if event_type == "event":
        envelope.add_event({"message": "Hello, World!"})
    elif event_type == "transaction":
        envelope.add_transaction(make_transaction({"event_id": event_id}))

    relay.send_envelope(project_id, envelope)
    expected_attachment = {
        "id": mock.ANY,
        "name": "test.txt",
        "content_type": "text/plain",
        "attachment_type": "event.attachment",
        "size": len(attachment_data),
        "rate_limited": False,
        "stored_id": mock.ANY,
    }

    if event_type == "event":
        _, event_message = attachments_consumer.get_event()
        assert len(event_message["attachments"]) == 1
        assert event_message["attachments"][0] == expected_attachment
        stored_id = event_message["attachments"][0]["stored_id"]
    else:
        attachment = attachments_consumer.get_individual_attachment()
        assert attachment == {
            "type": "attachment",
            "event_id": event_id,
            "project_id": project_id,
            "attachment": expected_attachment,
        }
        stored_id = attachment["attachment"]["stored_id"]

    objectstore_session = objectstore("attachments", project_id)
    assert objectstore_session.get(stored_id).payload.read() == attachment_data
