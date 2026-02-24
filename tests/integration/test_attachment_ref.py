import pytest
import uuid

from requests.exceptions import HTTPError
from sentry_sdk.envelope import Envelope, Item, PayloadRef


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
