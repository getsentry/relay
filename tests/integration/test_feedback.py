import pytest
import json
from sentry_sdk.envelope import Envelope, Item, PayloadRef


def generate_feedback_sdk_event():
    return {
        "type": "feedback",
        "event_id": "d2132d31b39445f1938d7e21b6bf0ec4",
        "timestamp": 1597977777.6189718,
        "dist": "1.12",
        "platform": "javascript",
        "environment": "production",
        "release": 42,
        "tags": {"transaction": "/organizations/:orgId/performance/:eventSlug/"},
        "sdk": {"name": "name", "version": "veresion"},
        "user": {
            "id": "123",
            "username": "user",
            "email": "user@site.com",
            "ip_address": "192.168.11.12",
        },
        "request": {
            "url": None,
            "headers": {
                "user-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15"
            },
        },
        "contexts": {
            "feedback": {
                "message": "test message",
                "contact_email": "test@example.com",
                "type": "feedback",
            },
            "trace": {
                "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
                "span_id": "FA90FDEAD5F74052",
                "type": "trace",
            },
            "replay": {
                "replay_id": "e2d42047b1c5431c8cba85ee2a8ab25d",
            },
        },
    }


def assert_expected_feedback(parsed_feedback, sent_feedback):
    """Assert required fields were returned."""
    assert parsed_feedback["event_id"]
    assert parsed_feedback["type"] == sent_feedback["type"]
    assert parsed_feedback["dist"] == sent_feedback["dist"]
    assert parsed_feedback["platform"] == sent_feedback["platform"]
    assert parsed_feedback["environment"] == sent_feedback["environment"]
    assert parsed_feedback["release"] == str(sent_feedback["release"])
    assert parsed_feedback["sdk"]["name"] == sent_feedback["sdk"]["name"]
    assert parsed_feedback["sdk"]["version"] == sent_feedback["sdk"]["version"]
    assert parsed_feedback["user"]["id"] == sent_feedback["user"]["id"]
    assert parsed_feedback["user"]["username"] == sent_feedback["user"]["username"]
    assert parsed_feedback["user"]["ip_address"] == sent_feedback["user"]["ip_address"]

    assert parsed_feedback["user"]["email"] == "[email]"
    assert parsed_feedback["timestamp"]

    # Assert the tags and requests objects were normalized to lists of doubles.
    assert parsed_feedback["tags"] == [
        ["transaction", sent_feedback["tags"]["transaction"]]
    ]
    assert parsed_feedback["request"] == {
        "headers": [["User-Agent", sent_feedback["request"]["headers"]["user-Agent"]]]
    }

    # Assert contexts object was pulled out.
    assert parsed_feedback["contexts"] == {
        "browser": {"name": "Safari", "version": "15.5", "type": "browser"},
        "device": {"brand": "Apple", "family": "Mac", "model": "Mac", "type": "device"},
        "os": {"name": "Mac OS X", "version": ">=10.15.7", "type": "os"},
        "replay": {
            "replay_id": sent_feedback["contexts"]["replay"]["replay_id"].lower(),
            "type": "replay",
        },
        "trace": {
            "status": "unknown",
            "trace_id": sent_feedback["contexts"]["trace"]["trace_id"].lower(),
            "span_id": sent_feedback["contexts"]["trace"]["span_id"].lower(),
            "type": "trace",
        },
        "feedback": sent_feedback["contexts"]["feedback"],
    }


def test_feedback_event_with_processing(
    mini_sentry,
    relay_with_processing,
    events_consumer,
    feedback_consumer,
):
    mini_sentry.add_basic_project_config(
        42, extra={"config": {"features": ["organizations:user-feedback-ingest"]}}
    )

    consumer = feedback_consumer(timeout=20)
    other_consumer = events_consumer(timeout=20)

    feedback = generate_feedback_sdk_event()
    relay = relay_with_processing()
    relay.send_user_feedback(42, feedback)

    event, message = consumer.get_event()
    assert event["type"] == "feedback"

    parsed_feedback = json.loads(message["payload"])
    # Assert required fields were returned
    assert_expected_feedback(parsed_feedback, feedback)

    # test message wasn't dup'd to the wrong topic
    other_consumer.assert_empty()


def test_feedback_events_without_processing(
    mini_sentry, relay_chain
):
    project_id = 42
    mini_sentry.add_basic_project_config(
        project_id,
        extra={"config": {"features": ["organizations:user-feedback-ingest"]}},
    )

    replay_item = generate_feedback_sdk_event()
    relay = relay_chain(min_relay_version="latest")
    relay.send_user_feedback(42, replay_item)

    envelope = mini_sentry.captured_events.get(timeout=20)
    assert len(envelope.items) == 1

    userfeedback = envelope.items[0]
    assert userfeedback.type == "feedback"


def test_feedback_with_attachment_in_same_envelope(
    mini_sentry,
    relay_with_processing,
    feedback_consumer,
    events_consumer,
    attachments_consumer,
):
    mini_sentry.add_basic_project_config(
        42, extra={"config": {"features": ["organizations:user-feedback-ingest"]}}
    )

    other_consumer = events_consumer(timeout=20)
    feedback_consumer = feedback_consumer(timeout=20)
    attachments_consumer = attachments_consumer(timeout=20)

    feedback = generate_feedback_sdk_event()
    event_id = feedback["event_id"]
    project_id = 42

    attachment_contents = b"Fake PNG bytes!"
    attachment_headers = {
        "length": len(attachment_contents),
        "filename": "screenshot.png",
        "content_type": "application/png",
    }

    envelope = Envelope(headers=[["event_id", event_id]])
    envelope.add_item(
        Item(PayloadRef(json=feedback), type="feedback"),
    )
    envelope.add_item(
        Item(
            PayloadRef(bytes=attachment_contents),
            type="attachment",
            headers=attachment_headers,
        )
    )
    relay = relay_with_processing()
    relay.send_envelope(project_id, envelope)

    # attachment headers
    attachment_event = attachments_consumer.get_individual_attachment()
    assert attachment_event["event_id"] == event_id
    assert attachment_event["project_id"] == project_id
    attachment = attachment_event["attachment"]
    assert attachment["name"] == attachment_headers["filename"]
    assert attachment["content_type"] == attachment_headers["content_type"]
    assert attachment["data"] == attachment_contents
    assert attachment["size"] == attachment_headers["length"]

    # feedback event sent to correct topic
    event, message = feedback_consumer.get_event()
    assert event["type"] == "feedback"

    parsed_feedback = json.loads(message["payload"])
    # Assert required fields were returned
    assert_expected_feedback(parsed_feedback, feedback)

    # test message wasn't dup'd to the wrong topic
    other_consumer.assert_empty()

    # test message wasn't sent to attachments topic
    attachments_consumer.assert_empty()
