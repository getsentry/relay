import pytest
import json


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


@pytest.mark.parametrize("use_feedback_topic", (False, True))
def test_feedback_event_with_processing(
    mini_sentry,
    relay_with_processing,
    events_consumer,
    feedback_consumer,
    use_feedback_topic,
):
    mini_sentry.add_basic_project_config(
        42, extra={"config": {"features": ["organizations:user-feedback-ingest"]}}
    )

    if use_feedback_topic:
        mini_sentry.set_global_config_option("feedback.ingest-topic.rollout-rate", 1.0)
        consumer = feedback_consumer(timeout=20)
        other_consumer = events_consumer(timeout=20)
    else:
        mini_sentry.set_global_config_option("feedback.ingest-topic.rollout-rate", 0.0)
        consumer = events_consumer(timeout=20)
        other_consumer = feedback_consumer(timeout=20)

    feedback = generate_feedback_sdk_event()
    relay = relay_with_processing()
    relay.send_user_feedback(42, feedback)

    event, message = consumer.get_event()
    assert event["type"] == "feedback"

    parsed_feedback = json.loads(message["payload"])
    # Assert required fields were returned.
    assert parsed_feedback["event_id"]
    assert parsed_feedback["type"] == feedback["type"]
    assert parsed_feedback["dist"] == feedback["dist"]
    assert parsed_feedback["platform"] == feedback["platform"]
    assert parsed_feedback["environment"] == feedback["environment"]
    assert parsed_feedback["release"] == str(feedback["release"])
    assert parsed_feedback["sdk"]["name"] == feedback["sdk"]["name"]
    assert parsed_feedback["sdk"]["version"] == feedback["sdk"]["version"]
    assert parsed_feedback["user"]["id"] == feedback["user"]["id"]
    assert parsed_feedback["user"]["username"] == feedback["user"]["username"]
    assert parsed_feedback["user"]["ip_address"] == feedback["user"]["ip_address"]

    assert parsed_feedback["user"]["email"] == "[email]"
    assert parsed_feedback["timestamp"]

    # Assert the tags and requests objects were normalized to lists of doubles.
    assert parsed_feedback["tags"] == [["transaction", feedback["tags"]["transaction"]]]
    assert parsed_feedback["request"] == {
        "headers": [["User-Agent", feedback["request"]["headers"]["user-Agent"]]]
    }

    # Assert contexts object was pulled out.
    assert parsed_feedback["contexts"] == {
        "browser": {"name": "Safari", "version": "15.5", "type": "browser"},
        "device": {"brand": "Apple", "family": "Mac", "model": "Mac", "type": "device"},
        "os": {"name": "Mac OS X", "version": ">=10.15.7", "type": "os"},
        "replay": {"replay_id": "e2d42047b1c5431c8cba85ee2a8ab25d", "type": "replay"},
        "trace": {
            "status": "unknown",
            "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
            "span_id": "fa90fdead5f74052",
            "type": "trace",
        },
        "feedback": {
            "message": "test message",
            "contact_email": "test@example.com",
            "type": "feedback",
        },
    }

    # test message wasn't dup'd to the wrong topic
    other_consumer.assert_empty()


@pytest.mark.parametrize("use_feedback_topic", (False, True))
def test_feedback_events_without_processing(
    mini_sentry, relay_chain, use_feedback_topic
):
    project_id = 42
    mini_sentry.add_basic_project_config(
        project_id,
        extra={"config": {"features": ["organizations:user-feedback-ingest"]}},
    )
    mini_sentry.set_global_config_option(
        "feedback.ingest-topic.rollout-rate", 1.0 if use_feedback_topic else 0.0
    )

    replay_item = generate_feedback_sdk_event()
    relay = relay_chain(min_relay_version="latest")
    relay.send_user_feedback(42, replay_item)

    envelope = mini_sentry.captured_events.get(timeout=20)
    assert len(envelope.items) == 1

    userfeedback = envelope.items[0]
    assert userfeedback.type == "feedback"
