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


def test_feedback_envelope_with_attachment(
    mini_sentry,
    relay_with_processing,
    feedback_consumer,
    attachments_consumer,
    use_feedback_topic,
):
    mini_sentry.add_basic_project_config(
        42, extra={"config": {"features": ["organizations:user-feedback-ingest"]}}
    )
    # mini_sentry.set_global_config_option("feedback.ingest-topic.rollout-rate", 1.0)
    # TODO: make and set a FF here
    feedback_consumer_ = feedback_consumer(timeout=20)
    attachments_consumer_ = attachments_consumer(timeout=20)

    feedback = generate_feedback_sdk_event()
    event_id = feedback["event_id"]

    # TODO: find an example envelope
    envelope = Envelope(headers=[["event_id", event_id]])
    envelope.add_item(
        Item(
            headers=[],  # TODO:
            type="event",
            payload=PayloadRef(json=feedback),
        ),
        Item(
            headers=[
                ["attachment_type", "event.view_hierarchy"]
            ],  # TODO: different attachment type?
            type="attachment",
            payload=PayloadRef(json={"rendering_system": "compose", "windows": []}),
        ),
    )

    relay.send_envelope(project_id, envelope)

    (payload, chunk) = attachments_consumer.get_attachment_chunk()
    attachment = attachments_consumer.get_individual_attachment()

    assert attachment == {
        "type": "attachment",
        "attachment": {
            "attachment_type": "event.view_hierarchy",
            "chunks": 1,
            "content_type": "application/json",
            "id": chunk["id"],
            "name": "Unnamed Attachment",
            "size": len(payload),
            "rate_limited": False,
        },
        "event_id": event_id,
        "project_id": project_id,
    }

    relay = relay_with_processing()
    relay.send_envelope(project_id, envelope)

    event, message = feedback_consumer_.get_event()
    assert event["type"] == "feedback"


# /// If there is a UserReportV2 item, . The attachments should be kept in
#     /// the event and not be returned as stand-alone attachments.
#     #[test]
#     fn test_store_attachment_in_event_when_not_a_transaction() {
#         let (start_time, event_id, scoping, attachment_vec) = arguments_extract_kafka_msgs();
#         let number_of_attachments = attachment_vec.len();

#         let item = Item::new(ItemType::Event);
#         let event_item = Some(&item);

#         let kafka_messages = StoreService::extract_kafka_messages_for_event(
#             event_item,
#             event_id,
#             scoping,
#             start_time,
#             None,
#             attachment_vec,
#         );

#         let (event, standalone_attachments): (Vec<_>, Vec<_>) =
#             kafka_messages.partition(|item| match item {
#                 KafkaMessage::Event(_) => true,
#                 KafkaMessage::Attachment(_) => false,
#                 _ => panic!("only expected events or attachment type"),
#             });

#         // Because it's not a transaction event, the attachment should be part of the event,
#         // and therefore the standalone_attachments vec should be empty.
#         assert!(standalone_attachments.is_empty());

#         // Checks that the attachment is part of the event.
#         let event = &event[0];
#         if let KafkaMessage::Event(event) = event {
#             assert!(event.attachments.len() == number_of_attachments);
#         } else {
#             panic!("No event found")
#         }
#     }
