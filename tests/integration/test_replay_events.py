import json
import uuid


def generate_replay_sdk_event(replay_id="d2132d31b39445f1938d7e21b6bf0ec4"):
    return {
        "type": "replay_event",
        "replay_id": replay_id,
        "replay_type": "session",
        "event_id": replay_id,
        "segment_id": 0,
        "timestamp": 1597977777.6189718,
        "replay_start_timestamp": 1597976392.6542819,
        "urls": ["sentry.io"],
        "error_ids": [str(uuid.uuid4())],
        "trace_ids": [str(uuid.uuid4())],
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
            "trace": {
                "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
                "span_id": "FA90FDEAD5F74052",
                "type": "trace",
            },
            "replay": {
                "error_sample_rate": 0.125,
                "session_sample_rate": 0.5,
            },
        },
    }


def assert_replay_payload_matches(produced, consumed):
    assert consumed["type"] == "replay_event"
    assert consumed["replay_id"] == produced["replay_id"]
    assert consumed["replay_type"] == produced["replay_type"]
    assert consumed["event_id"] == produced["event_id"]
    assert consumed["type"] == produced["type"]
    assert consumed["segment_id"] == produced["segment_id"]
    assert consumed["urls"] == produced["urls"]
    assert consumed["error_ids"] == produced["error_ids"]
    assert consumed["trace_ids"] == produced["trace_ids"]
    assert consumed["dist"] == produced["dist"]
    assert consumed["platform"] == produced["platform"]
    assert consumed["environment"] == produced["environment"]
    assert consumed["release"] == str(produced["release"])
    assert consumed["sdk"]["name"] == produced["sdk"]["name"]
    assert consumed["sdk"]["version"] == produced["sdk"]["version"]
    assert consumed["user"]["id"] == produced["user"]["id"]
    assert consumed["user"]["username"] == produced["user"]["username"]
    assert consumed["user"]["ip_address"] == produced["user"]["ip_address"]

    # Assert PII scrubbing.
    assert consumed["user"]["email"] == "[email]"

    # Round to account for float imprecision. Not a big deal. Decimals
    # are dropped in Clickhouse.
    assert int(consumed["replay_start_timestamp"]) == int(
        produced["replay_start_timestamp"]
    )
    assert int(consumed["timestamp"]) == int(produced["timestamp"])

    # Assert the tags and requests objects were normalized to lists of doubles.
    assert consumed["tags"] == [["transaction", produced["tags"]["transaction"]]]
    assert consumed["request"] == {
        "headers": [["User-Agent", produced["request"]["headers"]["user-Agent"]]]
    }

    # Assert contexts object was pulled out.
    assert consumed["contexts"] == {
        "browser": {"name": "Safari", "version": "15.5", "type": "browser"},
        "device": {"brand": "Apple", "family": "Mac", "model": "Mac", "type": "device"},
        "os": {"name": "Mac OS X", "version": ">=10.15.7", "type": "os"},
        "replay": {
            "type": "replay",
            "error_sample_rate": produced["contexts"]["replay"]["error_sample_rate"],
            "session_sample_rate": produced["contexts"]["replay"][
                "session_sample_rate"
            ],
        },
        "trace": {
            "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
            "span_id": "fa90fdead5f74052",
            "type": "trace",
        },
    }


def test_replay_event_with_processing(
    mini_sentry, relay_with_processing, replay_events_consumer
):
    relay = relay_with_processing()
    mini_sentry.add_basic_project_config(
        42, extra={"config": {"features": ["organizations:session-replay"]}}
    )

    replay_events_consumer = replay_events_consumer(timeout=10)
    replay = generate_replay_sdk_event()

    relay.send_replay_event(42, replay)

    replay_event, replay_event_message = replay_events_consumer.get_replay_event()
    assert_replay_payload_matches(replay, replay_event)


def test_replay_events_without_processing(mini_sentry, relay_chain):
    relay = relay_chain(min_relay_version="latest")

    project_id = 42
    mini_sentry.add_basic_project_config(
        project_id, extra={"config": {"features": ["organizations:session-replay"]}}
    )

    replay_item = generate_replay_sdk_event()

    relay.send_replay_event(42, replay_item)

    envelope = mini_sentry.captured_events.get(timeout=20)
    assert len(envelope.items) == 1

    replay_event = envelope.items[0]
    assert replay_event.type == "replay_event"
