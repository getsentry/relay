import json


def generate_replay_event():
    return {
        "replay_id": "d2132d31b39445f1938d7e21b6bf0ec4",
        "event_id": "123",
        "type": "replay_event",
        "transaction": "/organizations/:orgId/performance/:eventSlug/",
        "start_timestamp": 1597976392.6542819,
        "segment_id": 0,
        "replay_start_timestamp": 1597976392.6542819,
        "timestamp": 1597977777.6189718,
        "urls": ["sentry.io"],
        "error_ids": ["1", "2"],
        "trace_ids": ["3", "4"],
        "dist": "1.12",
        "platform": "Python",
        "environment": "production",
        "release": "version@1.3",
        "tags": {"transaction": "/organizations/:orgId/performance/:eventSlug/"},
        "sdk": {"name": "name", "version": "veresion"},
        "user": {
            "id": "123",
            "username": "user",
            "email": "user@site.com",
            "ip_address": "127.0.0.1",
        },
        "requests": {
            "url": None,
            "headers": {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15"
            },
        },
        "contexts": {
            "trace": {
                "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
                "span_id": "FA90FDEAD5F74052",
                "type": "trace",
            }
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
    replay = generate_replay_event()

    relay.send_replay_event(42, replay)

    replay_event, replay_event_message = replay_events_consumer.get_replay_event()
    assert replay_event["type"] == "replay_event"
    assert replay_event["replay_id"] == "d2132d31b39445f1938d7e21b6bf0ec4"
    assert replay_event_message["retention_days"] == 90

    parsed_replay = json.loads(bytes(replay_event_message["payload"]))
    # Assert required fields were returned.
    assert parsed_replay["replay_id"] == replay["replay_id"]
    assert parsed_replay["event_id"] == replay["event_id"]
    assert parsed_replay["type"] == replay["type"]
    assert parsed_replay["segment_id"] == replay["segment_id"]
    assert parsed_replay["urls"] == replay["urls"]
    assert parsed_replay["error_ids"] == replay["error_ids"]
    assert parsed_replay["trace_ids"] == replay["trace_ids"]
    assert parsed_replay["dist"] == replay["dist"]
    assert parsed_replay["platform"] == replay["platform"]
    assert parsed_replay["environment"] == replay["environment"]
    assert parsed_replay["release"] == replay["release"]
    assert parsed_replay["tags"]["transaction"] == replay["tags"]["transaction"]
    assert parsed_replay["sdk"]["name"] == replay["sdk"]["name"]
    assert parsed_replay["sdk"]["version"] == replay["sdk"]["version"]
    assert parsed_replay["user"]["id"] == replay["user"]["id"]
    assert parsed_replay["user"]["username"] == replay["user"]["username"]
    assert parsed_replay["user"]["email"] == replay["user"]["email"]
    assert parsed_replay["user"]["ip_address"] == replay["user"]["ip_address"]
    assert parsed_replay["requests"]["url"] == replay["requests"]["url"]
    assert (
        parsed_replay["requests"]["headers"]["User-Agent"]
        == replay["requests"]["headers"]["User-Agent"]
    )

    # Round to account for float imprecision. Not a big deal. Decimals
    # are dropped in Clickhouse.
    assert int(parsed_replay["replay_start_timestamp"]) == int(
        replay["replay_start_timestamp"]
    )
    assert int(parsed_replay["timestamp"]) == int(replay["timestamp"])

    # Assert contexts object was pulled out.
    assert parsed_replay["contexts"] == {
        "browser": {"name": "Safari", "version": "15.5",},
        "device": {"brand": "Apple", "family": "Mac", "model": "Mac",},
        "os": {"name": "Mac OS X", "version": "10.15.7",},
    }


def test_replay_events_without_processing(mini_sentry, relay_chain):
    relay = relay_chain(min_relay_version="latest")

    project_id = 42
    mini_sentry.add_basic_project_config(
        project_id, extra={"config": {"features": ["organizations:session-replay"]}}
    )

    replay_item = generate_replay_event()

    relay.send_replay_event(42, replay_item)

    envelope = mini_sentry.captured_events.get(timeout=20)
    assert len(envelope.items) == 1

    replay_event = envelope.items[0]
    assert replay_event.type == "replay_event"
