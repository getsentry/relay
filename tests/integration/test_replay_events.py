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
        "timestamp": 1597976400.6189718,
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

    replay_events_consumer = replay_events_consumer()

    replay_item = generate_replay_event()

    relay.send_replay_event(42, replay_item)

    replay_event, replay_event_message = replay_events_consumer.get_replay_event()
    assert replay_event["type"] == "replay_event"
    assert replay_event["replay_id"] == "d2132d31b39445f1938d7e21b6bf0ec4"
    assert replay_event_message["retention_days"] == 90

    message = json.loads(bytes(replay_event_message["payload"]))
    print(message)
    assert False


def test_replay_events_without_processing(mini_sentry, relay_chain):
    relay = relay_chain(min_relay_version="latest")

    project_id = 42
    mini_sentry.add_basic_project_config(
        project_id, extra={"config": {"features": ["organizations:session-replay"]}}
    )

    replay_item = generate_replay_event()

    relay.send_replay_event(42, replay_item)

    envelope = mini_sentry.captured_events.get(timeout=1)
    assert len(envelope.items) == 1

    replay_event = envelope.items[0]
    assert replay_event.type == "replay_event"
