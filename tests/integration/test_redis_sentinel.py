from sentry_sdk.envelope import Envelope

SENTINEL_CONFIG = {
    "processing": {
        "redis": {
            "sentinel_nodes": [
                "redis://127.0.0.1:26379",
                "redis://127.0.0.1:26380",
            ],
            "master_name": "redis-sentry",
        }
    }
}


def test_redis_sentinel(redis_sentinel_client):
    assert redis_sentinel_client, "Failed to discover redis master"


def test_startup_with_redis_sentinel(relay_with_processing):
    upstream = relay_with_processing(SENTINEL_CONFIG)
    assert upstream.wait_relay_health_check() is None, "Failed to start Relay"


def test_envelope_with_redis_sentinel(mini_sentry, relay_with_processing):
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    envelope = Envelope()
    envelope.add_event({"message": "Hello, World!"})

    upstream = relay_with_processing(SENTINEL_CONFIG)
    response = upstream.send_envelope(
        project_id,
        envelope,
    )

    assert response, "Failed to send event"

    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}
