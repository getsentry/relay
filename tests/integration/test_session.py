from datetime import datetime, timedelta, timezone
import json


def test_sessions(mini_sentry, relay_chain):
    relay = relay_chain()

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(hours=1)

    session_payload = {
        "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
        "did": "foobarbaz",
        "seq": 42,
        "init": True,
        "timestamp": timestamp.isoformat(),
        "started": started.isoformat(),
        "duration": 1947.49,
        "status": "exited",
        "errors": 0,
        "attrs": {
            "release": "sentry-test@1.0.0",
            "environment": "production",
        },
    }

    relay.send_session(project_id, session_payload)

    envelope = mini_sentry.captured_events.get(timeout=1)
    assert len(envelope.items) == 1

    session_item = envelope.items[0]
    assert session_item.type == "session"

    session = json.loads(session_item.get_bytes())
    assert session == session_payload


def test_session_age_discard(mini_sentry, relay_with_processing, sessions_consumer):
    relay = relay_with_processing()
    sessions_consumer = sessions_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["eventRetention"] = 17

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(days=5, hours=1)

    relay.send_session(
        project_id,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
            "timestamp": timestamp.isoformat(),
            "started": started.isoformat(),
            "attrs": {"release": "sentry-test@1.0.0"},
        },
    )

    sessions_consumer.assert_empty()


def test_session_age_discard_aggregates(
    mini_sentry, relay_with_processing, sessions_consumer
):
    relay = relay_with_processing()
    sessions_consumer = sessions_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["eventRetention"] = 17

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(days=5, hours=1)

    relay.send_session_aggregates(
        project_id,
        {
            "aggregates": [
                {
                    "started": started.isoformat(),
                    "did": "foobarbaz",
                    "exited": 2,
                    "errored": 3,
                },
            ],
            "attrs": {
                "release": "sentry-test@1.0.0",
                "environment": "production",
            },
        },
    )

    sessions_consumer.assert_empty()


def test_session_release_required(
    mini_sentry, relay_with_processing, sessions_consumer
):
    relay = relay_with_processing()
    sessions_consumer = sessions_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["eventRetention"] = 17

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(days=5, hours=1)

    relay.send_session(
        project_id,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
            "timestamp": timestamp.isoformat(),
            "started": started.isoformat(),
        },
    )

    sessions_consumer.assert_empty()


def test_session_aggregates_release_required(
    mini_sentry, relay_with_processing, sessions_consumer
):
    relay = relay_with_processing()
    sessions_consumer = sessions_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["eventRetention"] = 17

    started = datetime.now(tz=timezone.utc)

    relay.send_session_aggregates(
        project_id,
        {
            "aggregates": [
                {
                    "started": started.isoformat(),
                    "did": "foobarbaz",
                    "exited": 2,
                    "errored": 3,
                },
            ],
            "attrs": {
                "environment": "production",
            },
        },
    )

    sessions_consumer.assert_empty()


def test_session_disabled(mini_sentry, relay_with_processing, sessions_consumer):
    relay = relay_with_processing()
    sessions_consumer = sessions_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["eventRetention"] = 17
    project_config["config"]["quotas"] = [
        {
            "categories": ["session"],
            "scope": "key",
            "scopeId": str(project_config["publicKeys"][0]["numericId"]),
            "limit": 0,
            "reasonCode": "sessions_exceeded",
        }
    ]

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(hours=1)

    relay.send_session(
        project_id,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
            "timestamp": timestamp.isoformat(),
            "started": started.isoformat(),
            "attrs": {"release": "sentry-test@1.0.0"},
        },
    )

    sessions_consumer.assert_empty()


def test_session_invalid_release(mini_sentry, relay_with_processing, sessions_consumer):
    relay = relay_with_processing()
    sessions_consumer = sessions_consumer()

    PROJECT_ID = 42
    project_config = mini_sentry.add_full_project_config(PROJECT_ID)
    project_config["config"]["eventRetention"] = 17

    timestamp = datetime.now(tz=timezone.utc)
    relay.send_session(
        PROJECT_ID,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
            "timestamp": timestamp.isoformat(),
            "started": timestamp.isoformat(),
            "attrs": {"release": "latest"},
        },
    )

    sessions_consumer.assert_empty()


def test_session_aggregates_invalid_release(
    mini_sentry, relay_with_processing, sessions_consumer
):
    relay = relay_with_processing()
    sessions_consumer = sessions_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["eventRetention"] = 17

    timestamp = datetime.now(tz=timezone.utc)
    relay.send_session_aggregates(
        project_id,
        {
            "aggregates": [
                {
                    "started": timestamp.isoformat(),
                    "did": "foobarbaz",
                    "exited": 2,
                    "errored": 3,
                },
            ],
            "attrs": {"release": "latest"},
        },
    )

    sessions_consumer.assert_empty()
