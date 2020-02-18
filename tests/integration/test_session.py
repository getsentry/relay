from datetime import datetime, timedelta, timezone


def test_session_with_processing(mini_sentry, relay_with_processing, sessions_consumer):
    relay = relay_with_processing()
    relay.wait_relay_healthcheck()

    sessions_consumer = sessions_consumer()

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(hours=1)

    mini_sentry.project_configs[42] = mini_sentry.full_project_config()
    relay.send_session(
        42,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
            "did": "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf",
            "seq": 42,
            "timestamp": timestamp.isoformat(),
            "started": started.isoformat(),
            "sample_rate": 0.1,
            "duration": 1947.49,
            "status": "exited",
            "attrs": {
                "os": "iOS",
                "os_version": "13.3.1",
                "device_family": "iPhone12,3",
                "release": "sentry-test@1.0.0",
                "environment": "production",
            },
        },
    )

    session = sessions_consumer.get_session()
    assert session == {
        "org_id": 1,
        "project_id": 42,
        "session_id": "8333339f-5675-4f89-a9a0-1c935255ab58",
        "distinct_id": "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf",
        "seq": 42,
        "timestamp": timestamp.timestamp(),
        "started": started.timestamp(),
        "sample_rate": 2.0,
        "duration": 1947.49,
        "status": "exited",
        "os": "iOS",
        "os_version": "13.3.1",
        "device_family": "iPhone12,3",
        "release": "sentry-test@1.0.0",
        "environment": "production",
        "retention_days": 90,
    }


def test_session_with_custom_retention(
    mini_sentry, relay_with_processing, sessions_consumer
):
    relay = relay_with_processing()
    relay.wait_relay_healthcheck()

    sessions_consumer = sessions_consumer()

    project_config = mini_sentry.full_project_config()
    project_config["config"]["eventRetention"] = 17
    mini_sentry.project_configs[42] = project_config

    timestamp = datetime.now(tz=timezone.utc)
    relay.send_session(
        42,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
            "timestamp": timestamp.isoformat(),
            "started": timestamp.isoformat(),
        },
    )

    session = sessions_consumer.get_session()
    assert session["retention_days"] == 17


def test_session_age_discard(mini_sentry, relay_with_processing, sessions_consumer):
    relay = relay_with_processing()
    relay.wait_relay_healthcheck()

    sessions_consumer = sessions_consumer()

    project_config = mini_sentry.full_project_config()
    project_config["config"]["eventRetention"] = 17
    mini_sentry.project_configs[42] = project_config

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(days=5, hours=1)

    relay.send_session(
        42,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
            "timestamp": timestamp.isoformat(),
            "started": started.isoformat(),
        },
    )

    assert sessions_consumer.poll() is None
