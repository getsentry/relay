def test_session_with_processing(mini_sentry, relay_with_processing, sessions_consumer):
    relay = relay_with_processing()
    relay.wait_relay_healthcheck()

    sessions_consumer = sessions_consumer()

    project_config = mini_sentry.project_configs[42] = mini_sentry.full_project_config()
    relay.send_session(
        42,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
            "did": "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf",
            "seq": 42,
            "timestamp": "2020-02-07T15:17:00Z",
            "started": "2020-02-07T14:16:00Z",
            "sample_rate": 2.0,
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
        "timestamp": "2020-02-07T15:17:00+00:00",
        "started": "2020-02-07T14:16:00+00:00",
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
