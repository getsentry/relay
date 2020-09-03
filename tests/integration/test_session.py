from datetime import datetime, timedelta, timezone
import pytest
from requests.exceptions import HTTPError
import six
import uuid


def test_session_with_processing(mini_sentry, relay_with_processing, sessions_consumer):
    relay = relay_with_processing()
    sessions_consumer = sessions_consumer()

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(hours=1)

    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay.send_session(
        project_id,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
            "did": "foobarbaz",
            "seq": 42,
            "init": True,
            "timestamp": timestamp.isoformat(),
            "started": started.isoformat(),
            "duration": 1947.49,
            "status": "exited",
            "errors": 0,
            "attrs": {"release": "sentry-test@1.0.0", "environment": "production",},
        },
    )

    session = sessions_consumer.get_session()
    assert session == {
        "org_id": 1,
        "project_id": project_id,
        "session_id": "8333339f-5675-4f89-a9a0-1c935255ab58",
        "distinct_id": "367e2499-2b45-586d-814f-778b60144e87",
        "quantity": 1,
        # seq is forced to 0 when init is true
        "seq": 0,
        "received": timestamp.timestamp(),
        "started": started.timestamp(),
        "duration": 1947.49,
        "status": "exited",
        "errors": 0,
        "release": "sentry-test@1.0.0",
        "environment": "production",
        "retention_days": 90,
        "sdk": "raven-node/2.6.3",
    }


def test_session_with_processing_two_events(
    mini_sentry, relay_with_processing, sessions_consumer
):
    relay = relay_with_processing()
    sessions_consumer = sessions_consumer()

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(hours=1)

    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay.send_session(
        project_id,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
            "did": "foobarbaz",
            "seq": 42,
            "init": True,
            "timestamp": timestamp.isoformat(),
            "started": started.isoformat(),
            "status": "ok",
            "attrs": {"release": "sentry-test@1.0.0", "environment": "production",},
        },
    )
    session = sessions_consumer.get_session()
    assert session == {
        "org_id": 1,
        "project_id": project_id,
        "session_id": "8333339f-5675-4f89-a9a0-1c935255ab58",
        "distinct_id": "367e2499-2b45-586d-814f-778b60144e87",
        "quantity": 1,
        # seq is forced to 0 when init is true
        "seq": 0,
        "received": timestamp.timestamp(),
        "started": started.timestamp(),
        "duration": None,
        "status": "ok",
        "errors": 0,
        "release": "sentry-test@1.0.0",
        "environment": "production",
        "retention_days": 90,
        "sdk": "raven-node/2.6.3",
    }

    relay.send_session(
        project_id,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
            "did": "foobarbaz",
            "seq": 43,
            "timestamp": timestamp.isoformat(),
            "started": started.isoformat(),
            "duration": 1947.49,
            "status": "exited",
            "attrs": {"release": "sentry-test@1.0.0", "environment": "production",},
        },
    )
    session = sessions_consumer.get_session()
    assert session == {
        "org_id": 1,
        "project_id": project_id,
        "session_id": "8333339f-5675-4f89-a9a0-1c935255ab58",
        "distinct_id": "367e2499-2b45-586d-814f-778b60144e87",
        "quantity": 1,
        "seq": 43,
        "received": timestamp.timestamp(),
        "started": started.timestamp(),
        "duration": 1947.49,
        "status": "exited",
        "errors": 0,
        "release": "sentry-test@1.0.0",
        "environment": "production",
        "retention_days": 90,
        "sdk": "raven-node/2.6.3",
    }


def test_session_aggregates(mini_sentry, relay_with_processing, sessions_consumer):
    relay = relay_with_processing({"processing": {"explode_session_aggregates": False}})
    sessions_consumer = sessions_consumer()

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(hours=1)

    mini_sentry.project_configs[42] = mini_sentry.full_project_config()
    relay.send_session_aggregates(
        42,
        {
            "aggregates": [
                {
                    "started": started.isoformat(),
                    "did": "foobarbaz",
                    "exited": 2,
                    "errored": 3,
                }
            ],
            "attrs": {"release": "sentry-test@1.0.0", "environment": "production",},
        },
    )

    session = sessions_consumer.get_session()
    del session["received"]
    assert session == {
        "org_id": 1,
        "project_id": 42,
        "session_id": "00000000-0000-0000-0000-000000000000",
        "distinct_id": "367e2499-2b45-586d-814f-778b60144e87",
        "quantity": 2,
        "seq": 0,
        "started": started.timestamp(),
        "duration": None,
        "status": "exited",
        "errors": 0,
        "release": "sentry-test@1.0.0",
        "environment": "production",
        "retention_days": 90,
        "sdk": "raven-node/2.6.3",
    }

    session = sessions_consumer.get_session()
    del session["received"]
    assert session == {
        "org_id": 1,
        "project_id": 42,
        "session_id": "00000000-0000-0000-0000-000000000000",
        "distinct_id": "367e2499-2b45-586d-814f-778b60144e87",
        "quantity": 3,
        "seq": 0,
        "started": started.timestamp(),
        "duration": None,
        "status": "errored",
        "errors": 1,
        "release": "sentry-test@1.0.0",
        "environment": "production",
        "retention_days": 90,
        "sdk": "raven-node/2.6.3",
    }


def test_session_aggregates_explode(
    mini_sentry, relay_with_processing, sessions_consumer
):
    relay = relay_with_processing({"processing": {"explode_session_aggregates": True}})
    sessions_consumer = sessions_consumer()

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(hours=1)

    mini_sentry.project_configs[42] = mini_sentry.full_project_config()
    relay.send_session_aggregates(
        42,
        {
            "aggregates": [
                {"started": started.isoformat(), "did": "foobarbaz", "exited": 2,}
            ],
            "attrs": {"release": "sentry-test@1.0.0", "environment": "production",},
        },
    )

    expected = {
        "org_id": 1,
        "project_id": 42,
        "distinct_id": "367e2499-2b45-586d-814f-778b60144e87",
        "quantity": 1,
        "seq": 0,
        "started": started.timestamp(),
        "duration": None,
        "status": "exited",
        "errors": 0,
        "release": "sentry-test@1.0.0",
        "environment": "production",
        "retention_days": 90,
        "sdk": "raven-node/2.6.3",
    }

    session = sessions_consumer.get_session()
    del session["session_id"]
    del session["received"]
    assert session == expected

    session = sessions_consumer.get_session()
    del session["session_id"]
    del session["received"]
    assert session == expected


def test_session_with_custom_retention(
    mini_sentry, relay_with_processing, sessions_consumer
):
    relay = relay_with_processing()
    sessions_consumer = sessions_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["eventRetention"] = 17

    timestamp = datetime.now(tz=timezone.utc)
    relay.send_session(
        project_id,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
            "timestamp": timestamp.isoformat(),
            "started": timestamp.isoformat(),
            "attrs": {"release": "sentry-test@1.0.0"},
        },
    )

    session = sessions_consumer.get_session()
    assert session["retention_days"] == 17


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

    assert sessions_consumer.poll() is None


def test_session_force_errors_on_crash(
    mini_sentry, relay_with_processing, sessions_consumer
):
    relay = relay_with_processing()
    sessions_consumer = sessions_consumer()

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(hours=1)

    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay.send_session(
        project_id,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
            "did": "foobarbaz",
            "seq": 42,
            "init": True,
            "timestamp": timestamp.isoformat(),
            "started": started.isoformat(),
            "status": "crashed",
            "attrs": {"release": "sentry-test@1.0.0", "environment": "production"},
        },
    )

    session = sessions_consumer.get_session()
    assert session == {
        "org_id": 1,
        "project_id": project_id,
        "session_id": "8333339f-5675-4f89-a9a0-1c935255ab58",
        "distinct_id": "367e2499-2b45-586d-814f-778b60144e87",
        "quantity": 1,
        # seq is forced to 0 when init is true
        "seq": 0,
        "received": timestamp.timestamp(),
        "started": started.timestamp(),
        "duration": None,
        "status": "crashed",
        "errors": 1,
        "release": "sentry-test@1.0.0",
        "environment": "production",
        "retention_days": 90,
        "sdk": "raven-node/2.6.3",
    }


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

    assert sessions_consumer.poll() is None
    assert mini_sentry.test_failures
    mini_sentry.test_failures.clear()


def test_session_quotas(mini_sentry, relay_with_processing, sessions_consumer):
    relay = relay_with_processing()
    sessions_consumer = sessions_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["eventRetention"] = 17
    project_config["config"]["quotas"] = [
        {
            "id": "test_rate_limiting_{}".format(uuid.uuid4().hex),
            "categories": ["session"],
            "scope": "key",
            "scopeId": six.text_type(project_config["publicKeys"][0]["numericId"]),
            "window": 3600,
            "limit": 5,
            "reasonCode": "sessions_exceeded",
        }
    ]

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(hours=1)

    session = {
        "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
        "timestamp": timestamp.isoformat(),
        "started": started.isoformat(),
        "attrs": {"release": "sentry-test@1.0.0"},
    }

    for i in range(5):
        relay.send_session(project_id, session)
        sessions_consumer.get_session()

    # Rate limited, but responds with 200 because of deferred processing
    relay.send_session(project_id, session)
    assert sessions_consumer.poll() is None

    with pytest.raises(HTTPError):
        relay.send_session(project_id, session)
    assert sessions_consumer.poll() is None


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
            "scopeId": six.text_type(project_config["publicKeys"][0]["numericId"]),
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

    assert sessions_consumer.poll() is None


def test_session_auto_ip(mini_sentry, relay_with_processing, sessions_consumer):
    relay = relay_with_processing()
    sessions_consumer = sessions_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["eventRetention"] = 17

    timestamp = datetime.now(tz=timezone.utc)
    relay.send_session(
        project_id,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
            "timestamp": timestamp.isoformat(),
            "started": timestamp.isoformat(),
            "attrs": {"release": "sentry-test@1.0.0", "ip_address": "{{auto}}"},
        },
    )

    # Can't test ip_address since it's not posted to Kafka. Just test that it is accepted.
    session = sessions_consumer.get_session()
    assert session
