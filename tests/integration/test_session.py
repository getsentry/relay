from datetime import datetime, timedelta, timezone
import json
import pytest

from .test_metrics import metrics_without_keys
from .asserts.time import time_within_delta


@pytest.mark.parametrize("metric_extraction", [False, True])
def test_sessions(mini_sentry, relay_chain, metric_extraction):
    relay = relay_chain()

    project_id = 42
    project_config = mini_sentry.add_basic_project_config(project_id)
    if metric_extraction:
        project_config["config"]["sessionMetrics"] = {"version": 3}

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

    envelope = mini_sentry.captured_events.get(timeout=5)
    assert len(envelope.items) == 1

    if metric_extraction:
        metrics = envelope.items[0]
        assert metrics.type == "metric_buckets"

        received_metrics = metrics_without_keys(
            json.loads(metrics.get_bytes().decode()), keys={"metadata"}
        )
        assert received_metrics == [
            {
                "timestamp": time_within_delta(started),
                "width": 1,
                "name": "c:sessions/session@none",
                "type": "c",
                "value": 1.0,
                "tags": {
                    "environment": "production",
                    "release": "sentry-test@1.0.0",
                    "sdk": "raven-node/2.6.3",
                    "session.status": "init",
                },
            },
            {
                "timestamp": time_within_delta(started),
                "width": 1,
                "name": "s:sessions/user@none",
                "type": "s",
                "value": [1617781333],
                "tags": {
                    "environment": "production",
                    "release": "sentry-test@1.0.0",
                    "sdk": "raven-node/2.6.3",
                },
            },
        ]
    else:
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


def test_session_filtering(mini_sentry, relay_with_processing, sessions_consumer):
    relay = relay_with_processing()
    sessions_consumer = sessions_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    filter_settings = project_config["config"]["filterSettings"]
    filter_settings["webCrawlers"] = {"isEnabled": True}
    filter_settings["localhost"] = {"isEnabled": True}
    filter_settings["clientIps"] = {"blacklistedIps": ["1.2.3.0/24"]}
    filter_settings["releases"] = {"releases": ["sentry-bad*"]}

    timestamp = datetime.now(tz=timezone.utc)
    # should get filtered because web crawler filtering is enabled.
    relay.send_session(
        project_id,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
            "timestamp": timestamp.isoformat(),
            "started": timestamp.isoformat(),
            "attrs": {"user_agent": "BingBot"},
        },
    )

    # should get filtered because localhost filtering is enabled.
    relay.send_session(
        project_id,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab59",
            "timestamp": timestamp.isoformat(),
            "started": timestamp.isoformat(),
            "attrs": {"ip_address": "127.0.0.1"},
        },
    )

    # should get filtered because client IP filtering is enabled.
    relay.send_session(
        project_id,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab59",
            "timestamp": timestamp.isoformat(),
            "started": timestamp.isoformat(),
            "attrs": {},
        },
        headers={"X-Forwarded-For": "1.2.3.4"},
    )

    # should get filtered because release filtering is enabled.
    relay.send_session(
        project_id,
        {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab59",
            "timestamp": timestamp.isoformat(),
            "started": timestamp.isoformat(),
            "attrs": {"release": "sentry-bad@1.0.0"},
        },
    )

    # should get filtered because web crawler filtering is enabled.
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
            "attrs": {"user_agent": "BingBot"},
        },
    )

    # should get filtered because localhost filtering is enabled.
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
            "attrs": {"ip_address": "127.0.0.1"},
        },
    )

    # should get filtered because client IP filtering is enabled.
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
            "attrs": {},
        },
        headers={"X-Forwarded-For": "1.2.3.4"},
    )

    # should get filtered because release filtering is enabled.
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
            "attrs": {"release": "sentry-bad@1.0.0"},
        },
    )

    sessions_consumer.assert_empty()
