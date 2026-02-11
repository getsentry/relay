from datetime import datetime, timedelta, timezone
from unittest import mock

from .asserts import time_within_delta


def test_session_eap_double_write(
    mini_sentry,
    relay_with_processing,
    items_consumer,
):
    """
    Test that session metrics are double-written to the EAP snuba-items topic
    as TRACE_ITEM_TYPE_USER_SESSION TraceItems when the rollout rate is enabled.
    Asserts the full Kafka payload for both the counter (session_count) and
    set (user_id) metric buckets.
    """
    items_consumer = items_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["sessionMetrics"] = {"version": 3}

    # Enable EAP double-write via global config.
    mini_sentry.global_config["options"]["relay.sessions-eap.rollout-rate"] = 1.0

    relay = relay_with_processing()

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(hours=1)

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
            "attrs": {
                "release": "sentry-test@1.0.0",
                "environment": "production",
            },
        },
    )

    items = sorted(
        items_consumer.get_items(n=2),
        key=lambda x: not x["attributes"].get("session_count"),
    )
    assert items == [
        # Converted from: `c:sessions/session@none`
        {
            "organizationId": "1",
            "projectId": "42",
            "traceId": mock.ANY,
            "itemId": mock.ANY,
            "itemType": 12,
            "timestamp": time_within_delta(started, delta=timedelta(seconds=2)),
            "received": time_within_delta(),
            "retentionDays": 90,
            "downsampledRetentionDays": 90,
            "clientSampleRate": 1.0,
            "serverSampleRate": 1.0,
            "attributes": {
                "status": {"stringValue": "init"},
                "release": {"stringValue": "sentry-test@1.0.0"},
                "environment": {"stringValue": "production"},
                "sdk": {"stringValue": "raven-node/2.6.3"},
                "session_count": {"intValue": "1"},
            },
        },
        # Converted from `s:sessions/user@none`
        {
            "organizationId": "1",
            "projectId": "42",
            "traceId": mock.ANY,
            "itemId": mock.ANY,
            "itemType": 12,
            "timestamp": time_within_delta(started, delta=timedelta(seconds=2)),
            "received": time_within_delta(),
            "retentionDays": 90,
            "downsampledRetentionDays": 90,
            "clientSampleRate": 1.0,
            "serverSampleRate": 1.0,
            "attributes": {
                "release": {"stringValue": "sentry-test@1.0.0"},
                "environment": {"stringValue": "production"},
                "sdk": {"stringValue": "raven-node/2.6.3"},
                # 1617781333 is the CRC32 hash of "foobarbaz".
                "user_id_hash": {
                    "arrayValue": {"values": [{"intValue": "1617781333"}]}
                },
            },
        },
    ]


def test_session_eap_double_write_disabled(
    mini_sentry,
    relay_with_processing,
    items_consumer,
    metrics_consumer,
):
    """
    Test that sessions are NOT written to EAP when the rollout rate is 0 (default).
    """
    items_consumer = items_consumer()
    metrics_consumer = metrics_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["sessionMetrics"] = {"version": 3}

    # Don't set rollout-rate — defaults to 0.0.
    relay = relay_with_processing()

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(hours=1)

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
            "attrs": {
                "release": "sentry-test@1.0.0",
                "environment": "production",
            },
        },
    )

    # Wait for legacy metrics to confirm the session was fully processed.
    assert len(metrics_consumer.get_metrics(n=2)) == 2

    # No items should appear on the EAP topic.
    items_consumer.assert_empty()
