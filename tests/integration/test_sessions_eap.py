from datetime import datetime, timedelta, timezone
from unittest import mock

from .asserts import time_within_delta


TEST_CONFIG = {
    "outcomes": {
        "emit_outcomes": True,
        "batch_size": 1,
        "batch_interval": 1,
        "aggregator": {
            "bucket_interval": 1,
            "flush_interval": 1,
        },
    },
    "aggregator": {
        "bucket_interval": 1,
        "initial_delay": 0,
    },
}


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

    relay = relay_with_processing(options=TEST_CONFIG)

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

    items = items_consumer.get_items(n=2, timeout=10)

    # Separate counter and set items by their distinguishing attribute.
    counter_items = [i for i in items if "session_count" in i.get("attributes", {})]
    set_items = [i for i in items if "user_id" in i.get("attributes", {})]

    assert len(counter_items) == 1
    assert len(set_items) == 1

    # Assert counter metric (c:sessions/session@none).
    assert counter_items[0] == {
        "organizationId": "1",
        "projectId": "42",
        "traceId": mock.ANY,
        "itemId": mock.ANY,
        "itemType": "TRACE_ITEM_TYPE_USER_SESSION",
        "timestamp": time_within_delta(started, delta=timedelta(seconds=2)),
        "received": time_within_delta(),
        "retentionDays": mock.ANY,
        "downsampledRetentionDays": mock.ANY,
        "clientSampleRate": 1.0,
        "serverSampleRate": 1.0,
        "attributes": {
            "status": {"stringValue": "init"},
            "release": {"stringValue": "sentry-test@1.0.0"},
            "environment": {"stringValue": "production"},
            "sdk": {"stringValue": "raven-node/2.6.3"},
            "session_count": {"doubleValue": 1.0},
        },
    }

    # Assert set metric (s:sessions/user@none).
    # 1617781333 is the CRC32 hash of "foobarbaz".
    assert set_items[0] == {
        "organizationId": "1",
        "projectId": "42",
        "traceId": mock.ANY,
        "itemId": mock.ANY,
        "itemType": "TRACE_ITEM_TYPE_USER_SESSION",
        "timestamp": time_within_delta(started, delta=timedelta(seconds=2)),
        "received": time_within_delta(),
        "retentionDays": mock.ANY,
        "downsampledRetentionDays": mock.ANY,
        "clientSampleRate": 1.0,
        "serverSampleRate": 1.0,
        "attributes": {
            "release": {"stringValue": "sentry-test@1.0.0"},
            "environment": {"stringValue": "production"},
            "sdk": {"stringValue": "raven-node/2.6.3"},
            "user_id": {"intValue": "1617781333"},
        },
    }


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

    relay = relay_with_processing(options=TEST_CONFIG)

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
    metrics_consumer.get_metrics(n=2, timeout=10)

    # No items should appear on the EAP topic.
    items_consumer.assert_empty()
