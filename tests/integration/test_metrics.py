from datetime import datetime, timedelta, timezone
import json
import fnv


from .test_envelope import generate_transaction_item


TEST_CONFIG = {
    "aggregator": {
        "bucket_interval": 1,
        "initial_delay": 0,
        "debounce_delay": 0,
    }
}


def test_metrics(mini_sentry, relay):
    relay = relay(mini_sentry, options=TEST_CONFIG)

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    timestamp = int(datetime.now(tz=timezone.utc).timestamp())
    metrics_payload = f"foo:42|c\nbar:17|c"
    relay.send_metrics(project_id, metrics_payload, timestamp)

    envelope = mini_sentry.captured_events.get(timeout=2)
    assert len(envelope.items) == 1

    metrics_item = envelope.items[0]
    assert metrics_item.type == "metric_buckets"

    received_metrics = metrics_item.get_bytes()
    assert json.loads(received_metrics.decode()) == [
        {"timestamp": timestamp, "name": "foo", "value": 42.0, "type": "c"},
        {"timestamp": timestamp, "name": "bar", "value": 17.0, "type": "c"},
    ]


def test_metrics_backdated(mini_sentry, relay):
    relay = relay(mini_sentry, options=TEST_CONFIG)

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    timestamp = int(datetime.now(tz=timezone.utc).timestamp()) - 24 * 60 * 60
    metrics_payload = f"foo:42|c"
    relay.send_metrics(project_id, metrics_payload, timestamp)

    envelope = mini_sentry.captured_events.get(timeout=2)
    assert len(envelope.items) == 1

    metrics_item = envelope.items[0]
    assert metrics_item.type == "metric_buckets"

    received_metrics = metrics_item.get_bytes()
    assert json.loads(received_metrics.decode()) == [
        {"timestamp": timestamp, "name": "foo", "value": 42.0, "type": "c"},
    ]


def to_hash(value: str) -> int:
    return fnv.hash(value.encode(), bits=64)


def to_hash_str(value: str) -> str:
    return str(to_hash(value))


def test_metrics_with_processing(mini_sentry, relay_with_processing, metrics_consumer):
    relay = relay_with_processing(options=TEST_CONFIG)
    metrics_consumer = metrics_consumer()

    project_id = 42
    mini_sentry.add_full_project_config(project_id)

    timestamp = int(datetime.now(tz=timezone.utc).timestamp())
    metrics_payload = f"foo:42|c\nbar@s:17|c"
    relay.send_metrics(project_id, metrics_payload, timestamp)

    metric = metrics_consumer.get_metric()

    assert metric == {
        "org_id": 1,
        "project_id": project_id,
        "metric_id": to_hash("foo"),
        "unit": "",
        "value": 42.0,
        "type": "c",
        "timestamp": timestamp,
        "retention_days": 90,
    }

    metric = metrics_consumer.get_metric()

    assert metric == {
        "org_id": 1,
        "project_id": project_id,
        "metric_id": to_hash("bar"),
        "unit": "s",
        "value": 17.0,
        "type": "c",
        "timestamp": timestamp,
        "retention_days": 90,
    }

    metrics_consumer.assert_empty()


def test_metrics_full(mini_sentry, relay, relay_with_processing, metrics_consumer):
    metrics_consumer = metrics_consumer()

    upstream_config = {
        "aggregator": {
            "bucket_interval": 1,
            "initial_delay": 2,  # Give upstream some time to process downstream entries:
            "debounce_delay": 0,
        }
    }
    upstream = relay_with_processing(options=upstream_config)

    downstream = relay(upstream, options=TEST_CONFIG)

    # Create project config
    project_id = 42
    mini_sentry.add_full_project_config(project_id)

    # Send two events to downstream and one to upstream
    timestamp = int(datetime.now(tz=timezone.utc).timestamp())
    downstream.send_metrics(project_id, f"foo:7|c", timestamp)
    downstream.send_metrics(project_id, f"foo:5|c", timestamp)

    upstream.send_metrics(project_id, f"foo:3|c", timestamp)

    metric = metrics_consumer.get_metric(timeout=4)
    metric.pop("timestamp")
    assert metric == {
        "org_id": 1,
        "project_id": project_id,
        "metric_id": to_hash("foo"),
        "unit": "",
        "value": 15.0,
        "type": "c",
        "retention_days": 90,
    }

    metrics_consumer.assert_empty()


def test_session_metrics_feature_disabled(mini_sentry, relay):
    relay = relay(mini_sentry, options=TEST_CONFIG)

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

    # Get session envelope
    mini_sentry.captured_events.get(timeout=2)

    # Get metrics envelope
    assert mini_sentry.captured_events.empty()


def test_session_metrics(mini_sentry, relay_with_processing, metrics_consumer):
    relay = relay_with_processing(options=TEST_CONFIG)
    project_id = 42
    mini_sentry.add_full_project_config(project_id)

    metrics_consumer = metrics_consumer()

    mini_sentry.project_configs[project_id]["config"]["features"] = [
        "organizations:metrics-extraction"
    ]

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

    metric = metrics_consumer.get_metric()
    assert metric == {
        "org_id": 1,
        "project_id": 42,
        "timestamp": int(timestamp.timestamp()),
        "metric_id": to_hash("session"),
        "type": "c",
        "unit": "",
        "value": 1.0,
        "tags": {
            to_hash_str("environment"): to_hash("production"),
            to_hash_str("release"): to_hash("sentry-test@1.0.0"),
            to_hash_str("session.status"): to_hash("init"),
        },
        "retention_days": 90,
    }

    metric = metrics_consumer.get_metric()
    assert metric == {
        "org_id": 1,
        "project_id": 42,
        "timestamp": int(timestamp.timestamp()),
        "metric_id": to_hash("user"),
        "type": "s",
        "unit": "",
        "value": [1617781333],
        "tags": {
            to_hash_str("environment"): to_hash("production"),
            to_hash_str("release"): to_hash("sentry-test@1.0.0"),
            to_hash_str("session.status"): to_hash("init"),
        },
        "retention_days": 90,
    }

    metric = metrics_consumer.get_metric()
    assert metric == {
        "org_id": 1,
        "project_id": 42,
        "timestamp": int(timestamp.timestamp()),
        "metric_id": to_hash("session.duration"),
        "type": "d",
        "unit": "s",
        "value": [1947.49],
        "tags": {
            to_hash_str("environment"): to_hash("production"),
            to_hash_str("release"): to_hash("sentry-test@1.0.0"),
        },
        "retention_days": 90,
    }

    metrics_consumer.assert_empty()


def test_transaction_metrics(mini_sentry, relay_with_processing, metrics_consumer):
    metrics_consumer = metrics_consumer()

    for feature_enabled in (True, False):

        relay = relay_with_processing(options=TEST_CONFIG)
        project_id = 42
        mini_sentry.add_full_project_config(project_id)
        timestamp = datetime.now(tz=timezone.utc)

        mini_sentry.project_configs[project_id]["config"]["features"] = (
            ["organizations:metrics-extraction"] if feature_enabled else []
        )

        transaction = generate_transaction_item()
        transaction["timestamp"] = timestamp.isoformat()
        transaction["measurements"] = {
            "foo": {"value": 1.2},
            "bar": {"value": 1.3},
        }
        transaction["breakdowns"] = {
            "breakdown1": {
                "baz": {"value": 1.4},
            }
        }

        relay.send_event(42, transaction)

        # Send another transaction:
        transaction["measurements"] = {
            "foo": {"value": 2.2},
        }
        transaction["breakdowns"] = {
            "breakdown1": {
                "baz": {"value": 2.4},
            }
        }
        relay.send_event(42, transaction)

        if not feature_enabled:
            message = metrics_consumer.poll(timeout=None)
            assert message is None, message.value()

            continue

        metrics = {
            metric["metric_id"]: metric
            for metric in [metrics_consumer.get_metric() for _ in range(3)]
        }

        metrics_consumer.assert_empty()

        assert to_hash("measurement.foo") in metrics
        assert metrics[to_hash("measurement.foo")] == {
            "org_id": 1,
            "project_id": 42,
            "timestamp": int(timestamp.timestamp()),
            "metric_id": to_hash("measurement.foo"),
            "type": "d",
            "unit": "",
            "value": [1.2, 2.2],
            "retention_days": 90,
        }

        assert metrics[to_hash("measurement.bar")] == {
            "org_id": 1,
            "project_id": 42,
            "timestamp": int(timestamp.timestamp()),
            "metric_id": to_hash("measurement.bar"),
            "type": "d",
            "unit": "",
            "value": [1.3],
            "retention_days": 90,
        }

        assert metrics[to_hash("breakdown.breakdown1.baz")] == {
            "org_id": 1,
            "project_id": 42,
            "timestamp": int(timestamp.timestamp()),
            "metric_id": to_hash("breakdown.breakdown1.baz"),
            "type": "d",
            "unit": "",
            "value": [1.4, 2.4],
            "retention_days": 90,
        }
