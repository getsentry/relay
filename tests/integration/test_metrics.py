from datetime import datetime, timedelta, timezone
import json

import pytest

from .test_envelope import generate_transaction_item

TEST_CONFIG = {
    "aggregator": {"bucket_interval": 1, "initial_delay": 0, "debounce_delay": 0,}
}


def _session_payload(timestamp: datetime, started: datetime):
    return {
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
    }


def metrics_by_name(metrics_consumer, count, timeout=None):
    metrics = {
        metric["name"]: metric
        for metric in [metrics_consumer.get_metric(timeout) for _ in range(count)]
    }

    metrics_consumer.assert_empty()
    return metrics


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

    received_metrics = json.loads(metrics_item.get_bytes().decode())
    received_metrics = sorted(received_metrics, key=lambda x: x["name"])
    assert received_metrics == [
        {"timestamp": timestamp, "name": "bar", "value": 17.0, "type": "c"},
        {"timestamp": timestamp, "name": "foo", "value": 42.0, "type": "c"},
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


def test_metrics_with_processing(mini_sentry, relay_with_processing, metrics_consumer):
    relay = relay_with_processing(options=TEST_CONFIG)
    metrics_consumer = metrics_consumer()

    project_id = 42
    mini_sentry.add_full_project_config(project_id)

    timestamp = int(datetime.now(tz=timezone.utc).timestamp())
    metrics_payload = f"foo:42|c\nbar@s:17|c"
    relay.send_metrics(project_id, metrics_payload, timestamp)

    metrics = metrics_by_name(metrics_consumer, 2)

    assert metrics["foo"] == {
        "org_id": 1,
        "project_id": project_id,
        "name": "foo",
        "unit": "",
        "value": 42.0,
        "type": "c",
        "timestamp": timestamp,
    }

    assert metrics["bar"] == {
        "org_id": 1,
        "project_id": project_id,
        "name": "bar",
        "unit": "s",
        "value": 17.0,
        "type": "c",
        "timestamp": timestamp,
    }


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
        "name": "foo",
        "unit": "",
        "value": 15.0,
        "type": "c",
    }

    metrics_consumer.assert_empty()


@pytest.mark.parametrize(
    "extract_metrics", [True, False], ids=["extract", "don't extract"]
)
@pytest.mark.parametrize(
    "metrics_extracted", [True, False], ids=["extracted", "not extracted"]
)
def test_session_metrics_non_processing(
    mini_sentry, relay, extract_metrics, metrics_extracted
):
    """
        Tests metrics extraction in  a non processing relay

        If and only if the metrics-extraction feature is enabled and the metrics from the session were not already
        extracted the relay should extract the metrics from the session and mark the session item as "metrics extracted"
    """

    relay = relay(mini_sentry, options=TEST_CONFIG)

    if extract_metrics:
        # enable metrics extraction for the project
        extra_config = {"config": {"features": ["organizations:metrics-extraction"]}}
    else:
        extra_config = {}

    project_id = 42
    mini_sentry.add_basic_project_config(project_id, extra=extra_config)

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(hours=1)
    session_payload = _session_payload(timestamp=timestamp, started=started)

    relay.send_session(
        project_id,
        session_payload,
        item_headers={"metrics_extracted": metrics_extracted},
    )

    # Get session envelope
    first_envelope = mini_sentry.captured_events.get(timeout=2)

    try:
        second_envelope = mini_sentry.captured_events.get(timeout=2)
    except:
        second_envelope = None

    assert first_envelope is not None
    assert len(first_envelope.items) == 1
    first_item = first_envelope.items[0]

    if extract_metrics and not metrics_extracted:
        # here we have not yet extracted metrics and metric extraction is enabled
        # we expect to have two messages a session message and a metrics message
        assert second_envelope is not None
        assert len(second_envelope.items) == 1

        second_item = second_envelope.items[0]

        if first_item.type == "session":
            session_item = first_item
            metrics_item = second_item
        else:
            session_item = second_item
            metrics_item = first_item

        # check the metrics item
        assert metrics_item.type == "metric_buckets"

        session_metrics = json.loads(metrics_item.get_bytes().decode())
        session_metrics = sorted(session_metrics, key=lambda x: x["name"])

        ts = int(timestamp.timestamp())
        assert session_metrics == [
            {
                "name": "session",
                "tags": {
                    "environment": "production",
                    "release": "sentry-test@1.0.0",
                    "session.status": "init",
                },
                "timestamp": ts,
                "type": "c",
                "value": 1.0,
            },
            {
                "name": "session.duration",
                "tags": {"environment": "production", "release": "sentry-test@1.0.0"},
                "timestamp": ts,
                "type": "d",
                "unit": "s",
                "value": [1947.49],
            },
            {
                "name": "user",
                "tags": {
                    "environment": "production",
                    "release": "sentry-test@1.0.0",
                    "session.status": "init",
                },
                "timestamp": ts,
                "type": "s",
                "value": [1617781333],
            },
        ]
    else:
        # either the metrics are already extracted or we have metric extraction disabled
        # only the session message should be present
        assert second_envelope is None
        session_item = first_item

    assert session_item is not None
    assert session_item.type == "session"

    # we have marked the item as "metrics extracted" properly
    # already extracted metrics should keep the flag, newly extracted metrics should set the flag
    assert (
        session_item.headers.get("metrics_extracted", False) is extract_metrics
        or metrics_extracted
    )


def test_metrics_extracted_only_once(
    mini_sentry, relay, relay_with_processing, metrics_consumer
):
    """
    Tests that a chain of multiple relays only extracts metrics once

    Create a chain with 3 relays (all with metric extraction), and check that only the first
    relay does the extraction and the following relays just pass the metrics through
    """

    relay_chain = relay(
        relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG),
        options=TEST_CONFIG,
    )

    # enable metrics extraction for the project
    extra_config = {"config": {"features": ["organizations:metrics-extraction"]}}

    project_id = 42
    mini_sentry.add_full_project_config(project_id, extra=extra_config)

    metrics_consumer = metrics_consumer()

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(hours=1)
    session_payload = _session_payload(timestamp=timestamp, started=started)

    relay_chain.send_session(project_id, session_payload)

    metrics = metrics_by_name(metrics_consumer, 3, timeout=6)

    # if it is not 1 it means the session was extracted multiple times
    assert metrics["session"]["value"] == 1.0

    # if the vector contains multiple duration we have the session extracted multiple times
    assert len(metrics["session.duration"]["value"]) == 1


@pytest.mark.parametrize(
    "metrics_extracted", [True, False], ids=["extracted", "not extracted"]
)
def test_session_metrics_processing(
    mini_sentry, relay_with_processing, metrics_consumer, metrics_extracted
):
    """
        Tests that a processing relay with metrics-extraction enabled creates metrics
        from sessions if the metrics were not already extracted before.
    """
    relay = relay_with_processing(options=TEST_CONFIG)
    project_id = 42

    # enable metrics extraction for the project
    extra_config = {"config": {"features": ["organizations:metrics-extraction"]}}

    mini_sentry.add_full_project_config(project_id, extra=extra_config)

    metrics_consumer = metrics_consumer()

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(hours=1)
    session_payload = _session_payload(timestamp=timestamp, started=started)

    relay.send_session(project_id, session_payload)

    metrics = metrics_by_name(metrics_consumer, 3)

    assert metrics["session"] == {
        "org_id": 1,
        "project_id": 42,
        "timestamp": int(timestamp.timestamp()),
        "name": "session",
        "type": "c",
        "unit": "",
        "value": 1.0,
        "tags": {
            "environment": "production",
            "release": "sentry-test@1.0.0",
            "session.status": "init",
        },
    }

    assert metrics["user"] == {
        "org_id": 1,
        "project_id": 42,
        "timestamp": int(timestamp.timestamp()),
        "name": "user",
        "type": "s",
        "unit": "",
        "value": [1617781333],
        "tags": {
            "environment": "production",
            "release": "sentry-test@1.0.0",
            "session.status": "init",
        },
    }

    assert metrics["session.duration"] == {
        "org_id": 1,
        "project_id": 42,
        "timestamp": int(timestamp.timestamp()),
        "name": "session.duration",
        "type": "d",
        "unit": "s",
        "value": [1947.49],
        "tags": {"environment": "production", "release": "sentry-test@1.0.0",},
    }


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
        transaction["breakdowns"] = {"breakdown1": {"baz": {"value": 1.4},}}

        relay.send_event(42, transaction)

        # Send another transaction:
        transaction["measurements"] = {
            "foo": {"value": 2.2},
        }
        transaction["breakdowns"] = {"breakdown1": {"baz": {"value": 2.4},}}
        relay.send_event(42, transaction)

        if not feature_enabled:
            message = metrics_consumer.poll(timeout=None)
            assert message is None, message.value()

            continue

        metrics = metrics_by_name(metrics_consumer, 3)

        assert metrics["measurement.foo"] == {
            "org_id": 1,
            "project_id": 42,
            "timestamp": int(timestamp.timestamp()),
            "name": "measurement.foo",
            "type": "d",
            "unit": "",
            "value": [1.2, 2.2],
        }

        assert metrics["measurement.bar"] == {
            "org_id": 1,
            "project_id": 42,
            "timestamp": int(timestamp.timestamp()),
            "name": "measurement.bar",
            "type": "d",
            "unit": "",
            "value": [1.3],
        }

        assert metrics["breakdown.breakdown1.baz"] == {
            "org_id": 1,
            "project_id": 42,
            "timestamp": int(timestamp.timestamp()),
            "name": "breakdown.breakdown1.baz",
            "type": "d",
            "unit": "",
            "value": [1.4, 2.4],
        }
