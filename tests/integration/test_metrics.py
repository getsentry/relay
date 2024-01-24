import hashlib
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import signal

import pytest
import requests
import queue

from .test_envelope import generate_transaction_item

TEST_CONFIG = {
    "aggregator": {
        "bucket_interval": 1,
        "initial_delay": 0,
        "debounce_delay": 0,
        "shift_key": "none",
    }
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
        "attrs": {"release": "sentry-test@1.0.0", "environment": "production"},
    }


def metrics_by_name(metrics_consumer, count, timeout=None):
    metrics = {"headers": {}}

    for _ in range(count):
        metric, metric_headers = metrics_consumer.get_metric(timeout)
        metrics[metric["name"]] = metric
        metrics["headers"][metric["name"]] = metric_headers

    metrics_consumer.assert_empty()
    return metrics


def metrics_by_name_group_by_project(metrics_consumer, timeout=None):
    """
    Return list of pairs metric name and metric dict
    it useful when you have different projects
    """
    metrics_by_project = defaultdict(dict)
    while True:
        try:
            metric, _ = metrics_consumer.get_metric(timeout)
            metrics_by_project[metric["project_id"]][metric["name"]] = metric
        except AssertionError:
            metrics_consumer.assert_empty()
            return metrics_by_project


def test_metrics(mini_sentry, relay):
    relay = relay(mini_sentry, options=TEST_CONFIG)

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    timestamp = int(datetime.now(tz=timezone.utc).timestamp())
    metrics_payload = f"transactions/foo:42|c\ntransactions/bar:17|c|T{timestamp}"
    relay.send_metrics(project_id, metrics_payload)

    envelope = mini_sentry.captured_events.get(timeout=3)
    assert len(envelope.items) == 1

    metrics_item = envelope.items[0]
    assert metrics_item.type == "metric_buckets"

    received_metrics = json.loads(metrics_item.get_bytes().decode())
    received_metrics = sorted(received_metrics, key=lambda x: x["name"])
    assert received_metrics == [
        {
            "timestamp": timestamp,
            "width": 1,
            "name": "c:transactions/bar@none",
            "value": 17.0,
            "type": "c",
        },
        {
            "timestamp": timestamp,
            "width": 1,
            "name": "c:transactions/foo@none",
            "value": 42.0,
            "type": "c",
        },
    ]


def test_metrics_backdated(mini_sentry, relay):
    relay = relay(mini_sentry, options=TEST_CONFIG)

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    timestamp = int(datetime.now(tz=timezone.utc).timestamp()) - 24 * 60 * 60
    metrics_payload = f"transactions/foo:42|c|T{timestamp}"
    relay.send_metrics(project_id, metrics_payload)

    envelope = mini_sentry.captured_events.get(timeout=2)
    assert len(envelope.items) == 1

    metrics_item = envelope.items[0]
    assert metrics_item.type == "metric_buckets"

    received_metrics = metrics_item.get_bytes()
    assert json.loads(received_metrics.decode()) == [
        {
            "timestamp": timestamp,
            "width": 1,
            "name": "c:transactions/foo@none",
            "value": 42.0,
            "type": "c",
        },
    ]


@pytest.mark.parametrize(
    "metrics_partitions,expected_header",
    [(None, None), (0, "0"), (1, "0"), (128, "34")],
)
def test_metrics_partition_key(mini_sentry, relay, metrics_partitions, expected_header):
    forever = 100 * 365 * 24 * 60 * 60  # *almost forever
    relay_config = {
        "processing": {
            "max_session_secs_in_past": forever,
        },
        "aggregator": {
            "bucket_interval": 1,
            "initial_delay": 0,
            "debounce_delay": 0,
            "max_secs_in_past": forever,
            "max_secs_in_future": forever,
            "flush_partitions": metrics_partitions,
            "shift_key": "none",
        },
    }
    relay = relay(mini_sentry, options=relay_config)

    project_id = 42
    mini_sentry.add_basic_project_config(
        project_id,
        dsn_public_key={  # Need explicit DSN to get a consistent partition key
            "publicKey": "31a5a894b4524f74a9a8d0e27e21ba91",
            "isEnabled": True,
            "numericId": 42,
        },
    )

    metrics_payload = "transactions/foo:42|c|T999994711"
    relay.send_metrics(project_id, metrics_payload)

    mini_sentry.captured_events.get(timeout=3)

    headers, _ = mini_sentry.request_log[-1]
    if expected_header is None:
        assert "X-Sentry-Relay-Shard" not in headers
    else:
        assert headers["X-Sentry-Relay-Shard"] == expected_header, headers


@pytest.mark.parametrize(
    "max_batch_size,expected_events", [(1000, 1), (200, 2), (130, 3), (100, 6), (50, 0)]
)
def test_metrics_max_batch_size(mini_sentry, relay, max_batch_size, expected_events):
    forever = 100 * 365 * 24 * 60 * 60  # *almost forever
    relay_config = {
        "processing": {
            "max_session_secs_in_past": forever,
        },
        "aggregator": {
            "bucket_interval": 1,
            "initial_delay": 0,
            "debounce_delay": 0,
            "max_secs_in_past": forever,
            "max_secs_in_future": forever,
            "max_flush_bytes": max_batch_size,
        },
    }
    relay = relay(mini_sentry, options=relay_config)

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    metrics_payload = (
        "transactions/foo:1:2:3:4:5:6:7:8:9:10:11:12:13:14:15:16:17|d|T999994711"
    )
    relay.send_metrics(project_id, metrics_payload)

    for _ in range(expected_events):
        mini_sentry.captured_events.get(timeout=3)

    with pytest.raises(queue.Empty):
        mini_sentry.captured_events.get(timeout=1)


def test_global_metrics(mini_sentry, relay):
    relay = relay(
        mini_sentry, options={"http": {"global_metrics": True}, **TEST_CONFIG}
    )

    project_id = 42
    config = mini_sentry.add_basic_project_config(project_id)
    public_key = config["publicKeys"][0]["publicKey"]

    timestamp = int(datetime.now(tz=timezone.utc).timestamp())
    metrics_payload = f"transactions/foo:42|c\ntransactions/bar:17|c|T{timestamp}"
    relay.send_metrics(project_id, metrics_payload)

    metrics_batch = mini_sentry.captured_metrics.get(timeout=5)
    assert mini_sentry.captured_metrics.qsize() == 0  # we had only one batch

    metrics = sorted(metrics_batch[public_key], key=lambda x: x["name"])

    assert metrics == [
        {
            "timestamp": timestamp,
            "width": 1,
            "name": "c:transactions/bar@none",
            "value": 17.0,
            "type": "c",
        },
        {
            "timestamp": timestamp,
            "width": 1,
            "name": "c:transactions/foo@none",
            "value": 42.0,
            "type": "c",
        },
    ]


def test_global_metrics_no_config(mini_sentry, relay):
    relay = relay(mini_sentry, TEST_CONFIG)

    project_id = 42
    config = mini_sentry.add_basic_project_config(project_id)
    public_key = config["publicKeys"][0]["publicKey"]

    timestamp = int(datetime.now(tz=timezone.utc).timestamp())
    metrics = [
        {
            "timestamp": timestamp,
            "width": 1,
            "name": "c:transactions/foo@none",
            "value": 17.0,
            "type": "c",
        }
    ]
    relay.send_metrics_batch(
        {"buckets": {public_key: metrics}},
    )

    envelope = mini_sentry.captured_events.get(timeout=3)
    item = envelope.items[0]
    assert item.headers["type"] == "metric_buckets"
    metrics_batch = json.loads(item.payload.get_bytes())
    received_metrics = sorted(metrics_batch, key=lambda x: x["name"])

    assert received_metrics == metrics


def test_global_metrics_batching(mini_sentry, relay):
    # See `test_metrics_max_batch_size`: 200 should lead to 2 batches
    MAX_FLUSH_SIZE = 200

    relay = relay(
        mini_sentry,
        options={
            "http": {"global_metrics": True},
            "limits": {"max_concurrent_requests": 1},  # deterministic submission order
            "aggregator": {
                "bucket_interval": 1,
                "initial_delay": 0,
                "debounce_delay": 0,
                "max_flush_bytes": MAX_FLUSH_SIZE,
            },
        },
    )

    project_id = 42
    config = mini_sentry.add_basic_project_config(project_id)
    public_key = config["publicKeys"][0]["publicKey"]

    timestamp = int(datetime.now(tz=timezone.utc).timestamp())
    metrics_payload = (
        f"transactions/foo:1:2:3:4:5:6:7:8:9:10:11:12:13:14:15:16:17|d|T{timestamp}"
    )
    relay.send_metrics(project_id, metrics_payload)

    batch1 = mini_sentry.captured_metrics.get(timeout=5)
    batch2 = mini_sentry.captured_metrics.get(timeout=1)
    with pytest.raises(queue.Empty):
        mini_sentry.captured_metrics.get(timeout=1)

    assert batch1[public_key] == [
        {
            "timestamp": timestamp,
            "width": 1,
            "name": "d:transactions/foo@none",
            "value": [float(i) for i in range(1, 16)],
            "type": "d",
        }
    ]

    assert batch2[public_key] == [
        {
            "timestamp": timestamp,
            "width": 1,
            "name": "d:transactions/foo@none",
            "value": [16.0, 17.0],
            "type": "d",
        }
    ]


def test_metrics_with_processing(mini_sentry, relay_with_processing, metrics_consumer):
    relay = relay_with_processing(options=TEST_CONFIG)
    metrics_consumer = metrics_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = ["organizations:custom-metrics"]

    timestamp = int(datetime.now(tz=timezone.utc).timestamp())
    metrics_payload = f"transactions/foo:42|c\nbar@second:17|c|T{timestamp}"
    relay.send_metrics(project_id, metrics_payload)

    metrics = metrics_by_name(metrics_consumer, 2)

    assert metrics["headers"]["c:transactions/foo@none"] == [
        ("namespace", b"transactions")
    ]
    assert metrics["c:transactions/foo@none"] == {
        "org_id": 1,
        "project_id": project_id,
        "retention_days": 90,
        "name": "c:transactions/foo@none",
        "tags": {},
        "value": 42.0,
        "type": "c",
        "timestamp": timestamp,
    }

    assert metrics["headers"]["c:custom/bar@second"] == [("namespace", b"custom")]
    assert metrics["c:custom/bar@second"] == {
        "org_id": 1,
        "project_id": project_id,
        "retention_days": 90,
        "name": "c:custom/bar@second",
        "tags": {},
        "value": 17.0,
        "type": "c",
        "timestamp": timestamp,
    }


def test_global_metrics_with_processing(
    mini_sentry, relay, relay_with_processing, metrics_consumer
):
    # Set up a relay chain where the outer relay has global metrics enabled
    # and forwards to a processing Relay.
    processing_relay = relay_with_processing(options=TEST_CONFIG)
    relay = relay(
        processing_relay, options={"http": {"global_metrics": True}, **TEST_CONFIG}
    )

    metrics_consumer = metrics_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = ["organizations:custom-metrics"]

    timestamp = int(datetime.now(tz=timezone.utc).timestamp())
    metrics_payload = f"transactions/foo:42|c\nbar@second:17|c|T{timestamp}"
    relay.send_metrics(project_id, metrics_payload)

    metrics = metrics_by_name(metrics_consumer, 2)

    assert metrics["headers"]["c:transactions/foo@none"] == [
        ("namespace", b"transactions")
    ]
    assert metrics["c:transactions/foo@none"] == {
        "org_id": 1,
        "project_id": project_id,
        "retention_days": 90,
        "name": "c:transactions/foo@none",
        "tags": {},
        "value": 42.0,
        "type": "c",
        "timestamp": timestamp,
    }

    assert metrics["headers"]["c:custom/bar@second"] == [("namespace", b"custom")]
    assert metrics["c:custom/bar@second"] == {
        "org_id": 1,
        "project_id": project_id,
        "retention_days": 90,
        "name": "c:custom/bar@second",
        "tags": {},
        "value": 17.0,
        "type": "c",
        "timestamp": timestamp,
    }


def test_metrics_with_sharded_kafka(
    get_topic_name,
    mini_sentry,
    relay_with_processing,
    metrics_consumer,
    processing_config,
):
    options = processing_config(None)
    default_config = options["processing"]["kafka_config"]
    config = {
        "processing": {
            "secondary_kafka_configs": {"foo": default_config, "baz": default_config},
            "topics": {
                "metrics_generic": {
                    "shards": 3,
                    "mapping": {
                        0: {
                            "name": get_topic_name("metrics-1"),
                            "config": "foo",
                        },
                        2: {
                            "name": get_topic_name("metrics-2"),
                            "config": "baz",
                        },
                    },
                }
            },
        }
    }
    relay = relay_with_processing(options={**TEST_CONFIG, **config})

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["organizationId"] = 5
    m1 = metrics_consumer(topic="metrics-1")
    m2 = metrics_consumer(topic="metrics-2")

    timestamp = int(datetime.now(tz=timezone.utc).timestamp())
    metrics_payload = (
        f"transactions/foo:42|c\ntransactions/bar@second:17|c|T{timestamp}"
    )
    relay.send_metrics(project_id, metrics_payload)

    # There must be no messages in the metrics-1, since the shard was not picked up
    m1.assert_empty()

    metrics2 = metrics_by_name(m2, 2)
    assert metrics2["c:transactions/foo@none"] == {
        "org_id": 5,
        "project_id": project_id,
        "retention_days": 90,
        "name": "c:transactions/foo@none",
        "tags": {},
        "value": 42.0,
        "type": "c",
        "timestamp": timestamp,
    }

    assert metrics2["c:transactions/bar@second"] == {
        "org_id": 5,
        "project_id": project_id,
        "retention_days": 90,
        "name": "c:transactions/bar@second",
        "tags": {},
        "value": 17.0,
        "type": "c",
        "timestamp": timestamp,
    }


def test_metrics_full(mini_sentry, relay, relay_with_processing, metrics_consumer):
    metrics_consumer = metrics_consumer()

    upstream_config = {
        "aggregator": {
            "bucket_interval": 1,
            # Give upstream some time to process downstream entries:
            "initial_delay": 2,
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
    downstream.send_metrics(project_id, f"transactions/foo:7|c|T{timestamp}")
    downstream.send_metrics(project_id, f"transactions/foo:5|c|T{timestamp}")

    upstream.send_metrics(project_id, f"transactions/foo:3|c|T{timestamp}")

    metric, _ = metrics_consumer.get_metric(timeout=6)
    metric.pop("timestamp")
    assert metric == {
        "org_id": 1,
        "project_id": project_id,
        "retention_days": 90,
        "name": "c:transactions/foo@none",
        "tags": {},
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
        extra_config = {"config": {"sessionMetrics": {"version": 1}}}
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
    except Exception:
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

        ts = int(started.timestamp())
        assert session_metrics == [
            {
                "name": "c:sessions/session@none",
                "tags": {
                    "sdk": "raven-node/2.6.3",
                    "environment": "production",
                    "release": "sentry-test@1.0.0",
                    "session.status": "init",
                },
                "timestamp": ts,
                "width": 1,
                "type": "c",
                "value": 1.0,
            },
            {
                "name": "s:sessions/user@none",
                "tags": {
                    "sdk": "raven-node/2.6.3",
                    "environment": "production",
                    "release": "sentry-test@1.0.0",
                },
                "timestamp": ts,
                "width": 1,
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


def test_session_metrics_extracted_only_once(
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
    extra_config = {"config": {"sessionMetrics": {"version": 1}}}

    project_id = 42
    mini_sentry.add_full_project_config(project_id, extra=extra_config)

    metrics_consumer = metrics_consumer()

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(hours=1)
    session_payload = _session_payload(timestamp=timestamp, started=started)

    relay_chain.send_session(project_id, session_payload)

    metrics = metrics_by_name(metrics_consumer, 2, timeout=6)

    # if it is not 1 it means the session was extracted multiple times
    assert metrics["c:sessions/session@none"]["value"] == 1.0
    assert metrics["headers"]["c:sessions/session@none"] == [("namespace", b"sessions")]


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
    extra_config = {"config": {"sessionMetrics": {"version": 1}}}

    mini_sentry.add_full_project_config(project_id, extra=extra_config)

    metrics_consumer = metrics_consumer()

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(hours=1)
    session_payload = _session_payload(timestamp=timestamp, started=started)

    relay.send_session(
        project_id,
        session_payload,
        item_headers={"metrics_extracted": metrics_extracted},
    )

    if metrics_extracted:
        metrics_consumer.assert_empty(timeout=2)
        return

    metrics = metrics_by_name(metrics_consumer, 2)

    expected_timestamp = int(started.timestamp())
    assert metrics["c:sessions/session@none"] == {
        "org_id": 1,
        "project_id": 42,
        "retention_days": 90,
        "timestamp": expected_timestamp,
        "name": "c:sessions/session@none",
        "type": "c",
        "value": 1.0,
        "tags": {
            "sdk": "raven-node/2.6.3",
            "environment": "production",
            "release": "sentry-test@1.0.0",
            "session.status": "init",
        },
    }

    assert metrics["s:sessions/user@none"] == {
        "org_id": 1,
        "project_id": 42,
        "retention_days": 90,
        "timestamp": expected_timestamp,
        "name": "s:sessions/user@none",
        "type": "s",
        "value": [1617781333],
        "tags": {
            "sdk": "raven-node/2.6.3",
            "environment": "production",
            "release": "sentry-test@1.0.0",
        },
    }


@pytest.mark.parametrize(
    "extract_metrics,discard_data,with_external_relay",
    [
        (True, "transaction", True),
        (True, "transaction", False),
        (True, "trace", False),
        (True, False, False),
        (False, "transaction", False),
        (False, False, False),
        (False, False, True),
        ("corrupted", "transaction", False),
    ],
    ids=[
        "extract from transaction-sampled, external relay",
        "extract from transaction-sampled",
        "extract from trace-sampled",
        "extract from unsampled",
        "don't extract from transaction-sampled",
        "don't extract from unsampled",
        "don't extract from unsampled, external relay",
        "corrupted config",
    ],
)
def test_transaction_metrics(
    mini_sentry,
    relay,
    relay_with_processing,
    metrics_consumer,
    extract_metrics,
    discard_data,
    transactions_consumer,
    with_external_relay,
):
    metrics_consumer = metrics_consumer()
    transactions_consumer = transactions_consumer()

    if with_external_relay:
        relay = relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG)
    else:
        relay = relay_with_processing(options=TEST_CONFIG)

    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    config = mini_sentry.project_configs[project_id]["config"]

    timestamp = datetime.now(tz=timezone.utc)

    if extract_metrics:
        config["sessionMetrics"] = {"version": 1}
    config["breakdownsV2"] = {
        "span_ops": {"type": "spanOperations", "matches": ["react.mount"]}
    }

    if discard_data:
        # Make sure Relay drops the transaction
        ds = config.setdefault("sampling", {})
        ds.setdefault("version", 2)
        ds.setdefault("rules", []).append(
            {
                "samplingValue": {"type": "sampleRate", "value": 0.0},
                "type": discard_data,
                "condition": {"op": "and", "inner": []},
                "id": 1,
            }
        )

    if extract_metrics == "corrupted":
        config["transactionMetrics"] = 42

    elif extract_metrics:
        config["transactionMetrics"] = {
            "version": 1,
        }

    transaction = generate_transaction_item()
    transaction["timestamp"] = timestamp.isoformat()
    transaction["measurements"] = {
        "foo": {"value": 1.2},
        "bar": {"value": 1.3},
    }

    trace_info = {
        "trace_id": transaction["contexts"]["trace"]["trace_id"],
        "public_key": mini_sentry.get_dsn_public_key(project_id),
        "transaction": "transaction_which_starts_trace",
    }
    relay.send_transaction(42, transaction, trace_info=trace_info)

    # Send another transaction:
    transaction["measurements"] = {
        "foo": {"value": 2.2},
    }
    relay.send_transaction(42, transaction, trace_info=trace_info)

    if discard_data:
        transactions_consumer.assert_empty()
    else:
        event, _ = transactions_consumer.get_event()
        if with_external_relay:
            # there is some rounding error while serializing/deserializing
            # timestamps... haven't investigated too closely
            span_time = 9.910107
        else:
            span_time = 9.910106

        assert event["breakdowns"] == {
            "span_ops": {
                "ops.react.mount": {"value": span_time, "unit": "millisecond"},
                "total.time": {"value": span_time, "unit": "millisecond"},
            }
        }

    if not extract_metrics or extract_metrics == "corrupted":
        message = metrics_consumer.poll(timeout=None)
        assert message is None, message.value()

        return

    metrics = metrics_by_name(metrics_consumer, 7)
    common = {
        "timestamp": int(timestamp.timestamp()),
        "org_id": 1,
        "project_id": 42,
        "retention_days": 90,
        "tags": {
            "transaction": "/organizations/:orgId/performance/:eventSlug/",
            "platform": "other",
            "transaction.status": "unknown",
        },
    }

    assert metrics["c:transactions/usage@none"] == {
        **common,
        "name": "c:transactions/usage@none",
        "type": "c",
        "value": 2.0,
        "tags": {},
    }

    metrics["d:transactions/measurements.foo@none"]["value"].sort()
    assert metrics["d:transactions/measurements.foo@none"] == {
        **common,
        "name": "d:transactions/measurements.foo@none",
        "type": "d",
        "value": [1.2, 2.2],
    }

    assert metrics["d:transactions/measurements.bar@none"] == {
        **common,
        "name": "d:transactions/measurements.bar@none",
        "type": "d",
        "value": [1.3],
    }

    assert metrics[
        "d:transactions/breakdowns.span_ops.ops.react.mount@millisecond"
    ] == {
        **common,
        "name": "d:transactions/breakdowns.span_ops.ops.react.mount@millisecond",
        "type": "d",
        "value": [9.910106, 9.910106],
    }
    assert metrics["c:transactions/count_per_root_project@none"] == {
        "timestamp": int(timestamp.timestamp()),
        "org_id": 1,
        "project_id": 42,
        "retention_days": 90,
        "tags": {
            "decision": "drop" if discard_data else "keep",
            "transaction": "transaction_which_starts_trace",
        },
        "name": "c:transactions/count_per_root_project@none",
        "type": "c",
        "value": 2.0,
    }


def test_transaction_metrics_count_per_root_project(
    mini_sentry,
    relay,
    relay_with_processing,
    metrics_consumer,
    transactions_consumer,
):
    metrics_consumer = metrics_consumer()
    transactions_consumer = transactions_consumer()

    relay = relay_with_processing(options=TEST_CONFIG)

    project_id = 42
    mini_sentry.add_full_project_config(41)
    mini_sentry.add_full_project_config(project_id)
    timestamp = datetime.now(tz=timezone.utc)

    for project_id in (41, 42):
        config = mini_sentry.project_configs[project_id]["config"]
        config["sessionMetrics"] = {"version": 1}
        config["breakdownsV2"] = {
            "span_ops": {"type": "spanOperations", "matches": ["react.mount"]}
        }
        config["transactionMetrics"] = {
            "version": 1,
        }

    transaction = generate_transaction_item()
    transaction["timestamp"] = timestamp.isoformat()
    transaction["measurements"] = {
        "foo": {"value": 1.2},
        "bar": {"value": 1.3},
    }

    trace_info = {
        "trace_id": transaction["contexts"]["trace"]["trace_id"],
        "public_key": mini_sentry.get_dsn_public_key(41),
        "transaction": "test",
    }
    relay.send_transaction(42, transaction, trace_info=trace_info)

    transaction = generate_transaction_item()
    transaction["timestamp"] = timestamp.isoformat()
    transaction["measurements"] = {
        "test": {"value": 1.2},
    }
    relay.send_transaction(42, transaction)
    relay.send_transaction(42, transaction)

    event, _ = transactions_consumer.get_event()

    metrics_by_project = metrics_by_name_group_by_project(metrics_consumer, timeout=4)

    assert metrics_by_project[41]["c:transactions/count_per_root_project@none"] == {
        "timestamp": int(timestamp.timestamp()),
        "org_id": 1,
        "project_id": 41,
        "retention_days": 90,
        "tags": {"decision": "keep", "transaction": "test"},
        "name": "c:transactions/count_per_root_project@none",
        "type": "c",
        "value": 1.0,
    }
    assert metrics_by_project[42]["c:transactions/count_per_root_project@none"] == {
        "timestamp": int(timestamp.timestamp()),
        "org_id": 1,
        "project_id": 42,
        "retention_days": 90,
        "tags": {"decision": "keep"},
        "name": "c:transactions/count_per_root_project@none",
        "type": "c",
        "value": 2.0,
    }


@pytest.mark.parametrize(
    "send_extracted_header,expect_metrics_extraction",
    [(False, True), (True, False)],
    ids=["must extract metrics", "mustn't extract metrics"],
)
def test_transaction_metrics_extraction_external_relays(
    mini_sentry,
    relay,
    send_extracted_header,
    expect_metrics_extraction,
):
    if send_extracted_header:
        item_headers = {"metrics_extracted": True}
    else:
        item_headers = None

    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    config = mini_sentry.project_configs[project_id]["config"]
    config["transactionMetrics"] = {"version": 3}
    config["sampling"] = {
        "version": 2,
        "rules": [
            {
                "id": 1,
                "samplingValue": {"type": "sampleRate", "value": 0.0},
                "type": "transaction",
                "condition": {"op": "and", "inner": []},
            }
        ],
    }

    tx = generate_transaction_item()
    # Default timestamp is so old that relay drops metrics, setting a more recent one avoids the drop.
    timestamp = datetime.now(tz=timezone.utc)
    tx["timestamp"] = timestamp.isoformat()

    external = relay(mini_sentry, options=TEST_CONFIG)

    trace_info = {
        "trace_id": tx["contexts"]["trace"]["trace_id"],
        "public_key": mini_sentry.get_dsn_public_key(project_id),
        "transaction": "root_transaction",
    }
    external.send_transaction(project_id, tx, item_headers, trace_info)

    envelope = mini_sentry.captured_events.get(timeout=3)
    assert len(envelope.items) == 1

    if expect_metrics_extraction:
        metrics_envelope = mini_sentry.captured_events.get(timeout=3)
        assert len(metrics_envelope.items) == 1

        payload = json.loads(metrics_envelope.items[0].get_bytes().decode())
        assert len(payload) == 4

        by_name = {m["name"]: m for m in payload}
        light_metric = by_name["d:transactions/duration_light@millisecond"]
        assert (
            light_metric["tags"]["transaction"]
            == "/organizations/:orgId/performance/:eventSlug/"
        )
        duration_metric = by_name["d:transactions/duration@millisecond"]
        assert (
            duration_metric["tags"]["transaction"]
            == "/organizations/:orgId/performance/:eventSlug/"
        )
        count_metric = by_name["c:transactions/count_per_root_project@none"]
        assert count_metric["tags"]["transaction"] == "root_transaction"
        assert count_metric["value"] == 1.0
        usage_metric = by_name["c:transactions/usage@none"]
        assert not usage_metric.get("tags")  # empty or missing
        assert usage_metric["value"] == 1.0

    assert mini_sentry.captured_events.empty()


@pytest.mark.parametrize(
    "send_extracted_header,expect_metrics_extraction",
    [(False, True), (True, False)],
    ids=["must extract metrics", "mustn't extract metrics"],
)
def test_transaction_metrics_extraction_processing_relays(
    transactions_consumer,
    metrics_consumer,
    mini_sentry,
    relay_with_processing,
    send_extracted_header,
    expect_metrics_extraction,
):
    if send_extracted_header:
        item_headers = {"metrics_extracted": True}
    else:
        item_headers = None

    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    config = mini_sentry.project_configs[project_id]["config"]
    config["transactionMetrics"] = {
        "version": 1,
    }

    tx = generate_transaction_item()
    # Default timestamp is so old that relay drops metrics, setting a more recent one avoids the drop.
    timestamp = datetime.now(tz=timezone.utc)
    tx["timestamp"] = timestamp.isoformat()

    metrics_consumer = metrics_consumer()
    tx_consumer = transactions_consumer()
    processing = relay_with_processing(options=TEST_CONFIG)
    processing.send_transaction(project_id, tx, item_headers)

    tx, _ = tx_consumer.get_event()
    assert tx["transaction"] == "/organizations/:orgId/performance/:eventSlug/"
    tx_consumer.assert_empty()

    if expect_metrics_extraction:
        metrics = metrics_by_name(metrics_consumer, 4, timeout=3)
        metric_usage = metrics["c:transactions/usage@none"]
        assert metric_usage["tags"] == {}
        assert metric_usage["value"] == 1.0
        metric_duration = metrics["d:transactions/duration@millisecond"]
        assert (
            metric_duration["tags"]["transaction"]
            == "/organizations/:orgId/performance/:eventSlug/"
        )
        metric_duration_light = metrics["d:transactions/duration_light@millisecond"]
        assert (
            metric_duration_light["tags"]["transaction"]
            == "/organizations/:orgId/performance/:eventSlug/"
        )
        metric_count_per_project = metrics["c:transactions/count_per_root_project@none"]
        assert metric_count_per_project["value"] == 1.0

    metrics_consumer.assert_empty()


@pytest.mark.parametrize(
    "unsupported_version",
    [0, 1234567890],
    ids=["version is too small", "version is too big"],
)
def test_transaction_metrics_not_extracted_on_unsupported_version(
    metrics_consumer,
    transactions_consumer,
    mini_sentry,
    relay_with_processing,
    unsupported_version,
):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    config = mini_sentry.project_configs[project_id]["config"]
    config["transactionMetrics"] = {
        "version": unsupported_version,
    }

    tx = generate_transaction_item()
    # Default timestamp is so old that relay drops metrics, setting a more recent one avoids the drop.
    timestamp = datetime.now(tz=timezone.utc)
    tx["timestamp"] = timestamp.isoformat()

    metrics_consumer = metrics_consumer()
    tx_consumer = transactions_consumer()

    relay = relay_with_processing(options=TEST_CONFIG)
    relay.send_transaction(project_id, tx)

    tx, _ = tx_consumer.get_event()
    assert tx["transaction"] == "/organizations/:orgId/performance/:eventSlug/"
    tx_consumer.assert_empty()

    metrics_consumer.assert_empty()


def test_no_transaction_metrics_when_filtered(mini_sentry, relay):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    config = mini_sentry.project_configs[project_id]["config"]
    config["transactionMetrics"] = {
        "version": 1,
    }
    config["filterSettings"]["releases"] = {"releases": ["foo@1.2.4"]}

    tx = generate_transaction_item()
    tx["release"] = "foo@1.2.4"
    # Default timestamp is so old that relay drops metrics, setting a more recent one avoids the drop.
    timestamp = datetime.now(tz=timezone.utc)
    tx["timestamp"] = timestamp.isoformat()

    relay = relay(mini_sentry, options=TEST_CONFIG)
    relay.send_transaction(project_id, tx)

    # The only envelope received should be outcomes:
    envelope = mini_sentry.captured_events.get(timeout=3)
    assert {item.type for item in envelope.items} == {"client_report"}

    assert mini_sentry.captured_events.qsize() == 0


def test_transaction_name_too_long(
    transactions_consumer,
    metrics_consumer,
    mini_sentry,
    relay_with_processing,
):
    """When a transaction name is truncated, the transaction metric should get the truncated value"""
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    config = mini_sentry.project_configs[project_id]["config"]
    config["transactionMetrics"] = {
        "version": 1,
    }

    transaction = {
        "event_id": "d2132d31b39445f1938d7e21b6bf0ec4",
        "type": "transaction",
        "transaction": 201 * "x",
        "start_timestamp": 1597976392.6542819,
        "contexts": {
            "trace": {
                "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
                "span_id": "FA90FDEAD5F74052",
            }
        },
    }
    timestamp = datetime.now(tz=timezone.utc)
    transaction["timestamp"] = timestamp.isoformat()

    metrics_consumer = metrics_consumer()
    tx_consumer = transactions_consumer()
    processing = relay_with_processing(options=TEST_CONFIG)
    processing.send_transaction(project_id, transaction)

    expected_transaction_name = 197 * "x" + "..."

    transaction, _ = tx_consumer.get_event()
    assert transaction["transaction"] == expected_transaction_name

    metrics = metrics_consumer.get_metrics()
    for metric, _ in metrics:
        if "transaction" in metric["tags"]:
            assert metric["tags"]["transaction"] == expected_transaction_name


def test_graceful_shutdown(mini_sentry, relay):
    relay = relay(
        mini_sentry,
        options={
            "limits": {"shutdown_timeout": 2},
            "aggregator": {
                "bucket_interval": 1,
                "initial_delay": 100,
                "debounce_delay": 0,
                "shift_key": "none",
            },
        },
    )

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    timestamp = int(datetime.now(tz=timezone.utc).timestamp())

    # Backdated metric will be flushed immediately due to debounce delay
    past_timestamp = timestamp - 1000
    metrics_payload = f"transactions/past:42|c|T{past_timestamp}"
    relay.send_metrics(project_id, metrics_payload)

    # Future timestamp will not be flushed regularly, only through force flush
    future_timestamp = timestamp + 30
    metrics_payload = f"transactions/future:17|c|T{future_timestamp}"
    relay.send_metrics(project_id, metrics_payload)
    relay.process.send_signal(signal.SIGTERM)

    # Try to send another metric (will be rejected)
    metrics_payload = f"transactions/now:666|c|T{timestamp}"
    with pytest.raises(requests.ConnectionError):
        relay.send_metrics(project_id, metrics_payload)

    envelope = mini_sentry.captured_events.get(timeout=5)
    assert len(envelope.items) == 1
    metrics_item = envelope.items[0]
    assert metrics_item.type == "metric_buckets"
    received_metrics = json.loads(metrics_item.get_bytes().decode())
    received_metrics = sorted(received_metrics, key=lambda x: x["name"])
    assert received_metrics == [
        {
            "timestamp": future_timestamp,
            "width": 1,
            "name": "c:transactions/future@none",
            "value": 17.0,
            "type": "c",
        },
        {
            "timestamp": past_timestamp,
            "width": 1,
            "name": "c:transactions/past@none",
            "value": 42.0,
            "type": "c",
        },
    ]


def test_limit_custom_measurements(
    mini_sentry, relay, relay_with_processing, metrics_consumer, transactions_consumer
):
    """Custom measurement config is propagated to outer relay"""
    metrics_consumer = metrics_consumer()
    transactions_consumer = transactions_consumer()

    relay = relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG)

    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    config = mini_sentry.project_configs[project_id]["config"]
    timestamp = datetime.now(tz=timezone.utc)

    config["measurements"] = {
        "builtinMeasurements": [{"name": "foo", "unit": "none"}],
        "maxCustomMeasurements": 1,
    }
    config["transactionMetrics"] = {
        "version": 1,
    }

    transaction = generate_transaction_item()
    transaction["timestamp"] = timestamp.isoformat()
    transaction["measurements"] = {
        "foo": {"value": 1.2},
        "baz": {
            "value": 1.3
        },  # baz comes before bar, but custom measurements are picked in alphabetical order
        "bar": {"value": 1.4},
    }

    relay.send_transaction(42, transaction)

    event, _ = transactions_consumer.get_event()
    assert len(event["measurements"]) == 2

    # Expect exactly 5 metrics:
    # (transaction.duration, transaction.duration_light, transactions.count_per_root_project, 1 builtin, 1 custom)
    metrics = metrics_by_name(metrics_consumer, 6)
    metrics.pop("headers")

    assert metrics.keys() == {
        "c:transactions/usage@none",
        "d:transactions/duration@millisecond",
        "d:transactions/duration_light@millisecond",
        "c:transactions/count_per_root_project@none",
        "d:transactions/measurements.foo@none",
        "d:transactions/measurements.bar@none",
    }


@pytest.mark.parametrize(
    "sent_description, expected_description",
    [
        (
            "SELECT column FROM table1 WHERE another_col = %s",
            "SELECT column FROM table1 WHERE another_col = %s",
        ),
        (
            "SELECT column FROM table1 WHERE another_col = %s AND yet_another_col = something_very_longgggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
            "SELECT column FROM table1 WHERE another_col = %s AND yet_another_col = something_very_longggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg*",
        ),
    ],
    ids=["Must not truncate short descriptions", "Must truncate long descriptions"],
)
def test_span_metrics(
    transactions_consumer,
    metrics_consumer,
    mini_sentry,
    relay_with_processing,
    sent_description,
    expected_description,
):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    config = mini_sentry.project_configs[project_id]["config"]
    config["transactionMetrics"] = {
        "version": 1,
    }
    config.setdefault("features", []).append("projects:span-metrics-extraction")

    transaction = {
        "event_id": "d2132d31b39445f1938d7e21b6bf0ec4",
        "type": "transaction",
        "transaction": "/organizations/:orgId/performance/:eventSlug/",
        "transaction_info": {"source": "route"},
        "start_timestamp": 1597976392.6542819,
        "timestamp": 1597976400.6189718,
        "user": {"id": "user123", "geo": {"country_code": "ES"}},
        "contexts": {
            "trace": {
                "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
                "span_id": "FA90FDEAD5F74052",
                "type": "trace",
            }
        },
        "spans": [
            {
                "description": sent_description,
                "op": "db",
                "parent_span_id": "8f5a2b8768cafb4e",
                "span_id": "bd429c44b67a3eb4",
                "start_timestamp": 1597976393.4619668,
                "timestamp": 1597976393.4718769,
                "trace_id": "ff62a8b040f340bda5d830223def1d81",
            }
        ],
    }
    # Default timestamp is so old that relay drops metrics, setting a more recent one avoids the drop.
    timestamp = datetime.now(tz=timezone.utc)
    transaction["timestamp"] = transaction["spans"][0][
        "timestamp"
    ] = timestamp.isoformat()

    metrics_consumer = metrics_consumer()
    tx_consumer = transactions_consumer()
    processing = relay_with_processing(options=TEST_CONFIG)
    processing.send_transaction(project_id, transaction)

    transaction, _ = tx_consumer.get_event()
    assert transaction["spans"][0]["description"] == sent_description

    expected_group = hashlib.md5(sent_description.encode("utf-8")).hexdigest()[:16]

    metrics = metrics_consumer.get_metrics()
    span_metrics = [
        (metric, headers)
        for metric, headers in metrics
        if metric["name"].startswith("spans", 2)
    ]
    assert len(span_metrics) == 3
    for metric, headers in span_metrics:
        assert headers == [("namespace", b"spans")]
        if metric["name"] == "c:spans/count_per_op@none":
            continue
        assert metric["tags"]["span.description"] == expected_description
        assert metric["tags"]["span.group"] == expected_group


def test_generic_metric_extraction(mini_sentry, relay):
    PROJECT_ID = 42
    mini_sentry.add_full_project_config(PROJECT_ID)

    config = mini_sentry.project_configs[PROJECT_ID]["config"]
    config["metricExtraction"] = {
        "version": 1,
        "metrics": [
            {
                "category": "transaction",
                "mri": "c:transactions/on_demand@none",
                "condition": {"op": "gte", "name": "event.duration", "value": 1000.0},
                "tags": [{"key": "query_hash", "value": "c91c2e4d"}],
            }
        ],
    }
    config["transactionMetrics"] = {"version": 3}
    config["sampling"] = {
        "version": 2,
        "rules": [
            {
                "id": 1,
                "samplingValue": {"type": "sampleRate", "value": 0.0},
                "type": "transaction",
                "condition": {"op": "and", "inner": []},
            }
        ],
    }

    transaction = generate_transaction_item()
    timestamp = datetime.now(tz=timezone.utc)
    transaction["timestamp"] = timestamp.isoformat()
    transaction["start_timestamp"] = (timestamp - timedelta(seconds=2)).isoformat()

    relay = relay(relay(mini_sentry, options=TEST_CONFIG), options=TEST_CONFIG)
    relay.send_transaction(PROJECT_ID, transaction)

    envelope = mini_sentry.captured_events.get(timeout=3)
    envelope = mini_sentry.captured_events.get(timeout=3)

    for item in envelope.items:
        # Transaction items should be sampled and not among the envelope items.
        assert item.headers.get("type") != "transaction"

    item = envelope.items[0]
    assert item.headers.get("type") == "metric_buckets"
    metrics = json.loads(item.get_bytes().decode())

    assert {
        "timestamp": int(timestamp.timestamp()),
        "width": 1,
        "name": "c:transactions/on_demand@none",
        "type": "c",
        "value": 1.0,
        "tags": {"query_hash": "c91c2e4d"},
    } in metrics


def test_span_metrics_secondary_aggregator(
    metrics_consumer,
    mini_sentry,
    relay_with_processing,
):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    config = mini_sentry.project_configs[project_id]["config"]
    config["transactionMetrics"] = {
        "version": 1,
    }
    config.setdefault("features", []).append("projects:span-metrics-extraction")

    transaction = {
        "event_id": "d2132d31b39445f1938d7e21b6bf0ec4",
        "type": "transaction",
        "transaction": "/organizations/:orgId/performance/:eventSlug/",
        "transaction_info": {"source": "route"},
        "start_timestamp": 1597976392.6542819,
        "timestamp": 1597976400.6189718,
        "user": {"id": "user123", "geo": {"country_code": "ES"}},
        "contexts": {
            "trace": {
                "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
                "span_id": "FA90FDEAD5F74052",
                "type": "trace",
            }
        },
        "spans": [
            {
                "description": "SELECT %s FROM foo",
                "op": "db",
                "parent_span_id": "8f5a2b8768cafb4e",
                "span_id": "bd429c44b67a3eb4",
                "start_timestamp": 1597976393.4619668,
                "timestamp": 1597976393.4718769,
                "trace_id": "ff62a8b040f340bda5d830223def1d81",
            }
        ],
    }
    # Default timestamp is so old that relay drops metrics, setting a more recent one avoids the drop.
    timestamp = datetime.now(tz=timezone.utc)
    transaction["timestamp"] = transaction["spans"][0][
        "timestamp"
    ] = timestamp.isoformat()
    transaction["spans"][0]["start_timestamp"] = (
        timestamp - timedelta(milliseconds=123)
    ).isoformat()

    metrics_consumer = metrics_consumer()
    processing = relay_with_processing(
        options={
            "aggregator": {
                # No metrics will arrive through the default aggregator:
                "bucket_interval": 100,
                "initial_delay": 100,
                "debounce_delay": 100,
            },
            "secondary_aggregators": [
                {
                    "name": "spans",
                    "condition": {"op": "eq", "field": "namespace", "value": "spans"},
                    "config": {
                        # The spans-specific aggregator has config that will deliver metrics:
                        "bucket_interval": 1,
                        "initial_delay": 0,
                        "debounce_delay": 0,
                        "max_tag_value_length": 10,
                    },
                }
            ],
        }
    )
    processing.send_transaction(project_id, transaction)

    metrics = list(metrics_consumer.get_metrics())

    # Transaction metrics are still aggregated:
    assert all([m[0]["name"].startswith("spans", 2) for m in metrics])

    span_metrics = [
        (metric, headers)
        for metric, headers in metrics
        if metric["name"] == "d:spans/exclusive_time@millisecond"
    ]
    assert span_metrics == [
        (
            {
                "name": "d:spans/exclusive_time@millisecond",
                "org_id": 1,
                "project_id": 42,
                "retention_days": 90,
                "tags": {
                    "span.action": "SELECT",
                    "span.description": "SELECT %s*",
                    "span.category": "db",
                    "span.domain": ",foo,",
                    "span.op": "db",
                },
                "timestamp": int(timestamp.timestamp()),
                "type": "d",
                "value": [123],
            },
            [("namespace", b"spans")],
        ),
    ]


def test_custom_metrics_disabled(mini_sentry, relay_with_processing, metrics_consumer):
    relay = relay_with_processing(options=TEST_CONFIG)
    metrics_consumer = metrics_consumer()

    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    # NOTE: "organizations:custom-metrics" missing from features

    timestamp = int(datetime.now(tz=timezone.utc).timestamp())
    metrics_payload = f"transactions/foo:42|c\nbar@second:17|c|T{timestamp}"
    relay.send_metrics(project_id, metrics_payload)

    metrics = metrics_by_name(metrics_consumer, 1)

    assert "c:transactions/foo@none" in metrics
    assert "c:custom/bar@second" not in metrics


@pytest.mark.parametrize(
    "denied_tag", ["sdk", "release"], ids=["remove sdk tag", "remove release tag"]
)
@pytest.mark.parametrize(
    "denied_names", ["*user*", ""], ids=["deny user", "no denied names"]
)
def test_block_metrics_and_tags(mini_sentry, relay, denied_names, denied_tag):
    relay = relay(mini_sentry, options=TEST_CONFIG)

    extra_config = {
        "config": {
            "sessionMetrics": {"version": 1},
            "metrics": {
                "deniedNames": [denied_names],
                "deniedTags": [{"name": ["*"], "tag": [denied_tag]}],
            },
        }
    }

    project_id = 42
    mini_sentry.add_basic_project_config(project_id, extra=extra_config)

    timestamp = datetime.now(tz=timezone.utc)
    started = timestamp - timedelta(hours=1)
    session_payload = _session_payload(timestamp=timestamp, started=started)

    relay.send_session(
        project_id,
        session_payload,
    )

    envelope = mini_sentry.captured_events.get(timeout=2)
    assert len(envelope.items) == 1
    first_item = envelope.items[0]

    second_envelope = mini_sentry.captured_events.get(timeout=2)
    assert len(second_envelope.items) == 1
    second_item = second_envelope.items[0]

    if first_item.type == "session":
        metrics_item = second_item
    else:
        metrics_item = first_item

    assert metrics_item.type == "metric_buckets"

    session_metrics = json.loads(metrics_item.get_bytes().decode())
    session_metrics = sorted(session_metrics, key=lambda x: x["name"])

    if denied_names == "*user*":
        assert len(session_metrics) == 1
        assert session_metrics[0]["name"] == "c:sessions/session@none"
    elif denied_names == "":
        assert len(session_metrics) == 2
        assert session_metrics[0]["name"] == "c:sessions/session@none"
        assert session_metrics[1]["name"] == "s:sessions/user@none"
    else:
        assert False, "add new else-branch if you add another denied name"

    if denied_tag == "sdk":
        assert session_metrics[0]["tags"] == {
            "environment": "production",
            "release": "sentry-test@1.0.0",
            "session.status": "init",
        }
    elif denied_tag == "release":
        assert session_metrics[0]["tags"] == {
            "sdk": "raven-node/2.6.3",
            "environment": "production",
            "session.status": "init",
        }
    else:
        assert False, "add new else-branch if you add another denied tag"
