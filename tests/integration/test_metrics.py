from datetime import datetime, timezone
import json


TEST_CONFIG = {
    "aggregator": {"bucket_interval": 1, "initial_delay": 0, "debounce_delay": 0,}
}


def test_metrics(mini_sentry, relay):
    relay = relay(mini_sentry, options=TEST_CONFIG)

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    timestamp = int(datetime.now(tz=timezone.utc).timestamp())
    metrics_payload = f"foo:42|c|'{timestamp}\nbar:17|c|'{timestamp}"
    relay.send_metrics(project_id, metrics_payload)

    envelope = mini_sentry.captured_events.get(timeout=2)
    assert len(envelope.items) == 1

    metrics_item = envelope.items[0]
    assert metrics_item.type == "metric_buckets"

    received_metrics = metrics_item.get_bytes()
    assert json.loads(received_metrics.decode()) == [
        {"timestamp": timestamp, "name": "foo", "value": 42.0, "type": "c"},
        {"timestamp": timestamp, "name": "bar", "value": 17.0, "type": "c"},
    ]


def test_metrics_with_processing(mini_sentry, relay_with_processing, metrics_consumer):
    relay = relay_with_processing(options=TEST_CONFIG)
    metrics_consumer = metrics_consumer()

    project_id = 42
    mini_sentry.add_full_project_config(project_id)

    timestamp = int(datetime.now(tz=timezone.utc).timestamp())
    metrics_payload = f"foo:42|c|'{timestamp}\nbar:17|c|'{timestamp}"
    relay.send_metrics(project_id, metrics_payload)

    metric = metrics_consumer.get_metric()

    assert metric == {
        "org_id": 1,
        "project_id": project_id,
        "name": "foo",
        "unit": "",
        "value": 42.0,
        "type": "c",
        "timestamp": timestamp,
    }
