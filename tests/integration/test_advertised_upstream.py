from datetime import datetime, UTC
import json


def some_metric_bucket():
    return {
        "timestamp": int(datetime.now(UTC).timestamp()),
        "name": "d:spans/measurements.lcp@millisecond",
        "type": "d",
        "value": [1.0],
        "width": 1,
    }


def test_advertised_upstream_envelope(mini_sentry, mini_proxy, relay):
    project_id = 42

    proxy = mini_proxy(mini_sentry)

    project_config = mini_sentry.add_basic_project_config(project_id)
    project_config["upstream"] = proxy.url

    relay = relay(mini_sentry)

    relay.send_event(project_id)

    event = mini_sentry.get_captured_envelope().get_event()
    assert event is not None

    assert proxy.get_captured_request().path == "/api/42/envelope/"
    proxy.assert_empty()


def test_advertised_upstream_metrics(mini_sentry, mini_proxy, relay):
    project_id = 42

    proxy = mini_proxy(mini_sentry)

    project_config = mini_sentry.add_basic_project_config(project_id)
    project_config["upstream"] = proxy.url

    relay = relay(mini_sentry)

    bucket = some_metric_bucket()
    relay.send_metrics_buckets(project_id, [bucket])

    envelope = mini_sentry.get_captured_envelope()
    payload = json.loads(envelope.items[0].payload.get_bytes())[0]
    assert payload["name"] == bucket["name"]

    request = proxy.get_captured_request()
    assert request.path == "/api/42/envelope/"
    assert "x-sentry-relay-shard" in request.headers
    proxy.assert_empty()


def test_advertised_upstream_global_metrics(mini_sentry, mini_proxy, relay):
    project_id1 = 42
    project_id2 = 43

    proxy = mini_proxy(mini_sentry)

    project_config1 = mini_sentry.add_basic_project_config(project_id1)
    project_config1["upstream"] = proxy.url
    project_config2 = mini_sentry.add_basic_project_config(project_id2)

    relay = relay(mini_sentry, {"http": {"global_metrics": True}})

    bucket = some_metric_bucket()
    relay.send_metrics_buckets(project_id1, [bucket])
    relay.send_metrics_buckets(project_id2, [bucket])

    project_keys = {
        project_config1["publicKeys"][0]["publicKey"],
        project_config2["publicKeys"][0]["publicKey"],
    }
    for _ in range(2):
        metrics_batch = mini_sentry.captured_metrics.get(timeout=5)
        assert metrics_batch.keys() <= project_keys

    request = proxy.get_captured_request()
    assert request.path == "/api/0/relays/metrics/"
    assert "x-sentry-relay-shard" in request.headers
    # Only one project config has an upstream override through the proxy.
    proxy.assert_empty()
