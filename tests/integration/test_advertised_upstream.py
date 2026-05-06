from datetime import datetime, UTC
import json


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
    assert proxy.captured_requests.empty()


def test_advertised_upstream_metrics(mini_sentry, mini_proxy, relay):
    project_id = 42

    proxy = mini_proxy(mini_sentry)

    project_config = mini_sentry.add_basic_project_config(project_id)
    project_config["upstream"] = proxy.url

    relay = relay(mini_sentry)

    bucket_name = "d:spans/measurements.lcp@millisecond"
    bucket = {
        "org_id": 1,
        "project_id": project_id,
        "timestamp": int(datetime.now(UTC).timestamp()),
        "name": bucket_name,
        "type": "d",
        "value": [1.0],
        "width": 1,
    }

    relay.send_metrics_buckets(project_id, [bucket])

    envelope = mini_sentry.get_captured_envelope()
    payload = json.loads(envelope.items[0].payload.get_bytes())[0]
    assert payload["name"] == bucket_name

    request = proxy.get_captured_request()
    assert request.path == "/api/42/envelope/"
    assert "x-sentry-relay-shard" in request.headers
    assert proxy.captured_requests.empty()
