import time

from .test_metrics import TEST_CONFIG


def test_generic_metrics_are_not_produced(
    mini_sentry, relay_with_processing, metrics_consumer
):
    metrics_consumer = metrics_consumer()
    relay = relay_with_processing(options=TEST_CONFIG)

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    timestamp = int(time.time())
    relay.send_metrics_buckets(
        project_id,
        [
            {
                "timestamp": timestamp,
                "width": 0,
                "name": "d:spans/foo@none",
                "type": "d",
                "value": [1337],
            },
            {
                "timestamp": timestamp,
                "width": 0,
                "name": "s:spans/bar@none",
                "type": "s",
                "value": [42],
            },
        ],
    )

    assert metrics_consumer.poll(timeout=2) is None
