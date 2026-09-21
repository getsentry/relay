from .test_metrics import TEST_CONFIG


def test_generic_metrics_are_not_produced(
    mini_sentry, relay_with_processing, metrics_consumer
):
    metrics_consumer = metrics_consumer()
    relay = relay_with_processing(options=TEST_CONFIG)

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    relay.send_metrics(project_id, "spans/foo:1337|d\nspans/bar:42|s")

    assert metrics_consumer.poll(timeout=2) is None
