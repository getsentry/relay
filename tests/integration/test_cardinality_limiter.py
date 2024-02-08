from datetime import datetime, timezone

import pytest

from .test_metrics import metrics_by_name

TEST_CONFIG = {
    "aggregator": {
        "bucket_interval": 1,
        "initial_delay": 0,
        "debounce_delay": 0,
        "shift_key": "none",
    }
}


def metrics_by_namespace(metrics_consumer, count, timeout=None):
    metrics = metrics_by_name(metrics_consumer, count, timeout)

    result = dict()
    for metric, headers in metrics["headers"].items():
        namespace = dict(headers)["namespace"].decode("utf-8")
        result.setdefault(namespace, []).append(metric)

    return result


def add_project_config(mini_sentry, project_id, cardinality_limits=None):
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = ["organizations:custom-metrics"]
    project_config["config"]["metrics"] = {
        "cardinalityLimits": cardinality_limits or []
    }


def test_cardinality_limits(mini_sentry, relay_with_processing, metrics_consumer):
    relay = relay_with_processing(options=TEST_CONFIG)
    metrics_consumer = metrics_consumer()

    project_id = 42
    cardinality_limits = [
        {
            "id": "transactions",
            "window": {"windowSeconds": 3600, "granularitySeconds": 600},
            "limit": 1,
            "scope": "organization",
            "namespace": "transactions",
        },
        {
            "id": "custom",
            "window": {"windowSeconds": 3600, "granularitySeconds": 600},
            "limit": 2,
            "scope": "organization",
            "namespace": "custom",
        },
    ]

    add_project_config(mini_sentry, project_id, cardinality_limits)

    timestamp = int(datetime.now(tz=timezone.utc).timestamp())
    metrics_payload = "\n".join(
        [
            "transactions/foo@second:12|c",
            "transactions/bar@second:23|c",
            "sessions/foo@second:12|c",
            "foo@second:12|c",
            "bar@second:23|c",
            f"baz@second:17|c|T{timestamp}",
        ]
    )
    relay.send_metrics(project_id, metrics_payload)

    metrics = metrics_by_namespace(metrics_consumer, 4)
    assert len(metrics["custom"]) == 2
    assert len(metrics["sessions"]) == 1
    assert len(metrics["transactions"]) == 1


@pytest.mark.parametrize("mode", [None, "enabled", "disabled", "passive"])
def test_cardinality_limits_global_config_mode(
    mini_sentry, relay_with_processing, metrics_consumer, mode
):
    if mode is not None:
        mini_sentry.global_config["options"]["relay.cardinality-limiter.mode"] = mode

    relay = relay_with_processing(options=TEST_CONFIG)
    metrics_consumer = metrics_consumer()

    project_id = 42
    cardinality_limits = [
        {
            "id": "transactions",
            "window": {"windowSeconds": 3600, "granularitySeconds": 600},
            "limit": 1,
            "scope": "organization",
            "namespace": "transactions",
        },
    ]

    add_project_config(mini_sentry, project_id, cardinality_limits)

    metrics_payload = "transactions/foo@second:12|c\ntransactions/bar@second:23|c"
    relay.send_metrics(project_id, metrics_payload)

    if mode in [None, "enabled"]:
        metrics = metrics_by_namespace(metrics_consumer, 1)
        assert len(metrics["transactions"]) == 1
    else:
        metrics = metrics_by_namespace(metrics_consumer, 2)
        assert len(metrics["transactions"]) == 2
