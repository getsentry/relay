import copy
from typing import Any
import pytest
from attr import dataclass
from .test_metrics import TEST_CONFIG


@dataclass
class MetricStatsByMri:
    volume: dict[str, Any]
    other: list[Any]


def metric_stats_by_mri(metrics_consumer, count, timeout=None):
    volume = dict()
    other = list()

    for _ in range(count):
        metric, _ = metrics_consumer.get_metric(timeout)
        if metric["name"] == "c:metric_stats/volume@none":
            volume[metric["tags"]["mri"]] = metric
        else:
            other.append(metric)

    metrics_consumer.assert_empty()
    return MetricStatsByMri(volume=volume, other=other)


@pytest.mark.parametrize("mode", ["default", "chain"])
def test_metric_stats_simple(
    mini_sentry, relay, relay_with_processing, relay_credentials, metrics_consumer, mode
):
    mini_sentry.global_config["options"]["relay.metric-stats.rollout-rate"] = 1.0

    metrics_consumer = metrics_consumer()

    if mode == "default":
        relay = relay_with_processing(options=TEST_CONFIG)
    elif mode == "chain":
        credentials = relay_credentials()
        static_relays = {
            credentials["id"]: {
                "public_key": credentials["public_key"],
                "internal": True,
            },
        }
        relay = relay(
            relay_with_processing(options=TEST_CONFIG, static_relays=static_relays),
            options=TEST_CONFIG,
            credentials=credentials,
        )

    project_id = 42
    project_config = mini_sentry.add_basic_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:custom-metrics",
        "organizations:metric-stats",
    ]

    relay.send_metrics(
        project_id, "custom/foo:1337|d\ncustom/foo:12|d\ncustom/bar:42|s"
    )

    metrics = metric_stats_by_mri(metrics_consumer, 4)
    assert metrics.volume["d:custom/foo@none"]["org_id"] == 0
    assert metrics.volume["d:custom/foo@none"]["project_id"] == project_id
    assert metrics.volume["d:custom/foo@none"]["value"] == 2.0
    assert metrics.volume["d:custom/foo@none"]["tags"] == {
        "mri": "d:custom/foo@none",
        "mri.namespace": "custom",
        "outcome.id": "0",
    }
    assert metrics.volume["s:custom/bar@none"]["org_id"] == 0
    assert metrics.volume["s:custom/bar@none"]["project_id"] == project_id
    assert metrics.volume["s:custom/bar@none"]["value"] == 1.0
    assert metrics.volume["s:custom/bar@none"]["tags"] == {
        "mri": "s:custom/bar@none",
        "mri.namespace": "custom",
        "outcome.id": "0",
    }
    assert len(metrics.other) == 2


def test_metric_stats_max_flush_bytes(
    mini_sentry, relay_with_processing, metrics_consumer
):
    mini_sentry.global_config["options"]["relay.metric-stats.rollout-rate"] = 1.0

    metrics_consumer = metrics_consumer()

    relay_config = copy.deepcopy(TEST_CONFIG)
    relay_config["aggregator"]["max_flush_bytes"] = 150

    relay = relay_with_processing(options=relay_config)

    project_id = 42
    project_config = mini_sentry.add_basic_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:custom-metrics",
        "organizations:metric-stats",
    ]

    # Metric is big enough to be split into multiple smaller metrics when emitting to Kafka,
    # make sure the volume counted is still just 1.
    relay.send_metrics(
        project_id, "custom/foo:1:2:3:4:5:6:7:8:9:10:11:12:13:14:15:16:17:18:19:20|d"
    )

    metrics = metric_stats_by_mri(metrics_consumer, 3)
    assert metrics.volume["d:custom/foo@none"]["value"] == 1.0
    assert len(metrics.other) == 2
