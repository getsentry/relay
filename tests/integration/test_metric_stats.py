from typing import Any
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


def test_metric_stats_simple(mini_sentry, relay_with_processing, metrics_consumer):
    mini_sentry.global_config["options"]["relay.metric-stats.rollout-rate"] = 1.0

    metrics_consumer = metrics_consumer()
    relay = relay_with_processing(options=TEST_CONFIG)

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
