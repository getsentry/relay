import json
from datetime import datetime, timezone

from sentry_relay.consts import DataCategory

from .consts import Outcome


def metrics_project(mini_sentry, features=True):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = ["organizations:tracemetrics-ingestion"]
    if features:
        project_config["config"]["features"].append(
            "organizations:relay-otel-metrics-endpoint"
        )
    return project_id


def metrics_payload():
    ts_nanos = str(int(datetime.now(timezone.utc).timestamp() * 1e9))
    return json.dumps(
        {
            "resourceMetrics": [
                {
                    "scopeMetrics": [
                        {
                            "metrics": [
                                {
                                    "name": "http.requests",
                                    "sum": {
                                        "aggregationTemporality": 1,
                                        "isMonotonic": True,
                                        "dataPoints": [
                                            {"timeUnixNano": ts_nanos, "asInt": "3"}
                                        ],
                                    },
                                },
                                {
                                    "name": "http.requests.cumulative",
                                    "sum": {
                                        "aggregationTemporality": 2,
                                        "isMonotonic": True,
                                        "dataPoints": [
                                            {"timeUnixNano": ts_nanos, "asInt": "3"}
                                        ],
                                    },
                                },
                                {
                                    "name": "http.server.duration",
                                    "unit": "ms",
                                    "histogram": {
                                        "aggregationTemporality": 1,
                                        "dataPoints": [
                                            {
                                                "timeUnixNano": ts_nanos,
                                                "count": "3",
                                                "sum": 30.0,
                                                "bucketCounts": ["1", "2"],
                                                "explicitBounds": [10.0],
                                                "min": 5.0,
                                                "max": 15.0,
                                                "exemplars": [
                                                    {
                                                        "timeUnixNano": ts_nanos,
                                                        "asDouble": 12.0,
                                                        "traceId": "5b8efff798038103d269b633813fc60c",
                                                    }
                                                ],
                                            }
                                        ],
                                    },
                                },
                            ]
                        }
                    ]
                }
            ]
        }
    )


def test_otlp_metrics(
    mini_sentry,
    relay,
    relay_with_processing,
    relay_credentials,
    items_consumer,
    outcomes_consumer,
):
    items_consumer = items_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = metrics_project(mini_sentry)

    credentials = relay_credentials()
    relay = relay(
        relay_with_processing(static_credentials=credentials),
        credentials=credentials,
    )

    relay.send_otel_metrics(project_id, data=metrics_payload())

    items = items_consumer.get_items(n=3)

    def row(item):
        attributes = item["attributes"]
        return (
            attributes["sentry.metric_name"]["stringValue"],
            attributes["sentry.metric_type"]["stringValue"],
            attributes["sentry._internal.row_kind"]["stringValue"],
            attributes["sentry.value"]["doubleValue"],
        )

    assert sorted(map(row, items)) == [
        ("http.requests", "counter", "point", 3.0),
        ("http.server.duration", "histogram", "exemplar", 12.0),
        ("http.server.duration", "histogram", "point", 10.0),
    ]

    def array(item, name):
        values = item["attributes"][f"sentry._internal.{name}"]["arrayValue"]["values"]
        return [next(iter(v.values())) for v in values]

    (histogram,) = (i for i in items if row(i)[1:3] == ("histogram", "point"))
    assert array(histogram, "bucket_lowers") == [5.0, 10.0]
    assert array(histogram, "bucket_uppers") == [10.0, 15.0]
    assert array(histogram, "bucket_counts") == ["1", "2"]

    assert outcomes_consumer.get_aggregated_outcomes(n=1) == [
        {
            "category": DataCategory.TRACE_METRIC,
            "key_id": 123,
            "org_id": 1,
            "outcome": Outcome.INVALID,
            "project_id": 42,
            "quantity": 1,
            "reason": "invalid_trace_metric",
        },
    ]


def test_otlp_metrics_requires_feature(mini_sentry, relay):
    project_id = metrics_project(mini_sentry, features=False)
    payload = metrics_payload()

    relay = relay(mini_sentry)
    relay.send_otel_metrics(project_id, data=payload)

    assert mini_sentry.get_aggregated_outcomes(n=2) == [
        {
            "category": DataCategory.TRACE_METRIC,
            "outcome": Outcome.INVALID,
            "reason": "feature_disabled",
            "quantity": 1,
        },
        {
            "category": DataCategory.TRACE_METRIC_BYTE,
            "outcome": Outcome.INVALID,
            "reason": "feature_disabled",
            "quantity": len(payload),
        },
    ]
    assert mini_sentry.captured_envelopes.empty()


def test_otlp_metrics_processing_no_container_budget(
    mini_sentry, relay_with_processing, items_consumer
):
    items_consumer = items_consumer()
    project_id = metrics_project(mini_sentry)

    # Processing Relays do not forward a container, the expanded metrics may exceed its size.
    relay = relay_with_processing({"limits": {"max_container_size": "4KiB"}})

    ts_nanos = str(int(datetime.now(timezone.utc).timestamp() * 1e9))
    points = [{"timeUnixNano": ts_nanos, "asDouble": i} for i in range(40)]
    payload = json.dumps(
        {
            "resourceMetrics": [
                {
                    "scopeMetrics": [
                        {
                            "metrics": [
                                {"name": "queue.size", "gauge": {"dataPoints": points}}
                            ]
                        }
                    ]
                }
            ]
        }
    )
    assert len(payload) < 4096

    relay.send_otel_metrics(project_id, data=payload)

    assert len(items_consumer.get_items(n=40)) == 40
    items_consumer.assert_empty()
