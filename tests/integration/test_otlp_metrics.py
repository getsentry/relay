from datetime import datetime, timezone, timedelta

from sentry_relay.consts import DataCategory

from .asserts import matches_any, time_within, time_within_delta
from .consts import Outcome

TRACE_ID = "5b8efff798038103d269b633813fc60c"


def otel_metrics_payload(ts_nanos):
    return {
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
                                                    "traceId": TRACE_ID,
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


def test_otlp_metrics_conversion(
    mini_sentry,
    relay,
    relay_with_processing,
    relay_credentials,
    items_consumer,
    outcomes_consumer,
):
    items_consumer = items_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:tracemetrics-ingestion",
        "organizations:relay-otel-metrics-endpoint",
    ]

    # Histograms are only accepted from trusted Relays.
    credentials = relay_credentials()
    relay = relay(
        relay_with_processing(static_credentials=credentials),
        credentials=credentials,
    )

    ts = datetime.now(timezone.utc)
    ts_nanos = str(int(ts.timestamp() * 1e6) * 1000)

    relay.send_otel_metrics(project_id, json=otel_metrics_payload(ts_nanos))

    items = items_consumer.get_items(n=3)
    items.sort(key=lambda item: item["attributes"]["sentry.metric_type"]["stringValue"])

    def item(name, ty, unit, value, size, attributes, trace_id=matches_any()):
        return {
            "attributes": {
                f"sentry._internal.cooccuring.name.{name}": {"boolValue": True},
                f"sentry._internal.cooccuring.type.{ty}": {"boolValue": True},
                f"sentry._internal.cooccuring.unit.{unit}": {"boolValue": True},
                "sentry._internal.point_id": {"stringValue": matches_any()},
                "sentry.metric_name": {"stringValue": name},
                "sentry.metric_type": {"stringValue": ty},
                "sentry.metric_unit": {"stringValue": unit},
                "sentry.observed_timestamp_nanos": {
                    "stringValue": time_within(ts, expect_resolution="ns")
                },
                "sentry.origin": {"stringValue": "auto.otlp.metrics"},
                "sentry.payload_size_bytes": {"intValue": str(size)},
                "sentry.timestamp_precise": {
                    "intValue": time_within_delta(
                        ts,
                        delta=timedelta(seconds=0),
                        expect_resolution="ns",
                        precision="us",
                    )
                },
                "sentry.value": {"doubleValue": value},
                **attributes,
            },
            "clientSampleRate": 1.0,
            "downsampledRetentionDays": 90,
            "itemId": matches_any(),
            "itemType": "TRACE_ITEM_TYPE_METRIC",
            "organizationId": "1",
            "outcomes": {
                "categoryCount": [
                    {"dataCategory": DataCategory.TRACE_METRIC, "quantity": "1"},
                    {
                        "dataCategory": DataCategory.TRACE_METRIC_BYTE,
                        "quantity": str(size),
                    },
                ],
                "keyId": "123",
            },
            "projectId": "42",
            "received": time_within_delta(),
            "retentionDays": 90,
            "serverSampleRate": 1.0,
            "timestamp": time_within_delta(ts, delta=timedelta(seconds=1)),
            "traceId": trace_id,
        }

    assert items == [
        item("http.requests", "counter", "none", 3.0, 108, {}),
        item(
            "http.server.duration",
            "histogram",
            "millisecond",
            10.0,
            457,
            {
                "sentry._internal.bucket_counts": {
                    "arrayValue": {"values": [{"intValue": "1"}, {"intValue": "2"}]}
                },
                "sentry._internal.bucket_lowers": {
                    "arrayValue": {
                        "values": [{"doubleValue": 5.0}, {"doubleValue": 10.0}]
                    }
                },
                "sentry._internal.bucket_sum_shares": {
                    "arrayValue": {
                        "values": [
                            {"doubleValue": 6.666666666666667},
                            {"doubleValue": 23.333333333333332},
                        ]
                    }
                },
                "sentry._internal.bucket_uppers": {
                    "arrayValue": {
                        "values": [{"doubleValue": 10.0}, {"doubleValue": 15.0}]
                    }
                },
                "sentry._internal.histogram_count": {"intValue": "3"},
                "sentry._internal.histogram_max": {"doubleValue": 15.0},
                "sentry._internal.histogram_min": {"doubleValue": 5.0},
                "sentry._internal.histogram_sum": {"doubleValue": 30.0},
            },
        ),
        item(
            "http.server.duration",
            "histogram_exemplar",
            "millisecond",
            12.0,
            115,
            {},
            trace_id=TRACE_ID,
        ),
    ]

    # The exemplar references the histogram data point it belongs to.
    _, histogram, exemplar = items
    assert (
        exemplar["attributes"]["sentry._internal.point_id"]
        == histogram["attributes"]["sentry._internal.point_id"]
    )

    # Cumulative sums are not supported.
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
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:tracemetrics-ingestion",
    ]

    relay = relay(mini_sentry)

    ts_nanos = str(int(datetime.now(timezone.utc).timestamp() * 1e9))
    response = relay.send_otel_metrics(project_id, json=otel_metrics_payload(ts_nanos))

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
            "quantity": len(response.request.body),
        },
    ]
    assert mini_sentry.captured_envelopes.empty()


def test_otlp_metrics_processing_no_container_budget(
    mini_sentry, relay_with_processing, items_consumer
):
    items_consumer = items_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:tracemetrics-ingestion",
        "organizations:relay-otel-metrics-endpoint",
    ]

    # Processing Relays do not forward a container, the expanded metrics may exceed its size.
    relay = relay_with_processing({"limits": {"max_container_size": "4KiB"}})

    ts_nanos = str(int(datetime.now(timezone.utc).timestamp() * 1e9))
    points = [{"timeUnixNano": ts_nanos, "asDouble": i} for i in range(40)]
    payload = {
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

    response = relay.send_otel_metrics(project_id, json=payload)
    assert len(response.request.body) < 4096

    assert len(items_consumer.get_items(n=40)) == 40
    items_consumer.assert_empty()
