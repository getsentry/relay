use relay_statsd::{CounterMetric, GaugeMetric};

/// Counter metrics for Relay Metrics.
pub enum MetricCounters {
    /// Incremented every time two buckets or two metrics are merged.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    ///  - `namespace`: The namespace of the metric.
    MergeHit,
    /// Incremented every time a bucket is created.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    ///  - `namespace`: The namespace of the metric.
    MergeMiss,
    /// Incremented for every aggregator flush.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    FlushCount,
    /// Incremented with the cost of a partition for every aggregator flush.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    ///  - `namespace`: The namespace of the metric.
    FlushCost,
}

impl CounterMetric for MetricCounters {
    fn name(&self) -> &'static str {
        match *self {
            Self::MergeHit => "metrics.buckets.merge.hit",
            Self::MergeMiss => "metrics.buckets.merge.miss",
            Self::FlushCount => "metrics.buckets.flush.count",
            Self::FlushCost => "metrics.buckets.flush.cost",
        }
    }
}

/// Gauge metrics for Relay Metrics.
pub enum MetricGauges {
    /// The total number of metric buckets in Relay's metrics aggregator.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    ///  - `namespace`: The namespace of the metric.
    Buckets,
    /// The total storage cost of metric buckets in Relay's metrics aggregator.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    ///  - `namespace`: The namespace of the metric.
    BucketsCost,
}

impl GaugeMetric for MetricGauges {
    fn name(&self) -> &'static str {
        match *self {
            Self::Buckets => "metrics.buckets",
            Self::BucketsCost => "metrics.buckets.cost",
        }
    }
}
