use relay_statsd::{CounterMetric, GaugeMetric, HistogramMetric, SetMetric, TimerMetric};

use crate::BucketValue;

/// Set metrics for Relay Metrics.
pub enum MetricSets {
    /// Count the number of unique buckets created.
    ///
    /// This is a set of bucketing keys. The metric is basically equivalent to
    /// `metrics.buckets.merge.miss` for a single Relay, but could be useful to determine how much
    /// duplicate buckets there are when multiple instances are running.
    ///
    /// The hashing is platform-dependent at the moment, so all your relays that send this metric
    /// should run on the same CPU architecture, otherwise this metric is not reliable.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    ///  - `metric_name`: A low-cardinality representation of the metric name (see [`metric_name_tag`]).
    UniqueBucketsCreated,
}

impl SetMetric for MetricSets {
    fn name(&self) -> &'static str {
        match *self {
            Self::UniqueBucketsCreated => "metrics.buckets.created.unique",
        }
    }
}

/// Counter metrics for Relay Metrics.
pub enum MetricCounters {
    /// Incremented for every metric that is inserted.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    ///  - `metric_type`: The type of the metric (e.g. `"counter"`).
    InsertMetric,

    /// Incremented every time two buckets or two metrics are merged.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    ///  - `metric_name`: A low-cardinality representation of the metric name (see [`metric_name_tag`]).
    MergeHit,

    /// Incremented every time a bucket is created.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    ///  - `metric_name`: A low-cardinality representation of the metric name (see [`metric_name_tag`]).
    MergeMiss,

    /// Incremented every time a bucket is dropped.
    ///
    /// This should only happen when a project state is invalid during graceful shutdown.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    BucketsDropped,
}

impl CounterMetric for MetricCounters {
    fn name(&self) -> &'static str {
        match *self {
            Self::InsertMetric => "metrics.insert",
            Self::MergeHit => "metrics.buckets.merge.hit",
            Self::MergeMiss => "metrics.buckets.merge.miss",
            Self::BucketsDropped => "metrics.buckets.dropped",
        }
    }
}

/// Timer metrics for Relay Metrics.
pub enum MetricTimers {
    /// Time in milliseconds spent scanning metric buckets to flush.
    ///
    /// Relay scans metric buckets in regular intervals and flushes expired buckets. This timer
    /// shows the time it takes to perform this scan and remove the buckets from the internal cache.
    /// Sending the metric buckets to upstream is outside of this timer.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    BucketsScanDuration,
}

impl TimerMetric for MetricTimers {
    fn name(&self) -> &'static str {
        match *self {
            Self::BucketsScanDuration => "metrics.buckets.scan_duration",
        }
    }
}

/// Histogram metrics for Relay Metrics.
#[allow(clippy::enum_variant_names)]
pub enum MetricHistograms {
    /// The total number of metric buckets flushed in a cycle across all projects.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    BucketsFlushed,

    /// The number of metric buckets flushed in a cycle for each project.
    ///
    /// Relay scans metric buckets in regular intervals and flushes expired buckets. This histogram
    /// is logged for each project that is being flushed. The count of the histogram values is
    /// equivalent to the number of projects being flushed.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    BucketsFlushedPerProject,

    /// The reporting delay at which a bucket arrives in Relay.
    ///
    /// A positive delay indicates the bucket arrives after its stated timestamp. Large delays
    /// indicate backdating, particularly all delays larger than `bucket_interval + initial_delay`.
    /// Negative delays indicate that the bucket is dated into the future, likely due to clock drift
    /// on the client.
    ///
    /// This metric is tagged with:
    ///  - `backdated`: A flag indicating whether the metric was reported within the `initial_delay`
    ///    time period (`false`) or after the initial delay has expired (`true`).
    BucketsDelay,

    /// The number of batches emitted per partition by [`crate::aggregation::Aggregator`].
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    BatchesPerPartition,

    /// The number of buckets in a batch emitted by [`crate::aggregation::Aggregator`].
    ///
    /// This corresponds to the number of buckets that will end up in an envelope.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    BucketsPerBatch,

    /// Distribution of flush buckets over partition keys.
    ///
    /// The distribution of buckets should be even.
    /// If it is not, this metric should expose it.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    PartitionKeys,

    /// Distribution of invalid bucket timestamps observed, relative to the time of observation.
    ///
    /// This is a temporary metric to better understand why we see so many invalid timestamp errors.
    InvalidBucketTimestamp,
}

impl HistogramMetric for MetricHistograms {
    fn name(&self) -> &'static str {
        match *self {
            Self::BucketsFlushed => "metrics.buckets.flushed",
            Self::BucketsFlushedPerProject => "metrics.buckets.flushed_per_project",
            Self::BucketsDelay => "metrics.buckets.delay",
            Self::BatchesPerPartition => "metrics.buckets.batches_per_partition",
            Self::BucketsPerBatch => "metrics.buckets.per_batch",
            Self::PartitionKeys => "metrics.buckets.partition_keys",
            Self::InvalidBucketTimestamp => "metrics.buckets.invalid_timestamp",
        }
    }
}

/// Gauge metrics for Relay Metrics.
pub enum MetricGauges {
    /// The total number of metric buckets in Relay's metrics aggregator.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    Buckets,
    /// The total storage cost of metric buckets in Relay's metrics aggregator.
    ///
    /// This metric is tagged with:
    ///  - `aggregator`: The name of the metrics aggregator (usually `"default"`).
    BucketsCost,
    /// The average number of elements in a bucket when flushed.
    ///
    /// This metric is tagged with:
    ///  - `metric_type`: "counter", "distribution", "gauge" or "set".
    ///  - `metric_name`: Low-cardinality name of the metric.
    AvgBucketSize,
}

impl GaugeMetric for MetricGauges {
    fn name(&self) -> &'static str {
        match *self {
            Self::Buckets => "metrics.buckets",
            Self::BucketsCost => "metrics.buckets.cost",
            Self::AvgBucketSize => "metrics.buckets.size",
        }
    }
}

/// Returns a low-cardinality metric name for use as a tag key on statsd metrics.
///
/// In order to keep this low-cardinality, we only enumerate a handful of well-known, high volume
/// names. The rest gets mapped to "other".
pub(crate) fn metric_name_tag(value: &str) -> &'static str {
    if let Some(value) = [
        "c:sessions/session@none",
        "s:sessions/user@none",
        "s:sessions/error@none",
        "d:transactions/duration@millisecond",
        "s:transactions/user@none",
        "c:transactions/count_per_root_project@none",
    ]
    .into_iter()
    .find(|x| x == &value)
    {
        return value;
    }

    if value.starts_with("d:transactions/breakdowns.") {
        return "d:transactions/breakdowns.*";
    }
    if value.starts_with("d:transactions/measurements.") {
        return "d:transactions/measurements.*";
    }

    if value.starts_with("s:spans/") {
        return "s:spans/";
    }
    if value.starts_with("d:spans/") {
        return "d:spans/";
    }

    "other"
}

/// Returns the metric type for use as a tag key on statsd metrics.
pub(crate) fn metric_type_tag(value: &BucketValue) -> &'static str {
    match value {
        BucketValue::Counter(_) => "counter",
        BucketValue::Distribution(_) => "distribution",
        BucketValue::Set(_) => "set",
        BucketValue::Gauge(_) => "gauge",
    }
}
