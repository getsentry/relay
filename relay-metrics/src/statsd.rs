use relay_statsd::{CounterMetric, GaugeMetric, HistogramMetric, SetMetric, TimerMetric};

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
    /// Tagged by metric type and name.
    InsertMetric,

    /// Incremented every time two buckets or two metrics are merged.
    ///
    /// Tagged by metric type and name.
    MergeHit,

    /// Incremented every time a bucket is created.
    ///
    /// Tagged by metric type and name.
    MergeMiss,

    /// Incremented every time a bucket is dropped.
    ///
    /// This should only happen when a project state is invalid during graceful shutdown.
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
    BucketsFlushed,

    /// The number of metric buckets flushed in a cycle for each project.
    ///
    /// Relay scans metric buckets in regular intervals and flushes expired buckets. This histogram
    /// is logged for each project that is being flushed. The count of the histogram values is
    /// equivalent to the number of projects being flushed.
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
    /// This metric is only emitted if a partition key is set.
    ///
    /// Tags:
    ///   - `partition_key`: The logical sharding key for the current batch.
    BatchesPerPartition,

    /// The number of buckets in a batch emitted by [`crate::aggregation::Aggregator`].
    ///
    /// This corresponds to the number of buckets that will end up in an envelope.
    ///
    /// Tags:
    ///   - `partition_key`: The logical sharding key for the current batch.
    BucketsPerBatch,
}

impl HistogramMetric for MetricHistograms {
    fn name(&self) -> &'static str {
        match *self {
            Self::BucketsFlushed => "metrics.buckets.flushed",
            Self::BucketsFlushedPerProject => "metrics.buckets.flushed_per_project",
            Self::BucketsDelay => "metrics.buckets.delay",
            Self::BatchesPerPartition => "metrics.buckets.batches_per_partition",
            Self::BucketsPerBatch => "metrics.buckets.per_batch",
        }
    }
}

/// Gauge metrics for Relay Metrics.
pub enum MetricGauges {
    /// The total number of metric buckets in Relay's metrics aggregator.
    Buckets,
    /// The total storage cost of metric buckets in Relay's metrics aggregator.
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
