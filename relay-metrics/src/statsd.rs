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
    /// Tagged by metric type, name, and a `backdated` flag.
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

    /// The number of metric elements in the bucket.
    ///
    /// BucketRelativeSize measures how many distinct values are in a bucket and therefore
    /// BucketRelativeSize gives you a measurement of the bucket size and complexity.
    BucketRelativeSize,
}

impl HistogramMetric for MetricHistograms {
    fn name(&self) -> &'static str {
        match *self {
            Self::BucketsFlushed => "metrics.buckets.flushed",
            Self::BucketsFlushedPerProject => "metrics.buckets.flushed_per_project",
            Self::BucketRelativeSize => "metrics.buckets.relative_bucket_size",
        }
    }
}

/// Gauge metrics for Relay Metrics.
pub enum MetricGauges {
    /// The total number of metric buckets in Relay's metrics aggregator.
    Buckets,
}

impl GaugeMetric for MetricGauges {
    fn name(&self) -> &'static str {
        match *self {
            Self::Buckets => "metrics.buckets",
        }
    }
}
