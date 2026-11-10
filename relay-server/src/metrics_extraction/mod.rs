use relay_common::time::UnixTimestamp;
use relay_metrics::Bucket;

pub mod event;
pub mod sessions;

pub trait IntoMetric {
    fn into_metric(self, timestamp: UnixTimestamp) -> Bucket;
}

/// Metrics extracted from an envelope.
///
/// Metric extraction derives pre-computed metrics (time series data) from payload items in
/// envelopes
#[derive(Debug, Default)]
pub struct ExtractedMetrics(pub Vec<Bucket>);

impl ExtractedMetrics {
    /// Returns the number of buckets counted in the [`DataCategory::MetricBucket`] category.
    ///
    /// [`DataCategory::MetricBucket`]: relay_quotas::DataCategory::MetricBucket
    pub fn bucket_quantity(&self) -> usize {
        crate::metrics::extract_quantities(&self.0).buckets
    }
}
