use relay_common::time::UnixTimestamp;
use relay_metrics::Bucket;

pub mod event;
pub mod generic;
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
