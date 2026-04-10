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
/// envelopes. Depending on their semantics, these metrics can be ingested into the same project as
/// the envelope or a different project.
#[derive(Debug, Default)]
pub struct ExtractedMetrics {
    /// Metrics associated with the project of the envelope.
    pub project_metrics: Vec<Bucket>,

    /// Metrics associated with the project of the trace parent.
    pub sampling_metrics: Vec<Bucket>,
}
