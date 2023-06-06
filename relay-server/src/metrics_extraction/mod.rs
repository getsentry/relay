use relay_common::UnixTimestamp;
use relay_metrics::Metric;

mod conditional_tagging;
pub mod performance;
pub mod sessions;

pub trait IntoMetric {
    fn into_metric(self, timestamp: UnixTimestamp) -> Metric;
}
