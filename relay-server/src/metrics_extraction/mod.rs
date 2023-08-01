use relay_common::UnixTimestamp;
use relay_metrics::Metric;

pub mod conditional_tagging;
pub mod generic;
pub mod sessions;
pub mod spans;
pub mod transactions;

pub trait IntoMetric {
    fn into_metric(self, timestamp: UnixTimestamp) -> Metric;
}
