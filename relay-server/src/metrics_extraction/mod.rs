use relay_common::UnixTimestamp;
use relay_metrics::Metric;

pub mod conditional_tagging;
pub mod event;
pub mod sessions;
pub mod spans;
pub mod transactions;
pub mod utils;

pub trait IntoMetric {
    fn into_metric(self, timestamp: UnixTimestamp) -> Metric;
}
