use relay_common::UnixTimestamp;
use relay_metrics::Metric;

mod conditional_tagging;
mod spans;

pub mod event;
pub mod sessions;
pub mod transactions;
pub mod utils;

pub trait IntoMetric {
    fn into_metric(self, timestamp: UnixTimestamp) -> Metric;
}
