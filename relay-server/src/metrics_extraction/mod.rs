use relay_common::UnixTimestamp;
use relay_metrics::Metric;

mod conditional_tagging;
pub mod sessions;
mod spans;
pub mod transactions;

pub mod utils;

pub trait IntoMetric {
    fn into_metric(self, timestamp: UnixTimestamp) -> Metric;
}
