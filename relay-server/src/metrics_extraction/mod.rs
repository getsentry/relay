use relay_common::UnixTimestamp;
use relay_metrics::Metric;

mod conditional_tagging;
pub mod sessions;
pub mod transactions;

pub trait IntoMetric {
    fn into_metric(self, timestamp: UnixTimestamp) -> Metric;
}
