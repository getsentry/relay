use relay_common::time::UnixTimestamp;
use relay_metrics::Metric;

mod generic;

pub mod event;
pub mod sessions;
pub mod transactions;

pub trait IntoMetric {
    fn into_metric(self, timestamp: UnixTimestamp) -> Metric;
}
