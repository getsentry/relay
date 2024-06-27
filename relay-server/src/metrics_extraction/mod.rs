use relay_common::time::UnixTimestamp;
use relay_metrics::Bucket;

pub mod event;
pub mod generic;
mod metrics_summary;
pub mod sessions;
pub mod transactions;

pub trait IntoMetric {
    fn into_metric(self, timestamp: UnixTimestamp) -> Bucket;
}
