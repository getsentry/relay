use relay_common::time::UnixTimestamp;
use relay_metrics::Bucket;

pub mod event;
pub mod generic;
pub mod sessions;
pub mod transactions;
mod utils;

use self::utils::*;

pub trait IntoMetric {
    fn into_metric(self, timestamp: UnixTimestamp) -> Bucket;
}
