//! MetricBucketsItem APIs and accounting.

use super::MetricBucketsItem;
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};

impl Counted for MetricBucketsItem {
    fn quantities(&self) -> Quantities {
        Quantities::new()
    }
}

impl RejectionQuantities for MetricBucketsItem {}
