//! TraceMetricItem APIs and accounting.

use super::{ItemCount, TraceMetricItem};
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};
use relay_quotas::DataCategory;
use smallvec::smallvec;

impl Counted for TraceMetricItem {
    fn quantities(&self) -> Quantities {
        smallvec![
            (
                DataCategory::TraceMetric,
                self.item_count().unwrap_or(1) as usize,
            ),
            (DataCategory::TraceMetricByte, self.data().len().max(1)),
        ]
    }
}

impl RejectionQuantities for TraceMetricItem {}
