//! LogItem APIs and accounting.

use super::{ItemCount, LogItem};
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};
use relay_quotas::DataCategory;
use smallvec::smallvec;

impl Counted for LogItem {
    fn quantities(&self) -> Quantities {
        smallvec![
            (
                DataCategory::LogItem,
                self.item_count().unwrap_or(1) as usize,
            ),
            (DataCategory::LogByte, self.data().len().max(1)),
        ]
    }
}

impl RejectionQuantities for LogItem {}
