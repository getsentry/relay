//! SpanItem APIs and accounting.

use super::{ItemCount, SpanItem};
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};
use relay_quotas::DataCategory;
use smallvec::smallvec;

impl Counted for SpanItem {
    fn quantities(&self) -> Quantities {
        let count = self.item_count().unwrap_or(1) as usize;
        smallvec![
            (DataCategory::Span, count),
            (DataCategory::SpanIndexed, count),
        ]
    }
}

impl RejectionQuantities for SpanItem {}
