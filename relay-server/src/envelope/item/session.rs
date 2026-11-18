//! SessionItem APIs and accounting.

use super::{ItemCount, SessionItem};
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};
use relay_quotas::DataCategory;
use smallvec::smallvec;

impl Counted for SessionItem {
    fn quantities(&self) -> Quantities {
        smallvec![(
            DataCategory::Session,
            self.item_count().unwrap_or(1) as usize,
        )]
    }
}

impl RejectionQuantities for SessionItem {}
