//! ReplayEventItem APIs and accounting.

use super::ReplayEventItem;
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};
use relay_quotas::DataCategory;
use smallvec::smallvec;

impl Counted for ReplayEventItem {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::Replay, 1)]
    }
}

impl RejectionQuantities for ReplayEventItem {}
