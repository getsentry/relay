//! ReplayRecordingItem APIs and accounting.

use super::ReplayRecordingItem;
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};
use relay_quotas::DataCategory;
use smallvec::smallvec;

impl Counted for ReplayRecordingItem {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::Replay, 1)]
    }
}

impl RejectionQuantities for ReplayRecordingItem {}
