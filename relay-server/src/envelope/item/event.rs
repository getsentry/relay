//! EventItem APIs and accounting.

use super::EventItem;
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};
use relay_quotas::DataCategory;
use smallvec::smallvec;

impl Counted for EventItem {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::Error, 1)]
    }
}

impl RejectionQuantities for EventItem {
    fn rejected_quantities(&self) -> Quantities {
        // The logical event is accounted for once per envelope.
        Quantities::new()
    }
}
