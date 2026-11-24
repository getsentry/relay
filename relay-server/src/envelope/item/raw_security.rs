//! RawSecurityItem APIs and accounting.

use super::RawSecurityItem;
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};
use relay_quotas::DataCategory;
use smallvec::smallvec;

impl Counted for RawSecurityItem {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::Security, 1)]
    }
}

impl RejectionQuantities for RawSecurityItem {
    fn rejected_quantities(&self) -> Quantities {
        // The logical event is accounted for once per envelope.
        Quantities::new()
    }
}
