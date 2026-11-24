//! SecurityItem APIs and accounting.

use super::SecurityItem;
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};
use relay_quotas::DataCategory;
use smallvec::smallvec;

impl Counted for SecurityItem {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::Security, 1)]
    }
}

impl RejectionQuantities for SecurityItem {
    fn rejected_quantities(&self) -> Quantities {
        // The logical event is accounted for once per envelope.
        Quantities::new()
    }
}
