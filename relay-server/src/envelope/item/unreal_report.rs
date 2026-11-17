//! UnrealReportItem APIs and accounting.

use super::UnrealReportItem;
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};
use relay_quotas::DataCategory;
use smallvec::smallvec;

impl Counted for UnrealReportItem {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::Error, 1)]
    }
}

impl RejectionQuantities for UnrealReportItem {
    fn rejected_quantities(&self) -> Quantities {
        // The logical event is accounted for once per envelope.
        Quantities::new()
    }
}
