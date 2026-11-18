//! UserReportV2Item APIs and accounting.

use super::UserReportV2Item;
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};
use relay_quotas::DataCategory;
use smallvec::smallvec;

impl Counted for UserReportV2Item {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::UserReportV2, 1)]
    }
}

impl RejectionQuantities for UserReportV2Item {
    fn rejected_quantities(&self) -> Quantities {
        // The logical event is accounted for once per envelope.
        Quantities::new()
    }
}
