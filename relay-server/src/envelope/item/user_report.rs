//! UserReportItem APIs and accounting.

use super::UserReportItem;
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};
use relay_quotas::DataCategory;
use smallvec::smallvec;

impl Counted for UserReportItem {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::UserReportV2, 1)]
    }
}

impl RejectionQuantities for UserReportItem {}
