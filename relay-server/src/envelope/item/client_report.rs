//! ClientReportItem APIs and accounting.

use super::ClientReportItem;
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};

impl Counted for ClientReportItem {
    fn quantities(&self) -> Quantities {
        Quantities::new()
    }
}

impl RejectionQuantities for ClientReportItem {}
