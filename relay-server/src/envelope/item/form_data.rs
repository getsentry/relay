//! FormDataItem APIs and accounting.

use super::FormDataItem;
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};

impl Counted for FormDataItem {
    fn quantities(&self) -> Quantities {
        Quantities::new()
    }
}

impl RejectionQuantities for FormDataItem {}
