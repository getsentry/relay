//! UnknownItem APIs and accounting.

use super::UnknownItem;
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};

impl Counted for UnknownItem {
    fn quantities(&self) -> Quantities {
        Quantities::new()
    }
}

impl RejectionQuantities for UnknownItem {}
