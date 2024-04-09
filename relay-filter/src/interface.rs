//! TODO: docs

use relay_event_schema::protocol::{Csp, Event};

/// TODO: docs
pub trait Filterable {
    /// TODO: docs
    fn csp(&self) -> Option<&Csp>;
}

impl Filterable for Event {
    fn csp(&self) -> Option<&Csp> {
        self.csp.value()
    }
}
