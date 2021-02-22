use crate::protocol::{Event, EventType};
use crate::types::Annotated;

/// Ensure measurements interface is only present for transaction events
pub fn normalize_measurements(event: &mut Event) {
    if event.ty.value() != Some(&EventType::Transaction) {
        // Only transaction events may have a measurements interface
        event.measurements = Annotated::empty();
        return;
    }
}
