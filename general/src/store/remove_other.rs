use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::protocol::Event;
use crate::types::{Annotated, ErrorKind, Meta, Object, Value, ValueAction};

pub struct RemoveOtherProcessor;

impl Processor for RemoveOtherProcessor {
    fn process_other(&mut self, other: &mut Object<Value>, _state: &ProcessingState<'_>) {
        // Drop unknown attributes at all levels without error messages
        other.clear();
    }

    fn process_event(
        &mut self,
        event: &mut Event,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
        // Move the current map out so we don't clear it in `process_other`
        let mut other = std::mem::replace(&mut event.other, Default::default());

        // Drop known legacy attributes at top-level without errors
        other.remove("applecrashreport");
        other.remove("device");
        other.remove("repos");
        other.remove("query");

        // Replace remaining values and all existing meta with an errors
        for value in other.values_mut() {
            *value = Annotated::from_error(ErrorKind::InvalidAttribute, None);
        }

        // Recursively clean all `other`s now. Note that this won't touch the event's other
        event.process_child_values(self, state);

        event.other = other;
        ValueAction::Keep
    }
}
