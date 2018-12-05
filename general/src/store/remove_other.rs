use crate::processor::{ProcessingState, Processor};
use crate::protocol::Event;
use crate::types::{ErrorKind, Meta, ValueAction};

pub struct RemoveOtherProcessor;

impl Processor for RemoveOtherProcessor {
    fn process_event(
        &mut self,
        event: &mut Event,
        _meta: &mut Meta,
        _state: ProcessingState,
    ) -> ValueAction {
        for value in event.other.values_mut() {
            value.set_value(None);
            value.meta_mut().add_error(ErrorKind::InvalidAttribute);
        }

        ValueAction::Keep
    }
}
