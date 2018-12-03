use crate::processor::{ProcessingState, Processor};
use crate::protocol::Event;
use crate::types::{Meta, ValueAction};

pub struct RemoveOtherProcessor;

impl Processor for RemoveOtherProcessor {
    fn process_event(
        &mut self,
        event: &mut Event,
        meta: &mut Meta,
        _state: ProcessingState,
    ) -> ValueAction {
        for key in event.other.keys() {
            meta.add_error(format!("Unknown key: {}", key), None);
        }

        event.other.clear();
        ValueAction::Keep
    }
}
