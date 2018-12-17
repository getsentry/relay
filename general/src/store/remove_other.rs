use crate::processor::{ProcessingState, Processor};
use crate::protocol::Event;
use crate::types::{Annotated, ErrorKind, Meta, ValueAction};

pub struct RemoveOtherProcessor;

impl Processor for RemoveOtherProcessor {
    fn process_event(
        &mut self,
        event: &mut Event,
        _meta: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ValueAction {
        for (key, value) in event.other.iter_mut() {
            if key == "applecrashreport" || key == "device" || key == "repos" || key == "query" {
                continue;
            }

            // Replace the value and all existing meta with an error
            *value = Annotated::from_error(ErrorKind::InvalidAttribute, None);
        }

        ValueAction::Keep
    }
}
