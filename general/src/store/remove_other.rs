use crate::processor::{ProcessResult, ProcessingState, Processor};

use crate::protocol::Event;
use crate::types::Meta;

pub struct RemoveOtherProcessor;

impl Processor for RemoveOtherProcessor {
    fn process_event(
        &mut self,
        event: &mut Event,
        meta: &mut Meta,
        _state: ProcessingState,
    ) -> ProcessResult {
        for key in event.other.keys() {
            meta.add_error(format!("Unknown key: {}", key), None);
        }

        event.other.clear();
        ProcessResult::Keep
    }
}
