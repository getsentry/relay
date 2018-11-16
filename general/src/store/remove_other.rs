use crate::processor::{ProcessingState, Processor};

use crate::protocol::Event;
use crate::types::{Annotated, Meta};

pub struct RemoveOtherProcessor;

impl Processor for RemoveOtherProcessor {
    fn process_event(
        &mut self,
        event: Annotated<Event>,
        _state: ProcessingState,
    ) -> Annotated<Event> {
        event.filter_map(Annotated::is_valid, |mut event| {
            let mut meta = Meta::default();
            for key in event.other.keys() {
                meta.add_error(format!("Unknown key: {}", key), None);
            }

            event.other.clear();

            Annotated(Some(event), meta)
        })
    }
}
