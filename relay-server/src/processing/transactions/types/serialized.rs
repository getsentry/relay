use crate::envelope::{EnvelopeHeaders, Item};
use crate::managed::{Counted, Quantities};

/// A transaction in its serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedTransaction {
    pub headers: EnvelopeHeaders,
    pub event: Item,
    pub attachments: Vec<Item>,
    pub profiles: Vec<Item>,
}

impl Counted for SerializedTransaction {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            event,
            attachments,
            profiles,
        } = self;
        let mut quantities = event.quantities(); // counts spans based on `span_count` header.
        quantities.extend(attachments.quantities());
        quantities.extend(profiles.quantities());

        quantities
    }
}
