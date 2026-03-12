use smallvec::SmallVec;

use crate::envelope::{EnvelopeHeaders, Item, Items};
use crate::managed::{Counted, Quantities};

/// A transaction in its serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedTransaction {
    pub headers: EnvelopeHeaders,
    pub event: Item,
    pub attachments: Items,
    pub profiles: SmallVec<[Item; 3]>,
}

impl Counted for SerializedTransaction {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            event,
            attachments,
            profiles,
        } = self;
        debug_assert!(!event.spans_extracted());
        let mut quantities = event.quantities(); // counts spans based on `span_count` header.
        quantities.extend(attachments.quantities());
        quantities.extend(profiles.quantities());

        quantities
    }
}
