use crate::envelope::{AttachmentItem, EnvelopeHeaders, ProfileItem, TransactionItem};
use crate::managed::{Counted, Quantities};

/// A transaction in its serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedTransaction {
    pub headers: EnvelopeHeaders,
    pub event: TransactionItem,
    pub attachments: Vec<AttachmentItem>,
    pub profiles: Vec<ProfileItem>,
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
