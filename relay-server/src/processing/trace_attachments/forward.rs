use crate::Envelope;
use crate::managed::{Managed, Rejected};
use crate::processing::{Forward, ForwardContext, StoreHandle};
use crate::services::outcome::{DiscardReason, Outcome};

use super::Attachments;

impl Forward for Managed<Attachments> {
    fn serialize_envelope(
        self,
        _ctx: ForwardContext<'_>,
    ) -> Result<Managed<Box<crate::Envelope>>, Rejected<()>> {
        Ok(self.map(|attachments, _| {
            let Attachments { headers, items } = attachments;
            Envelope::from_parts(headers, items)
        }))
    }

    fn forward_store(
        self,
        s: StoreHandle<'_>,
        ctx: ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        todo!();
        s.upload(message);
    }
}
