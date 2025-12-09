use crate::envelope::{Item, Items};
use crate::managed::{Managed, ManagedEnvelope, Rejected};
use crate::processing::{Forward, Processor};

use super::{Context, Output};

struct TraceAttachmentsProcessor;

#[derive(Debug, thiserror::Error)]
enum Error {}

impl Processor for TraceAttachmentsProcessor {
    type UnitOfWork = Items;

    type Output = ProcessedAttachments;

    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let items = envelope
            .envelope_mut()
            .take_items_by(Item::is_trace_attachment);

        (!items.is_empty()).then(|| Managed::from_envelope(envelope, items))
    }

    async fn process(
        &self,
        work: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        Ok(Output::just(ProcessedAttachments(work)))

        // TODO: DS

        // TODO: rate limit

        // TODO: scrub
    }
}

struct ProcessedAttachments(Managed<Items>);

impl Forward for ProcessedAttachments {
    fn serialize_envelope(
        self,
        ctx: super::ForwardContext<'_>,
    ) -> Result<Managed<Box<crate::Envelope>>, Rejected<()>> {
        todo!()
    }

    fn forward_store(
        self,
        s: super::StoreHandle<'_>,
        ctx: super::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        todo!()
    }
}
