#[cfg(test)]
use relay_event_schema::protocol::Event;
#[cfg(test)]
use relay_protocol::Annotated;

use crate::Envelope;
use crate::envelope::{EnvelopeHeaders, Item};
use crate::managed::{Managed, ManagedEnvelope, ManagedResult, Rejected};
#[cfg(feature = "processing")]
use crate::processing::StoreHandle;
use crate::processing::spans::Indexed;
use crate::processing::transactions::types::ExpandedTransaction;
use crate::processing::{Forward, ForwardContext};
use crate::services::outcome::{DiscardReason, Outcome};
#[cfg(feature = "processing")]
use crate::services::store::StoreEnvelope;

/// Output of the transaction processor.
#[derive(Debug)]
pub enum TransactionOutput {
    Full(Managed<Box<ExpandedTransaction>>),
    Profile(EnvelopeHeaders, Managed<Box<Item>>),
    Indexed(Managed<Box<ExpandedTransaction<Indexed>>>),
}

impl TransactionOutput {
    #[cfg(test)]
    pub fn event(self) -> Option<Annotated<Event>> {
        match self {
            TransactionOutput::Full(managed) => Some(managed.accept(|x| x).event),
            TransactionOutput::Profile(_, _) => None,
            TransactionOutput::Indexed(managed) => Some(managed.accept(|x| x).event),
        }
    }
}

impl Forward for TransactionOutput {
    fn serialize_envelope(
        self,
        _ctx: ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        match self {
            TransactionOutput::Full(managed) => managed.try_map(|work, _| {
                work.serialize_envelope()
                    .map_err(drop)
                    .with_outcome(Outcome::Invalid(DiscardReason::Internal))
            }),
            TransactionOutput::Profile(headers, managed) => Ok(
                managed.map(|item, _| Envelope::from_parts(headers, smallvec::smallvec![*item]))
            ),
            TransactionOutput::Indexed(managed) => managed.try_map(|work, _| {
                // TODO: This should raise an error, Indexed output should go straight to kafka
                // instead of an envelope.
                work.serialize_envelope()
                    .map_err(drop)
                    .with_outcome(Outcome::Invalid(DiscardReason::Internal))
            }),
        }
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: StoreHandle<'_>,
        ctx: ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let envelope: ManagedEnvelope = self.serialize_envelope(ctx)?.into();

        s.store(StoreEnvelope {
            envelope: envelope.into_processed(),
        });

        Ok(())
    }
}
