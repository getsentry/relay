use relay_dynamic_config::Feature;

use crate::envelope::{EnvelopeHeaders, Item, Items};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Rejected};
use crate::processing::Processor;
use crate::services::outcome::{DiscardReason, Outcome};

use super::{Context, Output};

mod filter;
mod forward;
mod process;

/// Processor for trace attachments (attachment V2 without span association).
#[derive(Debug)]
pub struct TraceAttachmentsProcessor;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("feature disabled")]
    FeatureDisabled(Feature),
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match self {
            Error::FeatureDisabled(f) => Outcome::Invalid(DiscardReason::FeatureDisabled(f)),
        };
        (Some(outcome), self)
    }
}

impl Processor for TraceAttachmentsProcessor {
    type UnitOfWork = Attachments;

    type Output = Managed<Attachments>;

    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let headers = envelope.envelope().headers().clone();
        let items = envelope
            .envelope_mut()
            .take_items_by(Item::is_trace_attachment);

        (!items.is_empty()).then(|| {
            let work = Attachments { headers, items };
            Managed::from_envelope(envelope, work)
        })
    }

    async fn process(
        &self,
        work: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let work = filter::feature_flag(work, ctx)?;

        // TODO: DS

        // TODO: rate limit

        // TODO: scrub
        Ok(Output::just(work))
    }
}

#[derive(Debug)]
struct Attachments {
    headers: EnvelopeHeaders,
    items: Items,
}

impl Counted for Attachments {
    fn quantities(&self) -> crate::managed::Quantities {
        let Self { headers, items } = self;
        items.quantities()
    }
}
