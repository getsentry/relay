use relay_cogs::{AppFeature, FeatureWeights};

use crate::Envelope;
use crate::envelope::{EnvelopeHeaders, UnknownItem};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Quantities, Rejected};
use crate::processing::{self, Context, Forward, Output};
use crate::services::outcome::{DiscardReason, Outcome};

#[derive(Debug, thiserror::Error)]
#[error("the item is not allowed/supported in this envelope")]
pub struct UnsupportedItem;

impl OutcomeError for UnsupportedItem {
    type Error = Self;

    fn consume(self) -> (Option<crate::services::outcome::Outcome>, Self::Error) {
        (Some(Outcome::Invalid(DiscardReason::InvalidEnvelope)), self)
    }
}

/// A processor which only forwards unknown envelope items.
///
/// For compatibility
#[derive(Debug)]
pub struct ForwardUnknownProcessor {}

impl ForwardUnknownProcessor {
    /// Creates a new [`Self`].
    pub fn new() -> Self {
        Self {}
    }
}

impl processing::Processor for ForwardUnknownProcessor {
    type Input = UnknownItems;
    type Output = ForwardUnknownOutput;
    type Error = UnsupportedItem;

    fn cogs() -> FeatureWeights {
        AppFeature::UnattributedEnvelope.into()
    }

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let unknown_items: Vec<_> = envelope
            .envelope_mut()
            .drain_items::<UnknownItem>()
            .collect();

        if unknown_items.is_empty() {
            return None;
        }

        Some(Managed::with_meta_from_managed_envelope(
            envelope,
            UnknownItems {
                headers: envelope.envelope().headers().clone(),
                unknown_items,
            },
        ))
    }

    async fn process(
        &self,
        unknown_items: Managed<Self::Input>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        if !ctx.config.accept_unknown_items() {
            return Err(unknown_items.reject_err(UnsupportedItem));
        }

        Ok(Output::just(ForwardUnknownOutput(unknown_items)))
    }
}

/// Output produced by [`ForwardUnknownProcessor`].
#[derive(Debug)]
pub struct ForwardUnknownOutput(Managed<UnknownItems>);

impl Forward for ForwardUnknownOutput {
    fn serialize_envelope(
        self,
        _: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        let Self(ui) = self;
        Ok(ui.map(|ui, _| {
            Envelope::from_parts(
                ui.headers,
                ui.unknown_items.into_iter().map(Into::into).collect(),
            )
        }))
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        _s: processing::forward::StoreHandle<'_>,
        _ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        Err(self.0.reject_err(UnsupportedItem).map(drop))
    }
}

/// Unknown items extracted from an envelope.
#[derive(Debug)]
pub struct UnknownItems {
    /// Original envelope headers.
    pub headers: EnvelopeHeaders,
    /// List of unknown envelope items.
    pub unknown_items: Vec<UnknownItem>,
}

impl Counted for UnknownItems {
    fn quantities(&self) -> Quantities {
        Default::default()
    }
}
