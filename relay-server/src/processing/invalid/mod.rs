use relay_cogs::{AppFeature, FeatureWeights};

use crate::envelope::{Item, ItemType};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Quantities, Rejected};
use crate::processing::{self, Context, Nothing, Output};
use crate::services::outcome::{DiscardReason, Outcome};

#[derive(Debug, thiserror::Error)]
#[error("the item is not allowed/supported in this envelope")]
pub struct InvalidItem;

impl OutcomeError for InvalidItem {
    type Error = Self;

    fn consume(self) -> (Option<crate::services::outcome::Outcome>, Self::Error) {
        (Some(Outcome::Invalid(DiscardReason::InvalidEnvelope)), self)
    }
}

/// A processor which rejects unhandled invalid items.
///
/// This processor accepts certain envelope items which may be mixed into envelopes incorrectly.
#[derive(Debug)]
pub struct InvalidUnhandledProcessor {
    _priv: (),
}

impl InvalidUnhandledProcessor {
    /// Creates a new [`Self`].
    pub fn new() -> Self {
        Self { _priv: () }
    }
}

impl processing::Processor for InvalidUnhandledProcessor {
    type Input = InvalidUnhandledItems;
    type Output = Nothing;
    type Error = InvalidItem;

    fn cogs() -> FeatureWeights {
        AppFeature::UnattributedEnvelope.into()
    }

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let invalid_items = envelope
            .envelope_mut()
            // `FormData` only makes sense together with an item which creates an event,
            // if it's mixed into an envelope which does not create an event, it is invalid.
            .take_items_by(|item| matches!(*item.ty(), ItemType::FormData))
            .into_vec();

        if invalid_items.is_empty() {
            return None;
        }

        Some(Managed::with_meta_from_managed_envelope(
            envelope,
            InvalidUnhandledItems { invalid_items },
        ))
    }

    async fn process(
        &self,
        invalid: Managed<Self::Input>,
        _ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        Err(invalid.reject_err(InvalidItem))
    }
}

/// Unknown items extracted from an envelope.
#[derive(Debug)]
pub struct InvalidUnhandledItems {
    /// List of unknown envelope items.
    invalid_items: Vec<Item>,
}

impl Counted for InvalidUnhandledItems {
    fn quantities(&self) -> Quantities {
        self.invalid_items.quantities()
    }
}
