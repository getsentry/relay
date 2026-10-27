use relay_quotas::DataCategory;

use crate::envelope::{EnvelopeHeaders, Item};
use crate::managed::{Counted, OutcomeError, Quantities};
use crate::services::outcome::{DiscardReason, Outcome};

mod integrity;
mod nel;
mod otel;
mod vercel;

pub use integrity::IntegrityLogsIntegrationProcessor;
pub use nel::NelLogsIntegrationProcessor;
pub use otel::OtelLogsIntegrationProcessor;
pub use vercel::VercelLogsIntegrationProcessor;

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// The log is invalid.
    #[error("invalid: {0}")]
    Invalid(DiscardReason),
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<crate::services::outcome::Outcome>, Self::Error) {
        let outcome = match self {
            Self::Invalid(discard_reason) => Some(Outcome::Invalid(discard_reason)),
        };

        (outcome, self)
    }
}

#[derive(Debug)]
pub struct SerializedLogsIntegration {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// The first log integration item in the envelope.
    item: Item,
}

impl SerializedLogsIntegration {
    fn count(&self) -> usize {
        self.item.item_count().unwrap_or(1) as usize
    }

    fn bytes(&self) -> usize {
        self.item.len()
    }
}

impl Counted for SerializedLogsIntegration {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![
            (DataCategory::LogItem, self.count()),
            (DataCategory::LogByte, self.bytes())
        ]
    }
}
