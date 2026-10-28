use relay_event_schema::protocol::ourlog::container::{ContainerMetadata, IngestSettings};
use relay_quotas::DataCategory;

use crate::Envelope;
use crate::envelope::{Item, ItemContainer, ItemType, WithHeader};
use crate::extractors::RequestMeta;
use crate::managed::{Counted, OutcomeError, Quantities};
use crate::processing::Intermediates;
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
    /// Original request metadata.
    meta: RequestMeta,
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

fn create_intermediates(
    meta: crate::extractors::RequestMeta,
    logs: Vec<WithHeader<relay_event_schema::protocol::OurLog>>,
    settings: IngestSettings,
) -> Result<Intermediates, Error> {
    let metadata = ContainerMetadata {
        version: Some(2),
        ingest_settings: Some(settings),
    };

    let mut log_container = Item::new(ItemType::Log);
    ItemContainer::from_parts(metadata, logs)
        .write_to(&mut log_container)
        .map_err(|_| Error::Invalid(DiscardReason::Internal))?;

    let mut envelope = Envelope::from_request(None, meta);
    envelope.add_item(log_container);

    let mut intermediates = Intermediates::default();
    intermediates.add_envelope(envelope);

    Ok(intermediates)
}
