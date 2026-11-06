use relay_event_schema::protocol::ourlog::container::{ContainerMetadata, IngestSettings};
use relay_quotas::DataCategory;

use crate::Envelope;
use crate::envelope::{
    EnvelopeHeaders, IntegrationItem, ItemContainer, ItemPayload, LogItem, WithHeader,
};
use crate::managed::{Counted, OutcomeError, Quantities};
use crate::processing::Intermediates;
use crate::services::outcome::{DiscardReason, Outcome};

mod browser_reports;
mod heroku;
mod nel;
mod otel;
mod vercel;

pub use browser_reports::BrowserReportLogsIntegrationProcessor;
pub use heroku::HerokuLogsIntegrationProcessor;
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

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match self {
            Self::Invalid(discard_reason) => Some(Outcome::Invalid(discard_reason)),
        };

        (outcome, self)
    }
}

#[derive(Debug)]
pub struct SerializedLogsIntegration {
    /// Original integration envelope headers.
    headers: EnvelopeHeaders,
    /// The first log integration item in the envelope.
    item: IntegrationItem,
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
    headers: EnvelopeHeaders,
    logs: Vec<WithHeader<relay_event_schema::protocol::OurLog>>,
    settings: IngestSettings,
) -> Result<Intermediates, Error> {
    let metadata = ContainerMetadata {
        version: Some(2),
        ingest_settings: Some(settings),
    };

    let mut log_container = LogItem::new();
    ItemContainer::from_parts(metadata, logs)
        .write_to(&mut log_container)
        .map_err(|_| Error::Invalid(DiscardReason::Internal))?;

    let mut envelope = Envelope::from_request(None, headers.meta().clone());
    envelope.add_item(log_container);

    Ok(Intermediates::from_envelope(envelope))
}
