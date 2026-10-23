use relay_cogs::AppFeature;
use relay_conventions::attributes::SENTRY__RELAY__INGRESS;
use relay_event_normalization::eap::Ingress;
use relay_event_schema::protocol::ourlog::container::ContainerMetadata;
use relay_event_schema::protocol::{OurLog, OurLogHeader};
use relay_quotas::DataCategory;

use crate::Envelope;
use crate::envelope::{EnvelopeHeaders, Item, ItemContainer, ItemType, WithHeader};
use crate::integrations::{Integration, LogsIntegration};
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult, OutcomeError, Quantities, Rejected,
};
use crate::processing::{self, Forward, Output};
use crate::services::outcome::{DiscardReason, Outcome};

mod integrity;
mod nel;
mod otel;
mod vercel;

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// Multiple log integrations are not allowed to be in the same envelope.
    #[error("duplicate or mixed log items in the same envelope")]
    DuplicateItem,

    #[error("Expected a logs integration, got: {0:?}")]
    UnexpectedIntegration(Option<Integration>),

    /// The log is invalid.
    #[error("invalid: {0}")]
    Invalid(DiscardReason),
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<crate::services::outcome::Outcome>, Self::Error) {
        let outcome = match self {
            Self::DuplicateItem => Some(Outcome::Invalid(DiscardReason::DuplicateItem)),
            Self::UnexpectedIntegration(_integration) => {
                Some(Outcome::Invalid(DiscardReason::Internal))
            }
            Self::Invalid(discard_reason) => Some(Outcome::Invalid(discard_reason)),
        };

        (outcome, self)
    }
}

/// A processor for Logs Integrations.
///
/// This processor only produces intermediates (items
/// that are put back into the processing pipeline) by
/// converting log integration items into actual logs. It
/// never passes anything on to the store or another Relay.
#[derive(Debug)]
pub struct LogsIntegrationsProcessor {}

impl LogsIntegrationsProcessor {
    /// Creates a new [`Self`].
    pub fn new() -> Self {
        Self {}
    }
}

/// The "output" of a [`LogsIntegrationsProcessor`].
///
/// This type is uninhabited to enforce that the processor
/// can only produce intermediates.
#[derive(Debug, Clone, Copy)]
pub enum LogsIntegrationOutput {}

impl Forward for LogsIntegrationOutput {
    fn serialize_envelope(
        self,
        _ctx: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<crate::Envelope>>, Rejected<()>> {
        match self {}
    }

    fn forward_store(
        self,
        _s: processing::StoreHandle<'_>,
        _ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        match self {}
    }
}

#[derive(Debug)]
pub struct SerializedLogsIntegration {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// The first log integration item in the envelope.
    item: Item,
    /// Invalid log integration items which are not allowed to be in the envelope.
    invalid: Vec<Item>,
}

impl SerializedLogsIntegration {
    fn items(&self) -> impl Iterator<Item = &Item> {
        self.invalid.iter().chain(Some(&self.item))
    }

    fn count(&self) -> usize {
        self.items()
            .map(|item| item.item_count().unwrap_or(1) as usize)
            .sum()
    }

    fn bytes(&self) -> usize {
        self.items().map(|item| item.len()).sum()
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

impl processing::Processor for LogsIntegrationsProcessor {
    type Input = SerializedLogsIntegration;

    type Output = LogsIntegrationOutput;

    type Error = Error;

    fn cogs() -> relay_cogs::FeatureWeights {
        AppFeature::Logs.into()
    }

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let headers = envelope.envelope().headers().clone();

        let item = envelope
            .envelope_mut()
            .take_item_by(|item| matches!(item.integration(), Some(Integration::Logs(_))))?;

        // Duplicates which are not allowed to be in the envelope.
        let invalid = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(item.integration(), Some(Integration::Logs(_))))
            .to_vec();

        let work = SerializedLogsIntegration {
            headers,
            item,
            invalid,
        };
        Some(Managed::with_meta_from_managed_envelope(envelope, work))
    }

    async fn process(
        &self,
        work: Managed<Self::Input>,
        _ctx: processing::Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        invalid(&work).reject(&work)?;
        let intermediates = expand(work)?;

        Ok(Output {
            main: None,
            metrics: None,
            intermediates: Some(intermediates),
        })
    }
}

/// Validates that there is only a single log integration processed at a time.
pub fn invalid(logs: &Managed<SerializedLogsIntegration>) -> Result<(), Error> {
    if !logs.invalid.is_empty() {
        return Err(Error::DuplicateItem);
    }

    Ok(())
}

/// Expands a log [`Integration`] into an envelope of logs that can be
/// returned to the processing pipeline.
///
/// The function expects *only* log item integrations.
pub fn expand(
    work: Managed<SerializedLogsIntegration>,
) -> Result<ManagedEnvelope, Rejected<Error>> {
    let logs_envelope = work.try_map(|work, records| {
        let SerializedLogsIntegration {
            mut headers,
            item,
            invalid,
        } = work;

        debug_assert!(
            invalid.is_empty(),
            "invalid items should already be rejected"
        );

        // Log byte counts will change here as we go from an estimated count based on the body, to
        // accurately counted bytes.
        records.lenient(DataCategory::LogByte);

        let integration = match item.integration() {
            Some(Integration::Logs(integration)) => integration,
            integration => return Err(Error::UnexpectedIntegration(integration)),
        };

        let mut logs = Vec::new();
        let produce = |mut log: OurLog| {
            let byte_size = relay_ourlogs::calculate_size(&log);

            records.modify_by(DataCategory::LogItem, 1);
            records.modify_by(DataCategory::LogByte, byte_size as isize);

            // Set the `ingress` attribute. The logs processor will not overwrite it.
            log.attributes
                .value_mut()
                .get_or_insert_default()
                .insert(SENTRY__RELAY__INGRESS, Ingress::Integration.to_string());

            logs.push(WithHeader {
                header: Some(OurLogHeader {
                    byte_size: Some(byte_size),
                    other: Default::default(),
                }),
                value: log.into(),
            });
        };

        let payload = item.payload();

        let settings = match integration {
            LogsIntegration::Nel => nel::expand(&payload, &headers, produce)?,
            LogsIntegration::Integrity => integrity::expand(&payload, &headers, produce)?,
            LogsIntegration::OtelV1 { format } => otel::expand(format, &payload, produce)?,
            LogsIntegration::VercelDrainLog { format } => {
                vercel::expand(format, &payload, produce)?
            }
        };

        let metadata = ContainerMetadata {
            version: Some(2),
            ingest_settings: Some(settings),
        };

        let mut log_container = Item::new(ItemType::Log);
        ItemContainer::from_parts(metadata, logs)
            .write_to(&mut log_container)
            .map_err(|_| Error::Invalid(DiscardReason::Internal))?;

        // The envelope comes from a trusted Relay (this one).
        headers
            .meta_mut()
            .set_request_trust(crate::extractors::RequestTrust::Trusted);

        Ok(Envelope::from_parts(
            headers,
            smallvec::smallvec![log_container],
        ))
    })?;

    Ok(ManagedEnvelope::from(logs_envelope))
}
