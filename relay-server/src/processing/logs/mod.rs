use std::sync::Arc;

use relay_event_schema::processor::ProcessingAction;
use relay_event_schema::protocol::{OurLog, ourlog};
use relay_filter::FilterStatKey;
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{
    ContainerItems, ContainerWriteError, EnvelopeHeaders, Item, ItemContainer, ItemType, Items,
};
use crate::integrations::Integration;
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult as _, OutcomeError, Quantities,
};
use crate::processing::{
    self, Context, CountRateLimited, Forward, Output, QuotaRateLimiter, Rejected,
};
use crate::services::outcome::{DiscardItemType, DiscardReason, Outcome};

mod filter;
mod integrations;
mod process;
#[cfg(feature = "processing")]
mod store;
mod utils;
mod validate;

pub use self::utils::get_calculated_byte_size;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Multiple item containers and mixed log items are not allowed to be in the same envelope.
    #[error("duplicate or mixed log items in the same envelope")]
    DuplicateItem,
    /// Received log exceeds the configured size limit.
    #[error("log exeeds size limit")]
    TooLarge,
    /// Logs filtered because of a missing feature flag.
    #[error("logs feature flag missing")]
    FilterFeatureFlag,
    /// Logs filtered due to a filtering rule.
    #[error("log filtered")]
    Filtered(FilterStatKey),
    /// The logs are rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),
    /// A processor failed to process the logs.
    #[error("envelope processor failed")]
    ProcessingFailed(#[from] ProcessingAction),
    /// The log is invalid.
    #[error("invalid: {0}")]
    Invalid(DiscardReason),
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::DuplicateItem => Some(Outcome::Invalid(DiscardReason::DuplicateItem)),
            Self::TooLarge => Some(Outcome::Invalid(DiscardReason::TooLarge(
                DiscardItemType::Log,
            ))),
            Self::FilterFeatureFlag => None,
            Self::Filtered(f) => Some(Outcome::Filtered(f.clone())),
            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
            Self::ProcessingFailed(_) => Some(Outcome::Invalid(DiscardReason::Internal)),
            Self::Invalid(reason) => Some(Outcome::Invalid(*reason)),
        };

        (outcome, self)
    }
}

impl From<RateLimits> for Error {
    fn from(value: RateLimits) -> Self {
        Self::RateLimited(value)
    }
}

/// A processor for Logs.
///
/// It processes items of type: [`ItemType::Log`].
#[derive(Debug)]
pub struct LogsProcessor {
    limiter: Arc<QuotaRateLimiter>,
}

impl LogsProcessor {
    /// Creates a new [`Self`].
    pub fn new(limiter: Arc<QuotaRateLimiter>) -> Self {
        Self { limiter }
    }
}

impl processing::Processor for LogsProcessor {
    type Input = SerializedLogs;
    type Output = LogOutput;
    type Error = Error;

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let headers = envelope.envelope().headers().clone();

        let items = if let Some(container) = envelope
            .envelope_mut()
            .take_item_by(|item| matches!(*item.ty(), ItemType::Log))
        {
            LogItems::Container(container)
        } else if let Some(integration) = envelope
            .envelope_mut()
            .take_item_by(|item| matches!(item.integration(), Some(Integration::Logs(_))))
        {
            LogItems::Integration(integration)
        } else {
            // No log items found.
            return None;
        };

        // Duplicates which are not allowed to be in the envelope.
        let invalid = envelope
            .envelope_mut()
            .take_items_by(|item| {
                matches!(item.ty(), ItemType::Log)
                    || matches!(item.integration(), Some(Integration::Logs(_)))
            })
            .to_vec();

        let work = SerializedLogs {
            headers,
            items,
            invalid,
        };
        Some(Managed::with_meta_from(envelope, work))
    }

    async fn process(
        &self,
        logs: Managed<Self::Input>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Error>> {
        validate::invalid(&logs).reject(&logs)?;
        validate::dsc(&logs);

        // Fast filters, which do not need expanded logs.
        filter::feature_flag(ctx).reject(&logs)?;

        let mut logs = process::expand(logs)?;

        validate::size(&mut logs, ctx);

        process::normalize(&mut logs, ctx);
        filter::filter(&mut logs, ctx);

        let mut logs = self.limiter.enforce_quotas(logs, ctx).await?;

        process::scrub(&mut logs, ctx);

        Ok(Output::just(LogOutput(logs)))
    }
}

/// Output produced by [`LogsProcessor`].
#[derive(Debug)]
pub struct LogOutput(Managed<ExpandedLogs>);

impl Forward for LogOutput {
    fn serialize_envelope(
        self,
        _: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        self.0.try_map(|logs, r| {
            r.lenient(DataCategory::LogByte);
            logs.serialize_envelope()
                .map_err(drop)
                .with_outcome(Outcome::Invalid(DiscardReason::Internal))
        })
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: processing::StoreHandle<'_>,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let Self(logs) = self;

        let ctx = store::Context {
            scoping: logs.scoping(),
            received_at: logs.received_at(),
            retention: ctx.retention(|r| r.log.as_ref()),
        };

        for log in logs.split(|logs| logs.logs) {
            if let Ok(log) = log.try_map(|log, _| store::convert(log, &ctx)) {
                s.send_to_store(log)
            };
        }

        Ok(())
    }
}

/// Different log containers which can be expanded into logs.
#[derive(Debug)]
enum LogItems {
    /// A log container.
    Container(Item),
    /// A log integration item.
    Integration(Item),
}

/// Logs in their serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedLogs {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// Serialized logs.
    items: LogItems,

    /// Invalid log items which are not allowed to be in the envelope.
    invalid: Vec<Item>,
}

impl SerializedLogs {
    fn items(&self) -> impl Iterator<Item = &Item> {
        self.invalid.iter().chain(match &self.items {
            LogItems::Container(item) => Some(item),
            LogItems::Integration(item) => Some(item),
        })
    }

    /// Returns the total count of all logs contained.
    ///
    /// This contains all logical log items, not just envelope items and is safe
    /// to use for rate limiting.
    fn count(&self) -> usize {
        self.items()
            .map(|item| item.item_count().unwrap_or(1) as usize)
            .sum()
    }

    /// Returns the sum of bytes of all logs contained.
    fn bytes(&self) -> usize {
        self.items().map(|item| item.len()).sum()
    }
}

impl Counted for SerializedLogs {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![
            (DataCategory::LogItem, self.count()),
            (DataCategory::LogByte, self.bytes())
        ]
    }
}

impl CountRateLimited for Managed<SerializedLogs> {
    type Error = Error;
}

/// Settings controlling logs normalization.
#[derive(Debug, Default, Copy, Clone)]
struct Settings {
    /// Whether the ip address should be inferred from the client connection.
    infer_ip: bool,
    /// Whether the user agent/browser should inferred from client headers.
    infer_user_agent: bool,
}

/// Logs which have been parsed and expanded from their serialized state.
#[derive(Debug)]
pub struct ExpandedLogs {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// Client/protocol supplied settings controlling how logs should be normalized.
    settings: Settings,
    /// Expanded and parsed logs.
    logs: ContainerItems<OurLog>,
}

impl Counted for ExpandedLogs {
    fn quantities(&self) -> Quantities {
        let count = self.logs.len();
        let bytes = self.logs.iter().map(get_calculated_byte_size).sum();

        smallvec::smallvec![
            (DataCategory::LogItem, count),
            (DataCategory::LogByte, bytes)
        ]
    }
}

impl ExpandedLogs {
    fn serialize_envelope(self) -> Result<Box<Envelope>, ContainerWriteError> {
        let mut logs = Vec::new();

        if !self.logs.is_empty() {
            let mut item = Item::new(ItemType::Log);
            ItemContainer::from_parts(
                ourlog::container::ContainerMetadata {
                    // Latest supported version.
                    version: Some(2),
                    // Nothing to do for the next Relay.
                    ingest_settings: None,
                },
                self.logs,
            )
            .write_to(&mut item)
            .inspect_err(|err| relay_log::error!("failed to serialize logs: {err}"))?;
            logs.push(item);
        }

        Ok(Envelope::from_parts(self.headers, Items::from_vec(logs)))
    }
}

impl CountRateLimited for Managed<ExpandedLogs> {
    type Error = Error;
}
