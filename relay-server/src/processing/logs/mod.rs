use std::sync::Arc;

use relay_event_schema::processor::ProcessingAction;
use relay_event_schema::protocol::OurLog;
use relay_filter::FilterStatKey;
use relay_pii::PiiConfigError;
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
use crate::services::outcome::{DiscardReason, Outcome};

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
    /// A duplicated item container for logs.
    #[error("duplicate log container")]
    DuplicateContainer,
    /// Logs filtered because of a missing feature flag.
    #[error("logs feature flag missing")]
    FilterFeatureFlag,
    /// Logs filtered due to a filtering rule.
    #[error("log filtered")]
    Filtered(FilterStatKey),
    /// The logs are rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),
    /// Internal error, Pii config could not be loaded.
    #[error("Pii configuration error")]
    PiiConfig(PiiConfigError),
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
            Self::DuplicateContainer => Some(Outcome::Invalid(DiscardReason::DuplicateItem)),
            Self::FilterFeatureFlag => None,
            Self::Filtered(f) => Some(Outcome::Filtered(f.clone())),
            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
            Self::PiiConfig(_) => Some(Outcome::Invalid(DiscardReason::ProjectStatePii)),
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
    type UnitOfWork = SerializedLogs;
    type Output = LogOutput;
    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let headers = envelope.envelope().headers().clone();

        let logs = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Log))
            .into_vec();

        // TODO: there might be a better way to extract an item and its integration type type safe.
        // So later we don't have a fallible conversion for the integration.
        //
        // Maybe a 2 phase thing where we take items, then grab the integration again and debug
        // assert + return if the impossible happens.
        let integrations = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(item.integration(), Some(Integration::Logs(_))))
            .into_vec();

        let work = SerializedLogs {
            headers,
            logs,
            integrations,
        };
        Some(Managed::from_envelope(envelope, work))
    }

    async fn process(
        &self,
        logs: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Error>> {
        validate::container(&logs).reject(&logs)?;

        // Fast filters, which do not need expanded logs.
        filter::feature_flag(ctx).reject(&logs)?;

        let mut logs = process::expand(logs);
        process::normalize(&mut logs);
        filter::filter(&mut logs, ctx);

        self.limiter.enforce_quotas(&mut logs, ctx).await?;

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
            logs.serialize()
                .map_err(drop)
                .with_outcome(Outcome::Invalid(DiscardReason::Internal))
        })
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: &relay_system::Addr<crate::services::store::Store>,
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
                s.send(log)
            };
        }

        Ok(())
    }
}

/// Logs in their serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedLogs {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// Logs are sent in item containers, there is specified limit of a single container per
    /// envelope.
    ///
    /// But at this point this has not yet been validated.
    logs: Vec<Item>,

    /// Logs which Relay received from arbitrary integrations.
    integrations: Vec<Item>,
}

impl SerializedLogs {
    fn items(&self) -> impl Iterator<Item = &Item> {
        self.logs.iter().chain(self.integrations.iter())
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

/// Logs which have been parsed and expanded from their serialized state.
#[derive(Debug)]
pub struct ExpandedLogs {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
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
    fn serialize(self) -> Result<Box<Envelope>, ContainerWriteError> {
        let mut logs = Vec::new();

        if !self.logs.is_empty() {
            let mut item = Item::new(ItemType::Log);
            ItemContainer::from(self.logs)
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
