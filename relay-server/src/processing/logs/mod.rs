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
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult as _, OutcomeError, Quantities,
};
use crate::processing::{
    self, Context, Forward, Output, QuotaRateLimiter, RateLimited, RateLimiter, Rejected,
};
use crate::services::outcome::{DiscardReason, Outcome};

mod filter;
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

/// A processor for Logs.
///
/// It processes items of type: [`ItemType::OtelLog`] and [`ItemType::Log`].
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

        let otel_logs = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::OtelLog))
            .into_vec();
        let logs = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Log))
            .into_vec();

        let work = SerializedLogs {
            headers,
            otel_logs,
            logs,
        };
        Some(Managed::from_envelope(envelope, work))
    }

    async fn process(
        &self,
        mut logs: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Error>> {
        validate::container(&logs)?;

        if ctx.is_proxy() {
            // If running in proxy mode, just apply cached rate limits and forward without
            // processing.
            //
            // Static mode needs processing, as users can override project settings manually.
            self.limiter.enforce_quotas(&mut logs, ctx).await?;
            return Ok(Output::just(LogOutput::NotProcessed(logs)));
        }

        // Fast filters, which do not need expanded logs.
        filter::feature_flag(ctx).reject(&logs)?;

        let mut logs = process::expand(logs, ctx);
        process::normalize(&mut logs);
        filter::filter(&mut logs, ctx);
        process::scrub(&mut logs, ctx);

        self.limiter.enforce_quotas(&mut logs, ctx).await?;

        Ok(Output::just(LogOutput::Processed(logs)))
    }
}

/// Output produced by [`LogsProcessor`].
#[derive(Debug)]
pub enum LogOutput {
    NotProcessed(Managed<SerializedLogs>),
    Processed(Managed<ExpandedLogs>),
}

impl Forward for LogOutput {
    fn serialize_envelope(self) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        let logs = match self {
            Self::NotProcessed(logs) => logs,
            Self::Processed(logs) => logs.try_map(|logs, r| {
                r.lenient(DataCategory::LogByte);
                logs.serialize()
                    .map_err(drop)
                    .with_outcome(Outcome::Invalid(DiscardReason::Internal))
            })?,
        };

        Ok(logs.map(|logs, r| {
            r.lenient(DataCategory::LogByte);
            logs.serialize_envelope()
        }))
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: &relay_system::Addr<crate::services::store::Store>,
    ) -> Result<(), Rejected<()>> {
        let logs = match self {
            LogOutput::NotProcessed(logs) => {
                return Err(logs.internal_error(
                    "logs must be processed before they can be forwarded to the store",
                ));
            }
            LogOutput::Processed(logs) => logs,
        };

        let scoping = logs.scoping();
        let received_at = logs.received_at();

        let (logs, retention) = logs.split_with_context(|logs| (logs.logs, logs.retention));
        let ctx = store::Context {
            scoping,
            received_at,
            retention,
        };

        for log in logs {
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

    /// OTel Logs are not sent in containers, an envelope is very likely to contain multiple OTel logs.
    otel_logs: Vec<Item>,
    /// Logs are sent in item containers, there is specified limit of a single container per
    /// envelope.
    ///
    /// But at this point this has not yet been validated.
    logs: Vec<Item>,
}

impl SerializedLogs {
    fn serialize_envelope(self) -> Box<Envelope> {
        let mut items = self.logs;
        items.extend(self.otel_logs);
        Envelope::from_parts(self.headers, Items::from_vec(items))
    }

    fn items(&self) -> impl Iterator<Item = &Item> {
        self.otel_logs.iter().chain(self.logs.iter())
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

impl RateLimited for Managed<SerializedLogs> {
    type Error = Error;

    async fn enforce<T>(
        &mut self,
        mut rate_limiter: T,
        _ctx: Context<'_>,
    ) -> Result<(), Rejected<Self::Error>>
    where
        T: RateLimiter,
    {
        let scoping = self.scoping();

        let items = rate_limiter
            .try_consume(scoping.item(DataCategory::LogItem), self.count())
            .await;
        let bytes = rate_limiter
            .try_consume(scoping.item(DataCategory::LogByte), self.bytes())
            .await;

        let limits = items.merge_with(bytes);
        if !limits.is_empty() {
            return Err(self.reject_err(Error::RateLimited(limits)));
        }

        Ok(())
    }
}

/// Logs which have been parsed and expanded from their serialized state.
#[derive(Debug)]
pub struct ExpandedLogs {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// Expanded and parsed logs.
    logs: ContainerItems<OurLog>,

    // These fields are currently necessary as we don't pass any project config context to the
    // store serialization. The plan is to get rid of them by giving the serialization context,
    // including the project info, where these are pulled from: #4878.
    /// Retention in days.
    #[cfg(feature = "processing")]
    retention: Option<u16>,
}

impl Counted for ExpandedLogs {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![
            (DataCategory::LogItem, self.logs.len()),
            (DataCategory::LogByte, self.bytes())
        ]
    }
}

impl ExpandedLogs {
    /// Returns the total count of all logs contained.
    fn count(&self) -> usize {
        self.logs.len()
    }

    /// Returns the sum of bytes of all logs contained.
    fn bytes(&self) -> usize {
        self.logs.iter().map(get_calculated_byte_size).sum()
    }

    fn serialize(self) -> Result<SerializedLogs, ContainerWriteError> {
        let mut logs = Vec::new();

        if !self.logs.is_empty() {
            let mut item = Item::new(ItemType::Log);
            ItemContainer::from(self.logs)
                .write_to(&mut item)
                .inspect_err(|err| relay_log::error!("failed to serialize logs: {err}"))?;
            logs.push(item);
        }

        Ok(SerializedLogs {
            headers: self.headers,
            otel_logs: Default::default(),
            logs,
        })
    }
}

impl RateLimited for Managed<ExpandedLogs> {
    type Error = Error;

    async fn enforce<T>(
        &mut self,
        mut rate_limiter: T,
        _ctx: Context<'_>,
    ) -> Result<(), Rejected<Self::Error>>
    where
        T: RateLimiter,
    {
        let scoping = self.scoping();

        let items = rate_limiter
            .try_consume(scoping.item(DataCategory::LogItem), self.count())
            .await;
        let bytes = rate_limiter
            .try_consume(scoping.item(DataCategory::LogByte), self.bytes())
            .await;

        let limits = items.merge_with(bytes);
        if !limits.is_empty() {
            return Err(self.reject_err(Error::RateLimited(limits)));
        }

        Ok(())
    }
}
