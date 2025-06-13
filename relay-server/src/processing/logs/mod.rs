use std::sync::Arc;

use relay_event_schema::processor::ProcessingAction;
use relay_event_schema::protocol::OurLog;
use relay_pii::PiiConfigError;
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{
    ContainerItems, ContainerWriteError, EnvelopeHeaders, Item, ItemContainer, ItemType, Items,
};
use crate::processing::{
    self, Context, Counted, Forward, Managed, ManagedResult as _, OutcomeError, Output, Quantities,
    QuotaRateLimiter, RateLimited, RateLimiter, Rejected, if_processing,
};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::ProcessingError;
use crate::utils::ManagedEnvelope;

mod filter;
#[cfg(feature = "processing")]
mod process;
mod validate;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("")]
    DuplicateContainer,
    /// Events filtered because of a missing feature flag.
    #[error("")]
    FilterFeatureFlag,
    /// Events filtered either due to a global sampling rule.
    #[error("")]
    FilterSampling,
    #[error("")]
    RateLimited(RateLimits),
    #[error("")]
    PiiConfigError(PiiConfigError),
    #[error("envelope processor failed")]
    ProcessingFailed(#[from] ProcessingAction),
    #[error("")]
    Invalid(DiscardReason),
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::DuplicateContainer => Some(Outcome::Invalid(DiscardReason::DuplicateItem)),
            Self::FilterFeatureFlag => None,
            Self::FilterSampling => None,
            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
            Self::PiiConfigError(_) => Some(Outcome::Invalid(DiscardReason::ProjectStatePii)),
            Self::ProcessingFailed(_) => Some(Outcome::Invalid(DiscardReason::Internal)),
            Self::Invalid(reason) => Some(Outcome::Invalid(*reason)),
        };

        (outcome, self)
    }
}

impl From<Error> for ProcessingError {
    fn from(value: Error) -> Self {
        todo!()
    }
}

pub struct LogsProcessor {
    limiter: Arc<QuotaRateLimiter>,
}

impl LogsProcessor {
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
            .take_items_by(|item| matches!(*item.ty(), ItemType::OtelLog));
        let logs = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Log));

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
        validate::container(&logs, ctx)?;
        filter::feature_flag(ctx).reject(&logs)?;

        filter::sampled(ctx).reject(&logs)?;

        self.limiter.enforce_quotas(&mut logs, ctx).await?;

        if_processing!(ctx, {
            let mut logs = process::expand(logs);
            process::process(&mut logs, ctx);

            Ok(Output::just(logs.into()))
        } else {
            Ok(Output::just(logs.into()))
        })
    }
}

// TODO: `Items` is seemingly massive (smallvec of Item)
#[expect(clippy::large_enum_variant)]
pub enum LogOutput {
    NotProcessed(Managed<SerializedLogs>),
    Processed(Managed<ExpandedLogs>),
}

impl From<Managed<SerializedLogs>> for LogOutput {
    fn from(value: Managed<SerializedLogs>) -> Self {
        Self::NotProcessed(value)
    }
}

impl From<Managed<ExpandedLogs>> for LogOutput {
    fn from(value: Managed<ExpandedLogs>) -> Self {
        Self::Processed(value)
    }
}

impl Forward for LogOutput {
    fn serialize_envelope(self) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        let logs = match self {
            Self::NotProcessed(logs) => logs,
            Self::Processed(logs) => logs.try_map(|logs, _| {
                logs.serialize()
                    .map_err(drop)
                    .with_outcome(Outcome::Invalid(DiscardReason::Internal))
            })?,
        };

        Ok(logs.map(|logs, _| logs.serialize_envelope()))
    }
}

/// Logs in their serialized state, as transported in an envelope.
pub struct SerializedLogs {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// Otel Logs are not sent in containers, an envelope is very likely to contain multiple otel logs.
    otel_logs: Items,
    /// Logs are sent in item containers, there is specified limit of a single container per
    /// envelope.
    ///
    /// But at this point this has not yet been validated.
    // TODO: validation, that there is only a single log item (for now).
    logs: Items,
}

impl SerializedLogs {
    fn serialize_envelope(self) -> Box<Envelope> {
        let mut items = self.logs;
        items.extend(self.otel_logs);
        Envelope::from_parts(self.headers, items)
    }

    fn items(&self) -> impl Iterator<Item = &Item> {
        self.otel_logs.iter().chain(self.logs.iter())
    }

    fn count(&self) -> usize {
        self.items()
            .map(|item| item.item_count().unwrap_or(1) as usize)
            .sum()
    }
}

impl Counted for SerializedLogs {
    fn quantities(&self) -> Quantities {
        let bytes = self.items().map(|item| item.len()).sum();

        smallvec::smallvec![
            (DataCategory::LogItem, self.count()),
            (DataCategory::LogByte, bytes)
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
        // TODO: indexed/non-indexed categories
        // TODO: does quantities then need something for rate limits as well?
        // This seems very error prone to use item/drop quantities here.
        // for (category, count) in self.quantities() {
        //     checker.try_consume(self.scoping.item(category), count, false);
        // }
        let scoping = self.scoping();

        let items = rate_limiter
            .try_consume(scoping.item(DataCategory::LogItem), self.logs.len())
            .await;
        let bytes = rate_limiter
            // TODO: byte count needs to be fixed, eventually
            .try_consume(scoping.item(DataCategory::LogByte), 100)
            .await;
        let total_limits = items.merge_with(bytes);

        if !total_limits.is_empty() {
            return Err(self.reject_err(Error::RateLimited(total_limits)));
        }

        Ok(())
    }
}

/// Logs which have been parsed and expanded from their serialized state.
pub struct ExpandedLogs {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// Expanded and parsed logs.
    logs: ContainerItems<OurLog>,
}

impl Counted for ExpandedLogs {
    fn quantities(&self) -> Quantities {
        // TODO: bytes are missing here
        smallvec::smallvec![(DataCategory::LogItem, 1)]
    }
}

impl ExpandedLogs {
    fn serialize(self) -> Result<SerializedLogs, ContainerWriteError> {
        let mut item = Item::new(ItemType::Log);

        ItemContainer::from(self.logs)
            .write_to(&mut item)
            .inspect_err(|err| relay_log::debug!("failed to serialize logs: {err}"))?;

        Ok(SerializedLogs {
            headers: self.headers,
            otel_logs: Default::default(),
            logs: smallvec::smallvec![item],
        })
    }
}
