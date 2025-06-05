use relay_event_schema::protocol::OurLog;
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{ContainerItems, EnvelopeHeaders, Item, ItemContainer, ItemType, Items};
use crate::processing::{
    self, Context, Counted, Forward, Managed, Output, Quantities, QuotaRateLimiter, RateLimited,
    RateLimiter, if_processing,
};
use crate::services::processor::ProcessingError;
use crate::utils::ManagedEnvelope;

mod filter;
#[cfg(feature = "processing")]
mod process;
mod validate;

pub struct LogsProcessor {
    limiter: QuotaRateLimiter,
}

impl LogsProcessor {
    pub fn new() -> Self {
        Self {
            limiter: QuotaRateLimiter::default(),
        }
    }
}

impl processing::Processor for LogsProcessor {
    type UnitOfWork = EinsLog;
    type Output = LogOutput;
    type Error = ProcessingError;

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

        let work = EinsLog {
            headers,
            otel_logs,
            logs,
        };
        Some(Managed::from_envelope(envelope, work))
    }

    async fn process(
        &self,
        mut work: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, ProcessingError> {
        // TODO: here are the wrong outcomes emitted (only through the Drop -> Invalid(Internal))
        // There could be a Rejected() error which can only be obtained from the Managed<>
        // And the trait only allows Rejected<T> to bubble up.
        validate::container(&work, ctx)?;

        filter::feature_flag(ctx)?;
        filter::sampled(ctx)?;

        self.limiter.enforce_quotas(&mut work, ctx).await?;

        if_processing!(ctx, {
            let mut work = process::expand(work)?;
            process::process(&mut work, ctx)?;

            Ok(Output::just(work.into()))
        } else {
            Ok(Output::just(work.into()))
        })
    }
}

pub enum LogOutput {
    NotProcessed(Managed<EinsLog>),
    Processed(Managed<ZweiLog>),
}

impl From<Managed<EinsLog>> for LogOutput {
    fn from(value: Managed<EinsLog>) -> Self {
        Self::NotProcessed(value)
    }
}

impl From<Managed<ZweiLog>> for LogOutput {
    fn from(value: Managed<ZweiLog>) -> Self {
        Self::Processed(value)
    }
}

impl Forward for LogOutput {
    fn serialize_envelope(self) -> Result<Managed<Box<Envelope>>, ()> {
        let logs = match self {
            Self::NotProcessed(logs) => logs,
            Self::Processed(logs) => logs.try_map(|logs, _| logs.serialize())?,
        };

        Ok(logs.map(|logs, _| logs.serialize_envelope()))
    }
}

pub struct EinsLog {
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

impl EinsLog {
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

impl Counted for EinsLog {
    fn quantities(&self) -> Quantities {
        let bytes = self.items().map(|item| item.len()).sum();

        smallvec::smallvec![
            (DataCategory::LogItem, self.count()),
            (DataCategory::LogByte, bytes)
        ]
    }
}

impl RateLimited for Managed<EinsLog> {
    type Error = ProcessingError;

    async fn enforce<T>(
        &mut self,
        rate_limiter: T,
        _ctx: Context<'_>,
    ) -> Result<RateLimits, Self::Error>
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

        // TODO: maybe we need over-accept here?
        let items = rate_limiter
            .try_consume(scoping.item(DataCategory::LogItem), self.logs.len())
            .await;
        let bytes = rate_limiter
            .try_consume(scoping.item(DataCategory::LogByte), 100)
            .await;
        let total_limits = items.merge_with(bytes);

        // TODO: this check uses the current time, but maybe the 'checker' should only return
        // active limits (what currently is the case), then this check could be `is_empty()`.
        if total_limits.is_limited() {
            // TODO: we can discard the entire envelope here as rate limited
            // but in other cases we will have to discard single elements

            // TODO: Return an error here which will reject the data with the correct outcome.
            return Err(ProcessingError::NoEventPayload);
        }

        // TODO: how to enforce the limits here?

        Ok(total_limits)
    }
}

pub struct ZweiLog {
    headers: EnvelopeHeaders,
    logs: ContainerItems<OurLog>,
}

impl Counted for ZweiLog {
    fn quantities(&self) -> Quantities {
        // TODO: bytes are missing here
        smallvec::smallvec![(DataCategory::LogItem, 1)]
    }
}

impl ZweiLog {
    // TODO: error type
    fn serialize(self) -> Result<EinsLog, ()> {
        let mut item = Item::new(ItemType::Log);

        ItemContainer::from(self.logs)
            .write_to(&mut item)
            .inspect_err(|err| relay_log::debug!("failed to serialize logs: {err}"))
            .map_err(drop)?;

        Ok(EinsLog {
            headers: self.headers,
            otel_logs: Default::default(),
            logs: smallvec::smallvec![item],
        })
    }
}
