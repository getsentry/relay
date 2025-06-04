use relay_quotas::{DataCategory, RateLimits};

use crate::envelope::{ItemType, Items};
use crate::processing::{
    self, Context, Counted, Managed, Output, Quantities, QuotaRateLimiter, RateLimited, RateLimiter,
};
use crate::services::processor::ProcessingError;
use crate::utils::ManagedEnvelope;

mod filter;

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
    type Output = ();
    type Error = ProcessingError;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let otel_logs = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::OtelLog));
        let logs = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Log));

        // if !logs_items.is_empty() {
        //     grouped_envelopes.push((
        //         ProcessingGroup::Log,
        //         Envelope::from_parts(headers.clone(), logs_items),
        //     ))
        // }

        let work = EinsLog { otel_logs, logs };
        Some(Managed::from_envelope(envelope, work))
    }

    async fn process(
        &self,
        work: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<()>, ProcessingError> {
        filter::feature_flag(ctx)?;
        filter::sampled(ctx)?;

        // ourlog::filter(
        //     managed_envelope,
        //     &self.inner.config,
        //     &project_info,
        //     &self.inner.global_config.current(),
        // );

        self.limiter.enforce_quotas(work.as_ref(), ctx);

        //
        // self.enforce_quotas(
        //     managed_envelope,
        //     Annotated::empty(),
        //     &mut extracted_metrics,
        //     &project_info,
        //     &rate_limits,
        // )
        // .await?;
        //
        // if_processing!(self.inner.config, {
        //     ourlog::process(managed_envelope, &project_info)?;
        // });

        Ok(Output::just(()))
    }
}

struct EinsLog {
    /// Otel Logs are not sent in containers, an envelope is very likely to contain multiple otel logs.
    otel_logs: Items,
    /// Logs are sent in item containers, there is specified limit of a single container per
    /// envelope.
    ///
    /// But at this point this has not yet been validated.
    // TODO: validation, that there is only a single log item (for now).
    logs: Items,
}

impl Counted for EinsLog {
    fn quantities(&self) -> Quantities {
        todo!()
    }
}

impl RateLimited for Managed<EinsLog> {
    type Error = ProcessingError;

    async fn enforce<T>(
        &mut self,
        rate_limiter: T,
        ctx: Context<'_>,
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
