use std::sync::Arc;

use relay_event_schema::processor::ProcessingAction;
use relay_event_schema::protocol::TraceMetric;
use relay_filter::FilterStatKey;
use relay_pii::PiiConfigError;
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{ContainerItems, EnvelopeHeaders, Item, ItemType, Items};
use crate::envelope::{ContainerWriteError, ItemContainer};
use crate::managed::{Counted, Managed, ManagedEnvelope, ManagedResult as _, Quantities, Rejected};
use crate::processing::{self, Context, CountRateLimited, Forward, Output, QuotaRateLimiter};
use crate::services::outcome::{DiscardItemType, DiscardReason, Outcome};
use smallvec::smallvec;

mod filter;
mod process;
#[cfg(feature = "processing")]
mod store;
mod validate;

pub type Result<T, E = Error> = std::result::Result<T, E>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Internal error, Pii config could not be loaded.
    #[error("Pii configuration error")]
    PiiConfig(PiiConfigError),
    /// Received trace metric exceeds the configured size limit.
    #[error("trace metric exeeds size limit")]
    TooLarge,
    /// The trace metrics are rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),
    /// Trace metrics filtered because of a missing feature flag.
    #[error("trace metrics feature flag missing")]
    FilterFeatureFlag,
    /// Trace metrics filtered due to a filtering rule.
    #[error("trace metric filtered")]
    Filtered(FilterStatKey),
    /// A duplicated item container for trace metrics.
    #[error("duplicate trace metric container")]
    DuplicateContainer,
    /// A processor failed to process the trace metrics.
    #[error("envelope processor failed")]
    ProcessingFailed(#[from] ProcessingAction),
    /// The trace metric is invalid.
    #[error("invalid: {0}")]
    Invalid(DiscardReason),
}

impl From<RateLimits> for Error {
    fn from(value: RateLimits) -> Self {
        Self::RateLimited(value)
    }
}

impl crate::managed::OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::FilterFeatureFlag => None,
            Self::Filtered(f) => Some(Outcome::Filtered(f.clone())),
            Self::DuplicateContainer => Some(Outcome::Invalid(DiscardReason::DuplicateItem)),
            Self::TooLarge => Some(Outcome::Invalid(DiscardReason::TooLarge(
                DiscardItemType::TraceMetric,
            ))),
            Self::ProcessingFailed(_) => {
                relay_log::error!("internal error: trace metric processing failed");
                Some(Outcome::Invalid(DiscardReason::Internal))
            }
            Self::PiiConfig(_) => Some(Outcome::Invalid(DiscardReason::ProjectStatePii)),
            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
            Self::Invalid(reason) => Some(Outcome::Invalid(*reason)),
        };
        (outcome, self)
    }
}

/// A processor for trace metrics.
///
/// It processes items of type: [`ItemType::TraceMetric`].
#[derive(Debug)]
pub struct TraceMetricsProcessor {
    limiter: Arc<QuotaRateLimiter>,
}

impl TraceMetricsProcessor {
    /// Creates a new [`Self`].
    pub fn new(limiter: Arc<QuotaRateLimiter>) -> Self {
        Self { limiter }
    }
}

impl processing::Processor for TraceMetricsProcessor {
    type UnitOfWork = SerializedTraceMetrics;
    type Output = TraceMetricOutput;
    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let headers = envelope.envelope().headers().clone();

        let metrics = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::TraceMetric))
            .into_vec();

        if metrics.is_empty() {
            return None;
        }

        let work = SerializedTraceMetrics { headers, metrics };
        Some(Managed::from_envelope(envelope, work))
    }

    async fn process(
        &self,
        metrics: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Error>> {
        validate::container(&metrics)?;

        // Fast filters, which do not need expanded trace metrics.
        filter::feature_flag(ctx).reject(&metrics)?;

        let mut metrics = process::expand(metrics);
        validate::size(&mut metrics, ctx);
        validate::validate(&mut metrics);
        process::normalize(&mut metrics, ctx);
        filter::filter(&mut metrics, ctx);
        process::scrub(&mut metrics, ctx);

        self.limiter.enforce_quotas(&mut metrics, ctx).await?;

        Ok(Output::just(TraceMetricOutput(metrics)))
    }
}

/// Output produced by [`TraceMetricsProcessor`].
#[derive(Debug)]
pub struct TraceMetricOutput(Managed<ExpandedTraceMetrics>);

impl Forward for TraceMetricOutput {
    fn serialize_envelope(
        self,
        _: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<crate::Envelope>>, Rejected<()>> {
        self.0.try_map(|metrics, _| {
            metrics
                .serialize_envelope()
                .map_err(|error| {
                    relay_log::error!(
                        error = %error,
                        "internal error: failed to serialize trace metrics envelope"
                    );
                })
                .with_outcome(Outcome::Invalid(DiscardReason::Internal))
        })
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: processing::forward::StoreHandle<'_>,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let Self(metrics) = self;

        let ctx = store::Context {
            scoping: metrics.scoping(),
            received_at: metrics.received_at(),
            retention: ctx.retention(|r| r.trace_metric.as_ref()),
        };

        for metric in metrics.split(|metrics| metrics.metrics) {
            if let Ok(metric) = metric.try_map(|metric, _| store::convert(metric, &ctx)) {
                s.store(metric);
            }
        }

        Ok(())
    }
}

/// Serialized trace metrics extracted from an envelope.
#[derive(Debug)]
pub struct SerializedTraceMetrics {
    /// Original envelope headers.
    pub headers: EnvelopeHeaders,
    /// Trace metrics are sent in item containers, there is specified limit of a single container per
    /// envelope.
    ///
    /// But at this point this has not yet been validated.
    pub metrics: Vec<Item>,
}

impl Counted for SerializedTraceMetrics {
    fn quantities(&self) -> Quantities {
        let count = self
            .metrics
            .iter()
            .map(|item| item.item_count().unwrap_or(1) as usize)
            .sum();

        smallvec![(DataCategory::TraceMetric, count)]
    }
}

impl CountRateLimited for Managed<SerializedTraceMetrics> {
    type Error = Error;
}

impl CountRateLimited for Managed<ExpandedTraceMetrics> {
    type Error = Error;
}

/// Trace metrics which have been parsed and expanded from their serialized state.
#[derive(Debug)]
pub struct ExpandedTraceMetrics {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// Expanded and parsed trace metrics.
    metrics: ContainerItems<TraceMetric>,
}

impl Counted for ExpandedTraceMetrics {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::TraceMetric, self.metrics.len())]
    }
}

impl ExpandedTraceMetrics {
    fn serialize_envelope(self) -> Result<Box<Envelope>, ContainerWriteError> {
        let mut metrics = Vec::new();

        if !self.metrics.is_empty() {
            let mut item = Item::new(ItemType::TraceMetric);
            ItemContainer::from(self.metrics)
                .write_to(&mut item)
                .inspect_err(|err| relay_log::error!("failed to serialize trace metrics: {err}"))?;
            metrics.push(item);
        }

        Ok(Envelope::from_parts(self.headers, Items::from_vec(metrics)))
    }
}
