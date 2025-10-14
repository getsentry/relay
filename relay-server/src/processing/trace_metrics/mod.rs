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
use crate::services::outcome::{DiscardReason, Outcome};
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
            Self::ProcessingFailed(_) => Some(Outcome::Invalid(DiscardReason::Internal)),
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
        mut metrics: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Error>> {
        validate::container(&metrics)?;

        if ctx.is_proxy() {
            // If running in proxy mode, just apply cached rate limits and forward without
            // processing.
            self.limiter.enforce_quotas(&mut metrics, ctx).await?;
            return Ok(Output::just(TraceMetricOutput::NotProcessed(metrics)));
        }

        // Fast filters, which do not need expanded trace metrics.
        filter::feature_flag(ctx).reject(&metrics)?;

        let mut metrics = process::expand(metrics);
        validate::validate(&mut metrics);
        process::normalize(&mut metrics, ctx);
        filter::filter(&mut metrics, ctx);
        process::scrub(&mut metrics, ctx);

        self.limiter.enforce_quotas(&mut metrics, ctx).await?;

        Ok(Output::just(TraceMetricOutput::Processed(metrics)))
    }
}

/// Output produced by [`TraceMetricsProcessor`].
#[derive(Debug)]
pub enum TraceMetricOutput {
    NotProcessed(Managed<SerializedTraceMetrics>),
    Processed(Managed<ExpandedTraceMetrics>),
}

impl Forward for TraceMetricOutput {
    fn serialize_envelope(
        self,
        _: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<crate::Envelope>>, Rejected<()>> {
        let metrics = match self {
            Self::NotProcessed(metrics) => metrics,
            Self::Processed(metrics) => {
                let serialized = metrics.try_map(|metrics, _| {
                    metrics
                        .serialize()
                        .map_err(|_| (Some(Outcome::Invalid(DiscardReason::Internal)), ()))
                });
                match serialized {
                    Ok(s) => s,
                    Err(rejected) => return Err(rejected.map(|_| ())),
                }
            }
        };

        Ok(metrics.map(|metrics, _| {
            let SerializedTraceMetrics { headers, metrics } = metrics;
            Envelope::from_parts(headers, Items::from_vec(metrics))
        }))
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: &relay_system::Addr<crate::services::store::Store>,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let metrics = match self {
            TraceMetricOutput::NotProcessed(metrics) => {
                return Err(metrics.internal_error(
                    "trace metrics must be processed before they can be forwarded to the store",
                ));
            }
            TraceMetricOutput::Processed(metrics) => metrics,
        };

        let ctx = store::Context {
            scoping: metrics.scoping(),
            received_at: metrics.received_at(),
            retention: ctx.retention(|r| r.trace_metric.as_ref()),
        };

        for metric in metrics.split(|metrics| metrics.metrics) {
            if let Ok(metric) = metric.try_map(|metric, _| store::convert(metric, &ctx)) {
                s.send(metric);
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
        smallvec![(DataCategory::TraceMetric, self.metrics.len())]
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
    fn serialize(self) -> Result<SerializedTraceMetrics, ContainerWriteError> {
        let mut metrics = Vec::new();

        if !self.metrics.is_empty() {
            let mut item = Item::new(ItemType::TraceMetric);
            ItemContainer::from(self.metrics)
                .write_to(&mut item)
                .inspect_err(|err| relay_log::error!("failed to serialize trace metrics: {err}"))?;
            metrics.push(item);
        }

        Ok(SerializedTraceMetrics {
            headers: self.headers,
            metrics,
        })
    }
}
