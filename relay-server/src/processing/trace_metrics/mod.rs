use std::sync::Arc;

use relay_event_schema::protocol::TraceMetric;
use relay_quotas::RateLimits;

use crate::envelope::{EnvelopeHeaders, Item, WithHeader};
use crate::managed::{Counted, Managed, ManagedEnvelope, Rejected};
use crate::processing::{self, Context, Output, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};
use smallvec::smallvec;

pub mod process;
pub mod store;
pub mod validate;

pub use self::process::{SerializedTraceMetricsContainer, expand, normalize, scrub};

pub type ExpandedTraceMetrics = Vec<WithHeader<TraceMetric>>;

pub type Result<T, E = Error> = std::result::Result<T, E>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid trace metric")]
    Invalid(DiscardReason),
    /// The trace metrics are rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),
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
            Self::Invalid(reason) => Some(Outcome::Invalid(*reason)),
            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
        };
        (outcome, self)
    }
}

impl Counted for WithHeader<TraceMetric> {
    fn quantities(&self) -> smallvec::SmallVec<[(relay_quotas::DataCategory, usize); 1]> {
        smallvec![(relay_quotas::DataCategory::TraceMetric, 1)]
    }
}

impl Counted for SerializedTraceMetricsContainer {
    fn quantities(&self) -> smallvec::SmallVec<[(relay_quotas::DataCategory, usize); 1]> {
        let count = self.metrics.len();
        smallvec![(relay_quotas::DataCategory::TraceMetric, count)]
    }
}

impl Counted for Vec<WithHeader<TraceMetric>> {
    fn quantities(&self) -> smallvec::SmallVec<[(relay_quotas::DataCategory, usize); 1]> {
        smallvec![(relay_quotas::DataCategory::TraceMetric, self.len())]
    }
}

/// Serialized trace metrics extracted from an envelope.
#[derive(Debug)]
pub struct SerializedTraceMetrics {
    pub headers: EnvelopeHeaders,
    pub metrics: Vec<Item>,
}

impl Counted for SerializedTraceMetrics {
    fn quantities(&self) -> smallvec::SmallVec<[(relay_quotas::DataCategory, usize); 1]> {
        smallvec![(relay_quotas::DataCategory::TraceMetric, self.metrics.len())]
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
            .take_items_by(|item| matches!(*item.ty(), crate::envelope::ItemType::TraceMetric))
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
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        if ctx.is_proxy() {
            // If running in proxy mode, just apply cached rate limits and forward without
            // processing.
            self.limiter.enforce_quotas(&mut metrics, ctx).await?;
            return Ok(Output::just(TraceMetricOutput::NotProcessed(metrics)));
        }

        // Convert to container format for processing
        let container = SerializedTraceMetricsContainer {
            metrics: metrics.value().metrics.clone(),
        };
        let managed_container = Managed::from_envelope(&metrics, container);
        let mut expanded = expand(managed_container, metrics.envelope().headers().meta());

        normalize(&mut expanded, metrics.envelope().headers().meta());
        scrub(&mut expanded, ctx.project_info);

        self.limiter.enforce_quotas(&mut expanded, ctx).await?;

        Ok(Output::just(TraceMetricOutput::Processed(expanded)))
    }
}

/// Output produced by [`TraceMetricsProcessor`].
#[derive(Debug)]
pub enum TraceMetricOutput {
    NotProcessed(Managed<SerializedTraceMetrics>),
    Processed(Managed<ExpandedTraceMetrics>),
}

impl processing::Forward for TraceMetricOutput {
    fn serialize_envelope(self) -> Result<Managed<Box<crate::Envelope>>, Rejected<()>> {
        let metrics = match self {
            Self::NotProcessed(metrics) => metrics,
            Self::Processed(metrics) => metrics.try_map(|metrics, r| {
                r.lenient(relay_quotas::DataCategory::TraceMetric);
                // For processed metrics, we need to serialize them back to envelope format
                // This is similar to how logs work, but for now we'll just return an error
                // since processed trace metrics should go to store, not be forwarded
                Err(())
            })?,
        };

        Ok(metrics.map(|metrics, r| {
            r.lenient(relay_quotas::DataCategory::TraceMetric);
            let SerializedTraceMetrics { headers, metrics } = metrics;
            Box::new(crate::Envelope::from_parts(headers, metrics))
        }))
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: &relay_system::Addr<crate::services::store::Store>,
    ) -> Result<(), Rejected<()>> {
        let metrics = match self {
            TraceMetricOutput::NotProcessed(metrics) => {
                return Err(metrics.internal_error(
                    "trace metrics must be processed before they can be forwarded to the store",
                ));
            }
            TraceMetricOutput::Processed(metrics) => metrics,
        };

        let scoping = metrics.scoping();
        let received_at = metrics.received_at();

        let ctx = store::Context {
            scoping,
            received_at,
            // Hard-code retentions until we have a per data category retention
            retention: Some(30),
            downsampled_retention: Some(30),
        };

        for metric in metrics {
            if let Ok(metric) = metric.try_map(|metric, _| store::convert(metric, &ctx)) {
                s.send(metric);
            }
        }

        Ok(())
    }
}
