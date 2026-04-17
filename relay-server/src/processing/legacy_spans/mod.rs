use std::sync::Arc;

use either::Either;
use relay_event_normalization::GeoIpLookup;
use relay_event_schema::processor::ProcessingAction;
use relay_event_schema::protocol::Span;
use relay_protocol::Annotated;
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Quantities, Rejected};
use crate::metrics_extraction::ExtractedMetrics;
use crate::processing::{
    self, Context, CountRateLimited, Forward, Output, QuotaRateLimiter, RateLimited,
};
use crate::services::outcome::{DiscardReason, Outcome};

mod dynamic_sampling;
mod process;
#[cfg(feature = "processing")]
mod store;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("rate limited")]
    RateLimited(RateLimits),
    /// Spans filtered due to a filtering rule.
    #[error("spans filtered")]
    Filtered(relay_filter::FilterStatKey),
    /// A processor failed to process the spans.
    #[error("envelope processor failed")]
    ProcessingFailed(#[from] ProcessingAction),
    /// The span is invalid.
    #[error("invalid: {0}")]
    Invalid(DiscardReason),
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
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

/// A processor for legacy standalone Spans.
///
/// This processor will be replaced with the [`SpansProcessor`](super::spans::SpansProcessor), once
/// it is fully capable to deal with the legacy spans.
///
/// This is a port of the old legacy standalone processor, most of the original limitations of the
/// processor are not addressed with this one, such as only running in processing mode.
pub struct LegacySpansProcessor {
    limiter: Arc<QuotaRateLimiter>,
    geo_lookup: GeoIpLookup,
}

impl LegacySpansProcessor {
    /// Creates a new [`Self`].
    pub fn new(limiter: Arc<QuotaRateLimiter>, geo_lookup: GeoIpLookup) -> Self {
        Self {
            limiter,
            geo_lookup,
        }
    }
}

impl processing::Processor for LegacySpansProcessor {
    type Input = SerializedLegacySpans;
    type Output = LegacySpanOutput;
    type Error = Error;

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let headers = envelope.envelope().headers().clone();

        let spans = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(item.ty(), ItemType::Span))
            .into_vec();

        let work = SerializedLegacySpans { headers, spans };
        Some(Managed::with_meta_from(envelope, work))
    }

    async fn process(
        &self,
        spans: Managed<Self::Input>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        if !ctx.is_processing() {
            let spans = self.limiter.enforce_quotas(spans, ctx).await?;
            return Ok(Output::just(LegacySpanOutput::Serialized(spans)));
        }

        let mut spans = process::expand(spans);
        process::normalize(&mut spans, ctx, &self.geo_lookup);
        process::filter(&mut spans, ctx);

        let spans = match self.limiter.enforce_quotas(spans, ctx).await?.transpose() {
            Either::Left(spans) => spans,
            Either::Right(metrics) => return Ok(Output::metrics(metrics)),
        };

        let (mut spans, metrics) = match dynamic_sampling::run(spans, ctx).await {
            (Some(spans), metrics) => (spans, metrics),
            (None, metrics) => return Ok(Output::metrics(metrics)),
        };

        process::scrub(&mut spans, ctx);

        Ok(Output {
            main: Some(LegacySpanOutput::Indexed(spans)),
            metrics: Some(metrics),
        })
    }
}

/// Output produced by the [`LegacySpansProcessor`].
#[derive(Debug)]
pub enum LegacySpanOutput {
    Serialized(Managed<SerializedLegacySpans>),
    Indexed(Managed<ExpandedLegacySpans<Indexed>>),
}

impl Forward for LegacySpanOutput {
    fn serialize_envelope(
        self,
        _: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        let spans = match self {
            Self::Serialized(spans) => spans,
            Self::Indexed(spans) => {
                // If an indexed span is serialized back to an envelope, it loses the information
                // that metrics have been extracted and the span is ready to be stored.
                //
                // On a technical level we can include this as metadata in the envelope or span,
                // but our ingestion model does (no longer) allow for this.
                //
                // Metric extraction must be the last step in the pipeline.
                return Err(
                    spans.internal_error("an indexed span must be stored and not forwarded")
                );
            }
        };

        Ok(spans.map(|spans, _| Envelope::from_parts(spans.headers, Items::from_vec(spans.spans))))
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: processing::forward::StoreHandle<'_>,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let spans = match self {
            Self::Serialized(spans) => {
                return Err(spans
                    .internal_error("a span must have metrics extracted in order to be stored"));
            }
            Self::Indexed(spans) => spans,
        };

        let retention = ctx.retention(|r| r.span.as_ref());

        for span in spans.split(|spans| spans.spans) {
            if let Ok(span) = span.try_map(|span, _| store::convert(span, retention)) {
                s.send_to_store(span)
            };
        }

        Ok(())
    }
}

/// Spans in their serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedLegacySpans {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// A list of legacy, standalone "span v1" spans.
    spans: Vec<Item>,
}

impl Counted for SerializedLegacySpans {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![
            (DataCategory::Span, self.spans.len()),
            (DataCategory::SpanIndexed, self.spans.len()),
        ]
    }
}

impl CountRateLimited for Managed<SerializedLegacySpans> {
    type Error = Error;
}

/// The total and indexed category.
///
/// This category tracks spans in the total and indexed data categories.
/// Until a span has metrics extracted it owns both categories.
#[derive(Copy, Clone, Debug)]
pub struct TotalAndIndexed;

/// The indexed category.
///
/// Once metric extraction happened, spans no longer track/represent the total category, this was
/// transferred over to the metrics.
///
/// Every which is stored, must have metrics extracted and transferred this ownership.
#[derive(Copy, Clone, Debug)]
pub struct Indexed;

/// Spans in their de-serialized/parsed state.
#[derive(Debug)]
pub struct ExpandedLegacySpans<C = TotalAndIndexed> {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// Expanded list of standalone spans.
    spans: Vec<Annotated<Span>>,

    /// Category of the contained spans.
    ///
    /// Either [`TotalAndIndexed`] or [`Indexed`].
    #[expect(unused, reason = "marker field, only set never read")]
    category: C,
}

impl ExpandedLegacySpans<TotalAndIndexed> {
    /// Logically transforms contained spans into [`Indexed`].
    ///
    /// This must only be called during metric extraction.
    fn into_indexed(self) -> ExpandedLegacySpans<Indexed> {
        let Self {
            headers,
            spans,
            category: _,
        } = self;

        ExpandedLegacySpans {
            headers,
            spans,
            category: Indexed,
        }
    }
}

impl Counted for ExpandedLegacySpans<TotalAndIndexed> {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![
            (DataCategory::Span, self.spans.len()),
            (DataCategory::SpanIndexed, self.spans.len()),
        ]
    }
}

impl Counted for ExpandedLegacySpans<Indexed> {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![(DataCategory::SpanIndexed, self.spans.len())]
    }
}

impl RateLimited for Managed<ExpandedLegacySpans<TotalAndIndexed>> {
    type Output = Managed<Either<ExpandedLegacySpans<TotalAndIndexed>, ExtractedMetrics>>;
    type Error = Error;

    async fn enforce<R>(
        self,
        mut rate_limiter: R,
        ctx: Context<'_>,
    ) -> Result<Self::Output, Rejected<Self::Error>>
    where
        R: processing::RateLimiter,
    {
        let scoping = self.scoping();

        let limits = rate_limiter
            .try_consume(scoping.item(DataCategory::Span), self.spans.len())
            .await;
        if !limits.is_empty() {
            return Err(self.reject_err(Error::from(limits)));
        }

        let limits = rate_limiter
            .try_consume(scoping.item(DataCategory::SpanIndexed), self.spans.len())
            .await;
        if !limits.is_empty() {
            let total = dynamic_sampling::reject_indexed_spans(self, ctx, limits.into());
            return Ok(total.map(|total, _| Either::Right(total)));
        }

        Ok(self.map(|s, _| Either::Left(s)))
    }
}
