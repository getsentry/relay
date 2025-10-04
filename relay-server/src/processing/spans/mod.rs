use std::sync::Arc;

use relay_event_normalization::GeoIpLookup;
use relay_event_schema::processor::ProcessingAction;
#[cfg(feature = "processing")]
use relay_event_schema::protocol::CompatSpan;
use relay_event_schema::protocol::SpanV2;
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{
    ContainerItems, ContainerWriteError, EnvelopeHeaders, Item, ItemContainer, ItemType, Items,
};
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult, OutcomeError, Quantities, Rejected,
};
use crate::processing::{self, Context, CountRateLimited, Forward, Output, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::statsd::RelayCounters;

mod dynamic_sampling;
mod filter;
mod process;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Standalone spans filtered because of a missing feature flag.
    #[error("spans feature flag missing")]
    FilterFeatureFlag,
    /// The spans are rate limited.
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

/// A processor for Spans.
pub struct SpansProcessor {
    limiter: Arc<QuotaRateLimiter>,
    geo_lookup: GeoIpLookup,
}

impl SpansProcessor {
    /// Creates a new [`Self`].
    pub fn new(limiter: Arc<QuotaRateLimiter>, geo_lookup: GeoIpLookup) -> Self {
        Self {
            limiter,
            geo_lookup,
        }
    }
}

impl processing::Processor for SpansProcessor {
    type UnitOfWork = SerializedSpans;
    type Output = SpanOutput;
    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let headers = envelope.envelope().headers().clone();

        let spans = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Span))
            .into_vec();

        let work = SerializedSpans { headers, spans };
        Some(Managed::from_envelope(envelope, work))
    }

    async fn process(
        &self,
        mut spans: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        filter::feature_flag(ctx).reject(&spans)?;

        if ctx.is_proxy() {
            // If running in proxy mode, just apply cached rate limits and forward without
            // processing.
            //
            // Static mode needs processing, as users can override project settings manually.
            self.limiter.enforce_quotas(&mut spans, ctx).await?;
            return Ok(Output::just(SpanOutput::NotProcessed(spans)));
        }

        dynamic_sampling::validate_configs(ctx);
        let ds_result = dynamic_sampling::run(spans, ctx).await;
        relay_statsd::metric!(
            counter(RelayCounters::SamplingDecision) += 1,
            decision = match ds_result.is_ok() {
                true => "keep",
                false => "drop",
            },
            item = "span"
        );
        let spans = match ds_result {
            Ok(spans) => spans,
            Err(metrics) => return Ok(Output::metrics(metrics)),
        };

        let mut spans = process::expand(spans);

        process::normalize(&mut spans, &self.geo_lookup);
        filter::filter(&mut spans, ctx);

        self.limiter.enforce_quotas(&mut spans, ctx).await?;

        // TODO: pii scrubbing

        let metrics = dynamic_sampling::create_indexed_metrics(&spans, ctx);

        Ok(Output {
            main: Some(SpanOutput::Processed(spans)),
            metrics,
        })
    }
}

/// Output produced by the [`SpansProcessor`].
#[derive(Debug)]
pub enum SpanOutput {
    NotProcessed(Managed<SerializedSpans>),
    Processed(Managed<ExpandedSpans>),
}

impl Forward for SpanOutput {
    fn serialize_envelope(self) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        let spans = match self {
            Self::NotProcessed(spans) => spans,
            Self::Processed(spans) => spans.try_map(|spans, _| {
                spans
                    .serialize()
                    .map_err(drop)
                    .with_outcome(Outcome::Invalid(DiscardReason::Internal))
            })?,
        };

        Ok(spans.map(|spans, _| spans.serialize_envelope()))
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: &relay_system::Addr<crate::services::store::Store>,
    ) -> Result<(), Rejected<()>> {
        use crate::envelope::ContentType;
        use crate::services::store::StoreEnvelope;

        let spans = match self {
            SpanOutput::NotProcessed(spans) => {
                return Err(spans.internal_error(
                    "spans must be processed before they can be forwarded to the store",
                ));
            }
            SpanOutput::Processed(spans) => spans,
        };

        // Converts all SpanV2 spans into their SpanV1 counterparts and packages them into an
        // envelope to forward them.
        //
        // This is temporary until we have proper mapping code from SpanV2 -> SpanKafka,
        // similar to what we do for logs.
        let envelope = spans.map(|spans, records| {
            let mut items = Items::with_capacity(spans.spans.len());
            for span in spans.spans {
                use relay_protocol::Annotated;

                let mut span = match span.value.map_value(CompatSpan::try_from) {
                    Annotated(Some(Result::Err(error)), _) => {
                        // TODO: Use records.internal_error(error, span)
                        relay_log::error!(
                            error = &error as &dyn std::error::Error,
                            "Failed to create CompatSpan"
                        );
                        continue;
                    }
                    Annotated(Some(Result::Ok(compat_span)), meta) => {
                        Annotated(Some(compat_span), meta)
                    }
                    Annotated(None, meta) => Annotated(None, meta),
                };
                if let Some(span) = span.value_mut() {
                    inject_server_sample_rate(&mut span.span_v2, spans.server_sample_rate);
                }

                let mut item = Item::new(ItemType::Span);
                let payload = match span.to_json() {
                    Ok(payload) => payload,
                    Err(error) => {
                        records.internal_error(error, span);
                        continue;
                    }
                };
                item.set_payload(ContentType::CompatSpan, payload);
                if let Some(trace_id) = span.value().and_then(|s| s.span_v2.trace_id.value()) {
                    item.set_routing_hint(*trace_id.as_ref());
                }
                items.push(item);
            }

            Envelope::from_parts(spans.headers, items)
        });

        s.send(StoreEnvelope {
            envelope: ManagedEnvelope::from(envelope).into_processed(),
        });

        Ok(())
    }
}

/// Injects a server sample rate into a 'v1' span.
///
/// This is a temporary measure to correctly add the server sample rate to a span,
/// so the store can later read it again.
///
/// Ideally we forward a proper data structure to the store instead, then we don't
/// have to inject the sample rate into a measurement.
#[cfg(feature = "processing")]
fn inject_server_sample_rate(span: &mut SpanV2, server_sample_rate: Option<f64>) {
    let Some(server_sample_rate) = server_sample_rate.and_then(relay_protocol::FiniteF64::new)
    else {
        return;
    };

    let attributes = span.attributes.get_or_insert_with(Default::default);
    attributes.insert("sentry.server_sample_rate", server_sample_rate.to_f64());
}

/// Spans in their serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedSpans {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// A list of spans waiting to be processed.
    ///
    /// All items contained here must be spans.
    spans: Vec<Item>,
}

impl SerializedSpans {
    fn sampled(self, server_sample_rate: Option<f64>) -> SampledSpans {
        SampledSpans {
            headers: self.headers,
            spans: self.spans,
            server_sample_rate,
        }
    }

    fn serialize_envelope(self) -> Box<Envelope> {
        Envelope::from_parts(self.headers, Items::from_vec(self.spans))
    }
}

impl Counted for SerializedSpans {
    fn quantities(&self) -> Quantities {
        let quantity = outcome_count(&self.spans) as usize;
        smallvec::smallvec![
            (DataCategory::Span, quantity),
            (DataCategory::SpanIndexed, quantity),
        ]
    }
}

impl CountRateLimited for Managed<SerializedSpans> {
    type Error = Error;
}

/// Spans which have been parsed and expanded from their serialized state.
#[derive(Debug)]
pub struct ExpandedSpans {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// Server side applied (dynamic) sample rate.
    #[cfg_attr(not(feature = "processing"), expect(dead_code))]
    server_sample_rate: Option<f64>,

    /// Expanded and parsed spans.
    spans: ContainerItems<SpanV2>,
}

impl ExpandedSpans {
    fn serialize(self) -> Result<SerializedSpans, ContainerWriteError> {
        let mut spans = Vec::new();

        if !self.spans.is_empty() {
            let mut item = Item::new(ItemType::Span);
            ItemContainer::from(self.spans)
                .write_to(&mut item)
                .inspect_err(|err| relay_log::error!("failed to serialize spans: {err}"))?;
            spans.push(item);
        }

        Ok(SerializedSpans {
            headers: self.headers,
            spans,
        })
    }
}

impl Counted for ExpandedSpans {
    fn quantities(&self) -> Quantities {
        let quantity = self.spans.len();
        smallvec::smallvec![
            (DataCategory::Span, quantity),
            (DataCategory::SpanIndexed, quantity),
        ]
    }
}

impl CountRateLimited for Managed<ExpandedSpans> {
    type Error = Error;
}

/// Spans which have been sampled by dynamic sampling.
///
/// Note: Spans where dynamic sampling could not yet make a sampling decision,
/// are considered sampled.
struct SampledSpans {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// Expanded and parsed spans.
    spans: Vec<Item>,

    /// Server side applied (dynamic) sample rate.
    server_sample_rate: Option<f64>,
}

impl Counted for SampledSpans {
    fn quantities(&self) -> Quantities {
        let quantity = outcome_count(&self.spans) as usize;
        smallvec::smallvec![
            (DataCategory::Span, quantity),
            (DataCategory::SpanIndexed, quantity),
        ]
    }
}

/// Returns the amount of contained spans, this count is best effort and can be used for outcomes.
///
/// The function expects all passed items to only contain spans.
fn outcome_count(spans: &[Item]) -> u32 {
    // We rely here on the invariant that all items in `self.spans` are actually spans,
    // that's why sum up `item_count`'s blindly instead of checking again for the item type
    // or using `Item::quantities`.
    spans.iter().filter_map(|item| item.item_count()).sum()
}
