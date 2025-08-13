use std::sync::Arc;

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
    /// The spans are filtered due to dynamic sampling.
    #[error("filtered due to dynamic sampling: {0}")]
    DynamicSampling(Outcome),
    /// The span is invalid.
    #[error("invalid: {0}")]
    Invalid(DiscardReason),
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::FilterFeatureFlag => None,
            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
            Self::DynamicSampling(reason) => Some(reason.clone()),
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
}

impl SpansProcessor {
    /// Creates a new [`Self`].
    pub fn new(limiter: Arc<QuotaRateLimiter>) -> Self {
        Self { limiter }
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
        let sampling = dynamic_sampling::run(&spans, ctx).await;
        let server_sample_rate = sampling.sample_rate();
        if let Some(sampling_outcome) = sampling.into_dropped_outcome() {
            // TODO: metric extraction.
            let spans = spans.map(|spans, records| {
                records.lenient(DataCategory::Span);
                UnsampledSpans { spans: spans.spans }
            });
            return Err(spans.reject_err(Error::DynamicSampling(sampling_outcome)));
        }

        let mut spans = process::expand(spans, server_sample_rate);

        // TODO: normalization (conventions?)

        // TODO: metrics (usage and count per root)
        // TODO: possibly generic span metric extraction
        // TODO: pii scrubbing

        self.limiter.enforce_quotas(&mut spans, ctx).await?;

        Ok(Output::just(SpanOutput::Processed(spans)))
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
                let mut span = span.value.map_value(relay_spans::span_v2_to_span_v1);
                if let Some(span) = span.value_mut() {
                    inject_server_sample_rate(span, spans.server_sample_rate);

                    // TODO: this needs to be done in a normalization step, which is yet to be
                    // implemented.
                    span.received =
                        relay_event_schema::protocol::Timestamp(chrono::Utc::now()).into();
                }

                let mut item = Item::new(ItemType::Span);
                let payload = match span.to_json() {
                    Ok(payload) => payload,
                    Err(error) => {
                        records.internal_error(error, span);
                        continue;
                    }
                };
                item.set_payload(ContentType::Json, payload);
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
fn inject_server_sample_rate(
    span: &mut relay_event_schema::protocol::Span,
    server_sample_rate: Option<f64>,
) {
    let Some(server_sample_rate) = server_sample_rate.and_then(relay_protocol::FiniteF64::new)
    else {
        return;
    };

    let measurements = span.measurements.get_or_insert_with(Default::default);
    measurements.0.insert(
        "server_sample_rate".to_owned(),
        relay_event_schema::protocol::Measurement {
            value: server_sample_rate.into(),
            unit: None.into(),
        }
        .into(),
    );
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
    fn serialize_envelope(self) -> Box<Envelope> {
        Envelope::from_parts(self.headers, Items::from_vec(self.spans))
    }
}

impl Counted for SerializedSpans {
    fn quantities(&self) -> Quantities {
        let quantity = outcome_count(&self.spans);
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

/// Spans which have been rejected/dropped by dynamic sampling.
///
/// Contained spans will only count towards the [`DataCategory::SpanIndexed`] category,
/// as the total category is counted from now in in metrics.
struct UnsampledSpans {
    spans: Vec<Item>,
}

impl Counted for UnsampledSpans {
    fn quantities(&self) -> Quantities {
        let quantity = outcome_count(&self.spans);
        smallvec::smallvec![(DataCategory::SpanIndexed, quantity),]
    }
}

/// Returns the amount of contained spans, this count is best effort and can be used for outcomes.
///
/// The function expects all passed items to only contain spans.
fn outcome_count(spans: &[Item]) -> usize {
    // We rely here on the invariant that all items in `self.spans` are actually spans,
    // that's why sum up `item_count`'s blindly instead of checking again for the item type
    // or using `Item::quantities`.
    let c: u32 = spans.iter().filter_map(|item| item.item_count()).sum();
    c as usize
}
