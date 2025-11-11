use std::sync::Arc;

use relay_event_normalization::GeoIpLookup;
use relay_event_schema::processor::ProcessingAction;
use relay_event_schema::protocol::SpanV2;
use relay_pii::PiiConfigError;
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{
    ContainerItems, ContainerWriteError, ContentType, EnvelopeHeaders, Item, ItemContainer,
    ItemType, Items,
};
use crate::integrations::Integration;
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult, OutcomeError, Quantities, Rejected,
};
use crate::processing::{self, Context, CountRateLimited, Forward, Output, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};

mod dynamic_sampling;
mod filter;
mod integrations;
mod process;
#[cfg(feature = "processing")]
mod store;
mod validate;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Multiple item containers for spans in a single envelope are not allowed.
    #[error("duplicate span container")]
    DuplicateContainer,
    /// Standalone spans filtered because of a missing feature flag.
    #[error("spans feature flag missing")]
    FilterFeatureFlag,
    #[error("a dynamic sampling context is required")]
    MissingDynamicSamplingContext,
    #[error("the dynamic sampling context does not match the payload")]
    DynamicSamplingContextMismatch,
    /// The spans are rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),
    /// Spans filtered due to a filtering rule.
    #[error("spans filtered")]
    Filtered(relay_filter::FilterStatKey),
    /// A processor failed to process the spans.
    #[error("envelope processor failed")]
    ProcessingFailed(#[from] ProcessingAction),
    /// Internal error, Pii config could not be loaded.
    #[error("Pii configuration error")]
    PiiConfig(PiiConfigError),
    /// The span is invalid.
    #[error("invalid: {0}")]
    Invalid(DiscardReason),
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::DuplicateContainer => Some(Outcome::Invalid(DiscardReason::DuplicateItem)),
            Self::FilterFeatureFlag => None,
            Self::MissingDynamicSamplingContext => Some(Outcome::Invalid(
                DiscardReason::MissingDynamicSamplingContext,
            )),
            Self::DynamicSamplingContextMismatch => Some(Outcome::Invalid(
                DiscardReason::InvalidDynamicSamplingContext,
            )),
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
            .take_items_by(ItemContainer::<SpanV2>::is_container)
            .into_vec();

        let legacy = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(item.ty(), ItemType::Span))
            .into_vec();

        let integrations = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(item.integration(), Some(Integration::Spans(_))))
            .into_vec();

        let span_attachments = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(item.content_type(), Some(ContentType::AttachmentV2)))
            .to_vec();

        let work = SerializedSpans {
            headers,
            spans,
            legacy,
            integrations,
            span_attachments,
        };
        Some(Managed::from_envelope(envelope, work))
    }

    async fn process(
        &self,
        spans: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        filter::feature_flag(ctx).reject(&spans)?;
        validate::container(&spans).reject(&spans)?;

        dynamic_sampling::validate_configs(ctx);
        dynamic_sampling::validate_dsc_presence(&spans).reject(&spans)?;

        let spans = match dynamic_sampling::run(spans, ctx).await {
            Ok(spans) => spans,
            Err(metrics) => return Ok(Output::metrics(metrics)),
        };

        let mut spans = process::expand(spans);

        dynamic_sampling::validate_dsc(&spans).reject(&spans)?;

        process::normalize(&mut spans, &self.geo_lookup, ctx);
        filter::filter(&mut spans, ctx);

        self.limiter.enforce_quotas(&mut spans, ctx).await?;

        process::scrub(&mut spans, ctx);

        let metrics = dynamic_sampling::create_indexed_metrics(&spans, ctx);

        Ok(Output {
            main: Some(SpanOutput(spans)),
            metrics,
        })
    }
}

/// Output produced by the [`SpansProcessor`].
#[derive(Debug)]
pub struct SpanOutput(Managed<ExpandedSpans>);

impl Forward for SpanOutput {
    fn serialize_envelope(
        self,
        _: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        self.0.try_map(|spans, _| {
            spans
                .serialize_envelope()
                .map_err(drop)
                .with_outcome(Outcome::Invalid(DiscardReason::Internal))
        })
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: &relay_system::Addr<crate::services::store::Store>,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let Self(spans) = self;

        let ctx = store::Context {
            server_sample_rate: spans.server_sample_rate,
            retention: ctx.retention(|r| r.span.as_ref()),
        };

        for span in spans.split(|spans| spans.spans) {
            if let Ok(span) = span.try_map(|span, _| store::convert(span, &ctx)) {
                s.send(span)
            };
        }

        Ok(())
    }
}

/// Spans in their serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedSpans {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// A list of span 'v2' item containers.
    spans: Vec<Item>,

    /// A list of legacy span 'v1' items.
    legacy: Vec<Item>,

    /// Spans which Relay received from arbitrary integrations.
    integrations: Vec<Item>,

    /// A list of span attachments.
    span_attachments: Vec<Item>,
}

impl SerializedSpans {
    fn sampled(self, server_sample_rate: Option<f64>) -> SampledSpans {
        SampledSpans {
            inner: self,
            server_sample_rate,
        }
    }
}

impl Counted for SerializedSpans {
    fn quantities(&self) -> Quantities {
        let span_quantity = (outcome_count(&self.spans)
            + outcome_count(&self.legacy)
            + outcome_count(&self.integrations)) as usize;

        let attachment_quantity = self
            .span_attachments
            .iter()
            .fold(0, |acc, cur| acc + cur.len());

        smallvec::smallvec![
            (DataCategory::Span, span_quantity),
            (DataCategory::SpanIndexed, span_quantity),
            (DataCategory::Attachment, attachment_quantity)
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

    /// Expanded and parsed span attachments.
    span_attachments: Vec<ValidatedSpanAttachment>,
}

impl ExpandedSpans {
    fn serialize_envelope(self) -> Result<Box<Envelope>, ContainerWriteError> {
        let mut spans = Vec::new();

        if !self.spans.is_empty() {
            let mut item = Item::new(ItemType::Span);
            ItemContainer::from(self.spans)
                .write_to(&mut item)
                .inspect_err(|err| relay_log::error!("failed to serialize spans: {err}"))?;
            spans.push(item);
        }

        Ok(Envelope::from_parts(self.headers, Items::from_vec(spans)))
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

/// A validated and parsed span attachment.
#[derive(Debug)]
pub struct ValidatedSpanAttachment {
    /// The span this attachment belongs to.
    pub span_id: Option<SpanId>,

    // TODO: Replace with a struct
    /// The parsed metadata from the attachment.
    pub meta: Bytes,

    /// The raw attachment body.
    pub body: Bytes,
}

/// Spans which have been sampled by dynamic sampling.
///
/// Note: Spans where dynamic sampling could not yet make a sampling decision,
/// are considered sampled.
struct SampledSpans {
    /// Sampled spans.
    inner: SerializedSpans,

    /// Server side applied (dynamic) sample rate.
    server_sample_rate: Option<f64>,
}

impl Counted for SampledSpans {
    fn quantities(&self) -> Quantities {
        self.inner.quantities()
    }
}

/// Returns the amount of contained spans, this count is best effort and can be used for outcomes.
///
/// The function expects all passed items to only contain spans.
fn outcome_count(spans: &[Item]) -> u32 {
    // We rely here on the invariant that all items in `self.spans` are actually spans,
    // that's why sum up `item_count`'s blindly instead of checking again for the item type
    // or using `Item::quantities`.
    spans
        .iter()
        .map(|item| item.item_count().unwrap_or(1))
        .sum()
}
