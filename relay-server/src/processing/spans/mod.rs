use std::sync::Arc;

use either::Either;
use relay_event_normalization::GeoIpLookup;
use relay_event_schema::processor::ProcessingAction;
use relay_event_schema::protocol::SpanV2;
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{
    ContainerWriteError, EnvelopeHeaders, Item, ItemContainer, ItemType, Items, WithHeader,
};
use crate::integrations::Integration;
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult, OutcomeError, Quantities, Rejected,
};
use crate::processing::trace_attachments::forward::attachment_to_item;
use crate::processing::trace_attachments::process::ScrubAttachmentError;
use crate::processing::trace_attachments::types::ExpandedAttachment;
use crate::processing::{self, Context, Forward, Output, QuotaRateLimiter, RateLimited};
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
    PiiConfig,
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
            Self::PiiConfig => Some(Outcome::Invalid(DiscardReason::ProjectStatePii)),
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

impl From<ScrubAttachmentError> for Error {
    fn from(value: ScrubAttachmentError) -> Self {
        match value {
            ScrubAttachmentError::PiiConfig => Self::PiiConfig,
            ScrubAttachmentError::ProcessingFailed(action) => Self::ProcessingFailed(action),
        }
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

        let attachments = envelope
            .envelope_mut()
            .take_items_by(|item| item.is_span_attachment())
            .to_vec();

        let work = SerializedSpans {
            headers,
            spans,
            legacy,
            integrations,
            attachments,
        };
        Some(Managed::from_envelope(envelope, work))
    }

    async fn process(
        &self,
        spans: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let spans = filter::feature_flag_attachment(spans, ctx);
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

        Ok(match dynamic_sampling::create_indexed_metrics(spans, ctx) {
            Either::Left(spans) => Output::just(SpanOutput::TotalAndIndexed(spans)),
            Either::Right((spans, metrics)) => Output {
                main: Some(SpanOutput::Indexed(spans)),
                metrics: Some(metrics),
            },
        })
    }
}

/// Output produced by the [`SpansProcessor`].
#[derive(Debug)]
pub enum SpanOutput {
    TotalAndIndexed(Managed<ExpandedSpans<TotalAndIndexed>>),
    Indexed(Managed<ExpandedSpans<Indexed>>),
}

impl Forward for SpanOutput {
    fn serialize_envelope(
        self,
        _: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        let spans = match self {
            Self::TotalAndIndexed(spans) => spans,
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

        spans.try_map(|spans, _| {
            spans
                .serialize_envelope()
                .map_err(drop)
                .with_outcome(Outcome::Invalid(DiscardReason::Internal))
        })
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: processing::forward::StoreHandle<'_>,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let spans = match self {
            Self::TotalAndIndexed(spans) => {
                return Err(spans
                    .internal_error("a span must have metrics extracted in order to be stored"));
            }
            Self::Indexed(spans) => spans,
        };

        let ctx = store::Context {
            server_sample_rate: spans.server_sample_rate,
            retention: ctx.retention(|r| r.span.as_ref()),
        };

        let spans_and_attachments = spans.split(|spans| spans.into_parts());
        for either in spans_and_attachments {
            match either.transpose() {
                Either::Left(span) => {
                    if let Ok(span) = span.try_map(|span, _| store::convert(span, &ctx)) {
                        s.store(span);
                    }
                }
                Either::Right(attachment) => {
                    use crate::processing::trace_attachments;

                    if let Ok(attachment) = trace_attachments::store::convert(
                        attachment,
                        ctx.retention,
                        ctx.server_sample_rate,
                    ) {
                        s.upload(attachment);
                    }
                }
            }
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
    attachments: Vec<Item>,
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
            .attachments
            .iter()
            .map(Item::attachment_body_size)
            .sum();

        let mut quantities = smallvec::smallvec![];

        if span_quantity > 0 {
            quantities.push((DataCategory::Span, span_quantity));
            quantities.push((DataCategory::SpanIndexed, span_quantity));
        }

        if attachment_quantity > 0 {
            quantities.push((DataCategory::Attachment, attachment_quantity));
        }
        if !self.attachments.is_empty() {
            quantities.push((DataCategory::AttachmentItem, self.attachments.len()));
        }

        quantities
    }
}

struct ExpandedSpansQuantities {
    span: usize,
    attachment: usize,
    attachment_item: usize,
}

/// Spans which have been parsed and expanded from their serialized state.
#[derive(Debug)]
pub struct ExpandedSpans<C = TotalAndIndexed> {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// Server side applied (dynamic) sample rate.
    server_sample_rate: Option<f64>,

    /// Expanded and parsed spans, with optional associated attachments.
    spans: Vec<ExpandedSpan>,

    /// Span attachments that are not associated with any one specific span.
    stand_alone_attachments: Vec<ExpandedAttachment>,

    /// Category of the contained spans.
    ///
    /// Either [`TotalAndIndexed`] or [`Indexed`].
    #[expect(unused, reason = "marker field, only set never read")]
    category: C,
}

impl<C> ExpandedSpans<C> {
    fn serialize_envelope(self) -> Result<Box<Envelope>, ContainerWriteError> {
        let mut items = Vec::new();

        if !self.spans.is_empty() {
            let mut item = Item::new(ItemType::Span);
            let mut spans_without_attachments = Vec::new();

            for ExpandedSpan { span, attachments } in self.spans {
                for attachment in attachments {
                    items.push(attachment_to_item(attachment)?);
                }

                spans_without_attachments.push(span);
            }

            ItemContainer::from(spans_without_attachments)
                .write_to(&mut item)
                .inspect_err(|err| relay_log::error!("failed to serialize spans: {err}"))?;
            items.push(item);
        }

        for attachment in self.stand_alone_attachments {
            items.push(attachment_to_item(attachment)?);
        }

        Ok(Envelope::from_parts(self.headers, Items::from_vec(items)))
    }

    fn span_quantities(&self) -> ExpandedSpansQuantities {
        let quantity = self.spans.len();
        let mut attachment_quantity = 0;
        let mut attachment_count = 0;

        for span in &self.spans {
            for attachment in &span.attachments {
                attachment_quantity += attachment.body.len();
                attachment_count += 1;
            }
        }
        for attachment in &self.stand_alone_attachments {
            attachment_quantity += attachment.body.len();
            attachment_count += 1;
        }

        ExpandedSpansQuantities {
            span: quantity,
            attachment: attachment_quantity,
            attachment_item: attachment_count,
        }
    }
}

impl ExpandedSpans<TotalAndIndexed> {
    /// Logically transforms contained spans into [`Indexed`].
    ///
    /// This must only be called during metric extraction.
    fn into_indexed(self) -> ExpandedSpans<Indexed> {
        let Self {
            headers,
            server_sample_rate,
            spans,
            stand_alone_attachments,
            category: _,
        } = self;

        ExpandedSpans {
            headers,
            server_sample_rate,
            spans,
            stand_alone_attachments,
            category: Indexed,
        }
    }
}

impl ExpandedSpans<Indexed> {
    #[cfg(feature = "processing")]
    fn into_parts(self) -> impl Iterator<Item = Either<IndexedSpanOnly, ExpandedAttachment>> {
        let Self {
            headers: _,
            server_sample_rate: _,
            spans,
            stand_alone_attachments,
            category: _,
        } = self;
        spans
            .into_iter()
            .flat_map(|span| {
                let ExpandedSpan { span, attachments } = span;
                std::iter::once(Either::Left(IndexedSpanOnly(span)))
                    .chain(attachments.into_iter().map(Either::Right))
            })
            .chain(stand_alone_attachments.into_iter().map(Either::Right))
    }
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

impl Counted for ExpandedSpans<TotalAndIndexed> {
    fn quantities(&self) -> Quantities {
        let ExpandedSpansQuantities {
            span,
            attachment,
            attachment_item,
        } = self.span_quantities();

        let mut quantities = smallvec::smallvec![];
        if span > 0 {
            quantities.push((DataCategory::Span, span));
            quantities.push((DataCategory::SpanIndexed, span));
        }
        if attachment > 0 {
            quantities.push((DataCategory::Attachment, attachment));
        }
        if attachment_item > 0 {
            quantities.push((DataCategory::AttachmentItem, attachment_item));
        }

        quantities
    }
}

impl Counted for ExpandedSpans<Indexed> {
    fn quantities(&self) -> Quantities {
        let ExpandedSpansQuantities {
            span,
            attachment,
            attachment_item,
        } = self.span_quantities();

        let mut quantities = smallvec::smallvec![];
        if span > 0 {
            quantities.push((DataCategory::SpanIndexed, span));
        }
        if attachment > 0 {
            quantities.push((DataCategory::Attachment, attachment));
        }
        if attachment_item > 0 {
            quantities.push((DataCategory::AttachmentItem, attachment_item));
        }

        quantities
    }
}

impl RateLimited for Managed<ExpandedSpans<TotalAndIndexed>> {
    type Error = Error;

    async fn enforce<T>(
        &mut self,
        mut rate_limiter: T,
        _: Context<'_>,
    ) -> std::result::Result<(), Rejected<Self::Error>>
    where
        T: processing::RateLimiter,
    {
        let scoping = self.scoping();

        let ExpandedSpansQuantities {
            span,
            attachment,
            attachment_item,
        } = self.span_quantities();

        if span > 0 {
            let limits = rate_limiter
                .try_consume(scoping.item(DataCategory::Span), span)
                .await;
            if !limits.is_empty() {
                // If there is a span quota reject all the spans and the associated attachments.
                return Err(self.reject_err(Error::from(limits)));
            }

            let limits = rate_limiter
                .try_consume(scoping.item(DataCategory::SpanIndexed), span)
                .await;
            if !limits.is_empty() {
                // If there is a span quota reject all the spans and the associated attachments.
                return Err(self.reject_err(Error::from(limits)));
            }
        }

        if attachment > 0 {
            self.enforce_attachment_limits(
                &mut rate_limiter,
                scoping,
                DataCategory::Attachment,
                attachment,
            )
            .await;
        }

        if attachment_item > 0 {
            self.enforce_attachment_limits(
                &mut rate_limiter,
                scoping,
                DataCategory::AttachmentItem,
                attachment_item,
            )
            .await;
        }

        Ok(())
    }
}

impl Managed<ExpandedSpans<TotalAndIndexed>> {
    async fn enforce_attachment_limits<T>(
        &mut self,
        rate_limiter: &mut T,
        scoping: relay_quotas::Scoping,
        category: DataCategory,
        quantity: usize,
    ) where
        T: processing::RateLimiter,
    {
        let limits = rate_limiter
            .try_consume(scoping.item(category), quantity)
            .await;

        if !limits.is_empty() {
            self.modify(|this, record_keeper| {
                // Reject both associated attachments and standalone.
                for span in &mut this.spans {
                    let attachments = std::mem::take(&mut span.attachments);
                    record_keeper.reject_err(Error::from(limits.clone()), attachments);
                }

                record_keeper.reject_err(
                    Error::from(limits),
                    std::mem::take(&mut this.stand_alone_attachments),
                );
            });
        }
    }
}

#[cfg(feature = "processing")]
#[derive(Debug)]
struct IndexedSpanOnly(WithHeader<SpanV2>);

#[cfg(feature = "processing")]
impl Counted for IndexedSpanOnly {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![(DataCategory::SpanIndexed, 1)]
    }
}

/// Wrapper around a SpanV2 and an optional associated attachment.
///
/// Allows for dropping the attachment together with the Span.
#[derive(Debug)]
struct ExpandedSpan {
    span: WithHeader<SpanV2>,
    attachments: Vec<ExpandedAttachment>,
}

impl ExpandedSpan {
    fn new(span: WithHeader<SpanV2>) -> Self {
        Self {
            span,
            attachments: vec![],
        }
    }
}

impl Counted for ExpandedSpan {
    fn quantities(&self) -> Quantities {
        let Self { span, attachments } = self;
        let mut quantities = span.quantities();

        quantities.extend(attachments.quantities());

        quantities
    }
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
