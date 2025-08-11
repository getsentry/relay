use std::sync::Arc;

use relay_event_schema::protocol::SpanV2;
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{ContainerItems, EnvelopeHeaders, Item, ItemType};
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult, OutcomeError, Quantities, Rejected,
};
use crate::processing::{self, Context, CountRateLimited, Forward, Output, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};

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
        let _headers = envelope.envelope().headers().clone();

        let spans = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Span))
            .into_vec();

        let work = SerializedSpans { _headers, spans };
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

        // TODO: dynamic sampling
        // Dynamic sampling can now run without expanding the contents,
        // because we only support trace based rules -> it operates solely on the DSC.
        // For sampled spans, the sampling rate needs to be applied.

        let mut spans = process::expand(spans);

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
        debug_assert!(false, "Not Implemented Yet");
        Err(match self {
            Self::NotProcessed(spans) => {
                spans.reject_err((Outcome::Invalid(DiscardReason::Internal), ()))
            }
            Self::Processed(spans) => {
                spans.reject_err((Outcome::Invalid(DiscardReason::Internal), ()))
            }
        })
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        _s: &relay_system::Addr<crate::services::store::Store>,
    ) -> Result<(), Rejected<()>> {
        debug_assert!(false, "Not Implemented Yet");
        Err(match self {
            Self::NotProcessed(spans) => {
                spans.reject_err((Outcome::Invalid(DiscardReason::Internal), ()))
            }
            Self::Processed(spans) => {
                spans.reject_err((Outcome::Invalid(DiscardReason::Internal), ()))
            }
        })
    }
}

/// Spans in their serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedSpans {
    /// Original envelope headers.
    _headers: EnvelopeHeaders,

    /// A list of spans waiting to be processed.
    ///
    /// All items contained here must be spans.
    spans: Vec<Item>,
}

impl SerializedSpans {
    /// Returns the amount of contained spans, this count is best effort and can be used for outcomes.
    fn count(&self) -> usize {
        // We rely here on the invariant that all items in `self.spans` are actually spans,
        // that's why sum up `item_count`'s blindly instead of checking again for the item type
        // or using `Item::quantities`.
        let c: u32 = self.spans.iter().filter_map(|item| item.item_count()).sum();
        c as usize
    }
}

impl Counted for SerializedSpans {
    fn quantities(&self) -> Quantities {
        let quantity = self.count();
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
    _headers: EnvelopeHeaders,

    /// Expanded and parsed spans.
    spans: ContainerItems<SpanV2>,
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
