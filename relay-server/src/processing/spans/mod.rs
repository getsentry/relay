use std::sync::Arc;

use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{EnvelopeHeaders, Item, ItemType};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Quantities, Rejected};
use crate::processing::{self, Context, CountRateLimited, Forward, Output, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The spans are rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
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
        mut work: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        self.limiter.enforce_quotas(&mut work, ctx).await?;
        Ok(Output::just(SpanOutput(work)))
    }
}

#[derive(Debug)]
pub struct SpanOutput(Managed<SerializedSpans>);

impl Forward for SpanOutput {
    fn serialize_envelope(self) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        debug_assert!(false, "Not Implemented Yet");
        Err(self
            .0
            .reject_err((Outcome::Invalid(DiscardReason::Internal), ())))
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        _s: &relay_system::Addr<crate::services::store::Store>,
    ) -> Result<(), Rejected<()>> {
        debug_assert!(false, "Not Implemented Yet");
        Err(self
            .0
            .reject_err((Outcome::Invalid(DiscardReason::Internal), ())))
    }
}

#[derive(Debug)]
pub struct SerializedSpans {
    /// Original envelope headers.
    _headers: EnvelopeHeaders,

    /// A list of spans waiting to be processed.
    ///
    /// All items contained here must be spans.
    spans: Vec<Item>,
}

impl Counted for SerializedSpans {
    fn quantities(&self) -> Quantities {
        // We rely here on the invariant that all items in `self.spans` are actually spans,
        // that's why sum up `item_count`'s blindly instead of checking again for the item type
        // or using `Item::quantities`.
        let quantity: u32 = self.spans.iter().filter_map(|item| item.item_count()).sum();
        let quantity = usize::try_from(quantity).unwrap_or(usize::MAX);

        smallvec::smallvec![
            (DataCategory::Span, quantity),
            (DataCategory::SpanIndexed, quantity),
        ]
    }
}

impl CountRateLimited for Managed<SerializedSpans> {
    type Error = Error;
}
