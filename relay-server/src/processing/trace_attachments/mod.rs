use std::sync::Arc;

use relay_dynamic_config::Feature;
use relay_event_schema::processor::ProcessingAction;
use relay_quotas::RateLimits;

use crate::envelope::{EnvelopeHeaders, Item, Items};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Quantities, Rejected};
use crate::processing::trace_attachments::process::ScrubAttachmentError;
use crate::processing::trace_attachments::types::ExpandedAttachment;
use crate::processing::{CountRateLimited, Processor, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};

use super::{Context, Output};

mod filter;
pub mod forward;
pub mod process;
#[cfg(feature = "processing")]
pub mod store;
pub mod types;

/// Any error that can occur during trace attachment processing.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Ingestion blocked by feature flag.
    #[error("feature disabled")]
    FeatureDisabled(Feature),

    /// Internal error, failed to re-serialize payload.
    #[error("failed to serialize")]
    SerializeFailed(#[from] serde_json::Error),

    /// Payload dropped by dynamic sampling.
    #[error("dropped by server-side sampling")]
    Sampled(Outcome),

    /// The attachments are rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),

    /// Internal error, Pii config could not be loaded.
    #[error("Pii configuration error")]
    PiiConfig,

    /// A processor failed to process the spans.
    #[error("envelope processor failed")]
    ProcessingFailed(#[from] ProcessingAction),
}

impl From<ScrubAttachmentError> for Error {
    fn from(value: ScrubAttachmentError) -> Self {
        match value {
            ScrubAttachmentError::PiiConfig => Self::PiiConfig,
            ScrubAttachmentError::ProcessingFailed(action) => Self::ProcessingFailed(action),
        }
    }
}

impl From<RateLimits> for Error {
    fn from(value: RateLimits) -> Self {
        Self::RateLimited(value)
    }
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::FeatureDisabled(f) => Outcome::Invalid(DiscardReason::FeatureDisabled(*f)),
            Self::SerializeFailed(_) => Outcome::Invalid(DiscardReason::Internal),
            Self::Sampled(outcome) => outcome.clone(),
            Self::PiiConfig => Outcome::Invalid(DiscardReason::ProjectStatePii),
            Self::RateLimited(rate_limits) => {
                let reason_code = rate_limits
                    .longest()
                    .and_then(|limit| limit.reason_code.clone());
                Outcome::RateLimited(reason_code)
            }
            Self::ProcessingFailed(_) => Outcome::Invalid(DiscardReason::Internal),
        };
        (Some(outcome), self)
    }
}

/// Processor for trace attachments (attachment V2 without span association).
#[derive(Debug)]
pub struct TraceAttachmentsProcessor {
    limiter: Arc<QuotaRateLimiter>,
}

impl TraceAttachmentsProcessor {
    /// Creates a new instance of the attachment processor.
    pub fn new(limiter: Arc<QuotaRateLimiter>) -> Self {
        Self { limiter }
    }
}

impl Processor for TraceAttachmentsProcessor {
    type UnitOfWork = SerializedAttachments;

    type Output = Managed<ExpandedAttachments>;

    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let headers = envelope.envelope().headers().clone();
        let items = envelope
            .envelope_mut()
            .take_items_by(Item::is_trace_attachment);

        (!items.is_empty()).then(|| {
            let work = SerializedAttachments { headers, items };
            Managed::derive_from(envelope, work)
        })
    }

    async fn process(
        &self,
        work: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let work = filter::feature_flag(work, ctx)?;

        let work = process::sample(work, ctx).await?;

        let work = process::expand(work);

        let mut work = self.limiter.enforce_quotas(work, ctx).await?;

        process::scrub(&mut work, ctx);

        Ok(Output::just(work))
    }
}

/// The attachments coming out of an envelope.
#[derive(Debug)]
pub struct SerializedAttachments {
    headers: EnvelopeHeaders,
    items: Items,
}

impl Counted for SerializedAttachments {
    fn quantities(&self) -> Quantities {
        let Self { headers: _, items } = self;
        items.quantities()
    }
}

/// Unprocessed attachments, after dynamic sampling.
#[derive(Debug)]
pub struct SampledAttachments {
    headers: EnvelopeHeaders,
    server_sample_rate: Option<f64>,
    items: Items,
}

impl Counted for SampledAttachments {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            server_sample_rate: _,
            items: attachments,
        } = self;
        attachments.quantities()
    }
}

/// Processed attachments.
#[derive(Debug)]
pub struct ExpandedAttachments {
    headers: EnvelopeHeaders,
    #[cfg_attr(not(feature = "processing"), expect(unused))]
    server_sample_rate: Option<f64>,
    attachments: Vec<ExpandedAttachment>,
}

impl Counted for ExpandedAttachments {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            server_sample_rate: _,
            attachments,
        } = self;
        attachments.quantities()
    }
}

impl CountRateLimited for Managed<ExpandedAttachments> {
    type Error = Error;
}
