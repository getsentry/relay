use std::sync::Arc;

use relay_quotas::RateLimits;

use crate::Envelope;
use crate::envelope::{EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Quantities, Rejected};
use crate::processing::{self, CountRateLimited, Forward, Output, QuotaRateLimiter, utils};
#[cfg(feature = "processing")]
use crate::services::outcome::DiscardReason;
use crate::services::outcome::Outcome;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The Attachment was rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),

    /// The envelope did not contain an event ID.
    #[cfg(feature = "processing")]
    #[error("missing replay ID")]
    NoEventId,
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<crate::services::outcome::Outcome>, Self::Error) {
        let outcome = match &self {
            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
            #[cfg(feature = "processing")]
            Self::NoEventId => Some(Outcome::Invalid(DiscardReason::Internal)),
        };
        (outcome, self)
    }
}

impl From<RateLimits> for Error {
    fn from(value: RateLimits) -> Self {
        Self::RateLimited(value)
    }
}

/// A processor for Attachments.
pub struct AttachmentProcessor {
    limiter: Arc<QuotaRateLimiter>,
}

impl AttachmentProcessor {
    /// Creates a new [`Self`].
    pub fn new(limiter: Arc<QuotaRateLimiter>) -> Self {
        Self { limiter }
    }
}

impl processing::Processor for AttachmentProcessor {
    type UnitOfWork = SerializedAttachments;
    type Output = AttachmentsOutput;
    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        // For now only extract the standalone attachments.
        if envelope.envelope().items().any(Item::creates_event) {
            return None;
        };

        let attachments = envelope
            .envelope_mut()
            .take_items_by(|i| i.requires_event() && matches!(i.ty(), ItemType::Attachment));

        if attachments.is_empty() {
            return None;
        }

        let headers = envelope.envelope().headers().clone();
        let work = SerializedAttachments {
            headers,
            attachments,
        };
        Some(Managed::with_meta_from(envelope, work))
    }

    async fn process(
        &self,
        attachments: Managed<Self::UnitOfWork>,
        ctx: processing::Context<'_>,
    ) -> Result<processing::Output<Self::Output>, Rejected<Self::Error>> {
        let mut attachments = self.limiter.enforce_quotas(attachments, ctx).await?;
        scrub(&mut attachments, ctx)?;

        Ok(Output::just(AttachmentsOutput(attachments)))
    }
}

/// Runs PiiProcessors on the attachments.
pub fn scrub(
    attachments: &mut Managed<SerializedAttachments>,
    ctx: processing::Context<'_>,
) -> Result<(), Rejected<Error>> {
    attachments.try_modify(|attachments, _| {
        utils::attachments::scrub(attachments.attachments.iter_mut(), ctx.project_info);
        Ok::<_, Error>(())
    })
}

/// Serialized attachments extracted from an envelope.
#[derive(Debug)]
pub struct SerializedAttachments {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// A list of attachments.
    attachments: Items,
}

impl Counted for SerializedAttachments {
    fn quantities(&self) -> Quantities {
        self.attachments.quantities()
    }
}

impl CountRateLimited for Managed<SerializedAttachments> {
    type Error = Error;
}

/// Output produced by the [`AttachmentProcessor`].
#[derive(Debug)]
pub struct AttachmentsOutput(Managed<SerializedAttachments>);

impl Forward for AttachmentsOutput {
    fn serialize_envelope(
        self,
        _: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<crate::Envelope>>, Rejected<()>> {
        let Self(attachments) = self;
        Ok(attachments.map(|attachments, _| {
            Envelope::from_parts(attachments.headers, attachments.attachments)
        }))
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: processing::StoreHandle<'_>,
        _: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let Self(attachments) = self;

        let event_id = attachments
            .headers
            .event_id()
            .ok_or_else(|| attachments.reject_err(Error::NoEventId).map(drop))?;

        for attachment in attachments.split(|attachment| attachment.attachments) {
            let store_attachment = attachment.map(|attachment, _| {
                use crate::services::store::StoreAttachment;
                let quantities = attachment.quantities();
                StoreAttachment {
                    event_id,
                    attachment,
                    quantities,
                }
            });
            s.send_to_store(store_attachment);
        }

        Ok(())
    }
}
