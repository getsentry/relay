use std::sync::Arc;

use relay_quotas::RateLimits;

use crate::envelope::{EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Quantities, Rejected};
use crate::processing::{self, CountRateLimited, Output, QuotaRateLimiter};
#[cfg(feature = "processing")]
use crate::services::outcome::DiscardReason;
use crate::services::outcome::Outcome;
use crate::statsd::RelayDistributions;

mod forward;
mod process;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The Attachment was rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),

    /// The envelope did not contain an event ID.
    #[cfg(feature = "processing")]
    #[error("missing event ID")]
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
    type Input = SerializedAttachments;
    type Output = AttachmentsOutput;
    type Error = Error;

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        debug_assert!(
            !envelope.envelope().items().any(Item::creates_event),
            "AttachmentProcessor should not receive items that create events"
        );

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
        attachments: Managed<Self::Input>,
        ctx: processing::Context<'_>,
    ) -> Result<processing::Output<Self::Output>, Rejected<Self::Error>> {
        let client_name = crate::utils::client_name_tag(attachments.headers.meta().client_name());

        relay_statsd::metric!(
            distribution(RelayDistributions::StandaloneAttachmentCount) =
                attachments.attachments.len() as u64,
            sdk = client_name,
        );

        let mut log_emitted = false;
        for item in &attachments.attachments {
            let attachment_type_tag = match item.attachment_type() {
                Some(t) => &t.to_string(),
                None => "",
            };

            relay_statsd::metric!(
                distribution(RelayDistributions::StandaloneAttachmentSize) = item.len() as u64,
                sdk = client_name,
                attachment_type = attachment_type_tag,
            );
            if !log_emitted {
                relay_log::info!(
                    sdk = attachments.headers.meta().client(),
                    attachment_type = attachment_type_tag,
                    content_type = item.content_type().map_or("", |ct| ct.as_str()),
                    size = item.len(),
                    siblings = attachments.attachments.len(),
                    "standalone attachment",
                );
                log_emitted = true;
            }
        }

        let mut attachments = self.limiter.enforce_quotas(attachments, ctx).await?;
        process::scrub(&mut attachments, ctx)?;

        Ok(Output::just(AttachmentsOutput(attachments)))
    }
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
