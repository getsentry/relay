use std::sync::Arc;

use crate::Envelope;
use crate::envelope::{EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult as _, OutcomeError, Quantities, Rejected,
};
use crate::processing::errors::errors::SentryError as _;
use crate::processing::utils::event::EventFullyNormalized;
use crate::processing::{self, Context, Forward, Output, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::ProcessingError;
use crate::utils::EnvelopeSummary;

mod dynamic_sampling;
#[allow(
    clippy::module_inception,
    reason = "all error types of the errors processor"
)]
mod errors;
mod filter;
mod process;

pub use errors::SwitchProcessingError;
use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::Metrics;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("TODO")]
    InvalidJson(serde_json::Error),
    #[error("envelope processor failed")]
    ProcessingFailed(#[from] ProcessingError),
}

impl OutcomeError for Error {
    type Error = Error;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::InvalidJson(_) => Some(Outcome::Invalid(DiscardReason::InvalidJson)),
            Self::ProcessingFailed(e) => e.to_outcome(),
        };
        (outcome, self)
    }
}

/// A processor for Error Events.
///
/// It processes all kinds of error events, user feedback, crashes, ...
pub struct ErrorsProcessor {
    limiter: Arc<QuotaRateLimiter>,
    geoip_lookup: GeoIpLookup,
}

impl ErrorsProcessor {
    /// Creates a new [`Self`].
    pub fn new(limiter: Arc<QuotaRateLimiter>, geoip_lookup: GeoIpLookup) -> Self {
        Self {
            limiter,
            geoip_lookup,
        }
    }
}

impl processing::Processor for ErrorsProcessor {
    type UnitOfWork = SerializedError;
    type Output = ErrorOutput;
    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let has_transaction = envelope
            .envelope()
            .items()
            .any(|item| item.ty() == &ItemType::Transaction);

        if has_transaction {
            return None;
        }

        let items = envelope.envelope_mut().take_items_by(Item::requires_event);

        let errors = SerializedError {
            headers: envelope.envelope().headers().clone(),
            items,
        };

        Some(Managed::with_meta_from(envelope, errors))
    }

    async fn process(
        &self,
        error: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let mut error = process::expand(error, ctx);

        process::process(&mut error, ctx)?;

        process::finalize(&mut error, ctx)?;
        process::normalize(&mut error, &self.geoip_lookup, ctx)?;

        filter::filter(&error, ctx).reject(&error)?;

        dynamic_sampling::apply(&mut error, ctx).await;

        let mut error = self.limiter.enforce_quotas(error, ctx).await?;

        process::scrub(&mut error, ctx)?;

        Ok(Output::just(ErrorOutput(error)))
    }
}

#[derive(Debug)]
pub struct SerializedError {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// List of items which can be processed as an error.
    ///
    /// This is a mixture of items all of which return `true` for [`Item::requires_event`].
    items: Items,
}

impl Counted for SerializedError {
    fn quantities(&self) -> Quantities {
        EnvelopeSummary::compute_items(self.items.iter()).quantities()
    }
}

#[derive(Debug)]
struct Flags {
    pub fully_normalized: EventFullyNormalized,
}

#[derive(Debug)]
struct ExpandedError {
    // TODO: event_id is a very important header, maybe pull it out to a field
    pub headers: EnvelopeHeaders,
    pub flags: Flags,
    pub metrics: Metrics,

    pub error: errors::ErrorKind,
}

impl Counted for ExpandedError {
    fn quantities(&self) -> Quantities {
        self.error.quantities()
    }
}

impl processing::RateLimited for Managed<ExpandedError> {
    type Output = Self;
    type Error = Error;

    async fn enforce<R>(
        self,
        _rate_limiter: R,
        _ctx: processing::Context<'_>,
    ) -> Result<Self::Output, Rejected<Self::Error>>
    where
        R: processing::RateLimiter,
    {
        Ok(self)
    }
}

#[derive(Debug)]
pub struct ErrorOutput(Managed<ExpandedError>);

impl Forward for ErrorOutput {
    fn serialize_envelope(
        self,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        self.0
            .try_map(|errors, _| {
                let mut items = errors.error.serialize(ctx)?;

                if let Some(event) = items.event.as_mut() {
                    event.set_fully_normalized(errors.flags.fully_normalized.0);
                }

                // TODO: size limits?

                Ok::<_, Error>(Envelope::from_parts(errors.headers, items.into()))
            })
            .map_err(|err| err.map(|_| ()))
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: processing::StoreHandle<'_>,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let envelope = self.serialize_envelope(ctx)?;
        let envelope = ManagedEnvelope::from(envelope).into_processed();

        s.store(crate::services::store::StoreEnvelope { envelope });

        Ok(())
    }
}
