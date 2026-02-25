use std::sync::Arc;

use crate::Envelope;
use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemType};
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult as _, OutcomeError, Quantities, Rejected,
};
use crate::processing::errors::errors::SentryError as _;
use crate::processing::utils::event::{EventFullyNormalized, event_category};
use crate::processing::{self, Context, Forward, Output, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::ProcessingError;
use crate::statsd::RelayTimers;
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
use relay_event_schema::protocol::{Event, Metrics};
use relay_protocol::{Annotated, Empty};
use relay_quotas::DataCategory;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("envelope processor failed")]
    ProcessingFailed(#[from] ProcessingError),
}

impl OutcomeError for Error {
    type Error = Error;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
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

        let items = envelope
            .envelope_mut()
            .take_items_by(Item::requires_event)
            .into_vec();

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

        process::process(&mut error)?;

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
    items: Vec<Item>,
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

    /// The associated event.
    ///
    /// The event may be [`Annotated::empty`], if expansion of the event is delayed to a later
    /// Relay.
    ///
    /// Having no event in a processing Relay must result in an error and the entire event must be
    /// discarded.
    pub event: Annotated<Event>,
    /// Optional list of attachments sent with the event.
    pub attachments: Vec<Item>,
    /// Optional list of user reports sent with the event.
    pub user_reports: Vec<Item>,
    /// Custom event data.
    ///
    /// This may contain elements which are custom to the specific event shape being handled.
    pub data: errors::ErrorKind,
}

impl Counted for ExpandedError {
    fn quantities(&self) -> Quantities {
        let mut quantities = Quantities::default();

        // TODO: should this always count in the error category if empty?
        // TODO: how relevant is this for rate limits?
        //if !self.event.is_empty() {
        let category = event_category(&self.event).unwrap_or(DataCategory::Error);
        quantities.push((category, 1));
        //}

        quantities.extend(self.attachments.quantities());
        quantities.extend(self.user_reports.quantities());
        quantities.extend(self.data.quantities());

        quantities
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
            .try_map(|errors, _records| {
                let ExpandedError {
                    headers,
                    flags,
                    metrics: _,
                    event,
                    attachments,
                    user_reports,
                    data: error,
                } = errors;

                let mut items = Vec::with_capacity(1 + attachments.len() + user_reports.len());

                if let Some(ev) = event.value() {
                    let event_type = ev.ty.value().copied().unwrap_or_default();

                    let mut item = Item::new(ItemType::from_event_type(event_type));
                    item.set_payload(
                        ContentType::Json,
                        relay_statsd::metric!(timer(RelayTimers::EventProcessingSerialization), {
                            event.to_json().map_err(ProcessingError::SerializeFailed)?
                        }),
                    );

                    if flags.fully_normalized.0 {
                        item.set_fully_normalized(true);
                    }

                    items.push(item);
                }

                // The switch dying message counts as an error but also doesn't.
                // records.lenient(DataCategory::Error);

                error.serialize_into(&mut items, ctx)?;

                items.extend(attachments);
                items.extend(user_reports);

                // TODO: size limits?
                // TODO: metrics?

                Ok::<_, Error>(Envelope::from_parts(headers, items.into()))
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
