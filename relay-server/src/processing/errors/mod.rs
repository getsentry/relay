use std::sync::Arc;

use crate::Envelope;
use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemType};
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult as _, OutcomeError, Quantities, Rejected,
};
use crate::processing::errors::errors::SentryError as _;
use crate::processing::utils::event::EventFullyNormalized;
use crate::processing::{self, Context, Forward, Output, QuotaRateLimiter};
use crate::services::outcome::Outcome;
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
use relay_protocol::Annotated;
use relay_quotas::RateLimits;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("envelope processor failed")]
    ProcessingFailed(#[from] ProcessingError),
    #[error("rate limited")]
    RateLimited(RateLimits),
}

impl OutcomeError for Error {
    type Error = Error;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::ProcessingFailed(e) => e.to_outcome(),
            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
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
    type Input = SerializedError;
    type Output = ErrorOutput;
    type Error = Error;

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
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

        if items.is_empty() {
            return None;
        }

        let errors = SerializedError {
            headers: envelope.envelope().headers().clone(),
            items,
        };
        Some(Managed::with_meta_from(envelope, errors))
    }

    async fn process(
        &self,
        error: Managed<Self::Input>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let mut error = process::expand(error, ctx)?;
        #[cfg(feature = "processing")]
        process::validate_attachments(&mut error, ctx);

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

/// An error envelope which has been expanded.
#[derive(Debug)]
struct ExpandedError {
    /// Original envelope headers.
    pub headers: EnvelopeHeaders,
    /// Whether the payload has been fully normalized.
    pub fully_normalized: EventFullyNormalized,
    /// Metrics associated with the event.
    pub metrics: Metrics,

    /// The associated event.
    ///
    /// The event may be [`Annotated::empty`], if expansion of the event is delayed to a later
    /// Relay.
    ///
    /// Having no event in a processing Relay must result in an error and the entire event must be
    /// discarded.
    pub event: Box<Annotated<Event>>,
    /// Optional list of attachments sent with the event.
    pub attachments: Vec<Item>,
    /// Optional list of user reports sent with the event.
    pub user_reports: Vec<Item>,
    /// Custom event data.
    ///
    /// This may contain elements which are custom to the specific event shape being handled.
    pub data: errors::ErrorKind,
    /// Forward compatibility, unknown items.
    ///
    /// These items are not rate limited as Relay does not know about the items, so it will also
    /// not know how to rate limit them.
    /// They are still dropped if the entire event is rate limited/rejected.
    ///
    /// A processing Relay will always discard them, this Relay must know about all item types in
    /// use.
    pub other: Vec<Item>,
}

impl Counted for ExpandedError {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            fully_normalized: _,
            metrics: _,
            // Quantity inferred from `data.event_category()`.
            event: _,
            attachments,
            user_reports,
            data,
            other,
        } = self;

        let mut quantities = Quantities::default();

        quantities.push((data.event_category(), 1));
        quantities.extend(attachments.quantities());
        quantities.extend(user_reports.quantities());
        quantities.extend(data.quantities());
        quantities.extend(other.quantities());

        quantities
    }
}

impl processing::RateLimited for Managed<ExpandedError> {
    type Output = Self;
    type Error = Error;

    async fn enforce<R>(
        mut self,
        mut rate_limiter: R,
        _ctx: processing::Context<'_>,
    ) -> Result<Self::Output, Rejected<Self::Error>>
    where
        R: processing::RateLimiter,
    {
        let scoping = self.scoping();

        let limits = rate_limiter
            .try_consume(scoping.item(self.data.event_category()), 1)
            .await;

        if !limits.is_empty() {
            return Err(self.reject_err(Error::RateLimited(limits)));
        }

        for (category, quantity) in self.data.quantities() {
            let limits = rate_limiter
                .try_consume(scoping.item(category), quantity)
                .await;

            if !limits.is_empty() {
                self.try_modify(|this, records| {
                    this.data.apply_rate_limit(category, limits, records)
                })?;
                break;
            }
        }

        for (category, quantity) in self.attachments.quantities() {
            let limits = rate_limiter
                .try_consume(scoping.item(category), quantity)
                .await;

            if !limits.is_empty() {
                self.modify(|this, record_keeper| {
                    record_keeper.reject_err(
                        Error::RateLimited(limits),
                        std::mem::take(&mut this.attachments),
                    );
                });
                break;
            }
        }

        for (category, quantity) in self.user_reports.quantities() {
            let limits = rate_limiter
                .try_consume(scoping.item(category), quantity)
                .await;

            if !limits.is_empty() {
                self.modify(|this, record_keeper| {
                    record_keeper.reject_err(
                        Error::RateLimited(limits),
                        std::mem::take(&mut this.user_reports),
                    );
                });
                break;
            }
        }

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
                    fully_normalized,
                    metrics: _,
                    event,
                    attachments,
                    user_reports,
                    data,
                    other,
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

                    if fully_normalized.0 {
                        item.set_fully_normalized(true);
                    }

                    items.push(item);
                }

                data.serialize_into(&mut items, ctx)?;

                items.extend(attachments);
                items.extend(user_reports);

                if !ctx.config.processing_enabled() {
                    items.extend(other);
                } else {
                    debug_assert!(other.is_empty());
                }

                let envelope = Envelope::from_parts(headers, items.into());
                Ok::<_, Error>(envelope)
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
        s.send_envelope(ManagedEnvelope::from(envelope));
        Ok(())
    }
}
