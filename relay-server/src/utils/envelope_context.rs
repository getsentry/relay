//! Envelope context type and helpers to ensure outcomes.

use std::net;

use actix::prelude::dev::ToEnvelope;
use actix::prelude::*;
use chrono::{DateTime, Utc};
use futures01::prelude::*;

use relay_common::DataCategory;
use relay_general::protocol::EventId;
use relay_quotas::Scoping;

use crate::actors::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::actors::outcome_aggregator::OutcomeAggregator;
use crate::envelope::Envelope;
use crate::extractors::RequestMeta;
use crate::utils::EnvelopeSummary;

/// Contains the required envelope related information to create an outcome.
#[derive(Clone, Copy, Debug)]
pub struct EnvelopeContext {
    summary: EnvelopeSummary,
    received_at: DateTime<Utc>,
    event_id: Option<EventId>,
    remote_addr: Option<net::IpAddr>,
    scoping: Scoping,
}

impl EnvelopeContext {
    /// Creates an envelope context from the given request meta data.
    ///
    /// This context contains partial scoping and no envelope summary. There will be no outcomes
    /// logged without updating this context.
    pub fn from_request(meta: &RequestMeta) -> Self {
        Self {
            summary: EnvelopeSummary::empty(),
            received_at: relay_common::instant_to_date_time(meta.start_time()),
            event_id: None,
            remote_addr: meta.client_addr(),
            scoping: meta.get_partial_scoping(),
        }
    }

    /// Computes an envelope context from the given envelope.
    ///
    /// To provide additional scoping, use [`EnvelopeContext::scope`].
    pub fn from_envelope(envelope: &Envelope) -> Self {
        let mut context = Self::from_request(envelope.meta());
        context.update(envelope);
        context
    }

    /// Update the context with new envelope information.
    ///
    /// This updates the item summary as well as the event id.
    pub fn update(&mut self, envelope: &Envelope) -> &mut Self {
        self.event_id = envelope.event_id();
        self.summary = EnvelopeSummary::compute(envelope);
        self
    }

    /// Re-scopes this context to the given scoping.
    pub fn scope(&mut self, scoping: Scoping) -> &mut Self {
        self.scoping = scoping;
        self
    }

    /// Records outcomes for all items stored in this context.
    ///
    /// This does not send outcomes for empty envelopes or request-only contexts.
    pub fn send_outcomes(&self, outcome: Outcome) {
        if let Some(category) = self.summary.event_category {
            self.track_outcome(outcome.clone(), category, 1);
        }

        if self.summary.attachment_quantity > 0 {
            self.track_outcome(
                outcome.clone(),
                DataCategory::Attachment,
                self.summary.attachment_quantity,
            );
        }

        if self.summary.profile_quantity > 0 {
            self.track_outcome(
                outcome,
                DataCategory::Profile,
                self.summary.profile_quantity,
            );
        }
    }

    /// Records an outcome scoped to this envelope's context.
    pub fn track_outcome(&self, outcome: Outcome, category: DataCategory, quantity: usize) {
        let outcome_aggregator = OutcomeAggregator::from_registry();
        outcome_aggregator.do_send(TrackOutcome {
            timestamp: self.received_at,
            scoping: self.scoping,
            outcome,
            event_id: self.event_id,
            remote_addr: self.remote_addr,
            category,
            // Quantities are usually `usize` which lets us go all the way to 64-bit on our
            // machines, but the protocol and data store can only do 32-bit.
            quantity: quantity as u32,
        });
    }

    /// Returns scoping stored in this context.
    pub fn scoping(&self) -> Scoping {
        self.scoping
    }

    /// Returns the event id of this context, if any.
    pub fn event_id(&self) -> Option<EventId> {
        self.event_id
    }

    /// Returns the time at which the envelope was received at this Relay.
    pub fn received_at(&self) -> DateTime<Utc> {
        self.received_at
    }
}

/// Extension trait for [`Addr`] to log [`Outcome`] for failed messages.
pub trait SendWithOutcome<A> {
    /// Sends an asynchronous message to the actor, tracking outcomes on failure.
    ///
    /// Communication channel to the actor is bounded. If the returned `Future` object get dropped,
    /// the message cancels.
    ///
    /// If the actor rejects the message, an `Invalid` outcome with internal reason is logged. Any
    /// error within the message result is not tracked by this function.
    fn send_tracked<M>(
        &self,
        message: M,
        envelope_context: EnvelopeContext,
    ) -> ResponseFuture<M::Result, MailboxError>
    where
        M: Message,
        A: Handler<M>,
        A::Context: ToEnvelope<A, M>,
        M: Message + Send + 'static,
        M::Result: Send;
}

impl<A> SendWithOutcome<A> for Addr<A>
where
    A: Actor,
{
    fn send_tracked<M>(
        &self,
        message: M,
        envelope_context: EnvelopeContext,
    ) -> ResponseFuture<M::Result, MailboxError>
    where
        A: Handler<M>,
        A::Context: ToEnvelope<A, M>,
        M: Message + Send + 'static,
        M::Result: Send,
    {
        let future = self.send(message).map_err(move |mailbox_error| {
            envelope_context.send_outcomes(Outcome::Invalid(DiscardReason::Internal));
            mailbox_error
        });

        Box::new(future)
    }
}
