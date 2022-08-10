//! Envelope context type and helpers to ensure outcomes.

use std::net;
use std::time::Instant;

use actix::SystemService;
use chrono::{DateTime, Utc};

use relay_common::DataCategory;
use relay_general::protocol::EventId;
use relay_quotas::Scoping;

use crate::actors::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::actors::outcome_aggregator::OutcomeAggregator;
use crate::envelope::Envelope;
use crate::statsd::{RelayCounters, RelayTimers};
use crate::utils::EnvelopeSummary;

/// Contains the required envelope related information to create an outcome.
#[derive(Debug)]
pub struct EnvelopeContext {
    summary: EnvelopeSummary,
    start_time: Instant,
    received_at: DateTime<Utc>,
    event_id: Option<EventId>,
    remote_addr: Option<net::IpAddr>,
    scoping: Scoping,
    done: bool,
}

impl EnvelopeContext {
    /// Computes an envelope context from the given envelope.
    ///
    /// To provide additional scoping, use [`EnvelopeContext::scope`].
    pub fn from_envelope(envelope: &Envelope) -> Self {
        let meta = &envelope.meta();
        Self {
            summary: EnvelopeSummary::compute(envelope),
            start_time: meta.start_time(),
            received_at: relay_common::instant_to_date_time(meta.start_time()),
            event_id: envelope.event_id(),
            remote_addr: meta.client_addr(),
            scoping: meta.get_partial_scoping(),
            done: false,
        }
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

    /// Records an outcome scoped to this envelope's context.
    ///
    /// This envelope context should be updated using [`update`](Self::update) soon after this
    /// operation to ensure that subsequent outcomes are consistent.
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

    /// Accepts the envelope and drops the context.
    ///
    /// This should be called if the envelope has been accepted by the upstream, which means that
    /// the responsibility for logging outcomes has been moved. This function will not log any
    /// outcomes.
    pub fn accept(mut self) {
        if !self.done {
            self.finish(RelayCounters::EnvelopeAccepted);
        }
    }

    /// Records rejection outcomes for all items stored in this context.
    ///
    /// This does not send outcomes for empty envelopes or request-only contexts.
    pub fn reject(&mut self, outcome: Outcome) {
        if self.done {
            return;
        }

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

        self.finish(RelayCounters::EnvelopeRejected);
    }

    /// Returns scoping stored in this context.
    pub fn scoping(&self) -> Scoping {
        self.scoping
    }

    /// Returns the time at which the envelope was received at this Relay.
    pub fn received_at(&self) -> DateTime<Utc> {
        self.received_at
    }

    /// Resets inner state to ensure there's no more logging.
    fn finish(&mut self, counter: RelayCounters) {
        relay_statsd::metric!(counter(counter) += 1);
        relay_statsd::metric!(timer(RelayTimers::EnvelopeTotalTime) = self.start_time.elapsed());
        self.done = true;
    }
}

impl Drop for EnvelopeContext {
    fn drop(&mut self) {
        self.reject(Outcome::Invalid(DiscardReason::Internal));
    }
}
