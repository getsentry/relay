//! Envelope context type and helpers to ensure outcomes.

use std::net;
use std::time::Instant;

use chrono::{DateTime, Utc};

use relay_common::DataCategory;
use relay_general::protocol::EventId;
use relay_quotas::Scoping;

use crate::actors::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::actors::test_store::{Capture, TestStore};
use crate::envelope::Envelope;
use crate::statsd::{RelayCounters, RelayTimers};
use crate::utils::{EnvelopeSummary, SemaphorePermit};

/// Tracks the lifetime of an [`Envelope`] in Relay.
///
/// The envelope context accompanies envelopes through the processing pipeline in Relay and ensures
/// that outcomes are recorded when the Envelope is dropped. They can be dropped in one of three
/// ways:
///
///  - By calling [`accept`](Self::accept). Responsibility of the envelope has been transferred to
///    another service, and no further outcomes need to be recorded.
///  - By calling [`reject`](Self::reject). The entire envelope was dropped, and the outcome
///    specifies the reason.
///  - By dropping the envelope context. This indicates an issue or a bug and raises the
///    `"internal"` outcome. There should be additional error handling to report an error to Sentry.
///
/// The envelope context also holds a processing queue permit which is used for backpressure
/// management. It is automatically reclaimed when the context is dropped along with the envelope.
#[derive(Debug)]
pub struct EnvelopeContext {
    summary: EnvelopeSummary,
    start_time: Instant,
    received_at: DateTime<Utc>,
    event_id: Option<EventId>,
    remote_addr: Option<net::IpAddr>,
    scoping: Scoping,
    slot: Option<SemaphorePermit>,
    done: bool,
}

impl EnvelopeContext {
    /// Computes an envelope context from the given envelope.
    fn new_internal(envelope: &Envelope, slot: Option<SemaphorePermit>) -> Self {
        let meta = &envelope.meta();
        Self {
            summary: EnvelopeSummary::compute(envelope),
            start_time: meta.start_time(),
            received_at: relay_common::instant_to_date_time(meta.start_time()),
            event_id: envelope.event_id(),
            remote_addr: meta.client_addr(),
            scoping: meta.get_partial_scoping(),
            slot,
            done: false,
        }
    }

    /// Creates a standalone `EnvelopeContext` for testing purposes.
    ///
    /// As opposed to [`new`](Self::new), this does not require a queue permit. This makes it
    /// suitable for unit testing internals of the processing pipeline.
    #[cfg(test)]
    pub fn standalone(envelope: &Envelope) -> Self {
        Self::new_internal(envelope, None)
    }

    /// Computes an envelope context from the given envelope and binds it to the processing queue.
    ///
    /// To provide additional scoping, use [`EnvelopeContext::scope`].
    pub fn new(envelope: &Envelope, slot: SemaphorePermit) -> Self {
        Self::new_internal(envelope, Some(slot))
    }

    /// Update the context with new envelope information.
    ///
    /// This updates the item summary as well as the event id.
    pub fn update(&mut self, envelope: &Envelope) -> &mut Self {
        self.event_id = envelope.event_id();
        self.summary = EnvelopeSummary::compute(envelope);
        self
    }

    /// TODO(ja): What about this?
    pub fn tmp_set_metrics_extracted(&mut self) -> &mut Self {
        self.summary.event_metrics_extracted = true;
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
        let outcome_aggregator = TrackOutcome::from_registry();
        outcome_aggregator.send(TrackOutcome {
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

    /// Returns the data category of the event item in the envelope.
    ///
    /// If metrics have been extracted from the event item, this will return the indexing category.
    /// Outcomes for metrics (the base data category) will be logged by the metrics aggregator.
    fn event_category(&self) -> Option<DataCategory> {
        let category = self.summary.event_category?;

        match category.index_category() {
            Some(category) if self.summary.event_metrics_extracted => Some(category),
            _ => Some(category),
        }
    }

    /// Records rejection outcomes for all items stored in this context.
    ///
    /// This does not send outcomes for empty envelopes or request-only contexts.
    pub fn reject(&mut self, outcome: Outcome) {
        if self.done {
            return;
        }

        relay_log::debug!("dropped envelope: {}", outcome);
        // TODO: This could be optimized with Capture::should_capture
        TestStore::from_registry().send(Capture::rejected(self.event_id, &outcome));

        if let Some(category) = self.event_category() {
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

    /// Returns the instant at which the envelope was received at this Relay.
    ///
    /// This is the monotonic time equivalent to [`received_at`](Self::received_at).
    pub fn start_time(&self) -> Instant {
        self.start_time
    }

    /// Returns the time at which the envelope was received at this Relay.
    ///
    /// This is the date time equivalent to [`start_time`](Self::start_time).
    pub fn received_at(&self) -> DateTime<Utc> {
        self.received_at
    }

    /// Resets inner state to ensure there's no more logging.
    fn finish(&mut self, counter: RelayCounters) {
        self.slot.take();

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
