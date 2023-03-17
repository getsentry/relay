//! Envelope context type and helpers to ensure outcomes.

use std::time::Instant;

use chrono::{DateTime, Utc};
use relay_common::DataCategory;
use relay_quotas::Scoping;

use crate::actors::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::actors::test_store::{Capture, TestStore};
use crate::envelope::Envelope;
use crate::extractors::RequestMeta;
use crate::statsd::{RelayCounters, RelayTimers};
use crate::utils::{EnvelopeSummary, SemaphorePermit};

/// Denotes the success of handling an envelope.
#[derive(Clone, Copy, Debug)]
enum Handling {
    /// The envelope was handled successfully.
    ///
    /// This can be the case even if the envelpoe was dropped. For example, if a rate limit is in
    /// effect or if the corresponding project is disabled.
    Success,
    /// Handling the envelope failed due to an error or bug.
    Failure,
}

impl Handling {
    fn from_outcome(outcome: &Outcome) -> Self {
        if outcome.is_unexpected() {
            Self::Failure
        } else {
            Self::Success
        }
    }

    fn as_str(&self) -> &str {
        match self {
            Handling::Success => "success",
            Handling::Failure => "failure",
        }
    }
}

/// Tracks the lifetime of an [`Envelope`] in Relay.
///
/// The managed envelope accompanies envelopes through the processing pipeline in Relay and ensures
/// that outcomes are recorded when the Envelope is dropped. They can be dropped in one of three
/// ways:
///
///  - By calling [`accept`](Self::accept). Responsibility of the envelope has been transferred to
///    another service, and no further outcomes need to be recorded.
///  - By calling [`reject`](Self::reject). The entire envelope was dropped, and the outcome
///    specifies the reason.
///  - By dropping the managed envelope. This indicates an issue or a bug and raises the
///    `"internal"` outcome. There should be additional error handling to report an error to Sentry.
///
/// The managed envelope also holds a processing queue permit which is used for backpressure
/// management. It is automatically reclaimed when the context is dropped along with the envelope.
#[derive(Debug)]
pub struct ManagedEnvelope {
    envelope: Box<Envelope>,
    summary: EnvelopeSummary,
    scoping: Scoping,
    slot: Option<SemaphorePermit>,
    done: bool,
}

impl ManagedEnvelope {
    /// Computes an managed envelope from the given envelope.
    fn new_internal(envelope: Box<Envelope>, slot: Option<SemaphorePermit>) -> Self {
        let meta = &envelope.meta();
        let summary = EnvelopeSummary::compute(envelope.as_ref());
        // let start_time = meta.start_time();
        // let received_at = relay_common::instant_to_date_time(start_time);
        // let event_id = envelope.event_id();
        // let remote_addr = meta.client_addr();
        let scoping = meta.get_partial_scoping();
        Self {
            envelope,
            summary,
            scoping,
            slot,
            done: false,
        }
    }

    /// Creates a standalone `EnvelopeContext` for testing purposes.
    ///
    /// As opposed to [`new`](Self::new), this does not require a queue permit. This makes it
    /// suitable for unit testing internals of the processing pipeline.
    #[cfg(test)]
    pub fn standalone(envelope: Box<Envelope>) -> Self {
        Self::new_internal(envelope, None)
    }

    /// Computes an managed envelope from the given envelope and binds it to the processing queue.
    ///
    /// To provide additional scoping, use [`ManagedEnvelope::scope`].
    pub fn new(envelope: Box<Envelope>, slot: SemaphorePermit) -> Self {
        Self::new_internal(envelope, Some(slot))
    }

    /// Returns a reference to the contained [`Envelope`].
    pub fn envelope(&self) -> &Envelope {
        self.envelope.as_ref()
    }

    /// Returns a mutable reference to the contained [`Envelope`].
    pub fn envelope_mut(&mut self) -> &mut Envelope {
        self.envelope.as_mut()
    }

    /// Take the envelope out of the context and replace it with a dummy.
    ///
    /// Note that after taking out the envelope, the envelope summary is incorrect.
    pub(crate) fn take_envelope(&mut self) -> Box<Envelope> {
        Box::new(self.envelope.take_items())
    }

    /// Update the context with envelope information.
    ///
    /// This updates the item summary as well as the event id.
    pub fn update(&mut self) -> &mut Self {
        self.summary = EnvelopeSummary::compute(self.envelope());
        self
    }

    /// Record that event metrics have been extracted.
    ///
    /// This is usually done automatically as part of `EnvelopeContext::new` or `update`. However,
    /// if the context needs to be updated in-flight without recomputing the entire summary, this
    /// method can record that metric extraction for the event item has occurred.
    pub fn set_event_metrics_extracted(&mut self) -> &mut Self {
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
    /// This managed envelope should be updated using [`update`](Self::update) soon after this
    /// operation to ensure that subsequent outcomes are consistent.
    pub fn track_outcome(&self, outcome: Outcome, category: DataCategory, quantity: usize) {
        let outcome_aggregator = TrackOutcome::from_registry();
        outcome_aggregator.send(TrackOutcome {
            timestamp: self.received_at(),
            scoping: self.scoping,
            outcome,
            event_id: self.envelope.event_id(),
            remote_addr: self.meta().remote_addr(),
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
            self.finish(RelayCounters::EnvelopeAccepted, Handling::Success);
        }
    }

    /// Returns the data category of the event item in the envelope.
    ///
    /// If metrics have been extracted from the event item, this will return the indexing category.
    /// Outcomes for metrics (the base data category) will be logged by the metrics aggregator.
    fn event_category(&self) -> Option<DataCategory> {
        let category = self.summary.event_category?;

        match category.index_category() {
            Some(index_category) if self.summary.event_metrics_extracted => Some(index_category),
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

        // Errors are only logged for what we consider failed request handling. In other cases, we
        // "expect" errors and log them as debug level.
        let handling = Handling::from_outcome(&outcome);
        match handling {
            Handling::Success => relay_log::debug!("dropped envelope: {}", outcome),
            Handling::Failure => relay_log::error!("dropped envelope: {}", outcome),
        }

        // TODO: This could be optimized with Capture::should_capture
        TestStore::from_registry().send(Capture::rejected(self.envelope.event_id(), &outcome));

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

        self.finish(RelayCounters::EnvelopeRejected, handling);
    }

    /// Returns scoping stored in this context.
    pub fn scoping(&self) -> Scoping {
        self.scoping
    }

    pub fn meta(&self) -> &RequestMeta {
        self.envelope().meta()
    }

    /// Returns the instant at which the envelope was received at this Relay.
    ///
    /// This is the monotonic time equivalent to [`received_at`](Self::received_at).
    pub fn start_time(&self) -> Instant {
        self.meta().start_time()
    }

    /// Returns the time at which the envelope was received at this Relay.
    ///
    /// This is the date time equivalent to [`start_time`](Self::start_time).
    pub fn received_at(&self) -> DateTime<Utc> {
        relay_common::instant_to_date_time(self.envelope().meta().start_time())
    }

    /// Resets inner state to ensure there's no more logging.
    fn finish(&mut self, counter: RelayCounters, handling: Handling) {
        self.slot.take();

        relay_statsd::metric!(counter(counter) += 1, handling = handling.as_str());
        relay_statsd::metric!(timer(RelayTimers::EnvelopeTotalTime) = self.start_time().elapsed());

        self.done = true;
    }
}

impl Drop for ManagedEnvelope {
    fn drop(&mut self) {
        self.reject(Outcome::Invalid(DiscardReason::Internal));
    }
}
