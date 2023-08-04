//! Envelope context type and helpers to ensure outcomes.

use std::mem::size_of;
use std::time::Instant;

use chrono::{DateTime, Utc};
use relay_common::DataCategory;
use relay_quotas::Scoping;
use relay_system::Addr;

use crate::actors::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::actors::test_store::{Capture, TestStore};
use crate::envelope::{Envelope, Item};
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

/// Represents the decision on whether or not to keep an envelope item.
pub enum ItemAction {
    /// Keep the item.
    Keep,
    /// Drop the item and log an outcome for it.
    /// The outcome will only be logged if the item has a corresponding [`Item::outcome_category()`].
    Drop(Outcome),
    /// Drop the item without logging an outcome.
    DropSilently,
}

#[derive(Debug)]
struct EnvelopeContext {
    event_category: Option<DataCategory>,
    event_metrics_extracted: bool,
    scoping: Scoping,
    slot: Option<SemaphorePermit>,
    done: bool,
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
    context: EnvelopeContext,
    outcome_aggregator: Addr<TrackOutcome>,
    test_store: Addr<TestStore>,
}

impl ManagedEnvelope {
    /// Computes a managed envelope from the given envelope.
    fn new_internal(
        envelope: Box<Envelope>,
        slot: Option<SemaphorePermit>,
        outcome_aggregator: Addr<TrackOutcome>,
        test_store: Addr<TestStore>,
    ) -> Self {
        let meta = &envelope.meta();
        let summary = EnvelopeSummary::compute(envelope.as_ref(), None);
        let scoping = meta.get_partial_scoping();
        Self {
            envelope,
            context: EnvelopeContext {
                scoping,
                slot,
                done: false,
                event_category: summary.event_category,
                event_metrics_extracted: summary.event_metrics_extracted,
            },
            outcome_aggregator,
            test_store,
        }
    }

    /// Creates a standalone envelope for testing purposes.
    ///
    /// As opposed to [`new`](Self::new), this does not require a queue permit. This makes it
    /// suitable for unit testing internals of the processing pipeline.
    #[cfg(test)]
    pub fn standalone(
        envelope: Box<Envelope>,
        outcome_aggregator: Addr<TrackOutcome>,
        test_store: Addr<TestStore>,
    ) -> Self {
        Self::new_internal(envelope, None, outcome_aggregator, test_store)
    }

    #[cfg(test)]
    pub fn untracked(
        envelope: Box<Envelope>,
        outcome_aggregator: Addr<TrackOutcome>,
        test_store: Addr<TestStore>,
    ) -> Self {
        let mut envelope = Self::new_internal(envelope, None, outcome_aggregator, test_store);
        envelope.context.done = true;
        envelope
    }

    /// Computes a managed envelope from the given envelope and binds it to the processing queue.
    ///
    /// To provide additional scoping, use [`ManagedEnvelope::scope`].
    pub fn new(
        envelope: Box<Envelope>,
        slot: SemaphorePermit,
        outcome_aggregator: Addr<TrackOutcome>,
        test_store: Addr<TestStore>,
    ) -> Self {
        Self::new_internal(envelope, Some(slot), outcome_aggregator, test_store)
    }

    /// Returns a reference to the contained [`Envelope`].
    pub fn envelope(&self) -> &Envelope {
        self.envelope.as_ref()
    }

    /// Returns a mutable reference to the contained [`Envelope`].
    pub fn envelope_mut(&mut self) -> &mut Envelope {
        self.envelope.as_mut()
    }

    /// Consumes itself returning the managed envelope.
    ///
    /// This also releases the slot with [`SemaphorePermit`] and sets the internal context
    /// to done so there is no rejection issued once the [`ManagedEnvelope`] is consumed.
    pub fn into_envelope(mut self) -> Box<Envelope> {
        self.context.slot.take();
        self.context.done = true;
        Box::new(self.envelope.take_items())
    }

    /// Take the envelope out of the context and replace it with a dummy.
    ///
    /// Note that after taking out the envelope, the envelope summary is incorrect.
    pub(crate) fn take_envelope(&mut self) -> Box<Envelope> {
        Box::new(self.envelope.take_items())
    }

    /// Retains or drops items based on the [`ItemAction`].
    ///
    ///
    /// This method operates in place and preserves the order of the retained items.
    pub fn retain_items<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut Item) -> ItemAction,
    {
        let mut outcomes = vec![];
        let use_indexed = self.use_index_category();
        self.envelope.retain_items(|item| match f(item) {
            ItemAction::Keep => true,
            ItemAction::Drop(outcome) => {
                if let Some(category) = item.outcome_category(use_indexed) {
                    outcomes.push((outcome, category, item.quantity()));
                }

                false
            }
            ItemAction::DropSilently => false,
        });
        for (outcome, category, quantity) in outcomes {
            self.track_outcome(outcome, category, quantity);
        }
    }

    /// Assume that the envelope contains an event of the given data type.
    ///
    /// This is useful when the actual event item has already been removed for processing.
    pub fn assume_event(&mut self, category: Option<DataCategory>) {
        self.context.event_category = category;
    }

    /// Gets the value of the `event_metrics_extracted` flag.
    pub fn event_metrics_extracted(&self) -> bool {
        self.context.event_metrics_extracted
    }

    /// Record that event metrics have been extracted.
    ///
    /// This needs to be represented separately of the event's item header because during
    /// processing, the event item is removed from the envelope (see `assume_event`).
    pub fn set_event_metrics_extracted(&mut self, value: bool) -> &mut Self {
        self.context.event_metrics_extracted = value;
        self
    }

    /// Re-scopes this context to the given scoping.
    pub fn scope(&mut self, scoping: Scoping) -> &mut Self {
        self.context.scoping = scoping;
        self
    }

    /// Records an outcome scoped to this envelope's context.
    fn track_outcome(&self, outcome: Outcome, category: DataCategory, quantity: usize) {
        self.outcome_aggregator.send(TrackOutcome {
            timestamp: self.received_at(),
            scoping: self.context.scoping,
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
        if !self.context.done {
            self.finish(RelayCounters::EnvelopeAccepted, Handling::Success);
        }
    }

    /// Returns `true` if the indexed data category should be used for reporting.
    ///
    /// If metrics have been extracted from the event item, we use the indexed category
    /// (for example, [TransactionIndexed](`DataCategory::TransactionIndexed`)) for reporting
    /// rate limits and outcomes, because reporting of the main category
    /// (for example, [Transaction](`DataCategory::Transaction`) for processed transactions)
    /// will be handled by the metrics aggregator.
    fn use_index_category(&self) -> bool {
        self.context.event_metrics_extracted
    }

    /// Returns the data category of the event item in the envelope.
    ///
    /// If metrics have been extracted from the event item, this will return the indexing category.
    /// Outcomes for metrics (the base data category) will be logged by the metrics aggregator.
    fn event_category(&self) -> Option<DataCategory> {
        let category = self.context.event_category?;

        match category.index_category() {
            Some(index_category) if self.use_index_category() => Some(index_category),
            _ => Some(category),
        }
    }

    /// Records rejection outcomes for all items stored in this context.
    ///
    /// This does not send outcomes for empty envelopes or request-only contexts.
    pub fn reject(&mut self, outcome: Outcome) {
        if self.context.done {
            return;
        }

        let summary = self.compute_summary();

        // Errors are only logged for what we consider failed request handling. In other cases, we
        // "expect" errors and log them as debug level.
        let handling = Handling::from_outcome(&outcome);
        match handling {
            Handling::Success => relay_log::debug!("dropped envelope: {outcome}"),
            Handling::Failure => {
                relay_log::error!(
                    tags.has_attachments = summary.attachment_quantity > 0,
                    tags.has_sessions = summary.session_quantity > 0,
                    tags.has_profiles = summary.profile_quantity > 0,
                    tags.has_replays = summary.replay_quantity > 0,
                    tags.has_checkins = summary.checkin_quantity > 0,
                    tags.event_category = ?summary.event_category,
                    summary = ?summary,
                    "dropped envelope: {outcome}"
                );
            }
        }

        // TODO: This could be optimized with Capture::should_capture
        self.test_store
            .send(Capture::rejected(self.envelope.event_id(), &outcome));

        if let Some(category) = self.event_category() {
            self.track_outcome(outcome.clone(), category, 1);
        }

        if summary.attachment_quantity > 0 {
            self.track_outcome(
                outcome.clone(),
                DataCategory::Attachment,
                summary.attachment_quantity,
            );
        }

        if summary.profile_quantity > 0 {
            self.track_outcome(
                outcome,
                if self.use_index_category() {
                    DataCategory::ProfileIndexed
                } else {
                    DataCategory::Profile
                },
                summary.profile_quantity,
            );
        }

        self.finish(RelayCounters::EnvelopeRejected, handling);
    }

    /// Returns scoping stored in this context.
    pub fn scoping(&self) -> Scoping {
        self.context.scoping
    }

    pub fn meta(&self) -> &RequestMeta {
        self.envelope().meta()
    }

    /// Returns estimated size of this envelope.
    ///
    /// This is just an estimated size, which in reality can be somewhat bigger, depending on the
    /// list of additional attributes allocated on all of the inner types.
    ///
    /// NOTE: Current implementation counts in only the size of the items payload and stack
    /// allocated parts of [`Envelope`] and [`ManagedEnvelope`]. All the heap allocated fields
    /// within early mentioned types are skipped.
    pub fn estimated_size(&self) -> usize {
        let summary = self.compute_summary(); // TODO: expensive?

        // Always round it up to next 1KB.
        (f64::ceil(
            (summary.payload_size + size_of::<Self>() + size_of::<Envelope>()) as f64 / 1000.,
        ) * 1000.) as usize
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
        self.context.slot.take();

        relay_statsd::metric!(counter(counter) += 1, handling = handling.as_str());
        relay_statsd::metric!(timer(RelayTimers::EnvelopeTotalTime) = self.start_time().elapsed());

        self.context.done = true;
    }

    fn compute_summary(&self) -> EnvelopeSummary {
        let event_meta = self
            .context
            .event_category
            .map(|cat| (cat, self.context.event_metrics_extracted));
        EnvelopeSummary::compute(self.envelope(), event_meta)
    }
}

impl Drop for ManagedEnvelope {
    fn drop(&mut self) {
        self.reject(Outcome::Invalid(DiscardReason::Internal));
    }
}
