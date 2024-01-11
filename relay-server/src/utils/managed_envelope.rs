//! Envelope context type and helpers to ensure outcomes.

use std::mem::size_of;
use std::time::Instant;

use chrono::{DateTime, Utc};
use relay_quotas::{DataCategory, Scoping};
use relay_system::Addr;

use crate::actors::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::actors::processor::ProcessingGroup;
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
    summary: EnvelopeSummary,
    scoping: Scoping,
    slot: Option<SemaphorePermit>,
    partition_key: Option<u64>,
    done: bool,
    group: ProcessingGroup,
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
        group: ProcessingGroup,
    ) -> Self {
        let meta = &envelope.meta();
        let summary = EnvelopeSummary::compute(envelope.as_ref());
        let scoping = meta.get_partial_scoping();
        Self {
            envelope,
            context: EnvelopeContext {
                summary,
                scoping,
                slot,
                partition_key: None,
                done: false,
                group,
            },
            outcome_aggregator,
            test_store,
        }
    }

    #[cfg(test)]
    pub fn untracked(
        envelope: Box<Envelope>,
        outcome_aggregator: Addr<TrackOutcome>,
        test_store: Addr<TestStore>,
    ) -> Self {
        let mut envelope = Self::new_internal(
            envelope,
            None,
            outcome_aggregator,
            test_store,
            ProcessingGroup::Ungrouped,
        );
        envelope.context.done = true;
        envelope
    }

    /// Creates a new managed envelope like [`new`](Self::new) but without a queue permit.
    ///
    /// This is suitable for aggregated metrics. Metrics live outside the lifecycle of a normal
    /// event. They are extracted, aggregated and regularily flushed, after the
    /// source event has already been processed.
    ///
    /// The constructor is also suitable for unit testing internals of the processing pipeline.
    pub fn standalone(
        envelope: Box<Envelope>,
        outcome_aggregator: Addr<TrackOutcome>,
        test_store: Addr<TestStore>,
    ) -> Self {
        Self::new_internal(
            envelope,
            None,
            outcome_aggregator,
            test_store,
            ProcessingGroup::Ungrouped,
        )
    }

    /// Computes a managed envelope from the given envelope and binds it to the processing queue.
    ///
    /// To provide additional scoping, use [`ManagedEnvelope::scope`].
    pub fn new(
        envelope: Box<Envelope>,
        slot: SemaphorePermit,
        outcome_aggregator: Addr<TrackOutcome>,
        test_store: Addr<TestStore>,
        group: ProcessingGroup,
    ) -> Self {
        Self::new_internal(envelope, Some(slot), outcome_aggregator, test_store, group)
    }

    /// Returns a reference to the contained [`Envelope`].
    pub fn envelope(&self) -> &Envelope {
        self.envelope.as_ref()
    }

    /// Returns the [`ProcessingGroup`] where this envelope belongs to.
    pub fn group(&self) -> ProcessingGroup {
        self.context.group
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

    /// Update the context with envelope information.
    ///
    /// This updates the item summary as well as the event id.
    pub fn update(&mut self) -> &mut Self {
        self.context.summary = EnvelopeSummary::compute(self.envelope());
        self
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
                if let Some(category) = dbg!(item.outcome_category(use_indexed)) {
                    outcomes.push((outcome, category, item.quantity()));
                }

                false
            }
            ItemAction::DropSilently => false,
        });
        for (outcome, category, quantity) in outcomes {
            self.track_outcome(outcome, category, quantity);
        }
        // TODO: once `update` is private, it should be called here.
    }

    /// Record that event metrics have been extracted.
    ///
    /// This is usually done automatically as part of `EnvelopeContext::new` or `update`. However,
    /// if the context needs to be updated in-flight without recomputing the entire summary, this
    /// method can record that metric extraction for the event item has occurred.
    pub fn set_event_metrics_extracted(&mut self) -> &mut Self {
        self.context.summary.event_metrics_extracted = true;
        self
    }

    /// Re-scopes this context to the given scoping.
    pub fn scope(&mut self, scoping: Scoping) -> &mut Self {
        self.context.scoping = scoping;
        self
    }

    /// Remove event item(s) and log an outcome.
    ///
    /// Note: This function relies on the envelope summary being correct.
    pub fn reject_event(&mut self, outcome: Outcome) {
        if let Some(event_category) = self.event_category() {
            self.envelope.retain_items(|item| !item.creates_event());
            self.track_outcome(outcome, event_category, 1);
        }
    }

    /// Records an outcome scoped to this envelope's context.
    ///
    /// This managed envelope should be updated using [`update`](Self::update) soon after this
    /// operation to ensure that subsequent outcomes are consistent.
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
        self.context.summary.event_metrics_extracted
    }

    /// Returns the data category of the event item in the envelope.
    ///
    /// If metrics have been extracted from the event item, this will return the indexing category.
    /// Outcomes for metrics (the base data category) will be logged by the metrics aggregator.
    fn event_category(&self) -> Option<DataCategory> {
        let category = self.context.summary.event_category?;

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

        // Errors are only logged for what we consider failed request handling. In other cases, we
        // "expect" errors and log them as debug level.
        let handling = Handling::from_outcome(&outcome);
        match handling {
            Handling::Success => relay_log::debug!("dropped envelope: {outcome}"),
            Handling::Failure => {
                let summary = &self.context.summary;

                relay_log::error!(
                    tags.has_attachments = summary.attachment_quantity > 0,
                    tags.has_sessions = summary.session_quantity > 0,
                    tags.has_profiles = summary.profile_quantity > 0,
                    tags.has_transactions = summary.secondary_transaction_quantity > 0,
                    tags.has_replays = summary.replay_quantity > 0,
                    tags.has_checkins = summary.checkin_quantity > 0,
                    tags.event_category = ?summary.event_category,
                    cached_summary = ?summary,
                    recomputed_summary = ?EnvelopeSummary::compute(self.envelope()),
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

        if self.context.summary.attachment_quantity > 0 {
            self.track_outcome(
                outcome.clone(),
                DataCategory::Attachment,
                self.context.summary.attachment_quantity,
            );
        }

        if self.context.summary.profile_quantity > 0 {
            self.track_outcome(
                outcome.clone(),
                if self.use_index_category() {
                    DataCategory::ProfileIndexed
                } else {
                    DataCategory::Profile
                },
                self.context.summary.profile_quantity,
            );
        }

        // Track outcomes for attached secondary transactions, e.g. extracted from metrics.
        //
        // Primary transaction count is already tracked through the event category
        // (see: `Self::event_category()`).
        if self.context.summary.secondary_transaction_quantity > 0 {
            self.track_outcome(
                outcome,
                // Secondary transaction counts are never indexed transactions
                DataCategory::Transaction,
                self.context.summary.secondary_transaction_quantity,
            );
        }

        self.finish(RelayCounters::EnvelopeRejected, handling);
    }

    /// Returns scoping stored in this context.
    pub fn scoping(&self) -> Scoping {
        self.context.scoping
    }

    pub fn partition_key(&self) -> Option<u64> {
        self.context.partition_key
    }

    pub fn set_partition_key(&mut self, partition_key: Option<u64>) -> &mut Self {
        self.context.partition_key = partition_key;
        self
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
        // Always round it up to next 1KB.
        (f64::ceil(
            (self.context.summary.payload_size + size_of::<Self>() + size_of::<Envelope>()) as f64
                / 1000.,
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
        relay_common::time::instant_to_date_time(self.envelope().meta().start_time())
    }

    /// Resets inner state to ensure there's no more logging.
    fn finish(&mut self, counter: RelayCounters, handling: Handling) {
        self.context.slot.take();

        relay_statsd::metric!(counter(counter) += 1, handling = handling.as_str());
        relay_statsd::metric!(timer(RelayTimers::EnvelopeTotalTime) = self.start_time().elapsed());

        self.context.done = true;
    }
}

impl Drop for ManagedEnvelope {
    fn drop(&mut self) {
        self.reject(Outcome::Invalid(DiscardReason::Internal));
    }
}
