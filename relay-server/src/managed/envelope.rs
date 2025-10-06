use std::fmt::{Debug, Display};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use chrono::{DateTime, Utc};
use relay_quotas::{DataCategory, Scoping};
use relay_system::Addr;

use crate::envelope::{Envelope, Item};
use crate::extractors::RequestMeta;
use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::services::processor::{Processed, ProcessingGroup};
use crate::statsd::{RelayCounters, RelayTimers};
use crate::utils::EnvelopeSummary;

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
#[derive(Debug, Clone)]
pub enum ItemAction {
    /// Keep the item.
    Keep,
    /// Drop the item and log an outcome for it.
    Drop(Outcome),
    /// Drop the item without logging an outcome.
    DropSilently,
}

#[derive(Debug)]
struct EnvelopeContext {
    summary: EnvelopeSummary,
    scoping: Scoping,
    partition_key: Option<u32>,
    done: bool,
}

/// Error emitted when converting a [`ManagedEnvelope`] and a processing group into a [`TypedEnvelope`].
#[derive(Debug)]
pub struct InvalidProcessingGroupType(pub ManagedEnvelope, pub ProcessingGroup);

impl Display for InvalidProcessingGroupType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "failed to convert to the processing group {} based on the provided type",
            self.1.variant()
        ))
    }
}

impl std::error::Error for InvalidProcessingGroupType {}

/// A wrapper for [`ManagedEnvelope`] with assigned processing group type.
pub struct TypedEnvelope<G>(ManagedEnvelope, PhantomData<G>);

impl<G> TypedEnvelope<G> {
    /// Changes the typed of the current envelope to processed.
    ///
    /// Once it's marked processed it can be submitted to upstream.
    pub fn into_processed(self) -> TypedEnvelope<Processed> {
        TypedEnvelope::new(self.0)
    }

    /// Accepts the envelope and drops the internal managed envelope with its context.
    ///
    /// This should be called if the envelope has been accepted by the upstream, which means that
    /// the responsibility for logging outcomes has been moved. This function will not log any
    /// outcomes.
    pub fn accept(self) {
        self.0.accept()
    }

    /// Creates a new typed envelope.
    ///
    /// Note: this method is private to make sure that only `TryFrom` implementation is used, which
    /// requires the check for the error if conversion is failing.
    fn new(managed_envelope: ManagedEnvelope) -> Self {
        Self(managed_envelope, Default::default())
    }
}

impl<G: TryFrom<ProcessingGroup>> TryFrom<(ManagedEnvelope, ProcessingGroup)> for TypedEnvelope<G> {
    type Error = InvalidProcessingGroupType;
    fn try_from(
        (envelope, group): (ManagedEnvelope, ProcessingGroup),
    ) -> Result<Self, Self::Error> {
        match <ProcessingGroup as TryInto<G>>::try_into(group) {
            Ok(_) => Ok(TypedEnvelope::new(envelope)),
            Err(_) => Err(InvalidProcessingGroupType(envelope, group)),
        }
    }
}

impl<G> Debug for TypedEnvelope<G> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TypedEnvelope").field(&self.0).finish()
    }
}

impl<G> Deref for TypedEnvelope<G> {
    type Target = ManagedEnvelope;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<G> DerefMut for TypedEnvelope<G> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
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
    context: EnvelopeContext,
    outcome_aggregator: Addr<TrackOutcome>,
}

impl ManagedEnvelope {
    /// Computes a managed envelope from the given envelope and binds it to the processing queue.
    ///
    /// To provide additional scoping, use [`ManagedEnvelope::scope`].
    pub fn new(envelope: Box<Envelope>, outcome_aggregator: Addr<TrackOutcome>) -> Self {
        let meta = &envelope.meta();
        let summary = EnvelopeSummary::compute(envelope.as_ref());
        let scoping = meta.get_partial_scoping();

        Self {
            envelope,
            context: EnvelopeContext {
                summary,
                scoping,
                partition_key: None,
                done: false,
            },
            outcome_aggregator,
        }
    }

    /// An untracked instance which does not emit outcomes, useful for testing.
    #[cfg(test)]
    pub fn untracked(envelope: Box<Envelope>, outcome_aggregator: Addr<TrackOutcome>) -> Self {
        let mut envelope = Self::new(envelope, outcome_aggregator);
        envelope.context.done = true;
        envelope
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
    pub fn into_envelope(mut self) -> Box<Envelope> {
        self.context.done = true;
        self.take_envelope()
    }

    /// Converts current managed envelope into processed envelope.
    ///
    /// Once it's marked processed it can be submitted to upstream.
    pub fn into_processed(self) -> TypedEnvelope<Processed> {
        TypedEnvelope::new(self)
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
        let mut outcomes = Vec::new();
        self.envelope.retain_items(|item| match f(item) {
            ItemAction::Keep => true,
            ItemAction::DropSilently => false,
            ItemAction::Drop(outcome) => {
                for (category, quantity) in item.quantities() {
                    if let Some(indexed) = category.index_category() {
                        outcomes.push((outcome.clone(), indexed, quantity));
                    };
                    outcomes.push((outcome.clone(), category, quantity));
                }

                false
            }
        });
        for (outcome, category, quantity) in outcomes {
            self.track_outcome(outcome, category, quantity);
        }
        // TODO: once `update` is private, it should be called here.
    }

    /// Drops every item in the envelope.
    pub fn drop_items_silently(&mut self) {
        self.envelope.drop_items_silently();
    }

    /// Re-scopes this context to the given scoping.
    pub fn scope(&mut self, scoping: Scoping) -> &mut Self {
        self.context.scoping = scoping;
        self
    }

    /// Removes event item(s) and logs an outcome.
    ///
    /// Note: This function relies on the envelope summary being correct.
    pub fn reject_event(&mut self, outcome: Outcome) {
        if let Some(event_category) = self.event_category() {
            self.envelope.retain_items(|item| !item.creates_event());
            if let Some(indexed) = event_category.index_category() {
                self.track_outcome(outcome.clone(), indexed, 1);
            }
            self.track_outcome(outcome, event_category, 1);
        }
    }

    /// Records an outcome scoped to this envelope's context.
    ///
    /// This managed envelope should be updated using [`update`](Self::update) soon after this
    /// operation to ensure that subsequent outcomes are consistent.
    pub fn track_outcome(&self, outcome: Outcome, category: DataCategory, quantity: usize) {
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

    /// Returns the data category of the event item in the envelope.
    fn event_category(&self) -> Option<DataCategory> {
        self.context.summary.event_category
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
                    tags.project_key = self.scoping().project_key.to_string(),
                    tags.has_attachments = summary.attachment_quantity > 0,
                    tags.has_sessions = summary.session_quantity > 0,
                    tags.has_profiles = summary.profile_quantity > 0,
                    tags.has_transactions = summary.secondary_transaction_quantity > 0,
                    tags.has_span_metrics = summary.secondary_span_quantity > 0,
                    tags.has_replays = summary.replay_quantity > 0,
                    tags.has_user_reports = summary.user_report_quantity > 0,
                    tags.has_checkins = summary.monitor_quantity > 0,
                    tags.event_category = ?summary.event_category,
                    cached_summary = ?summary,
                    recomputed_summary = ?EnvelopeSummary::compute(self.envelope()),
                    "dropped envelope: {outcome}"
                );
            }
        }

        if let Some(category) = self.event_category() {
            if let Some(category) = category.index_category() {
                self.track_outcome(outcome.clone(), category, 1);
            }
            self.track_outcome(outcome.clone(), category, 1);
        }

        if self.context.summary.attachment_quantity > 0 {
            self.track_outcome(
                outcome.clone(),
                DataCategory::Attachment,
                self.context.summary.attachment_quantity,
            );
        }

        if self.context.summary.monitor_quantity > 0 {
            self.track_outcome(
                outcome.clone(),
                DataCategory::Monitor,
                self.context.summary.monitor_quantity,
            );
        }

        if self.context.summary.profile_quantity > 0 {
            self.track_outcome(
                outcome.clone(),
                DataCategory::Profile,
                self.context.summary.profile_quantity,
            );
            self.track_outcome(
                outcome.clone(),
                DataCategory::ProfileIndexed,
                self.context.summary.profile_quantity,
            );
        }

        if self.context.summary.span_quantity > 0 {
            self.track_outcome(
                outcome.clone(),
                DataCategory::Span,
                self.context.summary.span_quantity,
            );
            self.track_outcome(
                outcome.clone(),
                DataCategory::SpanIndexed,
                self.context.summary.span_quantity,
            );
        }

        if self.context.summary.log_item_quantity > 0 {
            self.track_outcome(
                outcome.clone(),
                DataCategory::LogItem,
                self.context.summary.log_item_quantity,
            );
        }
        if self.context.summary.log_byte_quantity > 0 {
            self.track_outcome(
                outcome.clone(),
                DataCategory::LogByte,
                self.context.summary.log_byte_quantity,
            );
        }

        // Track outcomes for attached secondary transactions, e.g. extracted from metrics.
        //
        // Primary transaction count is already tracked through the event category
        // (see: `Self::event_category()`).
        if self.context.summary.secondary_transaction_quantity > 0 {
            self.track_outcome(
                outcome.clone(),
                // Secondary transaction counts are never indexed transactions
                DataCategory::Transaction,
                self.context.summary.secondary_transaction_quantity,
            );
        }

        // Track outcomes for attached secondary spans, e.g. extracted from metrics.
        //
        // Primary span count is already tracked through `SpanIndexed`.
        if self.context.summary.secondary_span_quantity > 0 {
            self.track_outcome(
                outcome.clone(),
                // Secondary transaction counts are never indexed transactions
                DataCategory::Span,
                self.context.summary.secondary_span_quantity,
            );
        }

        if self.context.summary.replay_quantity > 0 {
            self.track_outcome(
                outcome.clone(),
                DataCategory::Replay,
                self.context.summary.replay_quantity,
            );
        }

        // Track outcomes for user reports, the legacy item type for user feedback.
        //
        // User reports are not events, but count toward UserReportV2 for quotas and outcomes.
        if self.context.summary.user_report_quantity > 0 {
            self.track_outcome(
                outcome.clone(),
                DataCategory::UserReportV2,
                self.context.summary.user_report_quantity,
            );
        }

        if self.context.summary.profile_chunk_quantity > 0 {
            self.track_outcome(
                outcome.clone(),
                DataCategory::ProfileChunk,
                self.context.summary.profile_chunk_quantity,
            );
        }

        if self.context.summary.profile_chunk_ui_quantity > 0 {
            self.track_outcome(
                outcome.clone(),
                DataCategory::ProfileChunkUi,
                self.context.summary.profile_chunk_ui_quantity,
            );
        }

        if self.context.summary.session_quantity > 0 {
            self.track_outcome(
                outcome.clone(),
                DataCategory::Session,
                self.context.summary.session_quantity,
            );
        }

        self.finish(RelayCounters::EnvelopeRejected, handling);
    }

    /// Returns scoping stored in this context.
    pub fn scoping(&self) -> Scoping {
        self.context.scoping
    }

    /// Returns the partition key, which is set on upstream requests in the `X-Sentry-Relay-Shard` header.
    pub fn partition_key(&self) -> Option<u32> {
        self.context.partition_key
    }

    /// Sets a new [`Self::partition_key`].
    pub fn set_partition_key(&mut self, partition_key: Option<u32>) -> &mut Self {
        self.context.partition_key = partition_key;
        self
    }

    /// Returns the contained original request meta.
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

    /// Returns the time at which the envelope was received at this Relay.
    ///
    /// This is the date time equivalent to [`start_time`](Self::received_at).
    pub fn received_at(&self) -> DateTime<Utc> {
        self.envelope.received_at()
    }

    /// Returns the time elapsed in seconds since the envelope was received by this Relay.
    ///
    /// In case the elapsed time is negative, it is assumed that no time elapsed.
    pub fn age(&self) -> Duration {
        self.envelope.age()
    }

    /// Escape hatch for the [`super::Managed`] type, to make it possible to construct
    /// from a managed envelope.
    pub(super) fn outcome_aggregator(&self) -> &Addr<TrackOutcome> {
        &self.outcome_aggregator
    }

    /// Resets inner state to ensure there's no more logging.
    fn finish(&mut self, counter: RelayCounters, handling: Handling) {
        relay_statsd::metric!(counter(counter) += 1, handling = handling.as_str());
        relay_statsd::metric!(timer(RelayTimers::EnvelopeTotalTime) = self.age());

        self.context.done = true;
    }
}

impl Drop for ManagedEnvelope {
    fn drop(&mut self) {
        self.reject(Outcome::Invalid(DiscardReason::Internal));
    }
}

impl<G> From<TypedEnvelope<G>> for ManagedEnvelope {
    fn from(value: TypedEnvelope<G>) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn span_metrics_are_reported() {
        let bytes =
            Bytes::from(r#"{"dsn":"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"}"#);
        let envelope = Envelope::parse_bytes(bytes).unwrap();

        let (outcome_aggregator, mut rx) = Addr::custom();
        let mut env = ManagedEnvelope::new(envelope, outcome_aggregator);
        env.context.summary.span_quantity = 123;
        env.context.summary.secondary_span_quantity = 456;

        env.reject(Outcome::Abuse);

        rx.close();

        let outcome = rx.blocking_recv().unwrap();
        assert_eq!(outcome.category, DataCategory::Span);
        assert_eq!(outcome.quantity, 123);
        assert_eq!(outcome.outcome, Outcome::Abuse);

        let outcome = rx.blocking_recv().unwrap();
        assert_eq!(outcome.category, DataCategory::SpanIndexed);
        assert_eq!(outcome.quantity, 123);
        assert_eq!(outcome.outcome, Outcome::Abuse);

        let outcome = rx.blocking_recv().unwrap();
        assert_eq!(outcome.category, DataCategory::Span);
        assert_eq!(outcome.quantity, 456);
        assert_eq!(outcome.outcome, Outcome::Abuse);

        assert!(rx.blocking_recv().is_none());
    }
}
