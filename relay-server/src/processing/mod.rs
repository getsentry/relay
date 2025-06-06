//! A new experimental way to structure Relay's processing pipeline.
//!
//! The idea is to slowly move processing steps from the original
//! [`processor`](crate::services::processor) module into this module.
//! This is where all the product processing logic should be located or called from.
//!
//! The processor service, will then do its actual work using the processing logic defined here.

use std::convert::Infallible;
use std::mem::ManuallyDrop;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::{DateTime, Utc};
use relay_config::Config;
use relay_dynamic_config::GlobalConfig;
use relay_event_schema::protocol::EventId;
use relay_protocol::Annotated;
use relay_quotas::{DataCategory, RateLimits, Scoping};
use relay_system::Addr;
use smallvec::SmallVec;

use crate::Envelope;
use crate::envelope::Item;
use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::services::processor::{ProcessingError, ProcessingExtractedMetrics};
use crate::services::projects::project::ProjectInfo;
use crate::services::test_store::TestStore;
use crate::utils::{EnvelopeSummary, ManagedEnvelope};

mod limits;
pub mod logs;
mod utils;

pub use self::limits::*;
pub use self::utils::*;

/// A processor, for an arbitrary unit of work extracted from an envelope.
///
/// The processor takes items from an envelope, to process it.
/// To fully process an envelope, multiple processor may need to take items from the same envelope.
///
/// Event based envelopes should only be handled by a single processor, as the protocol
/// defines all items in an event based envelope to relate to the envelope.
pub trait Processor {
    /// A unit of work, the processor can process.
    type UnitOfWork: Counted;
    /// The result after processing a [`Self::UnitOfWork`].
    type Output: Forward;
    /// The error returned by [`Self::process`].
    ///
    /// Implementations should use specific errors, instead of returning
    /// [`ProcessingError`] directly.
    type Error: Into<ProcessingError>;

    /// Extracts a [`Self::UnitOfWork`] from a [`ManagedEnvelope`].
    ///
    /// This is infallible, if a processor wants to report an error,
    /// it should return a [`Self::UnitOfWork`] which later, can produce an error when being
    ///
    // TODO: better name
    // TODO: specify that the unit of work needs to be tracked, maybe encode that as a Track<UnitOfWork>,
    // for outcomes etc.
    // Maybe unit of work needs to implement a trait which can describe contents for outcomes etc,
    // emitted by `Track`.
    // TODO: maybe we don't need this preparation step at all?
    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope)
    -> Option<Managed<Self::UnitOfWork>>;

    // TODO: move processing error when it makes sense
    // TODO: we may need multiple different entry points, one for each mode:
    //  - processing
    //  - managed internal (pop)
    //  - managed external
    //  - proxy
    //  - static
    // TODO: what are the outputs of the processing function?
    //  - extracted metrics
    //  - the result which can be converted into envelope + things for upstream?
    async fn process(
        &self,
        work: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>>;

    // TODO: maybe an on error here, might be hard to call with work (as ownership is gone)?
    // The handler could emit outcomes but propagate okay upwards or fail upwards?
}

/// Read-only context for processing.
#[derive(Copy, Clone)]
pub struct Context<'a> {
    pub config: &'a Config,
    pub global_config: &'a GlobalConfig,
    // pub scoping: Scoping, part of scoping
    pub project_info: &'a ProjectInfo,
    pub rate_limits: &'a RateLimits,
}

impl Context<'_> {
    /// Checks on-off feature flags for envelope items, like profiles and spans.
    ///
    /// It checks for the presence of the passed feature flag, but does not filter items
    /// when there is no full project config available. This is the case in stat and proxy
    /// Relays.
    pub fn should_filter(&self, feature: relay_dynamic_config::Feature) -> bool {
        use relay_config::RelayMode::*;

        match self.config.relay_mode() {
            Proxy | Static | Capture => false,
            Managed => !self.project_info.has_feature(feature),
        }
    }
}

// TODO: better docs:
/// A way to make sure outcomes have been emitted
#[derive(Debug, Clone, Copy)]
#[must_use = "a rejection must be propagated"]
pub struct Rejected<T>(T);

impl<T> Rejected<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

/// The main processing output and all of its by products.
///
/// TODO: the output needs to be tracked as well
pub struct Output<T> {
    pub main: T,
    pub metrics: ProcessingExtractedMetrics,
}

impl<T> Output<T> {
    pub fn just(main: T) -> Self {
        Self {
            main,
            metrics: ProcessingExtractedMetrics::new(),
        }
    }
}

pub trait Forward {
    // TODO: document what should happen in an error case
    // TODO: change the error type, it must contain information which outcomes to emit
    fn serialize_envelope(self) -> Result<Managed<Box<Envelope>>, Rejected<()>>;
}

// TODO: this could be an enumap
// TODO: this should probably be a new-type with the correct utilities to deal with indexed and
// non-indexed categories
pub type Quantities = SmallVec<[(DataCategory, usize); 1]>;

pub trait CountedType {
    fn quantities() -> Quantities;
}

impl<T> CountedType for Annotated<T>
where
    T: CountedType,
{
    fn quantities() -> Quantities {
        T::quantities()
    }
}

impl CountedType for relay_event_schema::protocol::OurLog {
    fn quantities() -> Quantities {
        // TODO: log byte size (and this is why byte sizes suck)
        smallvec::smallvec![(DataCategory::LogItem, 1)]
    }
}

pub trait Counted {
    // TODO: better docs, what type of quantities ('Usage::RateLimits' or 'Usage::Outcomes')
    /// This needs to be pure.
    fn quantities(&self) -> Quantities;
}

impl Counted for () {
    fn quantities(&self) -> Quantities {
        Quantities::new()
    }
}

impl Counted for Item {
    fn quantities(&self) -> Quantities {
        self.quantities()
    }
}

// TODO: just temporary, until `RecordKeeper` has better signatures
impl Counted for Annotated<relay_event_schema::protocol::OurLog> {
    fn quantities(&self) -> Quantities {
        // TODO: again, byte size
        smallvec::smallvec![(DataCategory::LogItem, 1)]
    }
}

impl Counted for Box<Envelope> {
    fn quantities(&self) -> Quantities {
        let mut quantities = Quantities::new();

        // TODO: longterm, this can be moved into `EnvelopeSummary` after we figure out
        // how indexed/non-indexed should behave. In this case here, we cannot generically
        // emit for the indexed category because we specifically need to differentiate.
        let summary = EnvelopeSummary::compute(self);
        if let Some(category) = summary.event_category {
            quantities.push((category, 1));
            if let Some(category) = category.index_category() {
                quantities.push((category, 1));
            }
        }

        let data = [
            (DataCategory::Attachment, summary.attachment_quantity),
            (DataCategory::Profile, summary.profile_quantity),
            (DataCategory::ProfileIndexed, summary.profile_quantity),
            (DataCategory::Span, summary.span_quantity),
            (DataCategory::SpanIndexed, summary.span_quantity),
            (
                DataCategory::Transaction,
                summary.secondary_transaction_quantity,
            ),
            (DataCategory::Span, summary.secondary_span_quantity),
            (DataCategory::Replay, summary.replay_quantity),
            (DataCategory::ProfileChunk, summary.profile_chunk_quantity),
            (
                DataCategory::ProfileChunkUi,
                summary.profile_chunk_ui_quantity,
            ),
            (DataCategory::LogItem, summary.log_item_quantity),
            (DataCategory::LogByte, summary.log_byte_quantity),
        ];

        for (category, quantity) in data {
            if quantity > 0 {
                quantities.push((category, quantity));
            }
        }

        quantities
    }
}

// TODO: this should be implemented, to make sure there are no conflicting impls,
// but currently can't be because of the `Counted for &T` impl.
// impl<T> Counted for T where T: CountedType {
//     fn quantities(&self) -> Quantities {
//         T::quantities()
//     }
// }

// TODO: maybe this wildcard impl sucks and counted should just be implemented on a reference but
// take self instead of &self
impl<T> Counted for &T
where
    T: Counted,
{
    fn quantities(&self) -> Quantities {
        (*self).quantities()
    }
}

pub struct Managed<T: Counted> {
    value: T,
    meta: Arc<Meta>,
    done: AtomicBool,
}

impl<T: Counted> Managed<T> {
    pub fn from_envelope(envelope: &ManagedEnvelope, value: T) -> Self {
        Self::from_parts(
            value,
            Arc::new(Meta {
                outcome_aggregator: envelope.outcome_aggregator().clone(),
                test_store: envelope.test_store().clone(),
                received_at: envelope.received_at(),
                scoping: envelope.scoping(),
                event_id: envelope.envelope().event_id(),
                remote_addr: envelope.meta().remote_addr(),
            }),
        )
    }

    pub fn wrap<S>(&self, other: S) -> Managed<S>
    where
        S: Counted,
    {
        Managed::from_parts(other, Arc::clone(&self.meta))
    }

    pub fn scoping(&self) -> Scoping {
        self.meta.scoping
    }

    pub fn map<S, F>(self, f: F) -> Managed<S>
    where
        F: FnOnce(T, &mut RecordKeeper) -> S,
        S: Counted,
    {
        self.try_map(move |inner, records| Ok::<_, Infallible>(f(inner, records)))
            .unwrap_or_else(|e| match e.0 {})
    }

    // TODO: this could take a trait as fn (a 'transformer')
    pub fn try_map<S, F, E>(self, f: F) -> Result<Managed<S>, Rejected<E::Error>>
    where
        F: FnOnce(T, &mut RecordKeeper) -> Result<S, E>,
        S: Counted,
        E: OutcomeError,
    {
        debug_assert!(!self.is_done());

        let (value, meta) = self.destructure();
        let quantities = value.quantities();

        let mut records = RecordKeeper::new(&meta, quantities);

        // TODO: make sure there are no double outcomes produced,
        // e.g. through multiple drop impls.
        match f(value, &mut records) {
            Ok(value) => {
                records.success(value.quantities());
                Ok(Managed::from_parts(value, meta))
            }
            Err(err) => Err(records.failure(err)),
        }

        // TODO: take quantities before and after and validate the amount of emitted outcomes
    }

    // TODO: this could be much nicer for iterated items, which need to be filtered
    pub fn modify<F>(&mut self, f: F)
    where
        F: FnOnce(&mut T, &mut RecordKeeper),
    {
        self.try_modify(move |inner, records| {
            f(inner, records);
            Ok::<_, Infallible>(())
        })
        .unwrap_or_else(|e| match e {})
    }

    pub fn try_modify<F, E>(&mut self, f: F) -> Result<(), Rejected<E::Error>>
    where
        F: FnOnce(&mut T, &mut RecordKeeper) -> Result<(), E>,
        E: OutcomeError,
    {
        debug_assert!(!self.is_done());

        let quantities = self.value.quantities();
        let mut records = RecordKeeper::new(&self.meta, quantities);

        match f(&mut self.value, &mut records) {
            Ok(()) => {
                records.success(self.value.quantities());
                Ok(())
            }
            Err(err) => {
                let err = records.failure(err);
                self.done.store(true, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    pub fn reject_err<E>(&self, error: E) -> Rejected<E::Error>
    where
        E: OutcomeError,
    {
        let (outcome, error) = error.consume();
        self.do_reject(outcome);
        Rejected(error)
    }

    fn do_reject(&self, outcome: Outcome) {
        if !self.done.fetch_or(true, Ordering::Relaxed) {
            for (category, quantity) in self.value.quantities() {
                self.meta.track_outcome(outcome.clone(), category, quantity);
            }
        }
    }

    /// Destructures this managed instance into its own parts.
    ///
    /// While destructured no outcomes will be emitted on drop.
    fn destructure(self) -> (T, Arc<Meta>) {
        // SAFETY: this follows an approach mentioned in the RFC
        // <https://github.com/rust-lang/rfcs/pull/3466> to move fields out of
        // a type with a drop implementation.
        //
        // The original type is wrapped in a manual drop to prevent running the
        // drop handler, afterwards all fields are moved out of the type.
        //
        // And the original type is forgotten, destructuring the original type
        // without running its drop implementation.
        let this = ManuallyDrop::new(self);
        let value = unsafe { std::ptr::read(&this.value) };
        let meta = unsafe { std::ptr::read(&this.meta) };
        (value, meta)
    }

    fn from_parts(value: T, meta: Arc<Meta>) -> Self {
        Self {
            value,
            meta,
            done: AtomicBool::new(false),
        }
    }

    fn is_done(&self) -> bool {
        self.done.load(Ordering::Relaxed)
    }
}

impl From<Managed<Box<Envelope>>> for ManagedEnvelope {
    fn from(value: Managed<Box<Envelope>>) -> Self {
        let (value, meta) = value.destructure();
        let mut envelope = ManagedEnvelope::new(
            value,
            meta.outcome_aggregator.clone(),
            meta.test_store.clone(),
        );
        envelope.scope(meta.scoping);
        envelope
    }
}

impl<T: Counted> Drop for Managed<T> {
    fn drop(&mut self) {
        self.do_reject(Outcome::Invalid(DiscardReason::Internal));
    }
}

impl<T: Counted> AsRef<T> for Managed<T> {
    fn as_ref(&self) -> &T {
        &self.value
    }
}

// TODO: maybe this should go, ideally we only want to allow modifications
// that make it impossible to mess up
impl<T: Counted> std::ops::Deref for Managed<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

pub trait OutcomeError {
    type Error;

    fn consume(self) -> (Outcome, Self::Error);
}

impl<E> OutcomeError for (Outcome, E) {
    type Error = E;

    fn consume(self) -> (Outcome, Self::Error) {
        self
    }
}

impl OutcomeError for ProcessingError {
    type Error = Self;

    fn consume(self) -> (Outcome, Self::Error) {
        // TODO: make our own, better error type than this pos
        let outcome = match &self {
            ProcessingError::DuplicateItem(_) => Outcome::Invalid(DiscardReason::DuplicateItem),
            _ => Outcome::Invalid(DiscardReason::Cors),
        };

        (outcome, self)
    }
}

impl OutcomeError for Infallible {
    type Error = Self;

    fn consume(self) -> (Outcome, Self::Error) {
        match self {}
    }
}

struct Meta {
    /// Outcome aggregator service.
    outcome_aggregator: Addr<TrackOutcome>,
    /// Test store service address.
    ///
    /// Only used for `ManagedEnvelope` <-> `Managed<T>` conversions.
    test_store: Addr<TestStore>,

    // TODO: only the 2 above are actually immutable, maybe it makes sense to split
    // the following fields out, to not arc them.
    /// Received timestamp, when the contained payload/information was received.
    ///
    /// See also: [`crate::extractors::RequestMeta::received_at`].
    received_at: DateTime<Utc>,
    /// Data scoping information of the contained item.
    scoping: Scoping,
    /// Optional event id associated with the contained data.
    event_id: Option<EventId>,
    /// Optional remote addr from where the data was received from.
    remote_addr: Option<IpAddr>,
}

impl Meta {
    pub fn track_outcome(&self, outcome: Outcome, category: DataCategory, quantity: usize) {
        self.outcome_aggregator.send(TrackOutcome {
            timestamp: self.received_at,
            scoping: self.scoping,
            outcome,
            event_id: self.event_id,
            remote_addr: self.remote_addr,
            category,
            quantity: quantity.try_into().unwrap_or(u32::MAX),
        });
    }
}

// TODO: actually write these docs
/// A thing that keeps outcomes (records) up to date, with what is happening.
pub struct RecordKeeper<'a> {
    meta: &'a Meta,
    on_drop: Quantities,
    // TODO: this doesn't actually just need to be quantities,
    // it also has to contain the outcomes to emit.
    in_flight: Quantities,
}

impl<'a> RecordKeeper<'a> {
    fn new(meta: &'a Meta, quantities: Quantities) -> Self {
        Self {
            meta,
            on_drop: quantities,
            in_flight: Default::default(),
        }
    }

    /// Defuses the drop guard and emits outcomes for the original quantities provided.
    fn failure<E>(mut self, error: E) -> Rejected<E::Error>
    where
        E: OutcomeError,
    {
        let (outcome, error) = error.consume();

        for (category, quantity) in std::mem::take(&mut self.on_drop) {
            self.meta.track_outcome(outcome.clone(), category, quantity);
        }

        Rejected(error)
    }

    /// Defuses the drop guard and emits the collected outcomes.
    fn success(mut self, _new: Quantities) {
        // TODO: we can debug assert, validate that the quantities before - in_flight,
        // equals the new quantities.

        let _original = std::mem::take(&mut self.on_drop);
        // TODO: assert_debug_eq!(new + in_flight, original);

        self.on_drop.clear();
        for (category, quantity) in std::mem::take(&mut self.in_flight) {
            self.meta.track_outcome(
                // TODO: this is the wrong outcome, the signatures need to be changed
                // to also contain outcomes.
                Outcome::Invalid(DiscardReason::Internal),
                category,
                quantity,
            );
        }
    }
}

impl<'a> Drop for RecordKeeper<'a> {
    fn drop(&mut self) {
        for (category, quantity) in std::mem::take(&mut self.on_drop) {
            self.meta.track_outcome(
                Outcome::Invalid(DiscardReason::Internal),
                category,
                quantity,
            );
        }
    }
}

// TODO: maybe some of these could be Result extensions
// TODO: quantities is not enough, we need outcomes here (an outcome reason)
impl RecordKeeper<'_> {
    pub fn or_default<T, E, Q>(&mut self, r: Result<T, E>, q: Q) -> T
    where
        T: Default,
        Q: Counted,
    {
        match r {
            Ok(result) => result,
            Err(_) => {
                let quantities = q.quantities();
                self.in_flight.extend_from_slice(&quantities);
                T::default()
            }
        }
    }

    // TODO: might need a better name
    pub fn with<T, E, F>(&mut self, r: Result<T, E>, f: F)
    where
        F: FnOnce(T),
        T: CountedType,
    {
        match r {
            Ok(result) => f(result),
            Err(_) => {
                let quantities = T::quantities();
                self.in_flight.extend_from_slice(&quantities);
            }
        }
    }
}

macro_rules! if_processing {
    ($ctx:expr, $if_true:block) => {
        #[cfg(feature = "processing")] {
            if $ctx.config.processing_enabled() $if_true
        }
    };
    ($ctx:expr, $if_true:block else $if_false:block) => {
        {
            #[cfg(feature = "processing")] {
                if $ctx.config.processing_enabled() $if_true else $if_false
            }
            #[cfg(not(feature = "processing"))] {
                $if_false
            }
        }
    };
}
use if_processing;
