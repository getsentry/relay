use std::convert::Infallible;
use std::fmt;
use std::mem::ManuallyDrop;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::{DateTime, Utc};
use relay_event_schema::protocol::EventId;
use relay_quotas::{DataCategory, Scoping};
use relay_system::Addr;
use smallvec::SmallVec;

use crate::Envelope;
use crate::managed::{Counted, ManagedEnvelope, Quantities};
use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::services::processor::ProcessingError;
use crate::services::test_store::TestStore;

/// An error which can be extracted into an outcome.
pub trait OutcomeError {
    /// Produced error, without attached outcome.
    type Error;

    /// Consumes the error and returns an outcome and [`Self::Error`].
    ///
    /// Returning a `None` outcome should discard the item(s) silently.
    fn consume(self) -> (Option<Outcome>, Self::Error);
}

impl<E> OutcomeError for (Outcome, E) {
    type Error = E;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        (Some(self.0), self.1)
    }
}

impl<E> OutcomeError for (Option<Outcome>, E) {
    type Error = E;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        self
    }
}

impl OutcomeError for ProcessingError {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        (self.to_outcome(), self)
    }
}

impl OutcomeError for Infallible {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        match self {}
    }
}

/// A wrapper type which ensures outcomes have been emitted for an error.
///
/// [`Managed`] wraps an error in [`Rejected`] once outcomes for have been emitted for the managed
/// item.
#[derive(Debug, Clone, Copy)]
#[must_use = "a rejection must be propagated"]
pub struct Rejected<T>(T);

impl<T> Rejected<T> {
    /// Extracts the underlying error.
    pub fn into_inner(self) -> T {
        self.0
    }

    /// Maps the rejected error to a different error.
    pub fn map<F, S>(self, f: F) -> Rejected<S>
    where
        F: FnOnce(T) -> S,
    {
        Rejected(f(self.0))
    }
}

pub struct Managed<T: Counted> {
    value: T,
    meta: Arc<Meta>,
    done: AtomicBool,
}

impl<T: Counted> Managed<T> {
    /// Creates a new managed instance with a `value` from a [`ManagedEnvelope`].
    ///
    /// The [`Managed`] instance, inherits all metadata from the passed [`ManagedEnvelope`],
    /// like received time or scoping.
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

    /// Creates another [`Managed`] instance, with a new value but shared metadata.
    pub fn wrap<S>(&self, other: S) -> Managed<S>
    where
        S: Counted,
    {
        Managed::from_parts(other, Arc::clone(&self.meta))
    }

    /// Original received timestamp.
    pub fn received_at(&self) -> DateTime<Utc> {
        self.meta.received_at
    }

    /// Scoping information stored in this context.
    pub fn scoping(&self) -> Scoping {
        self.meta.scoping
    }

    pub fn split<F, I, S>(self, mut f: F) -> Split<I::IntoIter, I::Item>
    where
        F: FnMut(T) -> I,
        I: IntoIterator<Item = S>,
        S: Counted,
    {
        self.split_with_context(|value| (f(value), ())).0
    }

    pub fn split_with_context<F, I, S, C>(self, mut f: F) -> (Split<I::IntoIter, I::Item>, C)
    where
        F: FnMut(T) -> (I, C),
        I: IntoIterator<Item = S>,
        S: Counted,
    {
        let (value, meta) = self.destructure();
        #[cfg(debug_assertions)]
        let quantities = value.quantities();

        let (items, context) = f(value);

        (
            Split {
                #[cfg(debug_assertions)]
                quantities,
                items: items.into_iter(),
                meta,
                exhausted: false,
            },
            context,
        )
    }

    /// Maps a [`Managed<T>`] to [`Managed<S>`] by applying the mapping function `f`.
    ///
    /// Like [`Self::try_map`] but not fallible.
    pub fn map<S, F>(self, f: F) -> Managed<S>
    where
        F: FnOnce(T, &mut RecordKeeper) -> S,
        S: Counted,
    {
        self.try_map(move |inner, records| Ok::<_, Infallible>(f(inner, records)))
            .unwrap_or_else(|e| match e.0 {})
    }

    /// Maps a [`Managed<T>`] to [`Managed<S>`] by applying the mapping function `f`.
    ///
    /// The mapping function gets access to a [`RecordKeeper`], to emit outcomes for partial
    /// discards.
    ///
    /// If the mapping function returns an error, the entire (original) [`Self`] is rejected,
    /// no partial outcomes are emitted.
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

        match f(value, &mut records) {
            Ok(value) => {
                records.success(value.quantities());
                Ok(Managed::from_parts(value, meta))
            }
            Err(err) => Err(records.failure(err)),
        }
    }

    /// Gives mutable access to the contained value to modify it.
    ///
    /// Like [`Self::try_modify`] but not fallible.
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

    /// Gives mutable access to the contained value to modify it.
    ///
    /// The modifying function gets access to a [`RecordKeeper`], to emit outcomes for partial
    /// discards.
    ///
    /// If the modifying function returns an error, the entire (original) [`Self`] is rejected,
    /// no partial outcomes are emitted.
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

    /// Accepts the item of this managed instance.
    ///
    /// This should be called if the item has been or is about to be accepted by the upstream, which means that
    /// the responsibility for logging outcomes has been moved. This function will not log any
    /// outcomes.
    ///
    /// Like [`Self::try_accept`], but infallible.
    pub fn accept<F, S>(self, f: F) -> S
    where
        F: FnOnce(T) -> S,
    {
        self.try_accept(|item| Ok::<_, Infallible>(f(item)))
            .unwrap_or_else(|err| match err.0 {})
    }

    /// Accepts the item of this managed instance.
    ///
    /// This should be called if the item has been or is about to be accepted by the upstream.
    ///
    /// Outcomes are only emitted when the accepting closure returns an error, which means that
    /// in the success case the responsibility for logging outcomes has been moved to the
    /// caller/upstream.
    pub fn try_accept<F, S, E>(self, f: F) -> Result<S, Rejected<E::Error>>
    where
        F: FnOnce(T) -> Result<S, E>,
        E: OutcomeError,
    {
        debug_assert!(!self.is_done());

        let (value, meta) = self.destructure();
        let records = RecordKeeper::new(&meta, value.quantities());

        match f(value) {
            Ok(value) => {
                records.accept();
                Ok(value)
            }
            Err(err) => Err(records.failure(err)),
        }
    }

    /// Rejects the entire [`Managed`] instance with an internal error.
    ///
    /// Internal errors should be reserved for uses where logical invariants are violated.
    /// Cases which should never happen and always indicate a logical bug.
    ///
    /// This function will panic in debug builds, but discard the item
    /// with an internal discard reason in release builds.
    #[track_caller]
    pub fn internal_error(&self, reason: &'static str) -> Rejected<()> {
        relay_log::error!("internal error: {reason}");
        debug_assert!(false, "internal error: {reason}");
        self.reject_err((Outcome::Invalid(DiscardReason::Internal), ()))
    }

    /// Rejects the entire [`Managed`] instance.
    pub fn reject_err<E>(&self, error: E) -> Rejected<E::Error>
    where
        E: OutcomeError,
    {
        debug_assert!(!self.is_done());

        let (outcome, error) = error.consume();
        if let Some(outcome) = outcome {
            self.do_reject(outcome);
        }
        Rejected(error)
    }

    fn do_reject(&self, outcome: Outcome) {
        if !self.done.fetch_or(true, Ordering::Relaxed) {
            for (category, quantity) in self.value.quantities() {
                self.meta.track_outcome(outcome.clone(), category, quantity);
            }
        }
    }

    /// De-structures this managed instance into its own parts.
    ///
    /// While de-structured no outcomes will be emitted on drop.
    fn destructure(self) -> (T, Arc<Meta>) {
        // SAFETY: this follows an approach mentioned in the RFC
        // <https://github.com/rust-lang/rfcs/pull/3466> to move fields out of
        // a type with a drop implementation.
        //
        // The original type is wrapped in a manual drop to prevent running the
        // drop handler, afterwards all fields are moved out of the type.
        //
        // And the original type is forgotten, de-structuring the original type
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

impl<T: Counted> Drop for Managed<T> {
    fn drop(&mut self) {
        self.do_reject(Outcome::Invalid(DiscardReason::Internal));
    }
}

impl<T: Counted + fmt::Debug> fmt::Debug for Managed<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Managed<{}>[", std::any::type_name::<T>())?;
        for (i, (category, quantity)) in self.value.quantities().iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{category}:{quantity}")?;
        }
        write!(f, "](")?;
        self.value.fmt(f)?;
        write!(f, ")")
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

impl<T: Counted> AsRef<T> for Managed<T> {
    fn as_ref(&self) -> &T {
        &self.value
    }
}

impl<T: Counted> std::ops::Deref for Managed<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

/// Internal metadata attached with a [`Managed`] instance.
struct Meta {
    /// Outcome aggregator service.
    outcome_aggregator: Addr<TrackOutcome>,
    /// Test store service address.
    ///
    /// Only used for `ManagedEnvelope` <-> `Managed<T>` conversions.
    test_store: Addr<TestStore>,

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

/// A record keeper makes sure modifications done on a [`Managed`] item are all accounted for
/// correctly.
pub struct RecordKeeper<'a> {
    meta: &'a Meta,
    on_drop: Quantities,
    #[cfg(debug_assertions)]
    lenient: SmallVec<[DataCategory; 1]>,
    in_flight: SmallVec<[(DataCategory, usize, Option<Outcome>); 2]>,
}

impl<'a> RecordKeeper<'a> {
    fn new(meta: &'a Meta, quantities: Quantities) -> Self {
        Self {
            meta,
            on_drop: quantities,
            #[cfg(debug_assertions)]
            lenient: Default::default(),
            in_flight: Default::default(),
        }
    }

    /// Marking a data category as lenient exempts this category from outcome quantity validations.
    ///
    /// This can be used in cases where the quantity is knowingly modified, which is quite common
    /// for data categories which count bytes.
    pub fn lenient(&mut self, category: DataCategory) {
        let _category = category;
        #[cfg(debug_assertions)]
        self.lenient.push(_category);
    }

    /// Finalizes all records and emits the necessary outcomes.
    ///
    /// This uses the quantities of the original item.
    fn failure<E>(mut self, error: E) -> Rejected<E::Error>
    where
        E: OutcomeError,
    {
        let (outcome, error) = error.consume();

        if let Some(outcome) = outcome {
            for (category, quantity) in std::mem::take(&mut self.on_drop) {
                self.meta.track_outcome(outcome.clone(), category, quantity);
            }
        }

        Rejected(error)
    }

    /// Finalizes all records and asserts that no additional outcomes have been tracked.
    ///
    /// Unlike [`Self::success`], this method does not allow for intermediate or partial outcomes,
    /// it also does not verify any outcomes.
    ///
    /// This method is useful for using the record keeper to track failure outcomes, either
    /// explicit failures or panics.
    fn accept(mut self) {
        debug_assert!(
            self.in_flight.is_empty(),
            "records accepted, but intermediate outcomes tracked"
        );
        self.on_drop.clear();
    }

    /// Finalizes all records and emits the created outcomes.
    ///
    /// This only emits the outcomes that have been explicitly registered.
    /// In a debug build, the function also ensure no outcomes have been missed by comparing
    /// quantities of the item before and after.
    fn success(mut self, new: Quantities) {
        let original = std::mem::take(&mut self.on_drop);
        self.assert_quantities(original, new);

        self.on_drop.clear();
        for (category, quantity, outcome) in std::mem::take(&mut self.in_flight) {
            if let Some(outcome) = outcome {
                self.meta.track_outcome(outcome, category, quantity);
            }
        }
    }

    /// Asserts that there have been no quantities lost.
    ///
    /// The original amount of quantities should match the new amount of quantities + all emitted
    /// outcomes.
    #[cfg(debug_assertions)]
    fn assert_quantities(&self, original: Quantities, new: Quantities) {
        let mut original_sums = original.iter().fold(
            std::collections::BTreeMap::<DataCategory, usize>::new(),
            |mut acc, (category, quantity)| {
                *acc.entry(*category).or_default() += *quantity;
                acc
            },
        );

        macro_rules! emit {
            ($category:expr, $($tt:tt)*) => {{
                match self.lenient.contains(&$category) {
                    // Certain categories are known to be not always correct,
                    // they are logged instead.
                    true => relay_log::debug!($($tt)*),
                    false  => {
                        relay_log::error!("Original: {original:?}");
                        relay_log::error!("New: {new:?}");
                        relay_log::error!("In Flight: {:?}", self.in_flight);
                        panic!($($tt)*)
                    }
                }
            }};
        }

        for (category, quantity, outcome) in &self.in_flight {
            match original_sums.get_mut(category) {
                Some(c) if *c >= *quantity => *c -= *quantity,
                Some(c) => emit!(
                    category,
                    "Emitted {quantity} outcomes ({outcome:?}) for {category}, but there were only {c} items in the category originally"
                ),
                None => emit!(
                    category,
                    "Emitted {quantity} outcomes ({outcome:?}) for {category}, but there never was an item in this category"
                ),
            }
        }

        for (category, quantity) in &new {
            match original_sums.get_mut(category) {
                Some(c) if *c >= *quantity => *c -= *quantity,
                Some(c) => emit!(
                    category,
                    "New item has {quantity} items in category '{category}', but original (after emitted outcomes) only has {c} left"
                ),
                None => emit!(
                    category,
                    "New item has {quantity} items in category '{category}', but after emitted outcomes there are none left"
                ),
            }
        }

        for (category, quantity) in original_sums {
            if quantity > 0 {
                emit!(
                    category,
                    "Missing outcomes or mismatched quantity in category '{category}', off by {quantity}"
                );
            }
        }
    }

    #[cfg(not(debug_assertions))]
    fn assert_quantities(&self, _: Quantities, _: Quantities) {}
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

impl RecordKeeper<'_> {
    /// Rejects an item if the passed result is an error and returns a default value.
    ///
    /// Similar to [`Self::reject_err`], this emits the necessary outcomes for an
    /// item, if there is an error.
    pub fn or_default<T, E, Q>(&mut self, r: Result<T, E>, q: Q) -> T
    where
        T: Default,
        E: OutcomeError,
        Q: Counted,
    {
        match r {
            Ok(result) => result,
            Err(err) => {
                self.reject_err(err, q);
                T::default()
            }
        }
    }

    /// Rejects an item with an error.
    ///
    /// Makes sure the correct outcomes are tracked for the item, that is discarded due to an
    /// error.
    pub fn reject_err<E, Q>(&mut self, err: E, q: Q) -> E::Error
    where
        E: OutcomeError,
        Q: Counted,
    {
        let (outcome, err) = err.consume();
        for (category, quantity) in q.quantities() {
            self.in_flight.push((category, quantity, outcome.clone()))
        }
        err
    }
}

/// Iterator returned by [`Managed::split`].
pub struct Split<I, S>
where
    I: Iterator<Item = S>,
    S: Counted,
{
    #[cfg(debug_assertions)]
    quantities: Quantities,
    items: I,
    meta: Arc<Meta>,
    exhausted: bool,
}

impl<I, S> Split<I, S>
where
    I: Iterator<Item = S>,
    S: Counted,
{
    /// Subtracts passed quantities from the total quantities to verify total quantity counts are
    /// matching.
    #[cfg(debug_assertions)]
    fn subtract(&mut self, q: Quantities) {
        for (category, quantities) in q {
            let Some(orig_quantities) = self
                .quantities
                .iter_mut()
                .find_map(|(c, q)| (*c == category).then_some(q))
            else {
                debug_assert!(
                    false,
                    "mismatching quantities, item split into category {category}, \
                    which originally was not present"
                );
                continue;
            };

            if *orig_quantities >= quantities {
                *orig_quantities -= quantities;
            } else {
                debug_assert!(
                    false,
                    "in total more items produced in category {category} than originally available"
                );
            }
        }
    }
}

impl<I, S> Iterator for Split<I, S>
where
    I: Iterator<Item = S>,
    S: Counted,
{
    type Item = Managed<S>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: if there is none, all quantities should be empty
        let next = match self.items.next() {
            Some(next) => next,
            None => {
                self.exhausted = true;
                return None;
            }
        };

        #[cfg(debug_assertions)]
        self.subtract(next.quantities());

        Some(Managed::from_parts(next, Arc::clone(&self.meta)))
    }
}

impl<I, S> Drop for Split<I, S>
where
    I: Iterator<Item = S>,
    S: Counted,
{
    fn drop(&mut self) {
        // If the inner iterator was exhausted, no items should be remaining.
        #[cfg(debug_assertions)]
        if self.exhausted {
            for (category, quantities) in &self.quantities {
                debug_assert!(
                    *quantities == 0,
                    "items split, but still {quantities} remaining in category {category}"
                );
            }
        }

        if self.exhausted {
            return;
        }

        // There may be items remaining in the iterator for multiple reasons:
        // - there was a panic
        // - the iterator was never fully consumed
        //
        // In any case, outcomes must be emitted for the remaining items.
        for item in &mut self.items {
            for (category, quantity) in item.quantities() {
                self.meta.track_outcome(
                    Outcome::Invalid(DiscardReason::Internal),
                    category,
                    quantity,
                );
            }
        }
    }
}
