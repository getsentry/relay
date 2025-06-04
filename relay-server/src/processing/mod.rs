//! A new experimental way to structure Relay's processing pipeline.
//!
//! The idea is to slowly move processing steps from the original
//! [`processor`](crate::services::processor) module into this module.
//! This is where all the product processing logic should be located or called from.
//!
//! The processor service, will then do its actual work using the processing logic defined here.

use std::convert::Infallible;
use std::net::IpAddr;

use chrono::{DateTime, Utc};
use relay_config::Config;
use relay_dynamic_config::GlobalConfig;
use relay_event_schema::protocol::EventId;
use relay_protocol::Annotated;
use relay_quotas::{DataCategory, RateLimits, Scoping};
use relay_system::Addr;
use smallvec::SmallVec;

use crate::envelope::{CountFor, Item};
use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::services::processor::{ProcessingError, ProcessingExtractedMetrics};
use crate::services::projects::project::ProjectInfo;
use crate::utils::ManagedEnvelope;

mod limits;
pub mod logs;

pub use self::limits::*;

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
    type Output;
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
    ) -> Result<Output<Self::Output>, Self::Error>;

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

// TODO: this could be an enumap
pub type Quantities = SmallVec<[(DataCategory, usize); 1]>;

trait CountedType {
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
        todo!()
    }
}

pub trait Counted {
    fn quantities(&self) -> Quantities;
}

impl Counted for () {
    fn quantities(&self) -> Quantities {
        Quantities::new()
    }
}

impl Counted for Item {
    fn quantities(&self) -> Quantities {
        // TODO: figure out differences with rate limits and outcomes,
        // ideally we get rid of this all-together.
        self.quantities(CountFor::Outcomes)
    }
}

// TODO: just temporary, until `RecordKeeper` has better signatures
impl Counted for Annotated<relay_event_schema::protocol::OurLog> {
    fn quantities(&self) -> Quantities {
        todo!()
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
    // TODO: store this stuff in an inner thing, maybe as Arc<Inner>
    outcome_aggregator: Addr<TrackOutcome>,
    // TODO: needs test_store here?
    received_at: DateTime<Utc>,
    scoping: Scoping,
    event_id: Option<EventId>,
    remote_addr: Option<IpAddr>,
}

impl<T: Counted> Managed<T> {
    pub fn from_envelope(envelope: &ManagedEnvelope, value: T) -> Self {
        Self {
            value,
            outcome_aggregator: todo!(),
            received_at: todo!(),
            scoping: todo!(),
            event_id: todo!(),
            remote_addr: todo!(),
        }
    }

    pub fn scoping(&self) -> Scoping {
        self.scoping
    }

    // pub fn split(self) -> (T, Managed<()>) {
    //     let other = self.wrap(());
    // }

    pub fn wrap<S>(&self, other: S) -> Managed<S>
    where
        S: Counted,
    {
        Managed {
            value: other,
            outcome_aggregator: self.outcome_aggregator.clone(),
            received_at: self.received_at,
            scoping: self.scoping,
            event_id: self.event_id,
            remote_addr: self.remote_addr,
        }
    }

    pub fn map<S, F>(self, f: F) -> Managed<S>
    where
        F: FnOnce(T, &mut RecordKeeper) -> S,
        S: Counted,
    {
        self.try_map(move |inner, records| Ok::<_, Infallible>(f(inner, records)))
            .unwrap_or_else(|e| match e {})
    }

    // TODO: this could take a trait as fn (a 'transformer')
    pub fn try_map<S, F, E>(self, f: F) -> Result<Managed<S>, E>
    where
        F: FnOnce(T, &mut RecordKeeper) -> Result<S, E>,
        S: Counted,
    {
        // TODO: take quantities before and after and validate the amount of emitted outcomes
        todo!()
    }

    // TODO: this could be much nicer for iterated items, which need to be filtered
    pub fn modify<F>(self, f: F)
    where
        F: FnOnce(&mut T, &mut RecordKeeper),
    {
        self.try_modify(move |inner, records| Ok::<_, Infallible>(f(inner, records)))
            .unwrap_or_else(|e| match e {})
    }

    pub fn try_modify<F, E>(self, f: F) -> Result<(), E>
    where
        F: FnOnce(&mut T, &mut RecordKeeper) -> Result<(), E>,
    {
        // TODO: take quantities before and after and validate the amount of emitted outcomes
        todo!()
    }

    pub fn reject(&mut self, outcome: Outcome) {
        for (category, quantity) in self.value.quantities() {
            self.track_outcome(outcome.clone(), category, quantity);
        }
    }

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

impl<T: Counted> Drop for Managed<T> {
    fn drop(&mut self) {
        self.reject(Outcome::Invalid(DiscardReason::Internal));
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

// TODO: actually write these docs
/// A thing that keeps outcomes (records) up to date, with what is happening.
pub struct RecordKeeper {}

// TODO: maybe some of these could be Result extensions
// TODO: quantities is not enough, we need outcomes here (an outcome reason)
impl RecordKeeper {
    pub fn or_default<T, E, Q>(&mut self, r: Result<T, E>, q: Q) -> T
    where
        T: Default,
        Q: Counted,
    {
        match r {
            Ok(result) => result,
            Err(_) => {
                let quantities = q.quantities();
                // TODO: use those quantities
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
                // TODO: use those quantities
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
pub(self) use if_processing;
