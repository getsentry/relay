//! A new experimental way to structure Relay's processing pipeline.
//!
//! The idea is to slowly move processing steps from the original
//! [`processor`](crate::services::processor) module into this module.
//! This is where all the product processing logic should be located or called from.
//!
//! The processor service, will then do its actual work using the processing logic defined here.

use std::net::IpAddr;

use chrono::{DateTime, Utc};
use relay_config::Config;
use relay_dynamic_config::GlobalConfig;
use relay_event_schema::protocol::EventId;
use relay_quotas::{DataCategory, RateLimits, Scoping};
use relay_system::Addr;
use smallvec::SmallVec;

use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::services::processor::{ProcessingError, ProcessingExtractedMetrics};
use crate::services::projects::project::ProjectInfo;
use crate::utils::ManagedEnvelope;

pub mod logs;

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
    fn process(
        &self,
        work: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Self::Error>;
}

/// Read-only context for processing.
pub struct Context<'a> {
    pub config: &'a Config,
    pub global_config: &'a GlobalConfig,
    // pub scoping: Scoping, part of scoping
    pub project_info: &'a ProjectInfo,
    pub rate_limits: &'a RateLimits,
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

pub type Quantities = SmallVec<[(DataCategory, usize); 3]>;

pub trait Counted {
    fn quantities(&self) -> Quantities;
}

impl Counted for () {
    fn quantities(&self) -> Quantities {
        Quantities::new()
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
