//! A new experimental way to structure Relay's processing pipeline.
//!
//! The idea is to slowly move processing steps from the original
//! [`processor`](crate::services::processor) module into this module.
//! This is where all the product processing logic should be located or called from.
//!
//! The processor service, will then do its actual work using the processing logic defined here.

use relay_config::Config;
use relay_dynamic_config::GlobalConfig;
use relay_quotas::RateLimits;

use crate::Envelope;
use crate::managed::{Counted, Managed, ManagedEnvelope, Rejected};
use crate::metrics_extraction::transactions::ExtractedMetrics;
use crate::services::projects::project::ProjectInfo;

mod common;
mod limits;
pub mod logs;
pub mod spans;

pub use self::common::*;
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
    type Output: Forward;
    /// The error returned by [`Self::process`].
    type Error: std::error::Error;

    /// Extracts a [`Self::UnitOfWork`] from a [`ManagedEnvelope`].
    ///
    /// This is infallible, if a processor wants to report an error,
    /// it should return a [`Self::UnitOfWork`] which later, can produce an error when being
    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope)
    -> Option<Managed<Self::UnitOfWork>>;

    /// Processes a [`Self::UnitOfWork`].
    async fn process(
        &self,
        work: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>>;
}

/// Read-only context for processing.
#[derive(Copy, Clone)]
pub struct Context<'a> {
    /// The Relay configuration.
    pub config: &'a Config,
    /// A view of the currently active global configuration.
    pub global_config: &'a GlobalConfig,
    /// Project configuration associated with the unit of work.
    pub project_info: &'a ProjectInfo,
    /// Project configuration associated with the root of the trace of the unit of work.
    pub sampling_project_info: Option<&'a ProjectInfo>,
    /// Cached rate limits associated with the unit of work.
    ///
    /// The caller needs to ensure the rate limits are not yet expired.
    pub rate_limits: &'a RateLimits,
}

impl Context<'_> {
    /// Returns `true` if Relay is running in proxy mode.
    pub fn is_proxy(&self) -> bool {
        matches!(self.config.relay_mode(), relay_config::RelayMode::Proxy)
    }

    /// Returns `true` if Relay has processing enabled.
    ///
    /// Processing indicates, this Relay is the final Relay processing this item.
    pub fn is_processing(&self) -> bool {
        self.config.processing_enabled()
    }

    /// Checks on-off feature flags for envelope items, like profiles and spans.
    ///
    /// It checks for the presence of the passed feature flag, but does not filter items
    /// when there is no full project config available. This is the case in stat and proxy
    /// Relays.
    pub fn should_filter(&self, feature: relay_dynamic_config::Feature) -> bool {
        use relay_config::RelayMode::*;

        match self.config.relay_mode() {
            Proxy => false,
            Managed => !self.project_info.has_feature(feature),
        }
    }
}

/// The main processing output and all of its by products.
#[derive(Debug)]
pub struct Output<T> {
    /// The main output of a [`Processor`].
    pub main: Option<T>,
    /// Metric by products.
    pub metrics: Option<Managed<ExtractedMetrics>>,
}

impl<T> Output<T> {
    /// Creates a new output with just a main output.
    pub fn just(main: T) -> Self {
        Self {
            main: Some(main),
            metrics: None,
        }
    }

    /// Creates a new output just containing metrics.
    pub fn metrics(metrics: Managed<ExtractedMetrics>) -> Self {
        Self {
            main: None,
            metrics: Some(metrics),
        }
    }

    /// Maps an `Output<T>` to `Output<S>` by applying a function to [`Self::main`].
    pub fn map<F, S>(self, f: F) -> Output<S>
    where
        F: FnOnce(T) -> S,
    {
        Output {
            main: self.main.map(f),
            metrics: self.metrics,
        }
    }
}

/// A processor output which can be forwarded to a different destination.
pub trait Forward {
    /// Serializes the output into an [`Envelope`].
    ///
    /// All output must be serializable as an envelope.
    fn serialize_envelope(self) -> Result<Managed<Box<Envelope>>, Rejected<()>>;

    /// Serializes the output into a [`crate::services::store::StoreService`] compatible format.
    ///
    /// This function must only be called when Relay is configured to be in processing mode.
    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: &relay_system::Addr<crate::services::store::Store>,
    ) -> Result<(), Rejected<()>>;
}
