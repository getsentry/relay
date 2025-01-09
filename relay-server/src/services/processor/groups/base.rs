use std::sync::Arc;

use relay_base_schema::project::ProjectId;
use relay_event_schema::protocol::Event;
use relay_quotas::RateLimits;

use crate::services::processor::groups::check_in::ProcessCheckIn;
use crate::services::processor::{
    InnerProcessor, ProcessingError, ProcessingExtractedMetrics, ProcessingGroup,
};
use crate::services::projects::project::ProjectInfo;
use crate::utils::ManagedEnvelope;

/// Configuration parameters required to instantiate an instance of [`ProcessGroup`].
/// Contains all necessary context and resources for processing group data.
pub struct GroupParams<'a> {
    pub managed_envelope: &'a mut ManagedEnvelope,
    pub processor: Arc<InnerProcessor>,
    pub rate_limits: Arc<RateLimits>,
    pub project_info: Arc<ProjectInfo>,
    pub project_id: ProjectId,
}

/// Result of a [`ProcessGroup`].
pub struct GroupResult {
    pub metrics: Option<ProcessingExtractedMetrics>,
}

impl GroupResult {
    /// Creates a new [`GroupResult`].
    pub fn new(metrics: Option<ProcessingExtractedMetrics>) -> Self {
        Self { metrics }
    }
}

/// Represents the payload data associated with a [`ProcessGroup`].
/// Provides access to the managed envelope being processed and optional event data.
/// This trait defines the core data structure that processing groups operate on.
pub trait GroupPayload<'a> {
    /// Gets a mutable reference to the [`ManagedEnvelope`] being processed.
    #[allow(dead_code)]
    fn managed_envelope_mut(&mut self) -> &mut ManagedEnvelope;

    /// Gets a reference to the [`ManagedEnvelope`] being processed.
    #[allow(dead_code)]
    fn managed_envelope(&self) -> &ManagedEnvelope;

    /// Gets a reference to the [`Event`] data if it exists.
    #[allow(dead_code)]
    fn event(&self) -> Option<&Event>;

    /// Removes the [`Event`] data from the payload.
    #[allow(dead_code)]
    fn remove_event(&mut self);
}

/// Defines the processing behavior for a specific group type.
/// Types implementing this trait represent the actual processing logic for different
/// kinds of data that flow through Relay.
pub trait ProcessGroup<'a>: Sized {
    /// Creates a new instance of the processing group with the given [`GroupParams`].
    fn create(params: GroupParams<'a>) -> Self;

    /// Processes the group data and returns extracted metrics if any.
    ///
    /// Returns a [`Result`] containing optional [`ProcessingExtractedMetrics`] or a
    /// [`ProcessingError`].
    fn process(self) -> Result<GroupResult, ProcessingError>;
}

/// A trait object wrapper for [`ProcessGroup`] that can be built dynamically.
pub trait DynProcessGroup {
    fn process(self: Box<Self>) -> Result<GroupResult, ProcessingError>;
}

impl<'a, T> DynProcessGroup for T
where
    T: ProcessGroup<'a> + 'a,
{
    fn process(self: Box<Self>) -> Result<GroupResult, ProcessingError> {
        T::process(*self)
    }
}

/// Builds a [`ProcessGroup`] given a [`ProcessingGroup`] and [`GroupParams`].
pub fn build_process_group<'a>(
    processing_group: ProcessingGroup,
    params: GroupParams<'a>,
) -> Option<Box<dyn DynProcessGroup + 'a>> {
    match processing_group {
        ProcessingGroup::CheckIn => Some(Box::new(ProcessCheckIn::create(params))),
        _ => None,
    }
}
