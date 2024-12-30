use std::sync::Arc;

use relay_base_schema::project::ProjectId;
use relay_event_schema::protocol::Event;
use relay_quotas::RateLimits;

use crate::services::processor::{
    InnerProcessor, ProcessingError, ProcessingExtractedMetrics, ProcessingGroup, ProcessingResult,
};
use crate::services::projects::project::ProjectInfo;
use crate::utils::{ManagedEnvelope, TypedEnvelope};

mod check_in;
mod payload;

pub use check_in::ProcessCheckIn;

/// A macro that creates a new processing group type and implements necessary traits.
/// This macro reduces boilerplate code for creating new processing group types by automatically
/// implementing the group trait and conversion traits to/from processing group.
#[macro_export]
macro_rules! group {
    ($variant:ident, $ty:ident) => {
        #[derive(Clone, Copy, Debug)]
        pub struct $ty;

        impl Group for $ty {}

        impl From<$ty> for ProcessingGroup {
            fn from(_: $ty) -> Self {
                ProcessingGroup::$variant
            }
        }

        impl TryFrom<ProcessingGroup> for $ty {
            type Error = GroupTypeError;

            fn try_from(value: ProcessingGroup) -> Result<Self, Self::Error> {
                if matches!(value, ProcessingGroup::$variant) {
                    return Ok($ty);
                }
                return Err(GroupTypeError);
            }
        }
    };
}

/// A macro that creates a function to process specific group types.
/// Takes pairs of ([`ProcessingGroup`], [`ProcessGroup`]) identifiers and generates match arms for
/// each.
macro_rules! build_process_group {
    ($(($group:ident, $process_group:ident)),* $(,)?) => {
        pub fn supports_new_processing(group: &ProcessingGroup) -> bool {
            $(matches!(group, ProcessingGroup::$group) ||)* false
        }

        pub fn process_group(
            group: ProcessingGroup,
            managed_envelope: ManagedEnvelope,
            processor: Arc<InnerProcessor>,
            project_info: Arc<ProjectInfo>,
            project_id: ProjectId,
            rate_limits: Arc<RateLimits>,
        ) -> Result<ProcessingResult, ProcessingError> {
                match group {
                    $(
                        ProcessingGroup::$group => {
                            let mut managed_envelope = managed_envelope.try_into()?;
                            let params = GroupParams {
                                managed_envelope: &mut managed_envelope,
                                processor: processor.clone(),
                                rate_limits: rate_limits.clone(),
                                project_info: project_info.clone(),
                                project_id,
                            };

                            let group = $process_group::create(params);
                            match group.process() {
                                Ok(extracted_metrics) => Ok(ProcessingResult {
                                    managed_envelope: managed_envelope.into_processed(),
                                    extracted_metrics: extracted_metrics
                                        .map_or(ProcessingExtractedMetrics::new(), |e| e),
                                }),
                                Err(error) => {
                                    if let Some(outcome) = error.to_outcome() {
                                        managed_envelope.reject(outcome);
                                    }

                                    return Err(error);
                                }
                            }
                    }
                )*
                _ => {
                    relay_log::error!("unknown processing group");

                    Ok(ProcessingResult::no_metrics(
                        managed_envelope.into_processed(),
                    ))
                }
            }
        }
    };
}

/// A marker trait that identifies types which can be processed as groups.
/// Types implementing this trait represent different categories of data that can be
/// processed by Relay.
pub trait Group {}

/// Configuration parameters required to instantiate an instance of [`ProcessGroup`].
/// Contains all necessary context and resources for processing group data.
pub struct GroupParams<'a, G: Group> {
    pub managed_envelope: &'a mut TypedEnvelope<G>,
    pub processor: Arc<InnerProcessor>,
    pub rate_limits: Arc<RateLimits>,
    pub project_info: Arc<ProjectInfo>,
    pub project_id: ProjectId,
}

/// Represents the payload data associated with a [`ProcessGroup`].
/// Provides access to the managed envelope being processed and optional event data.
/// This trait defines the core data structure that processing groups operate on.
pub trait GroupPayload<'a, G: Group> {
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
pub trait ProcessGroup<'a> {
    /// The group of this processing group.
    /// Must implement [`Group`].
    type Group: Group;

    /// The payload type associated with this processing group.
    /// Must implement [`GroupPayload`].
    type Payload: GroupPayload<'a, Self::Group>;

    /// Creates a new instance of the processing group with the given [`GroupParams`].
    fn create(params: GroupParams<'a, Self::Group>) -> Self;

    /// Processes the group data and returns extracted metrics if any.
    ///
    /// Returns a [`Result`] containing optional [`ProcessingExtractedMetrics`] or a
    /// [`ProcessingError`].
    fn process(self) -> Result<Option<ProcessingExtractedMetrics>, ProcessingError>;
}

// Invocation of the macro which generates the code for statically dispatching the processing to
// the respective processing functions.
build_process_group!((CheckIn, ProcessCheckIn));
