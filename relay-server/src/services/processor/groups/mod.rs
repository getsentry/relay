use crate::services::processor::{InnerProcessor, ProcessingError, ProcessingExtractedMetrics};
use crate::services::projects::project::ProjectInfo;
use crate::utils::{ManagedEnvelope, TypedEnvelope};
use relay_base_schema::project::ProjectId;
use relay_event_schema::protocol::Event;
use relay_quotas::RateLimits;
use std::sync::Arc;

mod check_in;
mod payload;

pub use check_in::ProcessCheckIn;

/// Creates a group that can be bound to a [`ProcessGroup`].
#[macro_export]
macro_rules! group {
    ($ty:ident, $variant:ident) => {
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

pub trait Group {}

pub struct GroupParams<'a, G: Group> {
    pub managed_envelope: &'a mut TypedEnvelope<G>,
    pub processor: Arc<InnerProcessor>,
    pub rate_limits: Arc<RateLimits>,
    pub project_info: Arc<ProjectInfo>,
    pub project_id: ProjectId,
}

pub trait GroupPayload<'a, Group> {
    fn managed_envelope_mut(&mut self) -> &mut ManagedEnvelope;

    fn managed_envelope(&self) -> &ManagedEnvelope;

    fn event(&self) -> Option<&Event>;

    fn remove_event(&mut self);
}

pub trait ProcessGroup<'a, G: Group> {
    type Payload: GroupPayload<'a, G>;

    fn create(params: GroupParams<'a, G>) -> Self;

    fn process(self) -> Result<Option<ProcessingExtractedMetrics>, ProcessingError>;
}
