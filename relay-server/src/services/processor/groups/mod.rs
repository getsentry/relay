use crate::services::processor::{InnerProcessor, ProcessingError, ProcessingExtractedMetrics};
use crate::services::projects::project::ProjectInfo;
use crate::utils::ManagedEnvelope;
use relay_base_schema::project::ProjectId;
use relay_event_schema::protocol::Event;
use relay_quotas::RateLimits;
use std::sync::Arc;

mod check_in;
mod payload;

pub use check_in::CheckInGroup;
pub use payload::DefaultPayload;

pub struct GroupParams<'a> {
    pub managed_envelope: &'a mut ManagedEnvelope,
    pub processor: Arc<InnerProcessor>,
    pub rate_limits: Arc<RateLimits>,
    pub project_info: Arc<ProjectInfo>,
    pub project_id: ProjectId,
}

pub trait GroupPayload<'a> {
    fn managed_envelope_mut(&mut self) -> &mut ManagedEnvelope;

    fn managed_envelope(&self) -> &ManagedEnvelope;

    fn event_mut(&mut self) -> Option<&mut Event>;

    fn event(&self) -> Option<&Event>;

    fn remove_event(&mut self);
}

pub trait Group<'a>: Sized {
    type Payload: GroupPayload<'a>;

    fn create(params: GroupParams<'a>) -> Self;

    fn process(self) -> Result<Option<ProcessingExtractedMetrics>, ProcessingError>;
}
