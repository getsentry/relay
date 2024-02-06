use std::fmt::Debug;
use std::sync::Arc;

use relay_base_schema::events::EventType;
use relay_metrics::Bucket;
use relay_quotas::DataCategory;

use crate::envelope::Envelope;
use crate::services::outcome::Outcome;
use crate::services::project::ProjectState;
use crate::utils::ManagedEnvelope;

/// Marker trait for general processign state.
pub trait ProcessState {
    fn project_state(&self) -> &Arc<ProjectState>;
}

/// Marker trait for the process event processing state.
pub trait ProcessEvent: ProcessState {
    fn event_category(&self) -> Option<DataCategory>;
    fn has_event(&self) -> bool;
    fn event_type(&self) -> Option<EventType>;
    fn remove_event(&mut self);
    fn reject_event<Data: Container>(&mut self, data: &mut Data, outcome: Outcome);
}

/// Marker trait for sampling in processing state.
pub trait ProcessSampling: ProcessState {}

/// Marker trait for extracted metrics in processing state.
pub trait ProcessExtractedMetrics: ProcessState {
    fn event_metrics_extracted(&self) -> bool;
    fn extend_metrics(&mut self, metrics: Vec<Bucket>);
}

/// Marker trait for transaction processing with processing state.
pub trait ProcessTransaction: ProcessState {}

/// Marker trait for span processing.
pub trait ProcessSpan: ProcessState {}

pub trait Container: Debug {
    type Group;

    fn envelope(&self) -> &Envelope;
    fn envelope_mut(&mut self) -> &mut Envelope;
    fn managed_envelope_mut(&mut self) -> &mut ManagedEnvelope;
    fn managed_envelope(&self) -> &ManagedEnvelope;
    fn into_managed_envelope(self) -> ManagedEnvelope;
}
