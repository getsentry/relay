pub mod error;

use crate::metrics_extraction::transactions::ExtractedMetrics;
use crate::services::processor::{InnerProcessor, Processed, ProcessingError};
use crate::utils::TypedEnvelope;
use relay_base_schema::data_category::DataCategory;
use relay_event_schema::protocol::{Event, Metrics};
use relay_protocol::Annotated;
use std::sync::Arc;

pub struct ProcGroupParams<Group> {
    pub managed_envelope: TypedEnvelope<Group>,
    pub metrics: Metrics,
    pub event_fully_normalized: bool,
}

pub struct ProcGroupResult {
    pub managed_envelope: TypedEnvelope<Processed>,
    pub extracted_metrics: ExtractedMetrics,
}

pub struct ProcGroupError {
    pub managed_envelope: TypedEnvelope<Processed>,
    pub error: ProcessingError,
}

#[macro_export]
macro_rules! try_err {
    ($exp:expr, $managed_envelope:expr) => {
        match $exp {
            Ok(val) => val,
            Err(err) => {
                return Err(ProcGroupError {
                    managed_envelope: $managed_envelope.into_processed(),
                    error: err,
                });
            }
        }
    };
}

pub trait ProcGroup<Group> {
    fn create(inner: Arc<InnerProcessor>, params: ProcGroupParams<Group>) -> Self;

    fn process(self) -> Result<ProcGroupResult, ProcGroupError>;
}

pub fn event_category(event: &Annotated<Event>) -> Option<DataCategory> {
    event
        .value()
        .map(|event| event.ty.value().copied().unwrap_or_default())
        .map(DataCategory::from)
}
