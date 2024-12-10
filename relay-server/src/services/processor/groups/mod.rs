/*

High-level design:

* Initialization struct
* State struct for each group
* Entry function for the group
* Each group has access to the processor (`InnerProcessor`)

The call to the function can be done statically

 */
pub mod error;

use crate::metrics_extraction::transactions::ExtractedMetrics;
use crate::services::processor::{InnerProcessor, Processed, ProcessingError};
use crate::utils::TypedEnvelope;
use std::sync::Arc;

pub struct ProcGroupParams<Group> {
    pub managed_envelope: TypedEnvelope<Group>,
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
    ($exp:expr) => {
        match $exp {
            Ok(val) => val,
            Err(err) => {
                return ProcGroupError {
                    managed_envelope: self.managed_envelope.into_processed(),
                    error: err,
                };
            }
        }
    };
}

pub trait ProcGroup<Group> {
    fn create(inner: Arc<InnerProcessor>, params: ProcGroupParams<Group>) -> Self;

    fn process(self) -> Result<ProcGroupResult, ProcGroupError>;
}
