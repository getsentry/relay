/*

High-level design:

* Initialization struct
* State struct for each group
* Entry function for the group
* Each group has access to the processor (`InnerProcessor`)

The call to the function can be done statically

 */
mod errors;

use crate::metrics_extraction::transactions::ExtractedMetrics;
use crate::services::processor::{ErrorGroup, InnerProcessor, Processed, ProcessingError};
use crate::utils::TypedEnvelope;
use std::sync::Arc;

pub struct ProcGroupParams<Group> {
    managed_envelope: TypedEnvelope<Group>,
}

pub struct ProcGroupResult {
    managed_envelope: TypedEnvelope<Processed>,
    extracted_metrics: ExtractedMetrics,
}

pub trait ProcGroup<Group> {
    fn create(inner: Arc<InnerProcessor>, params: ProcGroupParams<Group>) -> Self;

    fn process(self) -> Result<ProcGroupResult, ProcessingError>;
}
