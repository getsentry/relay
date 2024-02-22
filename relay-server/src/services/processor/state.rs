//! This module contains types and utils used in processor to decribe and support the processing pipeline.
//!

use crate::services::processor::ProcessEnvelopeState;

pub struct EnforcedQuotasState<'a, G> {
    pub state: ProcessEnvelopeState<'a, G>,
}

impl<'a, G> EnforcedQuotasState<'a, G> {
    pub fn new(state: ProcessEnvelopeState<'a, G>) -> EnforcedQuotasState<'a, G> {
        Self { state }
    }

    pub fn inner(self) -> ProcessEnvelopeState<'a, G> {
        self.state
    }
}
