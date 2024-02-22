//! This module contains types and utils used in processor to decribe and support the processing pipeline.
//!

use crate::services::processor::ProcessEnvelopeState;

macro_rules! state_type {
    ($name:ident) => {
        pub struct $name<'a, G>(ProcessEnvelopeState<'a, G>);

        impl<'a, G> $name<'a, G> {
            pub fn new(state: ProcessEnvelopeState<'a, G>) -> $name<'a, G> {
                Self(state)
            }

            pub fn inner(self) -> ProcessEnvelopeState<'a, G> {
                self.0
            }
        }
    };
}

state_type!(EnforceQuotasState);
state_type!(EnforcedQuotastate);
state_type!(ScrubAttachementState);
state_type!(ProcessedState);
state_type!(FilterState);
state_type!(ExtractEventState);
state_type!(ExtractedEventState);
state_type!(FinalizedEventState);
state_type!(NormalizedEventState);

pub struct SampledState<'a, G>(ProcessEnvelopeState<'a, G>);

impl<'a, G> SampledState<'a, G> {
    pub fn new(state: ProcessEnvelopeState<'a, G>) -> SampledState<'a, G> {
        Self(state)
    }

    pub fn inner(self) -> ProcessEnvelopeState<'a, G> {
        self.0
    }

    pub fn sampling_should_drop(&self) -> bool {
        self.0.sampling_result.should_drop()
    }
}

impl<'a, G> From<EnforceQuotasState<'a, G>> for EnforcedQuotastate<'a, G> {
    fn from(value: EnforceQuotasState<'a, G>) -> Self {
        EnforcedQuotastate::new(value.inner())
    }
}

impl<'a, G> From<EnforceQuotasState<'a, G>> for ProcessedState<'a, G> {
    fn from(value: EnforceQuotasState<'a, G>) -> Self {
        ProcessedState::new(value.inner())
    }
}

impl<'a, G> From<EnforceQuotasState<'a, G>> for FilterState<'a, G> {
    fn from(value: EnforceQuotasState<'a, G>) -> Self {
        FilterState::new(value.inner())
    }
}

impl<'a, G> From<EnforceQuotasState<'a, G>> for SampledState<'a, G> {
    fn from(value: EnforceQuotasState<'a, G>) -> Self {
        SampledState::new(value.inner())
    }
}

impl<'a, G> From<FilterState<'a, G>> for EnforceQuotasState<'a, G> {
    fn from(value: FilterState<'a, G>) -> Self {
        EnforceQuotasState::new(value.inner())
    }
}

impl<'a, G> From<FilterState<'a, G>> for ProcessedState<'a, G> {
    fn from(value: FilterState<'a, G>) -> Self {
        ProcessedState::new(value.inner())
    }
}

impl<'a, G> From<FilterState<'a, G>> for ExtractEventState<'a, G> {
    fn from(value: FilterState<'a, G>) -> Self {
        ExtractEventState::new(value.inner())
    }
}

impl<'a, G> From<ScrubAttachementState<'a, G>> for ExtractEventState<'a, G> {
    fn from(value: ScrubAttachementState<'a, G>) -> Self {
        ExtractEventState::new(value.inner())
    }
}
