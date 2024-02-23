//! This module contains types and utils used in processor to describe and support the processing pipeline.
//!
//! The idea is that every suitable processing step function returns the [`ProcessEnvelopeState`]
//! into one of the defined here struct, which is accepted only the following function, this
//! way enforcing the order the function can be executed.

use std::ops::{Deref, DerefMut};

use crate::services::processor::ProcessEnvelopeState;

/// Simple macro to create a struct and define few default methods.
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

// Returns from the `processor::light_normalize_event` function.
state_type!(NormalizedEventState);

// Only this type can be used by `processor::enforce_quotas` function.
state_type!(EnforceQuotasState);

impl<'a, G> From<FilterProfileState<'a, G>> for EnforceQuotasState<'a, G> {
    fn from(value: FilterProfileState<'a, G>) -> Self {
        EnforceQuotasState::new(value.inner())
    }
}

impl<'a, G> From<FilterSpanState<'a, G>> for EnforceQuotasState<'a, G> {
    fn from(value: FilterSpanState<'a, G>) -> Self {
        EnforceQuotasState::new(value.inner())
    }
}

// This type returns from the `processor::enforce_quotas` function.
state_type!(EnforcedQuotasState);

impl<'a, G> From<EnforceQuotasState<'a, G>> for EnforcedQuotasState<'a, G> {
    fn from(value: EnforceQuotasState<'a, G>) -> Self {
        EnforcedQuotasState::new(value.inner())
    }
}

// This type is accepted only by  `attachment::scrub` function.
state_type!(ScrubAttachementState);

impl<'a, G> Deref for ScrubAttachementState<'a, G> {
    type Target = ProcessEnvelopeState<'a, G>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, G> DerefMut for ScrubAttachementState<'a, G> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// This type is set only after all the steps are done and can be returned from the main entry
// function.
state_type!(ProcessedState);

impl<'a, G> From<EnforceQuotasState<'a, G>> for ProcessedState<'a, G> {
    fn from(value: EnforceQuotasState<'a, G>) -> Self {
        ProcessedState::new(value.inner())
    }
}

impl<'a, G> From<FilterSpanState<'a, G>> for ProcessedState<'a, G> {
    fn from(value: FilterSpanState<'a, G>) -> Self {
        ProcessedState::new(value.inner())
    }
}

// Returns from `event::filter` function.
state_type!(FilterEventState);

// Returns from the `span::filter` function.
state_type!(FilterSpanState);

// Accepted into the `event::extract` function.
state_type!(ExtractEventState);

impl<'a, G> From<FilterProfileState<'a, G>> for ExtractEventState<'a, G> {
    fn from(value: FilterProfileState<'a, G>) -> Self {
        ExtractEventState::new(value.inner())
    }
}

// Returns from the `event::extract` function.
state_type!(ExtractedEventState);

impl<'a, G> Deref for ExtractedEventState<'a, G> {
    type Target = ProcessEnvelopeState<'a, G>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, G> DerefMut for ExtractedEventState<'a, G> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// Returns from the `event::finalize` function.
state_type!(FinalizedEventState);

// Returns from the `profile::filter` function.
state_type!(FilterProfileState);

impl<'a, G> From<EnforcedQuotasState<'a, G>> for FilterProfileState<'a, G> {
    fn from(value: EnforcedQuotasState<'a, G>) -> Self {
        FilterProfileState::new(value.inner())
    }
}

// Returns from the `profile::process` function.
#[cfg(feature = "processing")]
state_type!(ProfileProcessedState);

/// Signalizes that the state was through the dynamic sampling.
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

    pub fn has_event(&self) -> bool {
        self.0.has_event()
    }
}

impl<'a, G> From<EnforcedQuotasState<'a, G>> for SampledState<'a, G> {
    fn from(value: EnforcedQuotasState<'a, G>) -> Self {
        SampledState::new(value.inner())
    }
}

#[cfg(feature = "processing")]
impl<'a, G> From<ProfileProcessedState<'a, G>> for SampledState<'a, G> {
    fn from(value: ProfileProcessedState<'a, G>) -> Self {
        SampledState::new(value.inner())
    }
}
