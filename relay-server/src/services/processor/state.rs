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

#[cfg(feature = "processing")]
state_type!(EnforcedQuotasState);
state_type!(ProcessedState);

pub enum EnforcedOrRaw<'a, G> {
    #[cfg(feature = "processing")]
    EnforcedQuotasState(EnforcedQuotasState<'a, G>),
    State(ProcessEnvelopeState<'a, G>),
}

impl<'a, G> EnforcedOrRaw<'a, G> {
    pub fn inner(self) -> ProcessEnvelopeState<'a, G> {
        match self {
            #[cfg(feature = "processing")]
            EnforcedOrRaw::EnforcedQuotasState(state) => state.inner(),
            EnforcedOrRaw::State(state) => state,
        }
    }
}
