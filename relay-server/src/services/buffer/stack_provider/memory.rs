use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_stack::memory::MemoryEnvelopeStack;
use crate::services::buffer::stack_provider::{
    InitializationState, StackCreationType, StackProvider,
};
use crate::utils::MemoryChecker;

#[derive(Debug)]
pub struct MemoryStackProvider {
    memory_checker: MemoryChecker,
}

impl MemoryStackProvider {
    /// Creates a new [`MemoryStackProvider`] with a given [`MemoryChecker`] that is used to
    /// estimate the capacity.
    pub fn new(memory_checker: MemoryChecker) -> Self {
        Self { memory_checker }
    }
}

impl StackProvider for MemoryStackProvider {
    type Stack = MemoryEnvelopeStack;

    async fn initialize(&self) -> InitializationState {
        InitializationState::empty()
    }

    fn create_stack(&self, _: StackCreationType, _: ProjectKeyPair) -> Self::Stack {
        MemoryEnvelopeStack::new()
    }

    fn has_store_capacity(&self) -> bool {
        self.memory_checker.check_memory().has_capacity()
    }

    async fn store_total_count(&self) -> u64 {
        // The memory implementation doesn't have a store, so the count is 0.
        0
    }

    fn stack_type<'a>(&self) -> &'a str {
        "memory"
    }

    async fn drain(self, _: impl IntoIterator<Item = Self::Stack>) {}
}
