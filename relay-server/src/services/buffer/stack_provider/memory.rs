use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_stack::memory::MemoryEnvelopeStack;
use crate::services::buffer::envelope_stack::CollectionStrategy;
use crate::services::buffer::stack_provider::{
    InitializationState, SpoolingStrategy, StackCreationType, StackProvider,
};
use crate::utils::MemoryChecker;
use crate::EnvelopeStack;

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

    fn stack_type(&self) -> &str {
        "memory"
    }

    async fn spool<'a>(
        &mut self,
        envelope_stacks: impl IntoIterator<Item = &'a mut Self::Stack>,
        spooling_strategy: SpoolingStrategy,
    ) {
        let collection_strategy: CollectionStrategy = spooling_strategy.into();
        for envelope_stack in envelope_stacks {
            // We just drop the data immediately because we can't spool anywhere with this strategy.
            let _ = envelope_stack.collect(collection_strategy);
        }
    }
}
