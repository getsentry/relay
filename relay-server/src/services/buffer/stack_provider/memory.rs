use crate::services::buffer::envelope_stack::memory::MemoryEnvelopeStack;
use crate::services::buffer::stack_provider::StackProvider;
use crate::utils::MemoryChecker;
use crate::Envelope;

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

    fn create_stack(&self, envelope: Box<Envelope>) -> Self::Stack {
        MemoryEnvelopeStack::new(envelope)
    }

    fn has_store_capacity(&self) -> bool {
        self.memory_checker.check_memory().has_capacity()
    }
}
