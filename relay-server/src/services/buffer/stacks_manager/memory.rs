use crate::services::buffer::envelope_stack::memory::MemoryEnvelopeStack;
use crate::services::buffer::stacks_manager::{Capacity, StacksManager};
use crate::utils::MemoryChecker;
use crate::Envelope;

#[derive(Debug)]
pub struct MemoryStacksManager {
    memory_checker: MemoryChecker,
}

impl MemoryStacksManager {
    pub fn new(memory_checker: MemoryChecker) -> Self {
        Self { memory_checker }
    }
}

impl StacksManager for MemoryStacksManager {
    type Stack = MemoryEnvelopeStack;

    fn create_stack(&self, envelope: Box<Envelope>) -> Self::Stack {
        MemoryEnvelopeStack::new(envelope)
    }

    fn capacity(&self) -> Capacity {
        if self.memory_checker.check_memory().has_capacity() {
            Capacity::Free
        } else {
            Capacity::Full
        }
    }
}
