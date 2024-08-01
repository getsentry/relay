use crate::services::buffer::envelope_stack::memory::MemoryEnvelopeStack;
use crate::services::buffer::envelope_stack::StackProvider;
use crate::Envelope;

#[derive(Debug)]
pub struct MemoryStackProvider;

impl StackProvider for MemoryStackProvider {
    type Stack = MemoryEnvelopeStack;

    fn create_stack(&self, envelope: Box<Envelope>) -> Self::Stack {
        MemoryEnvelopeStack::new(envelope)
    }
}
