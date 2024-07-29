use std::convert::Infallible;

use super::EnvelopeStack;
use crate::services::buffer::stack_provider::memory::MemoryStackProvider;
use crate::Envelope;

#[derive(Debug)]
pub struct MemoryEnvelopeStack(#[allow(clippy::vec_box)] Vec<Box<Envelope>>);

impl MemoryEnvelopeStack {
    fn new(envelope: Box<Envelope>) -> Self {
        Self(vec![envelope])
    }
}

impl EnvelopeStack for MemoryEnvelopeStack {
    type Error = Infallible;

    type Provider = MemoryStackProvider;

    async fn push(&mut self, envelope: Box<Envelope>) -> Result<(), Self::Error> {
        self.0.push(envelope);
        Ok(())
    }

    async fn peek(&mut self) -> Result<Option<&Envelope>, Self::Error> {
        Ok(self.0.last().map(Box::as_ref))
    }

    async fn pop(&mut self) -> Result<Option<Box<Envelope>>, Self::Error> {
        Ok(self.0.pop())
    }
}
