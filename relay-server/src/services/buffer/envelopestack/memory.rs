use std::convert::Infallible;

use crate::services::buffer::envelopestack::StackProvider;
use crate::Envelope;

use super::EnvelopeStack;

#[derive(Debug)]
pub struct InMemoryEnvelopeStack(#[allow(clippy::vec_box)] Vec<Box<Envelope>>);

impl EnvelopeStack for InMemoryEnvelopeStack {
    type Error = Infallible;
    type Provider = DummyProvider;

    fn new(envelope: Box<Envelope>) -> Self {
        Self(vec![envelope])
    }

    async fn push(&mut self, envelope: Box<Envelope>) -> Result<(), Self::Error> {
        self.0.push(envelope);
        Ok(())
    }

    async fn pop(&mut self) -> Result<Option<Box<Envelope>>, Self::Error> {
        Ok(self.0.pop())
    }

    async fn peek(&mut self) -> Result<Option<&Envelope>, Self::Error> {
        Ok(self.0.last().map(Box::as_ref))
    }
}

pub struct DummyProvider; // TODO: needs pub?

impl StackProvider for DummyProvider {
    type Stack = InMemoryEnvelopeStack;

    // TODO: create empty stack instead
    fn create_stack(&self, envelope: Box<Envelope>) -> Self::Stack {
        InMemoryEnvelopeStack::new(envelope)
    }
}
