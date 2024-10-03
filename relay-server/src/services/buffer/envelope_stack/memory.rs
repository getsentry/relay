use std::convert::Infallible;

use crate::Envelope;

use super::EnvelopeStack;

#[derive(Debug)]
pub struct MemoryEnvelopeStack(#[allow(clippy::vec_box)] Vec<Box<Envelope>>);

impl MemoryEnvelopeStack {
    pub fn new() -> Self {
        Self(vec![])
    }
}

impl EnvelopeStack for MemoryEnvelopeStack {
    type Error = Infallible;

    async fn push(&mut self, envelope: Box<Envelope>) -> Result<(), Self::Error> {
        self.0.push(envelope);
        Ok(())
    }

    async fn peek(&mut self) -> Result<Option<&Envelope>, Self::Error> {
        Ok(self.0.last().map(Box::as_ref))
    }

    async fn pop_many(
        &mut self,
        count: std::num::NonZeroUsize,
    ) -> Result<Vec<Box<Envelope>>, Self::Error> {
        Ok(self.0.split_off(self.0.len().saturating_sub(count.get())))
    }

    fn flush(self) -> Vec<Box<Envelope>> {
        self.0
    }
}
