use std::convert::Infallible;

use chrono::{DateTime, Utc};

use crate::Envelope;
use crate::managed::Managed;

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

    async fn push(&mut self, envelope: Managed<Box<Envelope>>) -> Result<(), Self::Error> {
        // Accept the envelope immediately since in-memory storage cannot fail
        let envelope = envelope.accept(|e| e);
        self.0.push(envelope);
        Ok(())
    }

    async fn peek(&mut self) -> Result<Option<DateTime<Utc>>, Self::Error> {
        Ok(self.0.last().map(|e| e.received_at()))
    }

    async fn pop(&mut self) -> Result<Option<Box<Envelope>>, Self::Error> {
        Ok(self.0.pop())
    }

    async fn flush(self) {}
}
