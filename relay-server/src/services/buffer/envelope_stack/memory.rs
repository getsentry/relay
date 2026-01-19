use std::convert::Infallible;

use chrono::{DateTime, Utc};

use crate::Envelope;
use crate::managed::Managed;

use super::EnvelopeStack;

#[derive(Debug)]
pub struct MemoryEnvelopeStack(Vec<Managed<Box<Envelope>>>);

impl MemoryEnvelopeStack {
    pub fn new() -> Self {
        Self(vec![])
    }
}

impl EnvelopeStack for MemoryEnvelopeStack {
    type Error = Infallible;

    async fn push(&mut self, envelope: Managed<Box<Envelope>>) -> Result<(), Self::Error> {
        // Store the managed envelope without accepting - it will be accepted on pop
        self.0.push(envelope);
        Ok(())
    }

    async fn peek(&mut self) -> Result<Option<DateTime<Utc>>, Self::Error> {
        Ok(self.0.last().map(|e| e.received_at()))
    }

    async fn pop(&mut self) -> Result<Option<Box<Envelope>>, Self::Error> {
        // Accept the envelope only when popping it
        Ok(self.0.pop().map(|envelope| envelope.accept(|e| e)))
    }

    async fn flush(self) {}
}
