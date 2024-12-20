use std::future::Future;

use chrono::{DateTime, Utc};

use crate::envelope::Envelope;

pub mod caching;
pub mod memory;
pub mod sqlite;

/// A stack-like data structure that holds [`Envelope`]s.
pub trait EnvelopeStack: Send + std::fmt::Debug {
    /// The error type that is returned when an error is encountered during reading or writing the
    /// [`EnvelopeStack`].
    type Error: std::fmt::Debug + std::error::Error;

    /// Pushes an [`Envelope`] on top of the stack.
    fn push(&mut self, envelope: Box<Envelope>) -> impl Future<Output = Result<(), Self::Error>>;

    /// Peeks the [`Envelope`] on top of the stack.
    fn peek(&mut self) -> impl Future<Output = Result<Option<DateTime<Utc>>, Self::Error>>;

    /// Pops the [`Envelope`] on top of the stack.
    fn pop(&mut self) -> impl Future<Output = Result<Option<Box<Envelope>>, Self::Error>>;

    /// Persists all envelopes in the [`EnvelopeStack`]s to external storage, if possible,
    /// and consumes the stack provider.
    fn flush(self) -> impl Future<Output = ()>;
}
