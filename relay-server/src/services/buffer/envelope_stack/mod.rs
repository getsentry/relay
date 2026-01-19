use std::future::Future;

use chrono::{DateTime, Utc};

use crate::envelope::Envelope;
use crate::managed::Managed;

pub mod caching;
pub mod memory;
pub mod sqlite;

/// A stack-like data structure that holds [`Envelope`]s.
pub trait EnvelopeStack: Send + std::fmt::Debug {
    /// The error type that is returned when an error is encountered during reading or writing the
    /// [`EnvelopeStack`].
    type Error: std::fmt::Debug + std::error::Error;

    /// Pushes a [`Managed`] envelope on top of the stack.
    ///
    /// The envelope is accepted (extracted from the Managed wrapper) only when it has been
    /// successfully stored. If any error occurs, the Managed wrapper is dropped, which
    /// automatically rejects the envelope with outcomes.
    fn push(
        &mut self,
        envelope: Managed<Box<Envelope>>,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    /// Peeks the [`Envelope`] on top of the stack.
    fn peek(&mut self) -> impl Future<Output = Result<Option<DateTime<Utc>>, Self::Error>>;

    /// Pops the [`Envelope`] on top of the stack.
    fn pop(&mut self) -> impl Future<Output = Result<Option<Box<Envelope>>, Self::Error>>;

    /// Persists all envelopes in the [`EnvelopeStack`]s to external storage, if possible,
    /// and consumes the stack provider.
    fn flush(self) -> impl Future<Output = ()>;
}
