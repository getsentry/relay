use std::future::Future;

use crate::envelope::Envelope;

pub mod file;
pub mod memory;
pub mod sqlite;

/// A stack-like data structure that holds [`Envelope`]s.
pub trait EnvelopeStack {
    /// The error type that is returned when an error is encountered during reading or writing the
    /// [`EnvelopeStack`].
    type Error;

    /// Pushes an [`Envelope`] on top of the stack.
    fn push(&mut self, envelope: Box<Envelope>) -> impl Future<Output = Result<(), Self::Error>>;

    /// Pops the [`Envelope`] on top of the stack.
    fn pop(&mut self) -> impl Future<Output = Result<Option<Box<Envelope>>, Self::Error>>;

    /// Persists all envelopes in the [`EnvelopeStack`]s to external storage, if possible,
    /// and consumes the stack provider.
    fn flush(self) -> Vec<Box<Envelope>>;
}
