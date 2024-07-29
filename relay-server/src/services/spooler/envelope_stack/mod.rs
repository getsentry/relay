use crate::envelope::Envelope;
use std::future::Future;

pub mod sqlite;

/// A stack-like data structure that holds [`Envelope`]s.
pub trait EnvelopeStack {
    /// The error type that is returned when an error is encountered during reading or writing the
    /// [`EnvelopeStack`].
    type Error;

    /// Pushes an [`Envelope`] on top of the stack.
    #[allow(dead_code)]
    fn push(&mut self, envelope: Box<Envelope>);

    /// Peeks the [`Envelope`] on top of the stack.
    ///
    /// If the stack is empty, an error is returned.
    #[allow(dead_code)]
    fn peek(&mut self) -> impl Future<Output = Result<Option<&Box<Envelope>>, Self::Error>>;

    /// Pops the [`Envelope`] on top of the stack.
    ///
    /// If the stack is empty, an error is returned.
    #[allow(dead_code)]
    fn pop(&mut self) -> impl Future<Output = Result<Option<Box<Envelope>>, Self::Error>>;
}
