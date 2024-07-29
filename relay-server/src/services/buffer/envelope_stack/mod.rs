use std::future::Future;

use crate::envelope::Envelope;

pub mod memory;
pub mod sqlite;

/// A stack-like data structure that holds [`Envelope`]s.
pub trait EnvelopeStack: Send {
    /// The error type that is returned when an error is encountered during reading or writing the
    /// [`EnvelopeStack`].
    type Error: std::fmt::Debug;

    type Provider: StackProvider;

    /// Creates a new stack with the given element.
    fn new(envelope: Box<Envelope>) -> Self;

    /// Pushes an [`Envelope`] on top of the stack.
    fn push(&mut self, envelope: Box<Envelope>) -> impl Future<Output = Result<(), Self::Error>>;

    /// Peeks the [`Envelope`] on top of the stack.
    ///
    /// If the stack is empty, an error is returned.
    fn peek(&mut self) -> impl Future<Output = Result<Option<&Envelope>, Self::Error>>;

    /// Pops the [`Envelope`] on top of the stack.
    ///
    /// If the stack is empty, an error is returned.
    fn pop(&mut self) -> impl Future<Output = Result<Option<Box<Envelope>>, Self::Error>>;
}

pub trait StackProvider {
    type Stack: EnvelopeStack;

    fn create_stack(&self, envelope: Box<Envelope>) -> Self::Stack;
}
