use std::future::Future;

use crate::envelope::Envelope;
use crate::services::buffer::stack_provider::SpoolingStrategy;

pub mod memory;
pub mod sqlite;

/// The strategy for collecting [`Envelope`]s from the [`EnvelopeStack`].
#[derive(Copy, Clone)]
pub enum CollectionStrategy {
    All,
    N(u64),
    None,
}

impl From<SpoolingStrategy> for CollectionStrategy {
    fn from(value: SpoolingStrategy) -> Self {
        match value {
            SpoolingStrategy::All => CollectionStrategy::All,
            SpoolingStrategy::N { n, envelope_stacks } => {
                // In case no stacks are there, we don't want to spool.
                if envelope_stacks == 0 {
                    CollectionStrategy::None
                } else {
                    CollectionStrategy::N(n.saturating_div(envelope_stacks))
                }
            }
        }
    }
}

/// A stack-like data structure that holds [`Envelope`]s.
pub trait EnvelopeStack: Send + std::fmt::Debug {
    /// The error type that is returned when an error is encountered during reading or writing the
    /// [`EnvelopeStack`].
    type Error: std::fmt::Debug;

    /// Pushes an [`Envelope`] on top of the stack.
    fn push(&mut self, envelope: Box<Envelope>) -> impl Future<Output = Result<(), Self::Error>>;

    /// Peeks the [`Envelope`] on top of the stack.
    fn peek(&mut self) -> impl Future<Output = Result<Option<&Envelope>, Self::Error>>;

    /// Pops the [`Envelope`] on top of the stack.
    fn pop(&mut self) -> impl Future<Output = Result<Option<Box<Envelope>>, Self::Error>>;

    /// Collects envelopes from the [`EnvelopeStack`]s given a [`CollectionStrategy`].
    fn collect(&mut self, collection_strategy: CollectionStrategy) -> Vec<Box<Envelope>>;
}
