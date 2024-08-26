use crate::{Envelope, EnvelopeStack};

pub mod memory;
pub mod sqlite;

/// Enum representing the current capacity of the [`StacksManager`] to accept new [`Envelope`]s.
pub enum Capacity {
    Available,
    Full,
}

/// A provider of [`EnvelopeStack`] instances that is responsible for creating them.
pub trait StacksManager: std::fmt::Debug {
    /// The implementation of [`EnvelopeStack`] that this manager creates.
    type Stack: EnvelopeStack;

    /// Creates an [`EnvelopeStack`].
    fn create_stack(&self, envelope: Box<Envelope>) -> Self::Stack;

    /// Returns the [`Capacity`] that the manager has to create new stacks or push elements to
    /// existing stacks.
    fn capacity(&self) -> Capacity;
}
