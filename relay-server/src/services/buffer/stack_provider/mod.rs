use crate::{Envelope, EnvelopeStack};

pub mod memory;
pub mod sqlite;

/// A provider of [`EnvelopeStack`] instances that is responsible for creating them.
pub trait StackProvider: std::fmt::Debug {
    /// The implementation of [`EnvelopeStack`] that this manager creates.
    type Stack: EnvelopeStack;

    /// Creates an [`EnvelopeStack`].
    fn create_stack(&self, envelope: Box<Envelope>) -> Self::Stack;

    /// Returns `true` if the store used by this [`StackProvider`] has space to add new
    /// stacks or items to the stacks.
    fn has_store_capacity(&self) -> bool;
}
