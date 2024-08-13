use crate::{Envelope, EnvelopeStack};

pub mod memory;
pub mod sqlite;

/// Enum representing the current capacity of the [`StacksManager`] to accept new [`Envelope`]s.
pub enum Capacity {
    Free,
    Full,
}

/// A provider of [`EnvelopeStack`] instances that is responsible for creating them.
pub trait StacksManager: std::fmt::Debug {
    type Stack: EnvelopeStack;

    fn create_stack(&self, envelope: Box<Envelope>) -> Self::Stack;

    fn capacity(&self) -> Capacity;
}
