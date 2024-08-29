use crate::services::buffer::envelope_store::EnvelopeProjectKeys;
use crate::EnvelopeStack;
use hashbrown::HashSet;
use std::future::Future;

pub mod memory;
pub mod sqlite;

pub struct InitializationState {
    pub envelopes_projects_keys: HashSet<EnvelopeProjectKeys>,
}

impl InitializationState {
    pub fn new(envelopes_projects_keys: HashSet<EnvelopeProjectKeys>) -> Self {
        Self {
            envelopes_projects_keys,
        }
    }

    pub fn empty() -> Self {
        Self {
            envelopes_projects_keys: HashSet::new(),
        }
    }
}

/// A provider of [`EnvelopeStack`] instances that is responsible for creating them.
pub trait StackProvider: std::fmt::Debug {
    /// The implementation of [`EnvelopeStack`] that this manager creates.
    type Stack: EnvelopeStack;

    /// Initializes the [`StackProvider`].
    fn initialize(&self) -> impl Future<Output = InitializationState>;

    /// Creates an [`EnvelopeStack`].
    fn create_stack(&self, envelope_project_keys: EnvelopeProjectKeys) -> Self::Stack;

    /// Returns `true` if the store used by this [`StackProvider`] has space to add new
    /// stacks or items to the stacks.
    fn has_store_capacity(&self) -> bool;
}
