use crate::services::buffer::common::ProjectKeyPair;
use crate::EnvelopeStack;
use hashbrown::HashSet;
use std::error::Error;
use std::future::Future;
use std::time::Duration;
use tokio::time::timeout;

pub mod file_backed;
pub mod memory;
pub mod sqlite;

/// State of the initialization of the [`StackProvider`].
///
/// This state is necessary for initializing resources whenever a [`StackProvider`] is used.
#[derive(Debug)]
pub struct InitializationState {
    pub project_key_pairs: HashSet<ProjectKeyPair>,
    pub store_total_count: u32,
}

impl InitializationState {
    /// Create a new [`InitializationState`].
    pub fn new(project_key_pairs: HashSet<ProjectKeyPair>, store_total_count: u32) -> Self {
        Self {
            project_key_pairs,
            store_total_count,
        }
    }

    /// Creates a new empty [`InitializationState`].
    pub fn empty() -> Self {
        Self {
            project_key_pairs: HashSet::new(),
            store_total_count: 0,
        }
    }
}

/// The creation type for the [`EnvelopeStack`].
pub enum StackCreationType {
    /// An [`EnvelopeStack`] that is created during initialization.
    Initialization,
    /// An [`EnvelopeStack`] that is created when an envelope is received.
    New,
}

/// A provider of [`EnvelopeStack`] instances that is responsible for creating them.
pub trait StackProvider: std::fmt::Debug {
    /// The implementation of [`EnvelopeStack`] that this manager creates.
    type Stack: EnvelopeStack;

    /// Initializes the [`StackProvider`].
    fn initialize(&self) -> impl Future<Output = InitializationState>;

    /// Creates an [`EnvelopeStack`].
    fn create_stack(
        &self,
        stack_creation_type: StackCreationType,
        project_key_pair: ProjectKeyPair,
    ) -> Self::Stack;

    /// Returns `true` if the store used by this [`StackProvider`] has space to add new
    /// stacks or items to the stacks.
    fn has_store_capacity(&self) -> bool;

    /// Returns the string representation of the stack type offered by this [`StackProvider`].
    fn stack_type<'a>(&self) -> &'a str;

    /// Flushes the supplied [`EnvelopeStack`]s and consumes the [`StackProvider`].
    ///
    /// Returns `true` if the envelopes have been flushed to storage and the buffer is safe to be
    /// dropped, `false` otherwise.
    fn flush(
        &mut self,
        envelope_stacks: impl IntoIterator<Item = Self::Stack>,
    ) -> impl Future<Output = bool>;
}
