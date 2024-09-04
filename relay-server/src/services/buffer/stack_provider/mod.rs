use crate::services::buffer::common::ProjectKeyPair;
use crate::EnvelopeStack;
use hashbrown::HashSet;
use std::future::Future;

pub mod memory;
pub mod sqlite;

/// State of the initialization of the [`StackProvider`].
///
/// This state is necessary for initializing resources whenever a [`StackProvider`] is used.
#[derive(Debug)]
pub struct InitializationState {
    pub project_key_pairs: HashSet<ProjectKeyPair>,
}

impl InitializationState {
    /// Create a new [`InitializationState`].
    pub fn new(project_key_pairs: HashSet<ProjectKeyPair>) -> Self {
        Self { project_key_pairs }
    }

    /// Creates a new empty [`InitializationState`].
    pub fn empty() -> Self {
        Self {
            project_key_pairs: HashSet::new(),
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

    /// Returns the total count of the store used by this [`StackProvider`].
    fn store_total_count(&self) -> impl Future<Output = u64>;

    /// Returns the string representation of the stack type offered by this [`StackProvider`].
    fn stack_type<'a>(&self) -> &'a str;

    /// Drains the supplied [`EnvelopeStack`]s and consumes the [`StackProvider`].
    fn drain(
        self,
        envelope_stacks: impl IntoIterator<Item = Self::Stack>,
    ) -> impl Future<Output = ()>;
}
