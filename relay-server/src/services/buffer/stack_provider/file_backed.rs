use relay_config::Config;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_stack::file_backed::FileBackedEnvelopeStack;
use crate::services::buffer::envelope_store::file_backed::{
    FileBackedEnvelopeStore, FileBackedEnvelopeStoreError,
};
use crate::services::buffer::stack_provider::{
    InitializationState, StackCreationType, StackProvider,
};

/// A stack provider that manages `FileBackedEnvelopeStack` instances.
///
/// This provider uses a file-backed envelope store to persist envelopes on disk.
/// It implements the `StackProvider` trait, providing methods to initialize,
/// create stacks, and manage the overall state of the envelope storage.
#[derive(Debug)]
pub struct FileBackedStackProvider {
    envelope_store: Arc<Mutex<FileBackedEnvelopeStore>>,
}

impl FileBackedStackProvider {
    /// Creates a new `FileBackedStackProvider` instance.
    ///
    /// This method initializes the underlying `FileBackedEnvelopeStore` using the provided
    /// configuration.
    pub async fn new(config: &Config) -> Result<Self, FileBackedEnvelopeStoreError> {
        let envelope_store = FileBackedEnvelopeStore::new(config).await?;

        Ok(Self {
            envelope_store: Arc::new(Mutex::new(envelope_store)),
        })
    }
}

impl StackProvider for FileBackedStackProvider {
    type Stack = FileBackedEnvelopeStack;

    /// Initializes the stack provider by loading existing project key pairs from the file system.
    ///
    /// If the initialization fails, it logs an error and returns an empty initialization state.
    async fn initialize(&self) -> InitializationState {
        let project_key_pairs_result = self
            .envelope_store
            .lock()
            .await
            .list_project_key_pairs()
            .await;

        match project_key_pairs_result {
            Ok(project_key_pairs) => InitializationState { project_key_pairs },
            Err(error) => {
                relay_log::error!(
                    error = &error as &dyn Error,
                    "failed to load project key pairs from the file system",
                );
                InitializationState::empty()
            }
        }
    }

    /// Creates a new `FileBackedEnvelopeStack` for the given project key pair.
    ///
    /// This method ignores the `stack_creation_type` parameter as the file-backed
    /// implementation doesn't distinguish between creation types.
    fn create_stack(
        &self,
        _stack_creation_type: StackCreationType,
        project_key_pair: ProjectKeyPair,
    ) -> Self::Stack {
        FileBackedEnvelopeStack::new(project_key_pair, self.envelope_store.clone())
    }

    /// Checks if there's available capacity in the store.
    ///
    /// This implementation always returns true, assuming unlimited disk capacity.
    /// It can be extended to check actual disk capacity if needed.
    fn has_store_capacity(&self) -> bool {
        // Implement logic to check disk capacity if needed
        true
    }

    /// Returns the total count of envelopes in the store.
    ///
    /// This implementation always returns 0. It can be extended to provide an
    /// actual count if needed.
    async fn store_total_count(&self) -> u64 {
        // Optionally implement this to count total envelopes
        0
    }

    /// Returns the type of the stack as a string.
    fn stack_type<'a>(&self) -> &'a str {
        "file_backed"
    }

    /// Flushes the given envelope stacks.
    ///
    /// This method is a no-op for file-backed stacks, as the data is already
    /// persisted on disk.
    async fn flush(&mut self, _envelope_stacks: impl IntoIterator<Item = Self::Stack>) {
        // Since data is already on disk, no action needed
    }
}
