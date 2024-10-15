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

    async fn initialize(&self) -> InitializationState {
        let project_key_pairs_with_counts = self
            .envelope_store
            .lock()
            .await
            .project_key_pairs_with_counts()
            .await;

        match project_key_pairs_with_counts {
            Ok(project_key_pairs_with_counts) => {
                let project_key_pairs = project_key_pairs_with_counts.keys().copied().collect();
                let store_total_count = project_key_pairs_with_counts.values().copied().sum();

                InitializationState::new(project_key_pairs, store_total_count)
            }
            Err(error) => {
                relay_log::error!(
                    error = &error as &dyn Error,
                    "failed to load project key pairs and envelope counts from the file system",
                );
                InitializationState::empty()
            }
        }
    }

    fn create_stack(
        &self,
        _stack_creation_type: StackCreationType,
        project_key_pair: ProjectKeyPair,
    ) -> Self::Stack {
        FileBackedEnvelopeStack::new(project_key_pair, self.envelope_store.clone())
    }

    fn has_store_capacity(&self) -> bool {
        // Implement logic to check disk capacity if needed
        true
    }

    async fn store_total_count(&self) -> u32 {
        // Optionally implement this to count total envelopes
        0
    }

    fn stack_type<'a>(&self) -> &'a str {
        "file_backed"
    }

    async fn flush(&mut self, _envelope_stacks: impl IntoIterator<Item = Self::Stack>) -> bool {
        // Since data is already on disk, no action needed
        true
    }
}
