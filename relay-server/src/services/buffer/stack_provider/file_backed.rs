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
/// It uses an LRU cache to keep at most `n` open files.
#[derive(Debug)]
pub struct FileBackedStackProvider {
    envelope_store: Arc<Mutex<FileBackedEnvelopeStore>>,
}

impl FileBackedStackProvider {
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

    async fn store_total_count(&self) -> u64 {
        // Optionally implement this to count total envelopes
        0
    }

    fn stack_type<'a>(&self) -> &'a str {
        "file_backed"
    }

    async fn flush(&mut self, _envelope_stacks: impl IntoIterator<Item = Self::Stack>) {
        // Since data is already on disk, no action needed
    }
}
