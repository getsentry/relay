use hashbrown::HashSet;
use relay_config::Config;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_stack::file_backed::FileBackedEnvelopeStack;
use crate::services::buffer::files_manager::{FilesManager, FilesManagerError};
use crate::services::buffer::stack_provider::{
    InitializationState, StackCreationType, StackProvider,
};

/// A stack provider that manages `FileBackedEnvelopeStack` instances.
///
/// It uses an LRU cache to keep at most `n` open files.
#[derive(Debug)]
pub struct FileBackedStackProvider {
    files_manager: Arc<Mutex<FilesManager>>,
}

impl FileBackedStackProvider {
    pub async fn new(config: &Config) -> Result<Self, FilesManagerError> {
        let files_manager = FilesManager::new(config).await?;

        Ok(Self {
            files_manager: Arc::new(Mutex::new(files_manager)),
        })
    }
}

impl StackProvider for FileBackedStackProvider {
    type Stack = FileBackedEnvelopeStack;

    async fn initialize(&self) -> InitializationState {
        // TODO: handle error when loading pairs.
        let project_key_pairs = self
            .files_manager
            .lock()
            .await
            .list_project_key_pairs()
            .await
            .unwrap_or(HashSet::new());

        InitializationState { project_key_pairs }
    }

    fn create_stack(
        &self,
        _stack_creation_type: StackCreationType,
        project_key_pair: ProjectKeyPair,
    ) -> Self::Stack {
        FileBackedEnvelopeStack::new(project_key_pair, self.files_manager.clone())
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
