use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_stack::file_backed::FileBackedEnvelopeStack;
use crate::services::buffer::envelope_store::file_backed::{
    FileBackedEnvelopeStore, FileBackedEnvelopeStoreError,
};
use crate::services::buffer::stack_provider::{
    InitializationState, StackCreationType, StackProvider,
};
use hashbrown::HashMap;
use relay_config::Config;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::timeout;

/// A stack provider that manages [`FileBackedEnvelopeStack`] instances.
///
/// This provider uses a file-backed envelope store to persist envelopes on disk.
/// It implements the [`StackProvider`] trait, providing methods to initialize,
/// create stacks, and manage the overall state of the envelope storage.
#[derive(Debug)]
pub struct FileBackedStackProvider {
    envelope_store: Arc<Mutex<FileBackedEnvelopeStore>>,
    max_disk_size: usize,
}

impl FileBackedStackProvider {
    /// Creates a new [`FileBackedStackProvider`] instance.
    ///
    /// This method initializes the underlying [`FileBackedEnvelopeStore`] using the provided
    /// configuration.
    pub async fn new(config: &Config) -> Result<Self, FileBackedEnvelopeStoreError> {
        let envelope_store = FileBackedEnvelopeStore::new(config).await?;

        Ok(Self {
            envelope_store: Arc::new(Mutex::new(envelope_store)),
            max_disk_size: config.spool_envelopes_max_disk_size(),
        })
    }

    /// Retrieves a mapping of project key pairs to their envelope counts from the file-backed envelope store.
    ///
    /// This method attempts to fetch the project key pairs and their associated envelope counts
    /// from the underlying file-backed envelope store. It uses a timeout mechanism to ensure
    /// the operation doesn't hang indefinitely.
    ///
    /// The timeout period is set to 1 second to prevent long-running operations from blocking
    /// the system.
    async fn project_key_pairs_with_counts(&self) -> HashMap<ProjectKeyPair, u32> {
        match timeout(
            Duration::from_secs(1),
            self.envelope_store
                .lock()
                .await
                .project_key_pairs_with_counts(),
        )
        .await
        {
            Ok(Ok(project_key_pairs_with_counts)) => project_key_pairs_with_counts,
            Ok(Err(error)) => {
                relay_log::error!(
                    error = &error as &dyn Error,
                    "Failed to retrieve project key pairs and envelope counts from the file-backed envelope store"
                );
                HashMap::new()
            }
            Err(_) => {
                relay_log::error!("Operation timed out while fetching project key pairs and envelope counts from the file-backed envelope store");
                HashMap::new()
            }
        }
    }
}

impl StackProvider for FileBackedStackProvider {
    type Stack = FileBackedEnvelopeStack;

    async fn initialize(&self) -> InitializationState {
        let project_key_pairs_with_counts = self.project_key_pairs_with_counts().await;

        let project_key_pairs = project_key_pairs_with_counts.keys().copied().collect();
        let store_total_count = project_key_pairs_with_counts.values().copied().sum();

        InitializationState::new(project_key_pairs, store_total_count)
    }

    fn create_stack(
        &self,
        _stack_creation_type: StackCreationType,
        project_key_pair: ProjectKeyPair,
    ) -> Self::Stack {
        FileBackedEnvelopeStack::new(project_key_pair, self.envelope_store.clone())
    }

    fn has_store_capacity(&self) -> bool {
        (self.envelope_store.blocking_lock().usage() as usize) < self.max_disk_size
    }

    fn stack_type<'a>(&self) -> &'a str {
        "file_backed"
    }

    async fn flush(&mut self, _envelope_stacks: impl IntoIterator<Item = Self::Stack>) {
        // Since data is already on disk, no action needed.
    }
}
