use relay_config::Config;
use std::error::Error;

use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_store::sqlite::{
    SqliteEnvelopeStore, SqliteEnvelopeStoreError,
};
use crate::services::buffer::stack_provider::{
    InitializationState, StackCreationType, StackProvider,
};
use crate::SqliteEnvelopeStack;

#[derive(Debug)]
pub struct SqliteStackProvider {
    envelope_store: SqliteEnvelopeStore,
    disk_batch_size: usize,
    max_batches: usize,
    max_disk_size: usize,
}

#[warn(dead_code)]
impl SqliteStackProvider {
    /// Creates a new [`SqliteStackProvider`] from the provided [`Config`].
    pub async fn new(config: &Config) -> Result<Self, SqliteEnvelopeStoreError> {
        let envelope_store = SqliteEnvelopeStore::prepare(config).await?;
        Ok(Self {
            envelope_store,
            disk_batch_size: config.spool_envelopes_stack_disk_batch_size(),
            max_batches: config.spool_envelopes_stack_max_batches(),
            max_disk_size: config.spool_envelopes_max_disk_size(),
        })
    }

    /// Returns `true` when there might be data residing on disk, `false` otherwise.
    fn assume_data_on_disk(stack_creation_type: StackCreationType) -> bool {
        matches!(stack_creation_type, StackCreationType::Initialization)
    }
}

impl StackProvider for SqliteStackProvider {
    type Stack = SqliteEnvelopeStack;

    async fn initialize(&self) -> InitializationState {
        match self.envelope_store.project_key_pairs().await {
            Ok(project_key_pairs) => InitializationState::new(project_key_pairs),
            Err(error) => {
                relay_log::error!(
                    error = &error as &dyn Error,
                    "failed to initialize the sqlite stack provider"
                );
                InitializationState::empty()
            }
        }
    }

    fn create_stack(
        &self,
        stack_creation_type: StackCreationType,
        project_key_pair: ProjectKeyPair,
    ) -> Self::Stack {
        SqliteEnvelopeStack::new(
            self.envelope_store.clone(),
            self.disk_batch_size,
            self.max_batches,
            project_key_pair.own_key,
            project_key_pair.sampling_key,
            // We want to check the disk by default if we are creating the stack for the first time,
            // since we might have some data on disk.
            // On the other hand, if we are recreating a stack, it means that we popped it because
            // it was empty, or we never had data on disk for that stack, so we assume by default
            // that there is no need to check disk until some data is spooled.
            Self::assume_data_on_disk(stack_creation_type),
        )
    }

    fn has_store_capacity(&self) -> bool {
        (self.envelope_store.usage() as usize) < self.max_disk_size
    }

    async fn store_total_count(&self) -> u64 {
        self.envelope_store
            .total_count()
            .await
            .unwrap_or_else(|error| {
                relay_log::error!(
                    error = &error as &dyn Error,
                    "failed to get the total count of envelopes for the sqlite envelope store",
                );
                // In case we have an error, we default to communicating a total count of 0.
                0
            })
    }

    fn stack_type<'a>(&self) -> &'a str {
        "sqlite"
    }
}
