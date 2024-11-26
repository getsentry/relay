use std::error::Error;

use relay_config::Config;

use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_stack::caching::CachingEnvelopeStack;
use crate::services::buffer::envelope_store::sqlite::{
    SqliteEnvelopeStore, SqliteEnvelopeStoreError,
};
use crate::services::buffer::stack_provider::{
    InitializationState, StackCreationType, StackProvider,
};
use crate::statsd::RelayTimers;
use crate::{EnvelopeStack, SqliteEnvelopeStack};

#[derive(Debug)]
pub struct SqliteStackProvider {
    envelope_store: SqliteEnvelopeStore,
    batch_size_bytes: usize,
    max_disk_size: usize,
}

#[warn(dead_code)]
impl SqliteStackProvider {
    /// Creates a new [`SqliteStackProvider`] from the provided [`Config`].
    pub async fn new(partition_id: u32, config: &Config) -> Result<Self, SqliteEnvelopeStoreError> {
        let envelope_store = SqliteEnvelopeStore::prepare(partition_id, config).await?;
        Ok(Self {
            envelope_store,
            batch_size_bytes: config.spool_envelopes_batch_size_bytes(),
            max_disk_size: config.spool_envelopes_max_disk_size(),
        })
    }

    /// Returns `true` when there might be data residing on disk, `false` otherwise.
    fn assume_data_on_disk(stack_creation_type: StackCreationType) -> bool {
        matches!(stack_creation_type, StackCreationType::Initialization)
    }
}

impl StackProvider for SqliteStackProvider {
    type Stack = CachingEnvelopeStack<SqliteEnvelopeStack>;

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
        let inner = SqliteEnvelopeStack::new(
            self.envelope_store.clone(),
            self.batch_size_bytes,
            project_key_pair.own_key,
            project_key_pair.sampling_key,
            // We want to check the disk by default if we are creating the stack for the first time,
            // since we might have some data on disk.
            // On the other hand, if we are recreating a stack, it means that we popped it because
            // it was empty, or we never had data on disk for that stack, so we assume by default
            // that there is no need to check disk until some data is spooled.
            Self::assume_data_on_disk(stack_creation_type),
        );

        CachingEnvelopeStack::new(inner)
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

    async fn flush(&mut self, envelope_stacks: impl IntoIterator<Item = Self::Stack>) {
        relay_log::trace!("Flushing sqlite envelope buffer");

        relay_statsd::metric!(timer(RelayTimers::BufferDrain), {
            for envelope_stack in envelope_stacks {
                envelope_stack.flush().await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use relay_base_schema::project::ProjectKey;
    use relay_config::Config;
    use uuid::Uuid;

    use crate::services::buffer::common::ProjectKeyPair;
    use crate::services::buffer::stack_provider::sqlite::SqliteStackProvider;
    use crate::services::buffer::stack_provider::{StackCreationType, StackProvider};
    use crate::services::buffer::testutils::utils::mock_envelopes;
    use crate::EnvelopeStack;

    fn mock_config() -> Arc<Config> {
        let path = std::env::temp_dir()
            .join(Uuid::new_v4().to_string())
            .into_os_string()
            .into_string()
            .unwrap();

        Config::from_json_value(serde_json::json!({
            "spool": {
                "envelopes": {
                    "path": path,
                    "disk_batch_size": 100,
                    "max_batches": 1,
                }
            }
        }))
        .unwrap()
        .into()
    }

    #[tokio::test]
    async fn test_flush() {
        let config = mock_config();
        let mut stack_provider = SqliteStackProvider::new(0, &config).await.unwrap();

        let own_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let sampling_key = ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap();

        let mut envelope_stack = stack_provider.create_stack(
            StackCreationType::New,
            ProjectKeyPair::new(own_key, sampling_key),
        );

        let envelopes = mock_envelopes(10);
        for envelope in envelopes {
            envelope_stack.push(envelope).await.unwrap();
        }

        let envelope_store = stack_provider.envelope_store.clone();

        // We make sure that no data is on disk since we will spool when more than 100 elements are
        // in the in-memory stack.
        assert_eq!(envelope_store.total_count().await.unwrap(), 0);

        // We drain the stack provider, and we expect all in-memory envelopes to be spooled to disk.
        stack_provider.flush(vec![envelope_stack]).await;
        assert_eq!(envelope_store.total_count().await.unwrap(), 10);
    }
}
