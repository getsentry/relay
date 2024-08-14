use relay_config::Config;

use crate::services::buffer::envelope_store::sqlite::{
    SqliteEnvelopeStore, SqliteEnvelopeStoreError,
};
use crate::services::buffer::envelope_store::EnvelopeStore;
use crate::services::buffer::stacks_manager::{Capacity, StacksManager};
use crate::{Envelope, SqliteEnvelopeStack};

#[derive(Debug)]
pub struct SqliteStacksManager {
    envelope_store: SqliteEnvelopeStore,
    disk_batch_size: usize,
    max_batches: usize,
    max_disk_size: usize,
}

#[warn(dead_code)]
impl SqliteStacksManager {
    /// Creates a new [`SqliteStacksManager`] from the provided [`Config`].
    pub async fn new(config: &Config) -> Result<Self, SqliteEnvelopeStoreError> {
        let envelope_store = SqliteEnvelopeStore::prepare(config).await?;
        Ok(Self {
            envelope_store,
            disk_batch_size: config.spool_envelopes_stack_disk_batch_size(),
            max_batches: config.spool_envelopes_stack_max_batches(),
            max_disk_size: config.spool_envelopes_max_disk_size(),
        })
    }
}

impl StacksManager for SqliteStacksManager {
    type Stack = SqliteEnvelopeStack;

    fn create_stack(&self, envelope: Box<Envelope>) -> Self::Stack {
        let own_key = envelope.meta().public_key();
        let sampling_key = envelope.sampling_key().unwrap_or(own_key);

        SqliteEnvelopeStack::new(
            self.envelope_store.clone(),
            self.disk_batch_size,
            self.max_batches,
            own_key,
            sampling_key,
        )
    }

    fn capacity(&self) -> Capacity {
        if (self.envelope_store.usage() as usize) < self.max_disk_size {
            Capacity::Free
        } else {
            Capacity::Full
        }
    }
}
