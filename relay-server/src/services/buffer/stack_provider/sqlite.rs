use relay_config::Config;

use crate::services::buffer::envelope_stack::StackProvider;
use crate::services::buffer::sqlite_envelope_store::{
    SqliteEnvelopeStore, SqliteEnvelopeStoreError,
};
use crate::{Envelope, SqliteEnvelopeStack};

#[derive(Debug)]
pub struct SqliteStackProvider {
    envelope_store: SqliteEnvelopeStore,
    disk_batch_size: usize,
    max_batches: usize,
    max_evictable_envelopes: usize,
}

#[warn(dead_code)]
impl SqliteStackProvider {
    /// Creates a new [`SqliteStackProvider`] from the provided path to the SQLite database file.
    pub async fn new(config: &Config) -> Result<Self, SqliteEnvelopeStoreError> {
        let envelope_store = SqliteEnvelopeStore::prepare(config).await?;
        Ok(Self {
            envelope_store,
            disk_batch_size: config.spool_envelopes_stack_disk_batch_size(),
            max_batches: config.spool_envelopes_stack_max_batches(),
            max_evictable_envelopes: config.spool_envelopes_stack_max_evictable_envelopes(),
        })
    }
}

impl StackProvider for SqliteStackProvider {
    type Stack = SqliteEnvelopeStack;

    fn create_stack(&self, envelope: Box<Envelope>) -> Self::Stack {
        let own_key = envelope.meta().public_key();
        let sampling_key = envelope.sampling_key().unwrap_or(own_key);

        SqliteEnvelopeStack::new(
            self.envelope_store.clone(),
            self.disk_batch_size,
            self.max_batches,
            self.max_evictable_envelopes,
            own_key,
            sampling_key,
        )
    }
}
