use relay_config::Config;

use crate::services::buffer::envelope_stack::StackProvider;
use crate::services::buffer::envelope_store::sqlite::{
    SqliteEnvelopeStore, SqliteEnvelopeStoreError,
};
use crate::{Envelope, SqliteEnvelopeStack};

#[derive(Debug)]
pub struct SqliteStackProvider {
    envelope_store: SqliteEnvelopeStore,
    disk_batch_size: usize,
    max_batches: usize,
}

impl SqliteStackProvider {
    /// Creates a new [`SqliteStackProvider`] from the provided path to the SQLite database file.
    pub async fn new(config: &Config) -> Result<Self, SqliteEnvelopeStoreError> {
        // TODO: error handling
        let envelope_store = SqliteEnvelopeStore::prepare(config).await?;
        Ok(Self {
            envelope_store,
            disk_batch_size: 100, // TODO: put in config
            max_batches: 2,       // TODO: put in config
        })
    }
}

impl StackProvider for SqliteStackProvider {
    type Stack = SqliteEnvelopeStack;

    fn create_stack(&self, envelope: Box<Envelope>) -> Self::Stack {
        let own_key = envelope.meta().public_key();
        // TODO: start loading from disk the initial batch of envelopes.
        SqliteEnvelopeStack::new(
            self.envelope_store.clone(),
            self.disk_batch_size,
            self.max_batches,
            own_key,
            envelope.sampling_key().unwrap_or(own_key),
        )
    }
}
