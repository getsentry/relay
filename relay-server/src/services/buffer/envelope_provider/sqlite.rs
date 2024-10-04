use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_store::sqlite::SqliteEnvelopeStoreError;
use crate::services::buffer::stack_provider::InitializationState;
use crate::{Envelope, SqliteEnvelopeStore};
use hashbrown::HashMap;
use relay_config::Config;
use std::error::Error;

/// An error returned when doing an operation on [`SQLiteEnvelopeProvider`].
#[derive(Debug, thiserror::Error)]
pub enum SqliteEnvelopeProviderError {
    #[error("an error occurred in the envelope store: {0}")]
    EnvelopeStoreError(#[from] SqliteEnvelopeStoreError),
}

#[derive(Debug)]
pub struct SQLiteEnvelopeProvider {
    #[allow(clippy::vec_box)]
    envelopes: HashMap<ProjectKeyPair, Vec<Box<Envelope>>>,
    envelope_store: SqliteEnvelopeStore,
    disk_batch_size: usize,
    // TODO: check how to implement check disk.
}

impl SQLiteEnvelopeProvider {
    pub fn new(config: &Config, envelope_store: SqliteEnvelopeStore) -> Self {
        Self {
            envelopes: HashMap::new(),
            envelope_store,
            disk_batch_size: config.spool_envelopes_stack_disk_batch_size(),
        }
    }

    pub async fn initialize(&self) -> InitializationState {
        // TODO: implement initialization state.
        InitializationState::empty()
    }

    pub async fn push(
        &mut self,
        project_key_pair: ProjectKeyPair,
        envelope: Box<Envelope>,
    ) -> Result<(), SqliteEnvelopeProviderError> {
        Ok(())
    }

    pub async fn peek(
        &self,
        project_key_pair: ProjectKeyPair,
    ) -> Result<Option<&Envelope>, SqliteEnvelopeProviderError> {
        Ok(None)
    }

    pub async fn pop(
        &mut self,
        project_key_pair: ProjectKeyPair,
    ) -> Result<Option<Box<Envelope>>, SqliteEnvelopeProviderError> {
        Ok(None)
    }

    pub async fn flush(mut self) {}

    pub fn has_store_capacity(&self) -> bool {
        true
    }

    pub async fn store_total_count(&self) -> u64 {
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
}
