use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_provider::InitializationState;
use crate::services::buffer::envelope_store::sqlite::SqliteEnvelopeStoreError;
use crate::statsd::{RelayCounters, RelayTimers};
use crate::{Envelope, SqliteEnvelopeStore};
use hashbrown::HashMap;
use relay_config::Config;
use std::collections::BTreeSet;
use std::error::Error;
use std::ops::Sub;

/// An error returned when doing an operation on [`SqliteEnvelopeProvider`].
#[derive(Debug, thiserror::Error)]
pub enum SqliteEnvelopeProviderError {
    /// Represents an error that occurred in the envelope store.
    #[error("an error occurred in the envelope store: {0}")]
    EnvelopeStoreError(#[from] SqliteEnvelopeStoreError),
}

/// A provider for storing and managing envelopes using SQLite as a backend.
///
/// This struct manages both in-memory and on-disk storage of envelopes,
/// implementing spooling and unspooling mechanisms to balance between
/// memory usage and disk I/O.
#[derive(Debug)]
pub struct SqliteEnvelopeProvider {
    #[allow(clippy::vec_box)]
    envelopes: HashMap<ProjectKeyPair, Vec<Box<Envelope>>>,
    envelope_store: SqliteEnvelopeStore,
    buffered_envelopes_size: u64,
    disk_empty: BTreeSet<ProjectKeyPair>,
    disk_batch_size: usize,
    max_disk_size: usize,
}

impl SqliteEnvelopeProvider {
    /// Creates a new [`SqliteEnvelopeProvider`] instance.
    pub async fn new(config: &Config) -> Result<Self, SqliteEnvelopeProviderError> {
        let envelope_store = SqliteEnvelopeStore::prepare(config).await?;
        Ok(Self {
            envelopes: HashMap::new(),
            envelope_store,
            buffered_envelopes_size: 0,
            disk_empty: BTreeSet::new(),
            disk_batch_size: config.spool_envelopes_stack_disk_batch_size(),
            max_disk_size: config.spool_envelopes_max_disk_size(),
        })
    }

    /// Creates a new [`SqliteEnvelopeProvider`] instance given a [`SqliteEnvelopeStore`].
    pub fn new_with_store(config: &Config, envelope_store: SqliteEnvelopeStore) -> Self {
        Self {
            envelopes: HashMap::new(),
            envelope_store,
            buffered_envelopes_size: 0,
            disk_empty: BTreeSet::new(),
            disk_batch_size: config.spool_envelopes_stack_disk_batch_size(),
            max_disk_size: config.spool_envelopes_max_disk_size(),
        }
    }

    /// Initializes the envelope provider.
    ///
    /// Retrieves the project key pairs from the envelope store and creates
    /// an initialization state.
    pub async fn initialize(&self) -> InitializationState {
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

    /// Pushes an envelope to the provider for the given project key pair.
    ///
    /// If the spool threshold is exceeded, it may trigger spooling to disk.
    pub async fn push(
        &mut self,
        project_key_pair: ProjectKeyPair,
        envelope: Box<Envelope>,
    ) -> Result<(), SqliteEnvelopeProviderError> {
        if self.above_spool_threshold() {
            self.spool_to_disk().await?;
        }

        self.envelopes
            .entry(project_key_pair)
            .or_default()
            .push(envelope);

        self.buffered_envelopes_size += 1;

        Ok(())
    }

    /// Peeks at the next envelope for the given project key pair without removing it.
    ///
    /// If no envelope is in the buffer, it will be loaded from disk and a reference will be
    /// returned.
    pub async fn peek(
        &mut self,
        project_key_pair: ProjectKeyPair,
    ) -> Result<Option<&Envelope>, SqliteEnvelopeProviderError> {
        if self.is_empty(project_key_pair) {
            let envelopes = self.unspool_from_disk(project_key_pair, 1).await?;
            if envelopes.is_empty() {
                return Ok(None);
            }

            self.buffered_envelopes_size += envelopes.len() as u64;
            self.envelopes
                .entry(project_key_pair)
                .or_default()
                .extend(envelopes);
        }

        Ok(self
            .envelopes
            .get(&project_key_pair)
            .and_then(|e| e.last().map(Box::as_ref)))
    }

    /// Pops and returns the next envelope for the given project key pair.
    ///
    /// If no envelope is in the buffer, it will be loaded from disk.
    pub async fn pop(
        &mut self,
        project_key_pair: ProjectKeyPair,
    ) -> Result<Option<Box<Envelope>>, SqliteEnvelopeProviderError> {
        let envelope = self
            .envelopes
            .get_mut(&project_key_pair)
            .and_then(|envelopes| envelopes.pop());

        // If we have no more envelopes to pop from the buffer, we want to remove the project key
        // pair to free up some memory.
        if self.is_empty(project_key_pair) {
            relay_statsd::metric!(counter(RelayCounters::BufferProjectKeyPairPopped) += 1);
            self.envelopes.remove(&project_key_pair);
        }

        if let Some(envelope) = envelope {
            // We only decrement the counter when removing data from the in memory buffer.
            self.buffered_envelopes_size -= 1;
            return Ok(Some(envelope));
        }

        let mut envelopes = self.unspool_from_disk(project_key_pair, 1).await?;
        if envelopes.is_empty() {
            return Ok(None);
        }

        Ok(envelopes.pop())
    }

    /// Flushes all remaining envelopes to disk.
    ///
    /// This method is typically called during shutdown to ensure all envelopes are persisted.
    pub async fn flush(&mut self) -> bool {
        relay_statsd::metric!(timer(RelayTimers::BufferFlush), {
            let mut envelopes = Vec::with_capacity(self.disk_batch_size);

            for (_, inner_invelopes) in self.envelopes.iter_mut() {
                for envelope in inner_invelopes.drain(..) {
                    if envelopes.len() >= self.disk_batch_size {
                        Self::flush_many(&mut self.envelope_store, envelopes).await;
                        envelopes = Vec::with_capacity(self.disk_batch_size);
                    }

                    envelopes.push(envelope);
                }
            }

            if !envelopes.is_empty() {
                Self::flush_many(&mut self.envelope_store, envelopes).await;
            }

            self.buffered_envelopes_size = 0;
        });

        true
    }

    /// Checks if there's capacity in the store for more envelopes.
    pub fn has_store_capacity(&self) -> bool {
        (self.envelope_store.usage() as usize) < self.max_disk_size
    }

    /// Retrieves the total count of envelopes in the store.
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

    /// Returns `true` if there are no [`Envelope`]s for the given [`ProjectKeyPair`], false
    /// otherwise.
    fn is_empty(&self, project_key_pair: ProjectKeyPair) -> bool {
        self.envelopes
            .get(&project_key_pair)
            .map_or(true, |e| e.is_empty())
    }

    /// Determines if the number of buffered envelopes is above the spool threshold.
    fn above_spool_threshold(&self) -> bool {
        self.buffered_envelopes_size >= self.disk_batch_size as u64
    }

    /// Spools envelopes to disk, evenly distributing across all project key pairs.
    ///
    /// This method attempts to spool `self.disk_batch_size` envelopes to disk, taking an equal
    /// number from each group of envelopes (represented by different project key pairs) in the
    /// `self.envelopes` HashMap.
    ///
    /// This approach ensures a balanced spooling across all project key pairs, preventing any
    /// single group from monopolizing the disk storage.
    async fn spool_to_disk(&mut self) -> Result<(), SqliteEnvelopeProviderError> {
        let mut envelopes_to_spool = Vec::with_capacity(self.disk_batch_size);

        // We take envelopes from each project key pair and keep track of which pairs have been
        // written to disk, to update the disk empty state accordingly.
        let mut written_project_key_pairs = BTreeSet::new();
        for (&project_key_pair, envelopes) in self.envelopes.iter_mut() {
            envelopes_to_spool.append(envelopes);
            written_project_key_pairs.insert(project_key_pair);
        }
        if envelopes_to_spool.is_empty() {
            return Ok(());
        }

        self.buffered_envelopes_size -= envelopes_to_spool.len() as u64;

        relay_statsd::metric!(
            counter(RelayCounters::BufferSpooledEnvelopes) += envelopes_to_spool.len() as u64
        );

        // Convert envelopes into a format which simplifies insertion in the store.
        let envelopes = envelopes_to_spool
            .iter()
            .filter_map(|e| e.as_ref().try_into().ok());
        relay_statsd::metric!(timer(RelayTimers::BufferSpool), {
            self.envelope_store
                .insert_many(envelopes)
                .await
                .map_err(SqliteEnvelopeProviderError::EnvelopeStoreError)?;
        });

        // If we successfully spooled to disk, we know that data should be there. We subtract all
        // pairs that previously were marked as not having data on disk and that now have it.
        self.disk_empty = self.disk_empty.sub(&written_project_key_pairs);

        Ok(())
    }

    /// Unspools from disk up to `n` envelopes and returns them.
    async fn unspool_from_disk(
        &mut self,
        project_key_pair: ProjectKeyPair,
        n: u64,
    ) -> Result<Vec<Box<Envelope>>, SqliteEnvelopeProviderError> {
        let envelopes = relay_statsd::metric!(timer(RelayTimers::BufferUnspool), {
            self.envelope_store
                .delete_many(
                    project_key_pair.own_key,
                    project_key_pair.sampling_key,
                    n as i64,
                )
                .await
                .map_err(SqliteEnvelopeProviderError::EnvelopeStoreError)?
        });

        if envelopes.is_empty() {
            // In case no envelopes were unspooled, we will mark this pair as having no data on disk.
            self.disk_empty.insert(project_key_pair);

            return Ok(vec![]);
        }

        relay_statsd::metric!(
            counter(RelayCounters::BufferUnspooledEnvelopes) += envelopes.len() as u64
        );

        Ok(envelopes)
    }

    /// Flushes multiple envelopes to the envelope store.
    ///
    /// If an error occurs during flushing, it logs the error but continues execution.
    #[allow(clippy::vec_box)]
    async fn flush_many(envelope_store: &mut SqliteEnvelopeStore, envelopes: Vec<Box<Envelope>>) {
        if let Err(error) = envelope_store
            .insert_many(
                envelopes
                    .into_iter()
                    .filter_map(|e| e.as_ref().try_into().ok()),
            )
            .await
        {
            relay_log::error!(
                error = &error as &dyn Error,
                "failed to flush envelopes, some might be lost",
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::buffer::testutils::utils::{
        mock_envelope, mock_envelopes, mock_envelopes_for_project, setup_db,
    };
    use relay_base_schema::project::ProjectKey;
    use std::time::{Duration, Instant};

    async fn setup_provider(
        run_migrations: bool,
        disk_batch_size: usize,
        max_disk_size: usize,
    ) -> SqliteEnvelopeProvider {
        let db = setup_db(run_migrations).await;
        let envelope_store = SqliteEnvelopeStore::new(db, Duration::from_millis(100));

        SqliteEnvelopeProvider {
            envelopes: HashMap::new(),
            envelope_store,
            buffered_envelopes_size: 0,
            disk_empty: Default::default(),
            disk_batch_size,
            max_disk_size,
        }
    }

    #[tokio::test]
    async fn test_initialize_with_unmigrated_db() {
        let provider = setup_provider(false, 2, 0).await;

        let initialization_state = provider.initialize().await;
        assert!(initialization_state.project_key_pairs.is_empty());
    }

    #[tokio::test]
    async fn test_push_with_unmigrated_db() {
        let mut provider = setup_provider(false, 1, 0).await;

        let project_key_pair = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        let envelope_1 = mock_envelope(Instant::now(), Some(project_key_pair.sampling_key));
        let envelope_2 = mock_envelope(Instant::now(), Some(project_key_pair.sampling_key));

        // Push should succeed as it doesn't interact with the database initially
        assert!(provider.push(project_key_pair, envelope_1).await.is_ok());

        // Push should fail because after the second insertion we try to spool
        let result = provider.push(project_key_pair, envelope_2).await;
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(matches!(
                error,
                SqliteEnvelopeProviderError::EnvelopeStoreError(_)
            ));
        }
    }

    #[tokio::test]
    async fn test_pop_with_unmigrated_db() {
        let mut provider = setup_provider(false, 1, 0).await;

        let project_key_pair = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        // Pop should fail because we can't unspool data from disk
        let result = provider.pop(project_key_pair).await;
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(matches!(
                error,
                SqliteEnvelopeProviderError::EnvelopeStoreError(_)
            ));
        }
    }

    #[tokio::test]
    async fn test_push_and_pop() {
        let mut provider = setup_provider(true, 2, 0).await;
        let project_key_pair = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        let envelopes = mock_envelopes(5);

        // Push 5 envelopes
        for envelope in envelopes.clone() {
            assert!(provider.push(project_key_pair, envelope).await.is_ok());
        }

        // Pop 5 envelopes
        for envelope in envelopes.iter().rev() {
            let popped_envelope = provider.pop(project_key_pair).await.unwrap().unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap()
            );
        }

        // Ensure the provider is empty
        assert!(provider.pop(project_key_pair).await.unwrap().is_none());
        assert!(!provider.envelopes.contains_key(&project_key_pair));
    }

    #[tokio::test]
    async fn test_peek() {
        let mut provider = setup_provider(true, 2, 0).await;
        let project_key_pair = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        let envelope = mock_envelope(Instant::now(), None);
        provider
            .push(project_key_pair, envelope.clone())
            .await
            .unwrap();

        // Peek at the envelope
        let peeked_envelope = provider.peek(project_key_pair).await.unwrap().unwrap();
        assert_eq!(
            peeked_envelope.event_id().unwrap(),
            envelope.event_id().unwrap()
        );

        // Ensure the envelope is still there after peeking
        let popped_envelope = provider.pop(project_key_pair).await.unwrap().unwrap();
        assert_eq!(
            popped_envelope.event_id().unwrap(),
            envelope.event_id().unwrap()
        );

        // Ensure the provider has deallocated the project key pair entry in the map since the last
        // element was popped
        assert!(!provider.envelopes.contains_key(&project_key_pair));
    }

    #[tokio::test]
    async fn test_spool_and_unspool_disk() {
        let mut provider = setup_provider(true, 5, 0).await;
        let project_key_pair = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        let envelopes = mock_envelopes(15);

        // Push 15 envelopes (should trigger spooling after 5)
        for envelope in envelopes.clone() {
            assert!(provider.push(project_key_pair, envelope).await.is_ok());
        }

        // Check that we have 5 envelopes in memory (1 batch of 3)
        assert_eq!(provider.buffered_envelopes_size, 5);
        assert_eq!(provider.store_total_count().await, 10);

        // Pop all envelopes
        for envelope in envelopes.iter().rev() {
            let popped_envelope = provider.pop(project_key_pair).await.unwrap().unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap(),
            );
        }

        // Ensure the provider is now empty
        assert!(provider.pop(project_key_pair).await.unwrap().is_none());
        assert_eq!(provider.buffered_envelopes_size, 0);
        assert_eq!(provider.store_total_count().await, 0);
    }

    #[tokio::test]
    async fn test_flush() {
        let mut provider = setup_provider(true, 2, 1000).await;
        let project_key_pair = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        let envelopes = mock_envelopes(5);

        // Push 5 envelopes
        for envelope in envelopes.clone() {
            assert!(provider.push(project_key_pair, envelope).await.is_ok());
        }

        // Flush all envelopes to disk
        assert!(provider.flush().await);

        // Check that all envelopes are now on disk
        assert_eq!(provider.store_total_count().await, 5);

        // Pop all envelopes (should trigger unspool from disk)
        for envelope in envelopes.iter().rev() {
            let popped_envelope = provider.pop(project_key_pair).await.unwrap().unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap()
            );
        }

        // Ensure the provider is empty
        assert!(provider.pop(project_key_pair).await.unwrap().is_none());
        assert_eq!(provider.store_total_count().await, 0);
    }

    #[tokio::test]
    async fn test_multiple_project_key_pairs() {
        let mut provider = setup_provider(true, 2, 1000).await;
        let project_key_pair1 = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b28ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        );
        let project_key_pair2 = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("c67ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        );

        let envelopes1 = mock_envelopes_for_project(3, project_key_pair1.sampling_key);
        let envelopes2 = mock_envelopes_for_project(2, project_key_pair2.sampling_key);

        // Push envelopes for both project key pairs
        for envelope in envelopes1.clone() {
            assert!(provider.push(project_key_pair1, envelope).await.is_ok());
        }
        for envelope in envelopes2.clone() {
            assert!(provider.push(project_key_pair2, envelope).await.is_ok());
        }

        // Pop envelopes for project_key_pair1
        for envelope in envelopes1.iter().rev() {
            let popped_envelope = provider.pop(project_key_pair1).await.unwrap().unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap()
            );
        }

        // Pop envelopes for project_key_pair2
        for envelope in envelopes2.iter().rev() {
            let popped_envelope = provider.pop(project_key_pair2).await.unwrap().unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap()
            );
        }

        // Ensure both project key pairs are empty
        assert!(provider.pop(project_key_pair1).await.unwrap().is_none());
        assert!(provider.pop(project_key_pair2).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_disk_empty() {
        let mut provider = setup_provider(true, 2, 0).await;
        let project_key_pair1 = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b28ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        );
        let project_key_pair2 = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("c67ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        );

        // Initially, disk_empty should be empty
        assert!(provider.disk_empty.is_empty());

        // Push 3 envelopes for project_key_pair1 (should trigger spooling)
        let envelopes1 = mock_envelopes_for_project(3, project_key_pair1.sampling_key);
        for envelope in envelopes1 {
            assert!(provider.push(project_key_pair1, envelope).await.is_ok());
        }

        // disk_empty should still be empty after spooling
        assert!(provider.disk_empty.is_empty());

        // Pop all envelopes for project_key_pair1
        while provider.pop(project_key_pair1).await.unwrap().is_some() {}

        // After popping all envelopes, project_key_pair1 should be in disk_empty
        assert!(provider.disk_empty.contains(&project_key_pair1));

        // Push 1 envelope for project_key_pair2 (should not trigger spooling)
        let envelope = mock_envelope(Instant::now(), Some(project_key_pair2.sampling_key));
        assert!(provider.push(project_key_pair2, envelope).await.is_ok());

        // Flush remaining envelopes to disk
        assert!(provider.flush().await);

        // After flushing, project_key_pair2 should not be in disk_empty
        assert!(!provider.disk_empty.contains(&project_key_pair2));

        // Pop all envelopes for project_key_pair1
        while provider.pop(project_key_pair2).await.unwrap().is_some() {}

        // After popping, project_key_pair2 should now be in disk_empty
        assert!(provider.disk_empty.contains(&project_key_pair2));

        // Final check: both project key pairs should be in disk_empty
        assert!(provider.disk_empty.contains(&project_key_pair1));
        assert!(provider.disk_empty.contains(&project_key_pair2));
    }
}
