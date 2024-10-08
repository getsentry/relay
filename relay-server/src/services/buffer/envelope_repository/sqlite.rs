use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_repository::InitializationState;
use crate::services::buffer::envelope_store::sqlite::SqliteEnvelopeStoreError;
use crate::statsd::{RelayCounters, RelayHistograms, RelayTimers};
use crate::{Envelope, SqliteEnvelopeStore};
use hashbrown::{HashMap, HashSet};
use relay_config::Config;
use std::error::Error;

/// An error returned when doing an operation on [`SqliteEnvelopeRepository`].
#[derive(Debug, thiserror::Error)]
pub enum SqliteEnvelopeRepositoryError {
    /// Represents an error that occurred in the envelope store.
    #[error("an error occurred in the envelope store: {0}")]
    EnvelopeStoreError(#[from] SqliteEnvelopeStoreError),
}

#[derive(Debug, Default)]
struct EnvelopeStack {
    #[allow(clippy::vec_box)]
    envelopes: Vec<Box<Envelope>>,
    check_disk: bool,
}

/// A repository for storing and managing envelopes using SQLite as a backend.
///
/// This struct manages both in-memory and on-disk storage of envelopes,
/// implementing spooling and unspooling mechanisms to balance between
/// memory usage and disk I/O.
#[derive(Debug)]
pub struct SqliteEnvelopeRepository {
    envelope_stacks: HashMap<ProjectKeyPair, EnvelopeStack>,
    envelope_store: SqliteEnvelopeStore,
    buffered_envelopes_size: u64,
    disk_batch_size: usize,
    max_disk_size: usize,
}

impl SqliteEnvelopeRepository {
    /// Creates a new [`SqliteEnvelopeRepository`] instance.
    pub async fn new(config: &Config) -> Result<Self, SqliteEnvelopeRepositoryError> {
        let envelope_store = SqliteEnvelopeStore::prepare(config).await?;
        Ok(Self {
            envelope_stacks: HashMap::new(),
            envelope_store,
            buffered_envelopes_size: 0,
            disk_batch_size: config.spool_envelopes_stack_disk_batch_size(),
            max_disk_size: config.spool_envelopes_max_disk_size(),
        })
    }

    /// Creates a new [`SqliteEnvelopeRepository`] instance given a [`SqliteEnvelopeStore`].
    pub fn new_with_store(config: &Config, envelope_store: SqliteEnvelopeStore) -> Self {
        Self {
            envelope_stacks: HashMap::new(),
            envelope_store,
            buffered_envelopes_size: 0,
            disk_batch_size: config.spool_envelopes_stack_disk_batch_size(),
            max_disk_size: config.spool_envelopes_max_disk_size(),
        }
    }

    /// Initializes the envelope repository.
    ///
    /// Retrieves the project key pairs from the envelope store and creates
    /// an initialization state.
    pub async fn initialize(&mut self) -> InitializationState {
        match self.envelope_store.project_key_pairs().await {
            Ok(project_key_pairs) => {
                self.initialize_empty_stacks(&project_key_pairs);
                InitializationState::new(project_key_pairs)
            }
            Err(error) => {
                relay_log::error!(
                    error = &error as &dyn Error,
                    "failed to initialize the sqlite stack repository"
                );
                InitializationState::empty()
            }
        }
    }

    /// Pushes an envelope to the repository for the given project key pair.
    ///
    /// If the spool threshold is exceeded, it may trigger spooling to disk.
    pub async fn push(
        &mut self,
        project_key_pair: ProjectKeyPair,
        envelope: Box<Envelope>,
    ) -> Result<(), SqliteEnvelopeRepositoryError> {
        if self.above_spool_threshold() {
            self.spool_to_disk().await?;
        }

        self.envelope_stacks
            .entry(project_key_pair)
            .or_default()
            .envelopes
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
    ) -> Result<Option<&Envelope>, SqliteEnvelopeRepositoryError> {
        if self.memory_empty(project_key_pair) && self.should_check_disk(project_key_pair) {
            let envelopes = self.unspool_from_disk(project_key_pair, 1).await?;
            // If we have no envelopes in the buffer and no on disk, we can be safe removing the entry
            // in the buffer.
            if envelopes.is_empty() {
                self.envelope_stacks.remove(&project_key_pair);
                return Ok(None);
            }

            self.buffered_envelopes_size += envelopes.len() as u64;
            self.envelope_stacks
                .entry(project_key_pair)
                .or_default()
                .envelopes
                .extend(envelopes);
        }

        Ok(self
            .envelope_stacks
            .get(&project_key_pair)
            .and_then(|e| e.envelopes.last().map(Box::as_ref)))
    }

    /// Pops and returns the next envelope for the given project key pair.
    ///
    /// If no envelope is in the buffer, it will be loaded from disk.
    pub async fn pop(
        &mut self,
        project_key_pair: ProjectKeyPair,
    ) -> Result<Option<Box<Envelope>>, SqliteEnvelopeRepositoryError> {
        let envelope = self
            .envelope_stacks
            .get_mut(&project_key_pair)
            .and_then(|envelopes| envelopes.envelopes.pop());
        if let Some(envelope) = envelope {
            // We only decrement the counter when removing data from the in memory buffer.
            self.buffered_envelopes_size -= 1;
            return Ok(Some(envelope));
        }

        // If we don't need to check disk, we assume there are no envelopes, so we early return
        // `None`.
        if !self.should_check_disk(project_key_pair) {
            return Ok(None);
        }

        // If we have no envelopes in the buffer, we try to pop and immediately return data from
        // disk.
        let mut envelopes = self.unspool_from_disk(project_key_pair, 1).await?;
        // If we have no envelopes in the buffer and no on disk, we can be safe removing the entry
        // in the buffer.
        if envelopes.is_empty() {
            self.envelope_stacks.remove(&project_key_pair);
        }

        Ok(envelopes.pop())
    }

    /// Flushes all remaining envelopes to disk.
    ///
    /// This method is typically called during shutdown to ensure all envelopes are persisted.
    pub async fn flush(&mut self) -> bool {
        relay_statsd::metric!(timer(RelayTimers::BufferFlush), {
            let mut envelopes = Vec::with_capacity(self.disk_batch_size);

            for (_, envelope_stack) in self.envelope_stacks.iter_mut() {
                envelope_stack.check_disk = true;
                for envelope in envelope_stack.envelopes.drain(..) {
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

    /// Initializes a set of empty [`EnvelopeStack`]s.
    fn initialize_empty_stacks(&mut self, project_key_pairs: &HashSet<ProjectKeyPair>) {
        for &project_key_pair in project_key_pairs {
            let envelope_stack = self.envelope_stacks.entry(project_key_pair).or_default();
            // When creating an envelope stack during initialization, we assume data is on disk.
            envelope_stack.check_disk = true;
        }
    }

    /// Returns `true` if there are no [`Envelope`]s for the given [`ProjectKeyPair`], false
    /// otherwise.
    fn memory_empty(&self, project_key_pair: ProjectKeyPair) -> bool {
        self.envelope_stacks
            .get(&project_key_pair)
            .map_or(true, |e| e.envelopes.is_empty())
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
    async fn spool_to_disk(&mut self) -> Result<(), SqliteEnvelopeRepositoryError> {
        let mut envelopes_to_spool = Vec::with_capacity(self.disk_batch_size);

        for envelope_stack in self.envelope_stacks.values_mut() {
            envelopes_to_spool.append(&mut envelope_stack.envelopes);
            envelope_stack.check_disk = true;
            relay_statsd::metric!(
                histogram(RelayHistograms::BufferInMemoryEnvelopesPerKeyPair) =
                    envelope_stack.envelopes.len() as u64
            );
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
                .map_err(SqliteEnvelopeRepositoryError::EnvelopeStoreError)?;
        });

        Ok(())
    }

    /// Unspools from disk up to `n` envelopes and returns them.
    async fn unspool_from_disk(
        &mut self,
        project_key_pair: ProjectKeyPair,
        n: u64,
    ) -> Result<Vec<Box<Envelope>>, SqliteEnvelopeRepositoryError> {
        let envelopes = relay_statsd::metric!(timer(RelayTimers::BufferUnspool), {
            self.envelope_store
                .delete_many(
                    project_key_pair.own_key,
                    project_key_pair.sampling_key,
                    n as i64,
                )
                .await
                .map_err(SqliteEnvelopeRepositoryError::EnvelopeStoreError)?
        });

        if envelopes.is_empty() {
            // In case no envelopes were unspooled, we mark this project key pair as having no
            // envelopes on disk.
            self.set_check_disk(project_key_pair, false);

            return Ok(vec![]);
        }

        relay_statsd::metric!(
            counter(RelayCounters::BufferUnspooledEnvelopes) += envelopes.len() as u64
        );

        Ok(envelopes)
    }

    /// Returns `true` whether the disk should be checked for data for a given [`ProjectKeyPair`],
    /// false otherwise.
    fn should_check_disk(&self, project_key_pair: ProjectKeyPair) -> bool {
        // If a project key pair is unknown, we don't want to check disk.
        self.envelope_stacks
            .get(&project_key_pair)
            .map_or(false, |e| e.check_disk)
    }

    /// Sets on the [`EnvelopeStack`] whether the disk should be checked or not.
    fn set_check_disk(&mut self, project_key_pair: ProjectKeyPair, check_disk: bool) {
        if let Some(envelope_stack) = self.envelope_stacks.get_mut(&project_key_pair) {
            envelope_stack.check_disk = check_disk;
        }
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

    async fn setup_repository(
        run_migrations: bool,
        disk_batch_size: usize,
        max_disk_size: usize,
    ) -> SqliteEnvelopeRepository {
        let db = setup_db(run_migrations).await;
        let envelope_store = SqliteEnvelopeStore::new(db, Duration::from_millis(100));

        SqliteEnvelopeRepository {
            envelope_stacks: HashMap::new(),
            envelope_store,
            buffered_envelopes_size: 0,
            disk_batch_size,
            max_disk_size,
        }
    }

    #[tokio::test]
    async fn test_initialize_with_unmigrated_db() {
        let mut repository = setup_repository(false, 2, 0).await;

        let initialization_state = repository.initialize().await;
        assert!(initialization_state.project_key_pairs.is_empty());
    }

    #[tokio::test]
    async fn test_push_with_unmigrated_db() {
        let mut repository = setup_repository(false, 1, 0).await;

        let project_key_pair = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        let envelope_1 = mock_envelope(Instant::now(), Some(project_key_pair.sampling_key));
        let envelope_2 = mock_envelope(Instant::now(), Some(project_key_pair.sampling_key));

        // Push should succeed as it doesn't interact with the database initially
        assert!(repository.push(project_key_pair, envelope_1).await.is_ok());

        // Push should fail because after the second insertion we try to spool
        let result = repository.push(project_key_pair, envelope_2).await;
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(matches!(
                error,
                SqliteEnvelopeRepositoryError::EnvelopeStoreError(_)
            ));
        }
    }

    #[tokio::test]
    async fn test_pop_with_unmigrated_db() {
        let mut repository = setup_repository(false, 1, 0).await;

        let project_key_pair = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        // We initialize empty stacks to make sure the repository checks for disk
        let mut project_key_pairs = HashSet::new();
        project_key_pairs.insert(project_key_pair);
        repository.initialize_empty_stacks(&project_key_pairs);

        // Pop should fail because we can't unspool data from disk
        let result = repository.pop(project_key_pair).await;
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(matches!(
                error,
                SqliteEnvelopeRepositoryError::EnvelopeStoreError(_)
            ));
        }
    }

    #[tokio::test]
    async fn test_push_and_pop() {
        let mut repository = setup_repository(true, 2, 0).await;
        let project_key_pair = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        let envelopes = mock_envelopes(5);

        // Push 5 envelopes
        for envelope in envelopes.clone() {
            assert!(repository.push(project_key_pair, envelope).await.is_ok());
        }

        // Pop 5 envelopes
        for envelope in envelopes.iter().rev() {
            let popped_envelope = repository.pop(project_key_pair).await.unwrap().unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap()
            );
        }

        // Ensure the repository is empty
        assert!(repository.pop(project_key_pair).await.unwrap().is_none());
        assert!(!repository.envelope_stacks.contains_key(&project_key_pair));
    }

    #[tokio::test]
    async fn test_peek() {
        let mut repository = setup_repository(true, 2, 0).await;
        let project_key_pair = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        let envelope = mock_envelope(Instant::now(), None);
        repository
            .push(project_key_pair, envelope.clone())
            .await
            .unwrap();

        // Peek at the envelope
        let peeked_envelope = repository.peek(project_key_pair).await.unwrap().unwrap();
        assert_eq!(
            peeked_envelope.event_id().unwrap(),
            envelope.event_id().unwrap()
        );

        // Ensure the envelope is still there after peeking
        let popped_envelope = repository.pop(project_key_pair).await.unwrap().unwrap();
        assert_eq!(
            popped_envelope.event_id().unwrap(),
            envelope.event_id().unwrap()
        );
    }

    #[tokio::test]
    async fn test_spool_and_unspool_disk() {
        let mut repository = setup_repository(true, 5, 0).await;
        let project_key_pair = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        let envelopes = mock_envelopes(15);

        // Push 15 envelopes (should trigger spooling after 5)
        for envelope in envelopes.clone() {
            assert!(repository.push(project_key_pair, envelope).await.is_ok());
        }

        // Check that we have 5 envelopes in memory (1 batch of 3)
        assert_eq!(repository.buffered_envelopes_size, 5);
        assert_eq!(repository.store_total_count().await, 10);

        // Pop all envelopes
        for envelope in envelopes.iter().rev() {
            let popped_envelope = repository.pop(project_key_pair).await.unwrap().unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap(),
            );
        }

        // Ensure the repository is now empty
        assert!(repository.pop(project_key_pair).await.unwrap().is_none());
        assert_eq!(repository.buffered_envelopes_size, 0);
        assert_eq!(repository.store_total_count().await, 0);
    }

    #[tokio::test]
    async fn test_flush() {
        let mut repository = setup_repository(true, 2, 1000).await;
        let project_key_pair = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        let envelopes = mock_envelopes(5);

        // Push 5 envelopes
        for envelope in envelopes.clone() {
            assert!(repository.push(project_key_pair, envelope).await.is_ok());
        }

        // Flush all envelopes to disk
        assert!(repository.flush().await);

        // Check that all envelopes are now on disk
        assert_eq!(repository.store_total_count().await, 5);

        // Pop all envelopes (should trigger unspool from disk)
        for envelope in envelopes.iter().rev() {
            let popped_envelope = repository.pop(project_key_pair).await.unwrap().unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap()
            );
        }

        // Ensure the repository is empty
        assert!(repository.pop(project_key_pair).await.unwrap().is_none());
        assert_eq!(repository.store_total_count().await, 0);
    }

    #[tokio::test]
    async fn test_multiple_project_key_pairs() {
        let mut repository = setup_repository(true, 2, 1000).await;
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
            assert!(repository.push(project_key_pair1, envelope).await.is_ok());
        }
        for envelope in envelopes2.clone() {
            assert!(repository.push(project_key_pair2, envelope).await.is_ok());
        }

        // Pop envelopes for project_key_pair1
        for envelope in envelopes1.iter().rev() {
            let popped_envelope = repository.pop(project_key_pair1).await.unwrap().unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap()
            );
        }

        // Pop envelopes for project_key_pair2
        for envelope in envelopes2.iter().rev() {
            let popped_envelope = repository.pop(project_key_pair2).await.unwrap().unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap()
            );
        }

        // Ensure both project key pairs are empty
        assert!(repository.pop(project_key_pair1).await.unwrap().is_none());
        assert!(repository.pop(project_key_pair2).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_check_disk() {
        let mut repository = setup_repository(true, 2, 0).await;
        let project_key_pair1 = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b28ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        );
        let project_key_pair2 = ProjectKeyPair::new(
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("c67ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        );

        // Push 3 envelopes for project_key_pair1 (should trigger spooling)
        let envelopes1 = mock_envelopes_for_project(3, project_key_pair1.sampling_key);
        for envelope in envelopes1 {
            assert!(repository.push(project_key_pair1, envelope).await.is_ok());
        }

        // Since we spool, we expect to be able to check disk for project_key_pair1
        for (&project_key_pair, envelope_stack) in repository.envelope_stacks.iter() {
            assert_eq!(
                envelope_stack.check_disk,
                project_key_pair == project_key_pair1
            );
        }

        // Pop all envelopes for project_key_pair1
        while repository.pop(project_key_pair1).await.unwrap().is_some() {}
        assert_eq!(repository.store_total_count().await, 0);

        // Push 1 envelope for project_key_pair2 (should not trigger spooling)
        let envelope = mock_envelope(Instant::now(), Some(project_key_pair2.sampling_key));
        assert!(repository.push(project_key_pair2, envelope).await.is_ok());

        // Flush remaining envelopes to disk
        assert!(repository.flush().await);

        // After flushing, we expect to be able to check disk for project_key_pair2
        for (&project_key_pair, envelope_stack) in repository.envelope_stacks.iter() {
            assert_eq!(
                envelope_stack.check_disk,
                project_key_pair == project_key_pair2
            );
        }

        // Pop all envelopes for project_key_pair1
        while repository.pop(project_key_pair2).await.unwrap().is_some() {}
        assert_eq!(repository.store_total_count().await, 0);
    }
}
