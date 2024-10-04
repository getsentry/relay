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
    max_batches: usize,
    max_disk_size: usize,
}

impl SqliteEnvelopeProvider {
    /// Creates a new `SQLiteEnvelopeProvider` instance.
    ///
    /// Initializes the provider with the given configuration and envelope store.
    pub async fn new(config: &Config) -> Result<Self, SqliteEnvelopeProviderError> {
        let envelope_store = SqliteEnvelopeStore::prepare(config).await?;
        Ok(Self {
            envelopes: HashMap::new(),
            envelope_store,
            buffered_envelopes_size: 0,
            disk_empty: BTreeSet::new(),
            disk_batch_size: config.spool_envelopes_stack_disk_batch_size(),
            max_batches: config.spool_envelopes_stack_max_batches(),
            max_disk_size: config.spool_envelopes_max_disk_size(),
        })
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

        Ok(())
    }

    /// Peeks at the next envelope for the given project key pair without removing it.
    ///
    /// If the unspool threshold is reached, it may trigger unspooling from disk.
    pub async fn peek(
        &mut self,
        project_key_pair: ProjectKeyPair,
    ) -> Result<Option<&Envelope>, SqliteEnvelopeProviderError> {
        if self.below_unspool_threshold() {
            self.unspool_from_disk(project_key_pair).await?;
        }

        Ok(self
            .envelopes
            .get(&project_key_pair)
            .and_then(|envelopes| envelopes.last().map(|boxed| boxed.as_ref())))
    }

    /// Pops and returns the next envelope for the given project key pair.
    ///
    /// If the unspool threshold is reached, it may trigger unspooling from disk.
    pub async fn pop(
        &mut self,
        project_key_pair: ProjectKeyPair,
    ) -> Result<Option<Box<Envelope>>, SqliteEnvelopeProviderError> {
        if self.below_unspool_threshold() {
            self.unspool_from_disk(project_key_pair).await?;
        }

        Ok(self
            .envelopes
            .get_mut(&project_key_pair)
            .and_then(|envelopes| envelopes.pop()))
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

    /// Determines if the number of buffered envelopes is above the spool threshold.
    fn above_spool_threshold(&self) -> bool {
        self.buffered_envelopes_size >= (self.disk_batch_size * self.max_batches) as u64
    }

    /// Determines if the number of buffered envelopes is below the unspool threshold.
    fn below_unspool_threshold(&self) -> bool {
        self.buffered_envelopes_size == 0
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
        let mut remaining = self.disk_batch_size;

        let group_count = self.envelopes.len();
        if group_count == 0 {
            return Ok(());
        }

        let per_group = remaining / group_count;
        if per_group == 0 {
            return Ok(());
        }

        // We take envelopes from each project key pair and keep track of which pairs have been
        // written to disk, to update the disk empty state accordingly.
        let mut written_project_key_pairs = BTreeSet::new();
        for (&project_key_pair, envelopes) in self.envelopes.iter_mut() {
            let to_take = std::cmp::min(per_group, envelopes.len());
            envelopes_to_spool.extend(envelopes.drain(..to_take));
            remaining -= to_take;

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

    /// Unspools from disk up to `disk_batch_size` envelopes and appends them to the `buffer`.
    ///
    /// In case a single deletion fails, the affected envelope will not be unspooled and unspooling
    /// will continue with the remaining envelopes.
    ///
    /// In case an envelope fails deserialization due to malformed data in the database, the affected
    /// envelope will not be unspooled and unspooling will continue with the remaining envelopes.
    async fn unspool_from_disk(
        &mut self,
        project_key_pair: ProjectKeyPair,
    ) -> Result<(), SqliteEnvelopeProviderError> {
        let mut envelopes = relay_statsd::metric!(timer(RelayTimers::BufferUnspool), {
            self.envelope_store
                .delete_many(
                    project_key_pair.own_key,
                    project_key_pair.sampling_key,
                    self.disk_batch_size as i64,
                )
                .await
                .map_err(SqliteEnvelopeProviderError::EnvelopeStoreError)?
        });

        if envelopes.is_empty() {
            // In case no envelopes were unspooled, we will mark this pair as having no data on disk.
            self.disk_empty.insert(project_key_pair);

            return Ok(());
        }

        relay_statsd::metric!(
            counter(RelayCounters::BufferUnspooledEnvelopes) += envelopes.len() as u64
        );

        // We push in the back of the buffer, since we still want to give priority to
        // incoming envelopes that have a more recent timestamp.
        self.buffered_envelopes_size += envelopes.len() as u64;
        self.envelopes
            .entry(project_key_pair)
            .or_default()
            .append(&mut envelopes);

        Ok(())
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
