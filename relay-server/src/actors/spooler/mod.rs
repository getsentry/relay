//! This module contains the [`BufferService`], which is responsible for spooling of the incoming
//! envelopes, either to in-memory or to persistent storage.
//!
//! The main entry point for the [`BufferService`] is the [`Buffer`] interface, which currently
//! supports:
//! - [`Enqueue`] - enqueueing a message into the backend storage
//! - [`DequeueMany`] - dequeueing all the requested [`QueueKey`] keys
//! - [`RemoveMany`] - removing and dropping the requested [`QueueKey`] keys.
//! - [`Health`] - checking the health of the [`BufferService`]
//!
//! To make sure the [`BufferService`] is fast the responsive, especially in the normal working
//! conditions, it keeps the internal [`BufferState`] state, which defines where the spooling will
//! be happening.
//!
//! The initial state is always [`InMemory`], and if the Relay can properly fetch all the
//! [`crate::actors::project::ProjectState`] it continues to use the memory as temporary spool.
//!
//! Keeping the envelopes in memory as long as we can, we ensure the fast unspool operations and
//! fast processing times.
//!
//! In case of an incident when the in-memory spool gets full (see, `spool.envelopes.max_memory_size` config option)
//! or if the processing pipeline gets too many messages in-flight, configured by
//! `cache.envelope_buffer_size` and once it reaches 80% of the defined amount, the internal state will be
//! switched to [`OnDisk`] and service will continue spooling all the incoming envelopes onto the disk.
//! This will happen also only when the disk spool is configured.
//!
//! The state can be changed to [`InMemory`] again only if all the on-disk spooled envelopes are
//! read out again and the disk is empty.
//!
//! Current on-disk spool implementation uses SQLite as a storage.

static NUMBER_OF_ENVELOPES_PER_KEY: usize = 1000;

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_system::{Addr, Controller, FromMessage, Interface, Sender, Service};
use sqlx::migrate::MigrateError;
use sqlx::sqlite::{
    SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow,
    SqliteSynchronous,
};
use sqlx::{Pool, Row, Sqlite};
use tokio::fs::DirBuilder;
use tokio::sync::mpsc;

use crate::actors::outcome::TrackOutcome;
use crate::actors::project_cache::{ProjectCache, UpdateBufferIndex};
use crate::actors::test_store::TestStore;
use crate::envelope::{Envelope, EnvelopeError};
use crate::extractors::StartTime;
use crate::statsd::{RelayCounters, RelayGauges, RelayHistograms};
use crate::utils::{BufferGuard, ManagedEnvelope};

mod sql;

/// The set of errors which can happend while working the the buffer.
#[derive(Debug, thiserror::Error)]
pub enum BufferError {
    #[error("failed to move envelope from disk to memory")]
    CapacityExceeded(#[from] crate::utils::BufferError),

    #[error("failed to get the size of the buffer on the filesystem")]
    DatabaseFileError(#[from] std::io::Error),

    #[error("failed to insert data into database: {0}")]
    InsertFailed(sqlx::Error),

    #[error("failed to delete data from the database: {0}")]
    DeleteFailed(sqlx::Error),

    #[error("failed to fetch data from the database: {0}")]
    FetchFailed(sqlx::Error),

    #[error("failed to get database file size: {0}")]
    FileSizeReadFailed(sqlx::Error),

    #[error("failed to setup the database: {0}")]
    SqlxSetupFailed(sqlx::Error),

    #[error("failed to create the spool file: {0}")]
    FileSetupError(std::io::Error),

    #[error(transparent)]
    EnvelopeError(#[from] EnvelopeError),

    #[error("failed to run migrations")]
    MigrationFailed(#[from] MigrateError),

    #[error("on-disk spool is full")]
    SpoolIsFull,
}

/// This key represents the index element in the queue.
///
/// It consists from two parts, the own key of the project and the sampling key which points to the
/// sampling project. The sampling key can be the same as the own key if the own and sampling
/// projects are the same.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueueKey {
    pub own_key: ProjectKey,
    pub sampling_key: ProjectKey,
}

impl QueueKey {
    pub fn new(own_key: ProjectKey, sampling_key: ProjectKey) -> Self {
        Self {
            own_key,
            sampling_key,
        }
    }
}

/// Adds the envelope and the managed envelope to the internal buffer.
#[derive(Debug)]
pub struct Enqueue {
    key: QueueKey,
    value: ManagedEnvelope,
}

impl Enqueue {
    pub fn new(key: QueueKey, value: ManagedEnvelope) -> Self {
        Self { key, value }
    }
}

/// Removes messages from the internal buffer and streams them to the sender.
#[derive(Debug)]
pub struct DequeueMany {
    project_key: ProjectKey,
    keys: Vec<QueueKey>,
    sender: mpsc::UnboundedSender<ManagedEnvelope>,
}

impl DequeueMany {
    pub fn new(
        project_key: ProjectKey,
        keys: Vec<QueueKey>,
        sender: mpsc::UnboundedSender<ManagedEnvelope>,
    ) -> Self {
        Self {
            project_key,
            keys,
            sender,
        }
    }
}

/// Removes the provided keys from the internal buffer.
///
/// If any of the provided keys are still have the envelopes, the error will be logged with the
/// number of envelopes dropped for the specific project key.
#[derive(Debug)]
pub struct RemoveMany {
    project_key: ProjectKey,
    keys: BTreeSet<QueueKey>,
}

impl RemoveMany {
    pub fn new(project_key: ProjectKey, keys: BTreeSet<QueueKey>) -> Self {
        Self { project_key, keys }
    }
}

/// Checks the health of the spooler.
#[derive(Debug)]
pub struct Health(pub Sender<bool>);

/// The interface for [`BufferService`].
///
/// Buffer maintaince internal storage (internal buffer) of the envelopes, which keep accumulating
/// till the request to dequeue them again comes in.
///
/// The envelopes first will be kept in memory buffer and it that hits the configured limit the
/// envelopes will be buffer to the disk.
///
/// To add the envelopes to the buffer use [`Enqueue`] which will persists the envelope in the
/// internal storage. To retrie the envelopes one can use [`DequeueMany`], where one expected
/// provide the list of [`QueueKey`]s and the [`mpsc::UnboundedSender`] - all the found envelopes
/// will be streamed back to this sender.
///
/// There is also a [`RemoveMany`] operation, which, when requested, removes the found keys from
/// the queue and drop them. If the any of the keys still have envelopes, the error will be logged.
#[derive(Debug)]
pub enum Buffer {
    Enqueue(Enqueue),
    DequeueMany(DequeueMany),
    RemoveMany(RemoveMany),
    Health(Health),
}

impl Interface for Buffer {}

impl FromMessage<Enqueue> for Buffer {
    type Response = relay_system::NoResponse;

    fn from_message(message: Enqueue, _: ()) -> Self {
        Self::Enqueue(message)
    }
}

impl FromMessage<DequeueMany> for Buffer {
    type Response = relay_system::NoResponse;

    fn from_message(message: DequeueMany, _: ()) -> Self {
        Self::DequeueMany(message)
    }
}

impl FromMessage<RemoveMany> for Buffer {
    type Response = relay_system::NoResponse;

    fn from_message(message: RemoveMany, _: ()) -> Self {
        Self::RemoveMany(message)
    }
}

impl FromMessage<Health> for Buffer {
    type Response = relay_system::NoResponse;

    fn from_message(message: Health, _: ()) -> Self {
        Self::Health(message)
    }
}

/// The configuration which describes the in-memory [`BufferState`].
#[derive(Debug)]
struct InMemory {
    buffer: BTreeMap<QueueKey, Vec<ManagedEnvelope>>,
    max_memory_size: usize,
    used_memory: usize,
    envelope_count: usize,
}

impl InMemory {
    /// Create a new [`InMemory`] state.
    fn new(max_memory_size: usize) -> Self {
        Self {
            max_memory_size,
            buffer: BTreeMap::new(),
            used_memory: 0,
            envelope_count: 0,
        }
    }

    /// Returns the number of envelopes in the memory buffer.
    fn count(&self) -> usize {
        self.buffer.values().map(|v| v.len()).sum()
    }

    /// Runs the check for in-memory buffer and emits the metrics and/or errors.
    fn check(&self) {
        let count = self.buffer.keys().len();
        relay_statsd::metric!(gauge(RelayGauges::BufferProjectsMemoryCount) = count as u64);

        // Go over the buffered envelopes and check if any of the keys exceeds the threshold.
        for (key, values) in &self.buffer {
            let number_of_envelopes = values.len();
            if number_of_envelopes > NUMBER_OF_ENVELOPES_PER_KEY {
                relay_log::error!(
                    tags.own_key = %key.own_key,
                    tags.sampling_key = %key.sampling_key,
                    count = number_of_envelopes,
                    "Number of envelopes per key in memory exceeds the threshold"
                );
            }
        }
    }

    /// Removes envelopes from the in-memory buffer.
    fn remove(&mut self, keys: &BTreeSet<QueueKey>) -> usize {
        let mut count = 0;
        for key in keys {
            let (current_count, current_size) = self.buffer.remove(key).map_or((0, 0), |k| {
                (k.len(), k.into_iter().map(|k| k.estimated_size()).sum())
            });
            count += current_count;
            self.used_memory -= current_size;
        }
        self.envelope_count = self.envelope_count.saturating_sub(count);
        relay_statsd::metric!(
            histogram(RelayHistograms::BufferEnvelopesMemoryBytes) = self.used_memory as f64
        );
        relay_statsd::metric!(
            gauge(RelayGauges::BufferEnvelopesMemoryCount) = self.envelope_count as u64
        );

        count
    }

    /// Dequeues the envelopes from the in-memory buffer and send them to provided `sender`.
    fn dequeue(&mut self, keys: &Vec<QueueKey>, sender: mpsc::UnboundedSender<ManagedEnvelope>) {
        for key in keys {
            for envelope in self.buffer.remove(key).unwrap_or_default() {
                self.used_memory -= envelope.estimated_size();
                self.envelope_count = self.envelope_count.saturating_sub(1);
                sender.send(envelope).ok();
            }
        }
        relay_statsd::metric!(
            histogram(RelayHistograms::BufferEnvelopesMemoryBytes) = self.used_memory as f64
        );
        relay_statsd::metric!(
            gauge(RelayGauges::BufferEnvelopesMemoryCount) = self.envelope_count as u64
        );
    }

    /// Enqueues the envelope into the in-memory buffer.
    fn enqueue(&mut self, key: QueueKey, managed_envelope: ManagedEnvelope) {
        self.envelope_count += 1;
        self.used_memory += managed_envelope.estimated_size();
        self.buffer.entry(key).or_default().push(managed_envelope);
        relay_statsd::metric!(
            histogram(RelayHistograms::BufferEnvelopesMemoryBytes) = self.used_memory as f64
        );
        relay_statsd::metric!(
            gauge(RelayGauges::BufferEnvelopesMemoryCount) = self.envelope_count as u64
        );
    }

    /// Returns `true` if the in-memory buffer is full, `false` otherwise.
    fn is_full(&self) -> bool {
        self.used_memory >= self.max_memory_size
    }
}

/// The configuration which describes the on-disk [`BufferState`].
#[derive(Debug)]
struct OnDisk {
    dequeue_attempts: usize,
    db: Pool<Sqlite>,
    buffer_guard: Arc<BufferGuard>,
    max_disk_size: usize,
    /// The number of items currently on disk.
    ///
    /// We do not track the count when we encounter envelopes in the database on startup,
    /// because counting those envelopes would risk locking the db for multiple seconds.
    count: Option<u64>,
}

impl OnDisk {
    /// Saves the provided buffer to the disk.
    ///
    /// Returns an error if the spooling failed, and the number of spooled envelopes on success.
    async fn spool(
        &mut self,
        buffer: BTreeMap<QueueKey, Vec<ManagedEnvelope>>,
    ) -> Result<(), BufferError> {
        relay_statsd::metric!(histogram(RelayHistograms::BufferEnvelopesMemoryBytes) = 0);
        let envelopes = buffer
            .into_iter()
            .flat_map(|(key, values)| {
                values
                    .into_iter()
                    .map(move |value| (key, value.received_at().timestamp_millis(), value))
            })
            .filter_map(
                |(key, received_at, managed)| match managed.into_envelope().to_vec() {
                    Ok(vec) => Some((key, vec, received_at)),
                    Err(err) => {
                        relay_log::error!(
                            error = &err as &dyn Error,
                            "failed to serialize the envelope"
                        );
                        None
                    }
                },
            );

        let inserted = sql::do_insert(stream::iter(envelopes), &self.db)
            .await
            .map_err(BufferError::InsertFailed)?;

        self.track_count(inserted as i64);

        Ok(())
    }

    /// Removes the envelopes from the on-disk spool.
    ///
    /// Returns the count of removed envelopes.
    async fn remove(&mut self, keys: &BTreeSet<QueueKey>) -> Result<usize, BufferError> {
        let mut count = 0;
        for key in keys {
            let result = sql::delete(*key)
                .execute(&self.db)
                .await
                .map_err(BufferError::DeleteFailed)?;
            count += result.rows_affected();
        }

        self.track_count(-(count as i64));

        Ok(count as usize)
    }

    /// Extracts the envelope from the `SqliteRow`.
    ///
    /// Reads the bytes and tries to perse them into `Envelope`.
    fn extract_envelope(
        &self,
        row: SqliteRow,
        services: &Services,
    ) -> Result<ManagedEnvelope, BufferError> {
        let envelope_row: Vec<u8> = row.try_get("envelope").map_err(BufferError::FetchFailed)?;
        let envelope_bytes = bytes::Bytes::from(envelope_row);
        let mut envelope = Envelope::parse_bytes(envelope_bytes)?;

        let received_at: i64 = row
            .try_get("received_at")
            .map_err(BufferError::FetchFailed)?;
        let start_time = StartTime::from_timestamp_millis(received_at as u64);

        envelope.set_start_time(start_time.into_inner());

        let managed_envelope = self.buffer_guard.enter(
            envelope,
            services.outcome_aggregator.clone(),
            services.test_store.clone(),
        )?;
        Ok(managed_envelope)
    }

    /// Tries to delete the envelopes from the persistent buffer in batches,
    /// extract and convert them to managed envelopes and send back into
    /// processing pipeline.
    ///
    /// If the error happens in the deletion/fetching phase, a key is returned
    /// to allow retrying later.
    ///
    /// Returns the amount of envelopes deleted from disk.
    async fn delete_and_fetch(
        &mut self,
        key: QueueKey,
        sender: &mpsc::UnboundedSender<ManagedEnvelope>,
        services: &Services,
    ) -> Result<(), QueueKey> {
        loop {
            // Before querying the db, make sure that the buffer guard has enough availability:
            self.dequeue_attempts += 1;
            if !self.buffer_guard.is_below_low_watermark() {
                return Err(key);
            }
            relay_statsd::metric!(
                histogram(RelayHistograms::BufferDequeueAttempts) = self.dequeue_attempts as u64
            );
            self.dequeue_attempts = 0;

            // Removing envelopes from the on-disk buffer in batches has following implications:
            // 1. It is faster to delete from the DB in batches.
            // 2. Make sure that if we panic and deleted envelopes cannot be read out fully, we do not lose all of them,
            // but only one batch, and the rest of them will stay on disk for the next iteration
            // to pick up.
            //
            // Right now we use 100 for batch size.
            let batch_size = 100;
            let mut envelopes = sql::delete_and_fetch(key, batch_size)
                .fetch(&self.db)
                .peekable();
            relay_statsd::metric!(counter(RelayCounters::BufferReads) += 1);

            // Stream is empty, we can break the loop, since we read everything by now.
            if Pin::new(&mut envelopes).peek().await.is_none() {
                return Ok(());
            }

            let mut count: i64 = 0;
            while let Some(envelope) = envelopes.next().await {
                count += 1;
                let envelope = match envelope {
                    Ok(envelope) => envelope,

                    // Bail if there are errors in the stream.
                    Err(err) => {
                        relay_log::error!(
                            error = &err as &dyn Error,
                            "failed to read the buffer stream from the disk",
                        );
                        self.track_count(-count);
                        return Err(key);
                    }
                };

                match self.extract_envelope(envelope, services) {
                    Ok(managed_envelope) => {
                        sender.send(managed_envelope).ok();
                    }
                    Err(err) => relay_log::error!(
                        error = &err as &dyn Error,
                        "failed to extract envelope from the buffer",
                    ),
                }
            }

            self.track_count(-count);
        }
    }

    /// Dequeues the envelopes from the on-disk spool and send them to the provided `sender`.
    ///
    /// The keys for which the envelopes could not be fetched, send back to `ProjectCache` to merge
    /// back into index.
    async fn dequeue(
        &mut self,
        project_key: ProjectKey,
        keys: &mut Vec<QueueKey>,
        sender: mpsc::UnboundedSender<ManagedEnvelope>,
        services: &Services,
    ) {
        let mut unused_keys = BTreeSet::new();
        while let Some(key) = keys.pop() {
            // If the error with a key is returned we must save it for the next iterration.
            if let Err(key) = self.delete_and_fetch(key, &sender, services).await {
                unused_keys.insert(key);
            };
        }
        if !unused_keys.is_empty() {
            services
                .project_cache
                .send(UpdateBufferIndex::new(project_key, unused_keys))
        }
    }

    /// Estimates the db size by multiplying `page_count * page_size`.
    async fn estimate_spool_size(&self) -> Result<i64, BufferError> {
        let size: i64 = sql::current_size()
            .fetch_one(&self.db)
            .await
            .and_then(|r| r.try_get(0))
            .map_err(BufferError::FileSizeReadFailed)?;

        relay_statsd::metric!(histogram(RelayHistograms::BufferDiskSize) = size as u64);
        Ok(size)
    }

    /// Returns `true` if the maximum size is reached, `false` otherwise.
    async fn is_full(&self) -> Result<bool, BufferError> {
        let current_size = self.estimate_spool_size().await?;
        Ok(current_size as usize >= self.max_disk_size)
    }

    /// Returns `true` if the spool is empty, `false` otherwise.
    async fn is_empty(&self) -> Result<bool, BufferError> {
        let is_empty = sql::select_one()
            .fetch_optional(&self.db)
            .await
            .map_err(BufferError::FetchFailed)?
            .is_none();

        Ok(is_empty)
    }

    /// Enqueues data into on-disk spool.
    async fn enqueue(
        &mut self,
        key: QueueKey,
        managed_envelope: ManagedEnvelope,
    ) -> Result<(), BufferError> {
        let received_at = managed_envelope.received_at().timestamp_millis();
        sql::insert(
            key,
            managed_envelope.into_envelope().to_vec().unwrap(),
            received_at,
        )
        .execute(&self.db)
        .await
        .map_err(BufferError::InsertFailed)?;

        self.track_count(1);
        relay_statsd::metric!(counter(RelayCounters::BufferWrites) += 1);
        Ok(())
    }

    fn track_count(&mut self, increment: i64) {
        // Track the number of envelopes read/written:
        let metric = if increment < 0 {
            RelayCounters::BufferEnvelopesRead
        } else {
            RelayCounters::BufferEnvelopesWritten
        };
        relay_statsd::metric!(counter(metric) += increment);

        if let Some(count) = &mut self.count {
            *count = count.saturating_add_signed(increment);
            relay_statsd::metric!(gauge(RelayGauges::BufferEnvelopesDiskCount) = *count);
        }
    }
}

/// The state which defines the [`BufferService`] behaviour.
#[derive(Debug)]
enum BufferState {
    /// Only memory buffer is enabled.
    Memory(InMemory),

    /// The memory buffer is used, but also disk spool is configured.
    ///
    /// The disk will be used when the memory limit will be hit.
    MemoryFileStandby { ram: InMemory, disk: OnDisk },

    /// Only disk used for read/write operations.
    Disk(OnDisk),
}

impl BufferState {
    /// Creates the initial [`BufferState`] depending on the provided config.
    ///
    /// If the on-disk spool is configured, the `BufferState::MemoryDiskStandby` will be returned:
    /// if the spool is empty, `BufferState::MemoryFileRead` if the spool contains some data,
    /// `BufferState::Memory` otherwise.
    async fn new(max_memory_size: usize, disk: Option<OnDisk>) -> Self {
        let ram = InMemory {
            buffer: BTreeMap::new(),
            max_memory_size,
            used_memory: 0,
            envelope_count: 0,
        };
        match disk {
            Some(disk) => {
                // When the old db file is picked up and it is not empty, we can also try to read from it
                // on dequeue requests.
                if disk.is_empty().await.unwrap_or_default() {
                    Self::MemoryFileStandby { ram, disk }
                } else {
                    BufferState::Disk(disk)
                }
            }
            None => Self::Memory(ram),
        }
    }

    /// Becomes a different state, depdending on the current state and the current conditions of
    /// underlying spool.
    async fn transition(self, config: &Arc<Config>) -> Self {
        match self {
            Self::MemoryFileStandby { ram, mut disk }
                if ram.is_full() || disk.buffer_guard.is_over_high_watermark() =>
            {
                if let Err(err) = disk.spool(ram.buffer).await {
                    relay_log::error!(
                        error = &err as &dyn Error,
                        "failed to spool the in-memory buffer to disk",
                    );
                }
                Self::Disk(disk)
            }
            Self::Disk(disk) if disk.is_empty().await.unwrap_or_default() => {
                Self::MemoryFileStandby {
                    ram: InMemory::new(config.spool_envelopes_max_memory_size()),
                    disk,
                }
            }
            Self::Memory(_) | Self::MemoryFileStandby { .. } | Self::Disk(_) => self,
        }
    }
}

impl Default for BufferState {
    fn default() -> Self {
        // Just use the all the memory we can now.
        Self::Memory(InMemory::new(usize::MAX))
    }
}

/// The services, which rely on the state of the envelopes in this buffer.
#[derive(Clone, Debug)]
pub struct Services {
    pub outcome_aggregator: Addr<TrackOutcome>,
    pub project_cache: Addr<ProjectCache>,
    pub test_store: Addr<TestStore>,
}

/// [`Buffer`] interface implementation backed by SQLite.
#[derive(Debug)]
pub struct BufferService {
    services: Services,
    state: BufferState,
    config: Arc<Config>,
}

impl BufferService {
    /// Set up the database and return the current number of envelopes.
    ///
    /// The directories and spool file will be created if they don't already
    /// exist.
    async fn setup(path: &Path) -> Result<(), BufferError> {
        BufferService::create_spool_directory(path).await?;

        let options = SqliteConnectOptions::new()
            .filename(path)
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true);

        let db = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .map_err(BufferError::SqlxSetupFailed)?;

        sqlx::migrate!("../migrations").run(&db).await?;
        Ok(())
    }

    /// Creates the directories for the spool file.
    async fn create_spool_directory(path: &Path) -> Result<(), BufferError> {
        let Some(parent) = path.parent() else {
            return Ok(());
        };
        if !parent.as_os_str().is_empty() && !parent.exists() {
            relay_log::debug!("creating directory for spooling file: {}", parent.display());
            DirBuilder::new()
                .recursive(true)
                .create(&parent)
                .await
                .map_err(BufferError::FileSetupError)?;
        }
        Ok(())
    }

    /// Prepares the disk state.
    async fn prepare_disk_state(
        config: Arc<Config>,
        buffer_guard: Arc<BufferGuard>,
    ) -> Result<Option<OnDisk>, BufferError> {
        // Only if persistent envelopes buffer file path provided, we create the pool and set the config.
        let Some(path) = config.spool_envelopes_path() else {
            return Ok(None);
        };

        relay_log::info!("buffer file {}", path.to_string_lossy());
        relay_log::info!(
            "max memory size {}",
            config.spool_envelopes_max_memory_size()
        );
        relay_log::info!("max disk size {}", config.spool_envelopes_max_disk_size());

        Self::setup(&path).await?;

        let options = SqliteConnectOptions::new()
            .filename(&path)
            // The WAL journaling mode uses a write-ahead log instead of a rollback journal to implement transactions.
            // The WAL journaling mode is persistent; after being set it stays in effect
            // across multiple database connections and after closing and reopening the database.
            //
            // 1. WAL is significantly faster in most scenarios.
            // 2. WAL provides more concurrency as readers do not block writers and a writer does not block readers. Reading and writing can proceed concurrently.
            // 3. Disk I/O operations tends to be more sequential using WAL.
            // 4. WAL uses many fewer fsync() operations and is thus less vulnerable to problems on systems where the fsync() system call is broken.
            .journal_mode(SqliteJournalMode::Wal)
            // WAL mode is safe from corruption with synchronous=NORMAL.
            // When synchronous is NORMAL, the SQLite database engine will still sync at the most critical moments, but less often than in FULL mode.
            // Which guarantees good balance between safety and speed.
            .synchronous(SqliteSynchronous::Normal)
            // The freelist pages are moved to the end of the database file and the database file is truncated to remove the freelist pages at every
            // transaction commit. Note, however, that auto-vacuum only truncates the freelist pages from the file.
            // Auto-vacuum does not defragment the database nor repack individual database pages the way that the VACUUM command does.
            //
            // This will helps us to keep the file size under some control.
            .auto_vacuum(SqliteAutoVacuum::Full)
            // If shared-cache mode is enabled and a thread establishes multiple
            // connections to the same database, the connections share a single data and schema cache.
            // This can significantly reduce the quantity of memory and IO required by the system.
            .shared_cache(true);

        let db = SqlitePoolOptions::new()
            .max_connections(config.spool_envelopes_max_connections())
            .min_connections(config.spool_envelopes_min_connections())
            .connect_with(options)
            .await
            .map_err(BufferError::SqlxSetupFailed)?;

        let mut on_disk = OnDisk {
            dequeue_attempts: 0,
            db,
            buffer_guard,
            max_disk_size: config.spool_envelopes_max_disk_size(),
            count: None,
        };

        if on_disk.is_empty().await? {
            // Only start live-tracking the count if there's nothing in the db yet.
            on_disk.count = Some(0);
        }

        Ok(Some(on_disk))
    }

    /// Creates a new [`BufferService`] from the provided path to the SQLite database file.
    pub async fn create(
        buffer_guard: Arc<BufferGuard>,
        services: Services,
        config: Arc<Config>,
    ) -> Result<Self, BufferError> {
        let on_disk_state = Self::prepare_disk_state(config.clone(), buffer_guard).await?;
        let state = BufferState::new(config.spool_envelopes_max_memory_size(), on_disk_state).await;
        Ok(Self {
            services,
            state,
            config,
        })
    }

    /// Handles the enqueueing messages into the internal buffer.
    async fn handle_enqueue(&mut self, message: Enqueue) -> Result<(), BufferError> {
        let Enqueue {
            key,
            value: managed_envelope,
        } = message;

        match self.state {
            BufferState::Memory(ref mut ram)
            | BufferState::MemoryFileStandby { ref mut ram, .. } => {
                ram.enqueue(key, managed_envelope);
            }
            BufferState::Disk(ref mut disk) => {
                // The disk is full, drop the incoming envelopes.
                if disk.is_full().await? {
                    return Err(BufferError::SpoolIsFull);
                }
                disk.enqueue(key, managed_envelope).await?;
            }
        }

        let state = std::mem::take(&mut self.state);
        self.state = state.transition(&self.config).await;
        Ok(())
    }

    /// Handles the dequeueing messages from the internal buffer.
    ///
    /// This method removes the envelopes from the buffer and stream them to the sender.
    async fn handle_dequeue(&mut self, message: DequeueMany) -> Result<(), BufferError> {
        let DequeueMany {
            project_key,
            mut keys,
            sender,
        } = message;

        match self.state {
            BufferState::Memory(ref mut ram)
            | BufferState::MemoryFileStandby { ref mut ram, .. } => {
                ram.dequeue(&keys, sender);
            }
            BufferState::Disk(ref mut disk) => {
                disk.dequeue(project_key, &mut keys, sender, &self.services)
                    .await;
            }
        }
        let state = std::mem::take(&mut self.state);
        self.state = state.transition(&self.config).await;

        Ok(())
    }

    /// Handles the remove request.
    ///
    /// This removes all the envelopes from the internal buffer for the provided keys.
    /// If any of the provided keys still have the envelopes, the error will be logged with the
    /// number of envelopes dropped for the specific project key.
    async fn handle_remove(&mut self, message: RemoveMany) -> Result<(), BufferError> {
        let RemoveMany { project_key, keys } = message;
        let mut count: usize = 0;

        match self.state {
            BufferState::Memory(ref mut ram)
            | BufferState::MemoryFileStandby { ref mut ram, .. } => {
                count += ram.remove(&keys);
            }
            BufferState::Disk(ref mut disk) => {
                count += disk.remove(&keys).await?;
            }
        }

        let state = std::mem::take(&mut self.state);
        self.state = state.transition(&self.config).await;

        if count > 0 {
            relay_log::with_scope(
                |scope| scope.set_tag("project_key", project_key),
                || relay_log::error!(count, "evicted project with envelopes"),
            );
        }

        Ok(())
    }

    async fn handle_health(&mut self, health: Health) -> Result<(), BufferError> {
        match self.state {
            // We do not check the ram, since we rely on `cache.envelope_buffer_size` to limit the
            // memory usage and the number of the envelopes in the memory.
            // Note: in the future we want to switch to `spool.envelopes.max_memory_size` option.
            BufferState::Memory(_) | BufferState::MemoryFileStandby { .. } => health.0.send(true),
            BufferState::Disk(ref disk) => health.0.send(!disk.is_full().await.unwrap_or_default()),
        }

        Ok(())
    }

    /// Handles all the incoming messages from the [`Buffer`] interface.
    async fn handle_message(&mut self, message: Buffer) -> Result<(), BufferError> {
        match message {
            Buffer::Enqueue(message) => self.handle_enqueue(message).await,
            Buffer::DequeueMany(message) => self.handle_dequeue(message).await,
            Buffer::RemoveMany(message) => self.handle_remove(message).await,
            Buffer::Health(message) => self.handle_health(message).await,
        }
    }

    /// Handle the shutdown notification.
    ///
    /// Tries to spool to disk if the current buffer state is `BufferState::MemoryDiskStandby`,
    /// which means we use the in-memory buffer active and disk still free or not used before.
    async fn handle_shutdown(&mut self) -> Result<(), BufferError> {
        let BufferState::MemoryFileStandby {
            ref mut ram,
            ref mut disk,
        } = self.state
        else {
            return Ok(());
        };

        let count: usize = ram.count();
        if count == 0 {
            return Ok(());
        }

        relay_log::info!(
            count,
            "received shutdown signal, spooling envelopes to disk"
        );

        let buffer = std::mem::take(&mut ram.buffer);
        disk.spool(buffer).await?;
        Ok(())
    }

    /// Handles the check of the envelopes buffer.
    ///
    /// Note: currently the check runs only on the in-memory buffer to track if there is "hot"
    /// projects keys which are always have too many envelopes in the queue.
    fn handle_check(&mut self) {
        let (BufferState::Memory(ref mut ram) | BufferState::MemoryFileStandby { ref mut ram, .. }) =
            self.state
        else {
            return;
        };

        ram.check();
    }
}

impl Service for BufferService {
    type Interface = Buffer;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let mut shutdown = Controller::shutdown_handle();
            let mut ticker = tokio::time::interval(self.config.spool_envelopes_check_interval());

            loop {
                tokio::select! {
                    biased;

                    _ = ticker.tick() => self.handle_check(),
                    Some(message) = rx.recv() => {
                        if let Err(err) = self.handle_message(message).await {
                            relay_log::error!(
                                error = &err as &dyn Error,
                                "failed to handle an incoming message",
                            );
                        }
                    }
                    _ = shutdown.notified() => {
                       if let Err(err) = self.handle_shutdown().await {
                            relay_log::error!(
                                error = &err as &dyn Error,
                                "failed while shutting down the service",
                            );
                        }
                    }
                    else => break,
                }
            }
        });
    }
}

impl Drop for BufferService {
    fn drop(&mut self) {
        // Count only envelopes from in-memory buffer.
        match &self.state {
            BufferState::Memory(ram) | BufferState::MemoryFileStandby { ram, .. } => {
                let count = ram.count();
                if count > 0 {
                    relay_log::error!("dropped {count} envelopes");
                }
            }
            BufferState::Disk(_) => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use insta::assert_debug_snapshot;
    use relay_test::mock_service;
    use uuid::Uuid;

    use crate::testutils::empty_envelope;

    use super::*;

    fn services() -> Services {
        let (project_cache, _) = mock_service("project_cache", (), |&mut (), _| {});
        let (outcome_aggregator, _) = mock_service("outcome_aggregator", (), |&mut (), _| {});
        let (test_store, _) = mock_service("test_store", (), |&mut (), _| {});

        Services {
            project_cache,
            outcome_aggregator,
            test_store,
        }
    }

    fn empty_managed_envelope() -> ManagedEnvelope {
        let envelope = empty_envelope();
        let Services {
            outcome_aggregator,
            test_store,
            ..
        } = services();
        ManagedEnvelope::untracked(envelope, outcome_aggregator, test_store)
    }

    #[tokio::test]
    async fn create_spool_directory_deep_path() {
        let parent_dir = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let target_dir = parent_dir
            .join("subdir1")
            .join("subdir2")
            .join("spool-file");
        let buffer_guard: Arc<_> = BufferGuard::new(1).into();
        let config: Arc<_> = Config::from_json_value(serde_json::json!({
            "spool": {
                "envelopes": {
                    "path": target_dir,
                }
            }
        }))
        .unwrap()
        .into();
        BufferService::create(buffer_guard, services(), config)
            .await
            .unwrap();
        assert!(target_dir.exists());
    }

    #[tokio::test]
    async fn ensure_start_time_restore() {
        let buffer_guard: Arc<_> = BufferGuard::new(10).into();
        let config: Arc<_> = Config::from_json_value(serde_json::json!({
            "spool": {
                "envelopes": {
                    "path": std::env::temp_dir().join(Uuid::new_v4().to_string()),
                    "max_memory_size": 0, // 0 bytes, to force to spool to disk all the envelopes.
                }
            }
        }))
        .unwrap()
        .into();
        let service = BufferService::create(buffer_guard, services(), config)
            .await
            .unwrap();
        let addr = service.start();
        let (tx, mut rx) = mpsc::unbounded_channel();

        // Test cases:
        let test_cases = [
            // The difference between the `Instant::now` and start_time is 2 seconds,
            // that the start time is restored to the correct point in time.
            (2, "a94ae32be2584e0bbd7a4cbb95971fee"),
            // There is no delay, and the `Instant::now` must be within the same second.
            (0, "aaaae32be2584e0bbd7a4cbb95971fff"),
        ];
        for (result, pub_key) in test_cases {
            let project_key = ProjectKey::parse(pub_key).unwrap();
            let key = QueueKey {
                own_key: project_key,
                sampling_key: project_key,
            };

            addr.send(Enqueue {
                key,
                value: empty_managed_envelope(),
            });

            // How long to wait to dequeue the message from the spool.
            // This will also ensure that the start time will have to be restored to the time
            // when the request first came in.
            tokio::time::sleep(Duration::from_millis(1000 * result)).await;

            addr.send(DequeueMany {
                project_key,
                keys: [key].into(),
                sender: tx.clone(),
            });

            let managed_envelope = rx.recv().await.unwrap();
            let start_time = managed_envelope.envelope().meta().start_time();

            assert_eq!((Instant::now() - start_time).as_secs(), result);
        }
    }

    #[tokio::test]
    async fn dequeue_waits_for_permits() {
        relay_test::setup();
        let num_permits = 3;
        let buffer_guard: Arc<_> = BufferGuard::new(num_permits).into();
        let config: Arc<_> = Config::from_json_value(serde_json::json!({
            "spool": {
                "envelopes": {
                    "path": std::env::temp_dir().join(Uuid::new_v4().to_string()),
                    "max_memory_size": 0, // 0 bytes, to force to spool to disk all the envelopes.
                }
            }
        }))
        .unwrap()
        .into();

        let services = services();

        let service = BufferService::create(buffer_guard.clone(), services.clone(), config)
            .await
            .unwrap();
        let addr = service.start();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let key = QueueKey {
            own_key: project_key,
            sampling_key: project_key,
        };

        // Enqueue an envelope:
        addr.send(Enqueue {
            key,
            value: empty_managed_envelope(),
        });

        // Nothing dequeued yet:
        assert!(rx.try_recv().is_err());

        // Dequeue an envelope:
        addr.send(DequeueMany {
            project_key,
            keys: [key].into(),
            sender: tx.clone(),
        });

        // There are enough permits, so get an envelope:
        let res = rx.recv().await;
        assert!(res.is_some(), "{res:?}");
        assert_eq!(buffer_guard.available(), 2);

        // Simulate a new envelope coming in via a web request:
        let new_envelope = buffer_guard
            .enter(
                empty_envelope(),
                services.outcome_aggregator,
                services.test_store,
            )
            .unwrap();

        assert_eq!(buffer_guard.available(), 1);

        // Enqueue & dequeue another envelope:
        addr.send(Enqueue {
            key,
            value: empty_managed_envelope(),
        });
        // Request to dequeue:
        addr.send(DequeueMany {
            project_key,
            keys: [key].into(),
            sender: tx.clone(),
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // There is one permit left, but we only dequeue if we gave >= 50% capacity:
        assert!(rx.try_recv().is_err());

        // Freeing one permit gives us enough capacity:
        assert_eq!(buffer_guard.available(), 1);
        drop(new_envelope);

        // Dequeue an envelope:
        addr.send(DequeueMany {
            project_key,
            keys: [key].into(),
            sender: tx.clone(),
        });
        assert_eq!(buffer_guard.available(), 2);
        tokio::time::sleep(Duration::from_millis(100)).await; // give time to flush
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn metrics_work() {
        let buffer_guard: Arc<_> = BufferGuard::new(999999).into();
        let config: Arc<_> = Config::from_json_value(serde_json::json!({
            "spool": {
                "envelopes": {
                    "path": std::env::temp_dir().join(Uuid::new_v4().to_string()),
                    "max_memory_size": "4KB",
                    "max_disk_size": "20KB",
                }
            }
        }))
        .unwrap()
        .into();
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let key = QueueKey {
            own_key: project_key,
            sampling_key: project_key,
        };

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let _guard = rt.enter();

        let captures = relay_statsd::with_capturing_test_client(|| {
            rt.block_on(async {
                let mut service = BufferService::create(buffer_guard, services(), config)
                    .await
                    .unwrap();

                // Send 5 envelopes
                for i in 0..5 {
                    let res = service
                        .handle_enqueue(Enqueue {
                            key,
                            value: empty_managed_envelope(),
                        })
                        .await;
                    if i > 2 {
                        assert!(res.is_err());
                    } else {
                        assert!(res.is_ok());
                    }
                }

                // Dequeue everything
                let (tx, mut rx) = mpsc::unbounded_channel();
                service
                    .handle_dequeue(DequeueMany {
                        project_key,
                        keys: [key].into(),
                        sender: tx,
                    })
                    .await
                    .unwrap();

                // Make sure that not only metrics are correct, but also the number of envelopes
                // flushed.
                let mut count = 0;
                while rx.recv().await.is_some() {
                    count += 1;
                }
                assert_eq!(count, 3);
            })
        });

        // Collect only the buffer metrics.
        let captures: Vec<_> = captures
            .into_iter()
            .filter(|name| name.contains("buffer."))
            .collect();

        assert_debug_snapshot!(captures, @r#"
        [
            "buffer.envelopes_mem:2000|h",
            "buffer.envelopes_mem_count:1|g",
            "buffer.envelopes_mem:4000|h",
            "buffer.envelopes_mem_count:2|g",
            "buffer.envelopes_mem:6000|h",
            "buffer.envelopes_mem_count:3|g",
            "buffer.envelopes_mem:0|h",
            "buffer.writes:1|c",
            "buffer.envelopes_written:3|c",
            "buffer.envelopes_disk_count:3|g",
            "buffer.disk_size:24576|h",
            "buffer.disk_size:24576|h",
            "buffer.dequeue_attempts:1|h",
            "buffer.reads:1|c",
            "buffer.envelopes_read:-3|c",
            "buffer.envelopes_disk_count:0|g",
            "buffer.dequeue_attempts:1|h",
            "buffer.reads:1|c",
        ]
        "#);
    }
}
