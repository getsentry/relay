use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use relay_common::ProjectKey;
use relay_config::Config;
use relay_log::LogError;
use relay_system::{Addr, Controller, FromMessage, Interface, Service};
use sqlx::migrate::MigrateError;
use sqlx::sqlite::{
    SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow,
    SqliteSynchronous,
};
use sqlx::{Pool, Row, Sqlite};
use tokio::sync::mpsc;

use crate::actors::outcome::TrackOutcome;
use crate::actors::project_cache::{ProjectCache, UpdateBufferIndex};
use crate::actors::test_store::TestStore;
use crate::envelope::{Envelope, EnvelopeError};
use crate::extractors::StartTime;
use crate::statsd::{RelayCounters, RelayHistograms};
use crate::utils::{BufferGuard, ManagedEnvelope};

mod sql;

/// The set of errors which can happend while working the the buffer.
#[derive(Debug, thiserror::Error)]
pub enum BufferError {
    #[error("failed to move envelope from disk to memory")]
    CapacityExceeded(#[from] crate::utils::BufferError),

    #[error("failed to spool to disk, reached maximum disk size: {0}")]
    DatabaseFull(i64),

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
    SetupFailed(sqlx::Error),

    #[error(transparent)]
    EnvelopeError(#[from] EnvelopeError),

    #[error("failed to run migrations")]
    MigrationFailed(#[from] MigrateError),
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

/// Contains the spool related configuration.
///
/// Contains the current backing spool engine pool (SQLite) and the max sizes for in-memory buffer
/// and on disk spool. All the sized are in bytes.
#[derive(Debug)]
struct BufferSpoolConfig {
    db: Pool<Sqlite>,
    max_disk_size: usize,
    max_memory_size: usize,
    is_disk_full: bool,
}

/// [`Buffer`] interface implementation backed by SQLite.
#[derive(Debug)]
pub struct BufferService {
    buffer: BTreeMap<QueueKey, Vec<ManagedEnvelope>>,
    buffer_guard: Arc<BufferGuard>,
    outcome_aggregator: Addr<TrackOutcome>,
    project_cache: Addr<ProjectCache>,
    test_store: Addr<TestStore>,
    spool_config: Option<BufferSpoolConfig>,
    used_memory: usize,

    #[cfg(test)]
    /// Create untracked envelopes when loading from disk. Only use in tests.
    untracked: bool,
}

impl BufferService {
    /// Set up the database and return the current number of envelopes.
    async fn setup(path: &PathBuf) -> Result<(), BufferError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true);

        let db = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .map_err(BufferError::SetupFailed)?;

        sqlx::migrate!("../migrations").run(&db).await?;
        Ok(())
    }

    /// Creates a new [`BufferService`] from the provided path to the SQLite database file.
    pub async fn create(
        buffer_guard: Arc<BufferGuard>,
        outcome_aggregator: Addr<TrackOutcome>,
        project_cache: Addr<ProjectCache>,
        test_store: Addr<TestStore>,
        config: Arc<Config>,
    ) -> Result<Self, BufferError> {
        let mut service = Self {
            buffer: BTreeMap::new(),
            buffer_guard,
            outcome_aggregator,
            project_cache,
            test_store,
            used_memory: 0,
            spool_config: None,
            #[cfg(test)]
            untracked: false,
        };

        // Only if persistent envelopes buffer file path provided, we create the pool and set the config.
        if let Some(path) = config.spool_envelopes_path() {
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
                .map_err(BufferError::SetupFailed)?;

            let spool_config = BufferSpoolConfig {
                db,
                max_disk_size: config.spool_envelopes_max_disk_size(),
                max_memory_size: config.spool_envelopes_max_memory_size(),
                is_disk_full: false,
            };

            service.spool_config = Some(spool_config);
        }

        Ok(service)
    }

    /// Estimates the db size by multiplying `page_count * page_size`.
    async fn estimate_buffer_size(db: &Pool<Sqlite>) -> Result<i64, BufferError> {
        let size: i64 = sql::current_size()
            .fetch_one(db)
            .await
            .and_then(|r| r.try_get(0))
            .map_err(BufferError::FileSizeReadFailed)?;

        relay_statsd::metric!(histogram(RelayHistograms::BufferDiskSize) = size as u64);
        Ok(size)
    }

    /// Saves the provided buffer to the disk.
    ///
    /// Returns an error if the spooling failed, and the number of spooled envelopes on success.
    async fn do_spool(
        db: &Pool<Sqlite>,
        buffer: BTreeMap<QueueKey, Vec<ManagedEnvelope>>,
    ) -> Result<(), BufferError> {
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
                        relay_log::error!("failed to serialize the envelope: {}", LogError(&err));
                        None
                    }
                },
            );

        sql::do_insert(stream::iter(envelopes), db)
            .await
            .map_err(BufferError::InsertFailed)
    }

    /// Tries to save in-memory buffer to disk.
    ///
    /// It will spool to disk only if the persistent storage enabled in the configuration.
    async fn try_spool(&mut self) -> Result<(), BufferError> {
        let Self {
            ref mut spool_config,
            ref mut buffer,
            ..
        } = self;

        // Buffer to disk only if the DB and config are provided.
        let Some(BufferSpoolConfig {
            db,
            max_disk_size,
            max_memory_size,
            ref mut is_disk_full,
            ..
        }) = spool_config else { return Ok(()) };

        if self.used_memory < *max_memory_size {
            return Ok(());
        }

        // Return 0 if we fail to read the database size.
        let estimated_db_size = Self::estimate_buffer_size(db).await?;

        if estimated_db_size as usize >= *max_disk_size {
            *is_disk_full = true;
            return Err(BufferError::DatabaseFull(estimated_db_size));
        }

        let buf = std::mem::take(buffer);
        self.used_memory = 0;
        relay_statsd::metric!(
            histogram(RelayHistograms::BufferEnvelopesMemory) = self.used_memory as f64
        );

        Self::do_spool(db, buf).await
    }

    /// Extracts the envelope from the `SqliteRow`.
    ///
    /// Reads the bytes and tries to perse them into `Envelope`.
    fn extract_envelope(&self, row: SqliteRow) -> Result<ManagedEnvelope, BufferError> {
        let envelope_row: Vec<u8> = row.try_get("envelope").map_err(BufferError::FetchFailed)?;
        let envelope_bytes = bytes::Bytes::from(envelope_row);
        let mut envelope = Envelope::parse_bytes(envelope_bytes)?;

        let received_at: i64 = row
            .try_get("received_at")
            .map_err(BufferError::FetchFailed)?;
        let start_time = StartTime::from_timestamp_millis(received_at as u64);

        envelope.set_start_time(start_time.into_inner());

        #[cfg(test)]
        if self.untracked {
            return Ok(ManagedEnvelope::untracked(
                envelope,
                self.outcome_aggregator.clone(),
                self.test_store.clone(),
            ));
        }

        let managed_envelope = self.buffer_guard.enter(
            envelope,
            self.outcome_aggregator.clone(),
            self.test_store.clone(),
        )?;
        Ok(managed_envelope)
    }

    /// Handles the enqueueing messages into the internal buffer.
    async fn handle_enqueue(&mut self, message: Enqueue) -> Result<(), BufferError> {
        let Enqueue {
            key,
            value: managed_envelope,
        } = message;

        // save to the internal buffer
        self.used_memory += managed_envelope.estimated_size();
        self.buffer.entry(key).or_default().push(managed_envelope);

        relay_statsd::metric!(
            histogram(RelayHistograms::BufferEnvelopesMemory) = self.used_memory as f64
        );

        // If disk is full, the service stop spooling till the disk has more space.
        // The auto-vacuum will kick in on each delete from the database, and the spool status will
        // be checked and we more space is available the `is_disk_full` flag set to `false`.
        //
        // It falls back to the `cache.envelope_buffer_size`, and will try to keep the rest
        // of the incoming envelopes in the memory till the `BufferGuard` is exhausted and it
        // starts dropping the envelopes.
        if let Some(ref spool_config) = self.spool_config {
            if !spool_config.is_disk_full {
                self.try_spool().await?;
            }
        }

        Ok(())
    }

    /// Tries to delete the envelops from persistent buffer in batches, extract and convert them to
    /// managed envelopes and send to back into processing pipeline.
    ///
    /// If the error happens in the deletion/fetching phase, a key is returned to allow retrying later.
    ///
    /// Returns the amount of envelopes deleted from disk.
    async fn delete_and_fetch(
        &self,
        db: &Pool<Sqlite>,
        key: QueueKey,
        sender: &mpsc::UnboundedSender<ManagedEnvelope>,
    ) -> Result<(), QueueKey> {
        loop {
            // Removing envelopes from the on-disk buffer in batches has following implications:
            // 1. It is faster to delete from the DB in batches.
            // 2. Make sure that if we panic and deleted envelopes cannot be read out fully, we do not lose all of them,
            // but only one batch, and the rest of them will stay on disk for the next iteration
            // to pick up.
            //
            // Right now we use 100 for batch size.
            let mut envelopes = sql::delete_and_fetch(key, 100).fetch(db).peekable();
            relay_statsd::metric!(counter(RelayCounters::BufferReads) += 1);

            // Stream is empty, we can break the loop, since we read everything by now.
            if Pin::new(&mut envelopes).peek().await.is_none() {
                return Ok(());
            }

            while let Some(envelope) = envelopes.next().await {
                let envelope = match envelope {
                    Ok(envelope) => envelope,

                    // Bail if there are errors in the stream.
                    Err(err) => {
                        relay_log::error!(
                            "failed to read the buffer stream from the disk: {}",
                            LogError(&err)
                        );
                        return Err(key);
                    }
                };

                match self.extract_envelope(envelope) {
                    Ok(managed_envelope) => {
                        sender.send(managed_envelope).ok();
                    }
                    Err(err) => relay_log::error!(
                        "failed to extract envelope from the buffer: {}",
                        LogError(&err)
                    ),
                }
            }
        }
    }

    /// Checks if the spool still has space and sets `is_disk_full` flag accordingly.
    async fn refresh_spool_state(&mut self) {
        let Some(BufferSpoolConfig {
            db,
            max_disk_size,
            ref mut is_disk_full, ..
        }) = &mut self.spool_config else { return; };

        // If disk is not full, we can ignore this check and exit earlier.
        if !*is_disk_full {
            return;
        }

        // Refresh DB state only if we get the proper reading on the file.
        if let Ok(estimated_size) = Self::estimate_buffer_size(db).await {
            *is_disk_full = (estimated_size as usize) >= *max_disk_size;
        }
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

        for key in &keys {
            for value in self.buffer.remove(key).unwrap_or_default() {
                self.used_memory -= value.estimated_size();
                sender.send(value).ok();
            }
        }

        // Persistent buffer is configured, lets try to get data from the disk.
        let Some(BufferSpoolConfig { db, .. }) = &self.spool_config else { return Ok(()) };
        let mut unused_keys = BTreeSet::new();

        while let Some(key) = keys.pop() {
            // If the error with a key is returned we must save it for the next iterration.
            if let Err(key) = self.delete_and_fetch(db, key, &sender).await {
                unused_keys.insert(key);
            };
        }

        self.refresh_spool_state().await;

        relay_statsd::metric!(
            histogram(RelayHistograms::BufferEnvelopesMemory) = self.used_memory as f64
        );
        if !unused_keys.is_empty() {
            self.project_cache
                .send(UpdateBufferIndex::new(project_key, unused_keys))
        }

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
        for key in &keys {
            let (current_count, current_size) = self.buffer.remove(key).map_or((0, 0), |k| {
                (k.len(), k.into_iter().map(|k| k.estimated_size()).sum())
            });
            count += current_count;
            self.used_memory -= current_size;
        }

        if let Some(BufferSpoolConfig { db, .. }) = &self.spool_config {
            for key in keys {
                let result = sql::delete(key)
                    .execute(db)
                    .await
                    .map_err(BufferError::DeleteFailed)?;
                count += result.rows_affected() as usize;
            }
            self.refresh_spool_state().await;
        }

        if count > 0 {
            relay_log::with_scope(
                |scope| scope.set_tag("project_key", project_key),
                || relay_log::error!("evicted project with {} envelopes", count),
            );
        }

        Ok(())
    }

    /// Handles all the incoming messages from the [`Buffer`] interface.
    async fn handle_message(&mut self, message: Buffer) -> Result<(), BufferError> {
        match message {
            Buffer::Enqueue(message) => self.handle_enqueue(message).await,
            Buffer::DequeueMany(message) => self.handle_dequeue(message).await,
            Buffer::RemoveMany(message) => self.handle_remove(message).await,
        }
    }

    /// Handle the shutdown notification.
    ///
    /// When the service notified to shut down, it tries to immediatelly spool to disk all the
    /// content of the in-memory buffer. The service will spool to disk only if the spooling for
    /// envelops is enabled.
    async fn handle_shutdown(&mut self) -> Result<(), BufferError> {
        // Buffer to disk only if the DB and config are provided.
        if let Some(BufferSpoolConfig { db, .. }) = &self.spool_config {
            let count: usize = self.buffer.values().map(|v| v.len()).sum();
            if count == 0 {
                return Ok(());
            }
            relay_log::info!(
                "received shutdown signal, spooling {} envelopes to disk",
                count
            );

            let buf = std::mem::take(&mut self.buffer);
            self.used_memory = 0;
            Self::do_spool(db, buf).await?;
        }
        Ok(())
    }
}

impl Service for BufferService {
    type Interface = Buffer;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let mut shutdown = Controller::shutdown_handle();

            loop {
                tokio::select! {
                    biased;

                    Some(message) = rx.recv() => {
                        if let Err(err) = self.handle_message(message).await {
                            relay_log::error!("failed to handle an incoming message: {}", LogError(&err))
                        }
                    }
                    _ = shutdown.notified() => {
                       if let Err(err) = self.handle_shutdown().await {
                            relay_log::error!("failed while shutting down the service: {}", LogError(&err))
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
        let count: usize = self.buffer.values().map(|v| v.len()).sum();
        // We have envelopes in memory, try to buffer them to the disk.
        if count > 0 {
            relay_log::error!("dropped {} envelopes", count);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use insta::assert_debug_snapshot;
    use relay_common::Uuid;
    use relay_general::protocol::EventId;
    use relay_test::mock_service;

    use crate::extractors::RequestMeta;

    use super::*;

    fn services() -> (Addr<ProjectCache>, Addr<TrackOutcome>, Addr<TestStore>) {
        let (project_cache, _) = mock_service("project_cache", (), |&mut (), _| {});
        let (outcome_aggregator, _) = mock_service("outcome_aggregator", (), |&mut (), _| {});
        let (test_store, _) = mock_service("test_store", (), |&mut (), _| {});

        (project_cache, outcome_aggregator, test_store)
    }

    fn empty_envelope() -> ManagedEnvelope {
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let envelope = Envelope::from_request(Some(EventId::new()), RequestMeta::new(dsn));
        let (_, outcome_aggregator, test_store) = services();
        ManagedEnvelope::untracked(envelope, outcome_aggregator, test_store)
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
        let (project_cache, outcome_aggregator, test_store) = services();
        let mut service = BufferService::create(
            buffer_guard,
            outcome_aggregator,
            project_cache,
            test_store,
            config,
        )
        .await
        .unwrap();
        service.untracked = true; // so we do not panic when dropping envelopes
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
                value: empty_envelope(),
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

    #[test]
    fn metrics_work() {
        let buffer_guard: Arc<_> = BufferGuard::new(999999).into();
        let config: Arc<_> = Config::from_json_value(serde_json::json!({
            "spool": {
                "envelopes": {
                    "path": std::env::temp_dir().join(Uuid::new_v4().to_string()),
                    "max_memory_size": 2048, // 2KB limit
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

        let (project_cache, outcome_aggregator, test_store) = services();

        let captures = relay_statsd::with_capturing_test_client(|| {
            rt.block_on(async {
                let mut service = BufferService::create(
                    buffer_guard,
                    outcome_aggregator,
                    project_cache,
                    test_store,
                    config,
                )
                .await
                .unwrap();

                service.untracked = true; // so we do not panic when dropping envelopes

                // Send 5 envelopes
                for _ in 0..5 {
                    service
                        .handle_enqueue(Enqueue {
                            key,
                            value: empty_envelope(),
                        })
                        .await
                        .unwrap();
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
                assert_eq!(count, 5);
            })
        });

        // Collect only the buffer metrics.
        let captures: Vec<_> = captures
            .into_iter()
            .filter(|name| name.contains("buffer."))
            .collect();

        assert_debug_snapshot!(captures, @r###"
        [
            "buffer.envelopes_mem:2000|h",
            "buffer.envelopes_mem:4000|h",
            "buffer.disk_size:24576|h",
            "buffer.envelopes_mem:0|h",
            "buffer.writes:1|c",
            "buffer.envelopes_mem:2000|h",
            "buffer.envelopes_mem:4000|h",
            "buffer.disk_size:24576|h",
            "buffer.envelopes_mem:0|h",
            "buffer.writes:1|c",
            "buffer.envelopes_mem:2000|h",
            "buffer.reads:1|c",
            "buffer.reads:1|c",
            "buffer.envelopes_mem:0|h",
        ]
        "###);
    }
}
