use std::collections::{BTreeMap, BTreeSet};
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use relay_common::ProjectKey;
use relay_config::Config;
use relay_log::LogError;
use relay_system::{Addr, FromMessage, Interface, Service};
use sqlx::migrate::MigrateError;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
};
use sqlx::{Pool, QueryBuilder, Row, Sqlite};
use tokio::sync::mpsc;

use crate::actors::project_cache::{ProjectCache, UpdateBufferIndex};
use crate::envelope::{Envelope, EnvelopeError};
use crate::statsd::{RelayCounters, RelayHistograms};
use crate::utils::{BufferGuard, ManagedEnvelope};

/// SQLite allocates space to hold all host parameters between 1 and the largest host parameter number used.
///
/// To prevent excessive memory allocations, the maximum value of a host parameter number is SQLITE_MAX_VARIABLE_NUMBER,
/// which defaults to 999 for SQLite versions prior to 3.32.0 (2020-05-22) or 32766 for SQLite versions after 3.32.0.
///
/// Keep it on the lower side for now.
const SQLITE_LIMIT_VARIABLE_NUMBER: usize = 999;

/// The set of errors which can happend while working the the buffer.
#[derive(Debug, thiserror::Error)]
pub enum BufferError {
    #[error("failed to store the envelope in the buffer, max size {0} reached")]
    Full(u64),

    #[error("failed to move envelope from disk to memory")]
    CapacityExceeded(#[from] crate::utils::BufferError),

    #[error("failed to get the size of the buffer on the filesystem")]
    DatabaseSizeError(#[from] std::io::Error),

    /// Describes the errors linked with the `Sqlite` backed buffer.
    #[error("failed to fetch data from the database")]
    DatabaseError(#[from] sqlx::Error),

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

#[derive(Debug)]
struct BufferSpoolConfig {
    db: Pool<Sqlite>,
    max_disk_size: usize,
    max_memory_size: usize,
}

/// [`Buffer`] interface implementation backed by SQLite.
#[derive(Debug)]
pub struct BufferService {
    buffer: BTreeMap<QueueKey, Vec<ManagedEnvelope>>,
    buffer_guard: Arc<BufferGuard>,
    config: Arc<Config>,
    project_cache: Addr<ProjectCache>,
    spool_config: Option<BufferSpoolConfig>,
    count_mem_envelopes: i64,
    count_disk_envelopes: i64,

    #[cfg(test)]
    /// Create untracked envelopes when loading from disk. Only use in tests.
    untracked: bool,
}

impl BufferService {
    /// Set up database and return the current number of envelopes.
    async fn setup(path: &PathBuf) -> Result<i64, BufferError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true);

        let db = SqlitePoolOptions::new().connect_with(options).await?;
        sqlx::migrate!("../migrations").run(&db).await?;

        let count: i64 = sqlx::query("SELECT count(*) FROM envelopes")
            .fetch_one(&db)
            .await?
            .try_get(0)?;

        Ok(count)
    }

    /// Creates a new [`BufferService`] from the provided path to the SQLite database file.
    pub async fn create(
        buffer_guard: Arc<BufferGuard>,
        project_cache: Addr<ProjectCache>,
        config: Arc<Config>,
    ) -> Result<Self, BufferError> {
        let mut service = Self {
            buffer: BTreeMap::new(),
            buffer_guard,
            config: config.clone(),
            project_cache,
            count_mem_envelopes: 0,
            count_disk_envelopes: 0,
            spool_config: None,
            #[cfg(test)]
            untracked: false,
        };

        // Only if persistent buffer enabled, we create the pool and set the config.
        if let Some(path) = config.cache_persistent_buffer_path() {
            relay_log::info!("Using the buffer file: {}", path.to_string_lossy());

            service.count_disk_envelopes = Self::setup(&path).await?;

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
                // If shared-cache mode is enabled and a thread establishes multiple
                // connections to the same database, the connections share a single data and schema cache.
                // This can significantly reduce the quantity of memory and IO required by the system.
                .shared_cache(true);

            let db = SqlitePoolOptions::new()
                .max_connections(config.cache_persistent_buffer_max_connections())
                .min_connections(config.cache_persistent_buffer_min_connections())
                .connect_with(options)
                .await?;

            let spool_config = BufferSpoolConfig {
                db,
                max_disk_size: config.cache_persistent_buffer_max_disk_size(),
                max_memory_size: config.cache_persistent_buffer_max_memory_size(),
            };

            service.spool_config = Some(spool_config);
        }

        Ok(service)
    }

    /// Checks the provided path and gets the size of the file this path points to.
    fn estimate_buffer_size(path: Option<PathBuf>) -> Result<u64, BufferError> {
        path.and_then(|path| std::fs::metadata(path).ok())
            .map(|m| m.len())
            .ok_or(BufferError::DatabaseSizeError(Error::from(
                ErrorKind::NotFound,
            )))
    }

    /// Saves the provided buffer to the disk.
    ///
    /// Returns an error if the spooling failed, and the number of spooled envelopes on success.
    async fn do_spool(
        db: &Pool<Sqlite>,
        buffer: BTreeMap<QueueKey, Vec<ManagedEnvelope>>,
    ) -> Result<i64, BufferError> {
        // A builder type for constructing queries at runtime.
        // This by default creates a prepared sql statement, which is cached and
        // re-used for sequential queries.
        let mut query_builder: QueryBuilder<Sqlite> =
            QueryBuilder::new("INSERT INTO envelopes (own_key, sampling_key, envelope) ");

        // Since we have 3 variables we have to bind, we devide the SQLite limit by 3
        // here to prepare the chunks which will be preparing the batch inserts.
        let mut envelopes = stream::iter(buffer.into_iter())
            .flat_map(|(key, values)| {
                stream::iter(values.into_iter())
                    .map(move |value| (key, value.into_envelope().to_vec()))
            })
            .chunks(SQLITE_LIMIT_VARIABLE_NUMBER / 3);

        let mut count = 0;
        while let Some(chunk) = envelopes.next().await {
            query_builder.push_values(chunk, |mut b, (key, value)| match value {
                Ok(value) => {
                    b.push_bind(key.own_key.to_string())
                        .push_bind(key.sampling_key.to_string())
                        .push_bind(value);
                }
                Err(err) => {
                    relay_log::error!("failed to serialize the envelope: {}", LogError(&err));
                }
            });
            let result = query_builder.build().execute(db).await?;
            relay_statsd::metric!(counter(RelayCounters::BufferWrites) += 1);
            let rows_written = result.rows_affected();
            count += rows_written as i64;

            // Reset the builder to initial state set by `QueryBuilder::new` function,
            // so it can be reused for another chunk.
            query_builder.reset();
        }
        Ok(count)
    }

    /// Tries to save in-memory buffer to disk.
    ///
    /// It will spool to disk only if the persistent storage enabled in the configuration.
    async fn try_spool(&mut self) -> Result<(), BufferError> {
        let Self {
            spool_config,
            ref mut buffer,
            ..
        } = self;

        // Buffer to disk only if the DB and config are provided.
        if let Some(BufferSpoolConfig {
            db,
            max_disk_size,
            max_memory_size,
        }) = spool_config
        {
            if self.count_mem_envelopes < *max_memory_size as i64 {
                return Ok(());
            }

            let estimated_db_size =
                Self::estimate_buffer_size(self.config.cache_persistent_buffer_path())?;

            relay_statsd::metric!(histogram(RelayHistograms::BufferDiskSize) = estimated_db_size);

            // Reject all the enqueue requests if we exceed the max size of the buffer.
            if estimated_db_size as usize > *max_disk_size {
                return Err(BufferError::Full(estimated_db_size));
            }

            let buf = std::mem::take(buffer);
            self.count_mem_envelopes = 0;
            relay_statsd::metric!(
                histogram(RelayHistograms::BufferEnvelopesMemory) = self.count_mem_envelopes as f64
            );

            let count = Self::do_spool(db, buf).await?;
            self.count_disk_envelopes += count;
            relay_statsd::metric!(
                histogram(RelayHistograms::BufferEnvelopesDisk) = self.count_disk_envelopes as f64
            );
        }

        Ok(())
    }

    /// Extreacts the envelope from the `SqliteRow`.
    ///
    /// Reads the bytes and tries to perse them into `Envelope`.
    fn extract_envelope(&self, row: SqliteRow) -> Result<ManagedEnvelope, BufferError> {
        let envelope_row: Vec<u8> = row.try_get("envelope")?;
        let envelope_bytes = bytes::Bytes::from(envelope_row);
        let envelope = Envelope::parse_bytes(envelope_bytes)?;
        #[cfg(test)]
        if self.untracked {
            return Ok(ManagedEnvelope::untracked(envelope));
        }
        let managed_envelope = self.buffer_guard.enter(envelope)?;
        Ok(managed_envelope)
    }

    /// Handles the enqueueing messages into the internal buffer.
    async fn handle_enqueue(&mut self, message: Enqueue) -> Result<(), BufferError> {
        let Enqueue {
            key,
            value: managed_envelope,
        } = message;

        self.try_spool().await?;

        // save to the internal buffer
        self.buffer.entry(key).or_default().push(managed_envelope);
        self.count_mem_envelopes += 1;

        relay_statsd::metric!(
            histogram(RelayHistograms::BufferEnvelopesMemory) = self.count_mem_envelopes as f64
        );

        Ok(())
    }

    /// Tries to delete the envelops from persistent buffer in batches, extract and convert them to
    /// managed envelopes and send to back into processing pipeline.
    ///
    /// If the error happens in the deletion/fetching phase, a key is returned to allow retrying later.
    ///
    /// Returns the amount of envelopes deleted from disk.
    async fn fetch_and_delete(
        &self,
        db: &Pool<Sqlite>,
        key: QueueKey,
        sender: &mpsc::UnboundedSender<ManagedEnvelope>,
    ) -> Result<i64, QueueKey> {
        let mut count = 0;
        loop {
            // By default this creates a prepared statement which is cached and re-used.
            //
            // Removing envelopes from the on-disk buffer in batches has following implications:
            // 1. It is faster to delete from the DB in batches.
            // 2. Make sure that if we panic and deleted envelopes cannot be read out fully, we do not lose all of them,
            // but only one batch, and the rest of them will stay on disk for the next iteration
            // to pick up.
            let mut envelopes = sqlx::query(
                "DELETE FROM envelopes WHERE id IN (SELECT id FROM envelopes WHERE own_key = ? AND sampling_key = ? LIMIT 100) RETURNING envelope",
            )
            .bind(key.own_key.to_string())
            .bind(key.sampling_key.to_string())
            .fetch(db).peekable();

            relay_statsd::metric!(counter(RelayCounters::BufferReads) += 1);

            // Stream is empty, we can break the loop, since we read everything by now.
            if Pin::new(&mut envelopes).peek().await.is_none() {
                return Ok(count);
            }

            while let Some(envelope) = envelopes.next().await {
                count += 1;
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
                self.count_mem_envelopes -= 1;
                sender.send(value).ok();
            }
        }

        // Persistent buffer is configured, lets try to get data from the disk.
        if let Some(BufferSpoolConfig { db, .. }) = &self.spool_config {
            let mut unused_keys = BTreeSet::new();

            while let Some(key) = keys.pop() {
                // If the error with a key is returned we must save it for the next iterration.
                match self.fetch_and_delete(db, key, &sender).await {
                    Ok(count) => self.count_disk_envelopes -= count,
                    Err(key) => {
                        unused_keys.insert(key);
                    }
                };
            }
            if !unused_keys.is_empty() {
                self.project_cache
                    .send(UpdateBufferIndex::new(project_key, unused_keys))
            }

            relay_statsd::metric!(
                histogram(RelayHistograms::BufferEnvelopesDisk) = self.count_disk_envelopes as f64
            );
            relay_statsd::metric!(
                histogram(RelayHistograms::BufferEnvelopesMemory) = self.count_mem_envelopes as f64
            );
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
        let mut count: u64 = 0;
        for key in &keys {
            count += self.buffer.remove(key).map_or(0, |k| k.len() as u64);
            self.count_mem_envelopes -= count as i64;
        }

        let mut disk_count = 0;
        if let Some(BufferSpoolConfig { db, .. }) = &self.spool_config {
            for key in keys {
                let result =
                    sqlx::query("DELETE FROM envelopes where own_key = ? AND sampling_key = ?")
                        .bind(key.own_key.to_string())
                        .bind(key.sampling_key.to_string())
                        .execute(db)
                        .await?;

                disk_count = result.rows_affected();
            }
        }

        self.count_disk_envelopes -= disk_count as i64;
        relay_statsd::metric!(
            histogram(RelayHistograms::BufferEnvelopesDisk) = self.count_disk_envelopes as f64
        );

        count += disk_count;

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
}

impl Service for BufferService {
    type Interface = Buffer;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                if let Err(err) = self.handle_message(message).await {
                    relay_log::error!("failed to handle an incoming message: {}", LogError(&err))
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
    use insta::assert_debug_snapshot;
    use relay_common::Uuid;
    use relay_general::protocol::EventId;
    use relay_test::mock_service;

    use crate::extractors::RequestMeta;

    use super::*;

    fn empty_envelope() -> ManagedEnvelope {
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let envelope = Envelope::from_request(Some(EventId::new()), RequestMeta::new(dsn));
        ManagedEnvelope::untracked(envelope)
    }

    #[test]
    fn metrics_work() {
        let buffer_guard = BufferGuard::new(999999).into();
        let config = Config::from_json_value(serde_json::json!({
            "cache": {
                "persistent_envelope_buffer": {
                    "path": dbg!(std::env::temp_dir().join(Uuid::new_v4().to_string())),
                    "max_memory_size": 2,
                }
            }
        }))
        .unwrap();
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
                let (project_cache, _) = mock_service("project_cache", (), |&mut (), _| {});
                let mut service = BufferService::create(buffer_guard, project_cache, config.into())
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

        assert_debug_snapshot!(captures, @r###"
        [
            "service.back_pressure:0|g|#service:project_cache",
            "buffer.envelopes_mem:1|h",
            "buffer.envelopes_mem:2|h",
            "buffer.disk_size:4096|h",
            "buffer.envelopes_mem:0|h",
            "buffer.writes:1|c",
            "buffer.envelopes_disk:2|h",
            "buffer.envelopes_mem:1|h",
            "buffer.envelopes_mem:2|h",
            "buffer.disk_size:4096|h",
            "buffer.envelopes_mem:0|h",
            "buffer.writes:1|c",
            "buffer.envelopes_disk:4|h",
            "buffer.envelopes_mem:1|h",
            "buffer.reads:1|c",
            "buffer.reads:1|c",
            "buffer.envelopes_disk:0|h",
            "buffer.envelopes_mem:0|h",
        ]
        "###);
    }
}
