use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use futures::TryStreamExt;
use relay_common::ProjectKey;
use relay_config::{Config, PersistentBuffer};
use relay_log::LogError;
use relay_system::{FromMessage, Interface, Service};
use sqlx::migrate::MigrateError;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Pool, QueryBuilder, Row, Sqlite};
use tokio::sync::mpsc;

use crate::envelope::{Envelope, EnvelopeError};
use crate::utils::{BufferGuard, ManagedEnvelope};

/// The set of errors which can happend while working the the buffer.
#[derive(Debug, thiserror::Error)]
pub enum BufferError {
    #[error("failed to store the envelope in the buffer, max size {0} reached")]
    Full(u64),

    #[error("failed to issue a permit, too many envelopes currently in-flight")]
    Overloaded,

    #[error("failed to get the size of the buffer on the filesystem")]
    DatabaseSizeError(#[from] std::io::Error),

    /// Describes the errors linked with the `Sqlite` backed buffer.
    #[error("failed to fetch data from the database")]
    DatabaseError(#[from] sqlx::Error),

    #[error(transparent)]
    EnvelopeError(#[from] EnvelopeError),

    #[error("failed to run migrations")]
    MigrationFailed(#[from] MigrateError),

    #[error("failed to read the migrations directory")]
    MissingMigrations,
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
    keys: Vec<QueueKey>,
    sender: mpsc::UnboundedSender<ManagedEnvelope>,
}

impl DequeueMany {
    pub fn new(keys: Vec<QueueKey>, sender: mpsc::UnboundedSender<ManagedEnvelope>) -> Self {
        Self { keys, sender }
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

/// [`Buffer`] interface implementation backed by SQLite.
#[derive(Debug)]
pub struct BufferService {
    buffer: BTreeMap<QueueKey, Vec<ManagedEnvelope>>,
    buffer_guard: Arc<BufferGuard>,
    config: Option<PersistentBuffer>,
    // this is half of the `envelope_buffer_size` defined in the [`relay_config::Config`].
    memory_limit: Option<usize>,
    count_mem_envelopes: i64,
    db: Option<Pool<Sqlite>>,
}

impl BufferService {
    /// Creates a new [`BufferService`] from the provided path to the SQLite database file.
    pub async fn create(
        buffer_guard: Arc<BufferGuard>,
        config: Arc<Config>,
    ) -> Result<Self, BufferError> {
        let mut service = Self {
            buffer: BTreeMap::new(),
            buffer_guard,
            config: None,
            memory_limit: None,
            count_mem_envelopes: 0,
            db: None,
        };

        // Only iof persistent buffer enabled, we create the pool and set the config.
        if let Some(buffer_config) = config.cache_persistent_buffer() {
            let path = buffer_config.buffer_path().to_owned();
            let options = SqliteConnectOptions::new()
                .filename(PathBuf::from("sqlite://").join(&path))
                .journal_mode(SqliteJournalMode::Wal)
                .create_if_missing(true);

            let db = SqlitePoolOptions::new()
                .max_connections(buffer_config.max_connections())
                .min_connections(buffer_config.min_connections())
                .connect_with(options)
                .await?;

            // Set the buffer memory limit, which must not be bigger then `envelope_buffer_size`.
            let limit = buffer_config
                .memory_limit()
                .unwrap_or(config.envelope_buffer_size() / 2);
            service.memory_limit = Some(limit.min(config.envelope_buffer_size()));

            service.db = Some(db);
            service.config = Some(buffer_config.to_owned());
        }

        Ok(service)
    }

    /// Tries to save in-memory buffer to disk.
    ///
    /// It will spool to disk only if the persistent storage enabled in the configuration.
    fn try_enqueue(&mut self) -> Result<(), BufferError> {
        let Self {
            db,
            config,
            memory_limit,
            ref mut buffer,
            ..
        } = self;

        // Buffer to disk only if the DB and config are provided.
        if let (Some(db), Some(config), Some(memory_limit)) = (db, config, memory_limit) {
            // And if the count of in memory envelopes is over the defined max buffer size.
            if self.count_mem_envelopes > *memory_limit as i64 {
                // Reject all the enqueue requests if we exceed the max size of the buffer.
                let current_size = std::fs::metadata(config.buffer_path())
                    .ok()
                    .map(|meta| meta.len());
                if current_size.map_or(false, |size| size >= config.max_buffer_size()) {
                    return Err(BufferError::Full(current_size.unwrap_or_default()));
                }

                // Do not drain and just keep the buffer around, so we do not have to allocate it
                // again.
                let buf = std::mem::take(buffer);
                let db = db.clone();
                self.count_mem_envelopes = 0;

                // spawn the task to enqueue the entire buffer
                tokio::spawn(async move {
                    let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(
                        "INSERT INTO envelopes (own_key, sampling_key, envelope) ",
                    );

                    // Flatten all the envelopes
                    let envelopes = buf
                        .into_iter()
                        .flat_map(|(k, vals)| {
                            vals.into_iter()
                                .map(move |v| (k, v.into_envelope().to_vec()))
                        })
                        .collect::<Vec<_>>();

                    // TODO: have this number configured.
                    for chunk in envelopes.chunks(1000) {
                        query_builder.push_values(chunk, |mut b, v| match &v.1 {
                            Ok(envelope_bytes) => {
                                b.push_bind(v.0.own_key.to_string())
                                    .push_bind(v.0.sampling_key.to_string())
                                    .push_bind(envelope_bytes);
                            }
                            Err(err) => {
                                relay_log::error!(
                                    "failed to serialize the envelope: {}",
                                    LogError(&err)
                                )
                            }
                        });

                        let query = query_builder.build();
                        if let Err(err) = query.execute(&db).await {
                            relay_log::error!(
                                "failed to buffer envelopes to disk: {}",
                                LogError(&err)
                            )
                        }
                    }
                });
            }
        }
        Ok(())
    }

    /// Handles the enqueueing messages into the internal buffer.
    async fn handle_enqueue(&mut self, message: Enqueue) -> Result<(), BufferError> {
        let Enqueue {
            key,
            value: managed_envelope,
        } = message;

        self.try_enqueue()?;

        // save to the internal buffer
        self.buffer.entry(key).or_default().push(managed_envelope);
        self.count_mem_envelopes += 1;

        Ok(())
    }

    /// Handles the dequeueing messages from the internal buffer.
    ///
    /// This method removes the envelopes from the buffer and stream them to the sender.
    async fn handle_dequeue(&mut self, message: DequeueMany) -> Result<(), BufferError> {
        let DequeueMany { keys, sender } = message;
        for key in &keys {
            for value in self.buffer.remove(key).unwrap_or_default() {
                self.count_mem_envelopes -= 1;
                sender.send(value).ok();
            }
        }

        // Persistent buffer is configured, lets try to get data from the disk.
        if let Some(db) = &self.db {
            // TODO: we also want to limit the number of fetched envelopes from the DB.
            // DO NOT fetch all the envelopes at once.
            for key in keys {
                let mut envelopes = sqlx::query(
                "DELETE FROM envelopes WHERE own_key = ? AND sampling_key = ? RETURNING envelope",
            )
            .bind(key.own_key.to_string())
            .bind(key.sampling_key.to_string())
            .fetch(db);

                while let Some(row) = envelopes.try_next().await? {
                    let envelope_bytes_slice: &[u8] = row.try_get("envelope")?;
                    let envelope_bytes = bytes::Bytes::from(envelope_bytes_slice);
                    let envelope = Envelope::parse_bytes(envelope_bytes)?;
                    let managed_envelope = self
                        .buffer_guard
                        .enter(envelope)
                        .map_err(|_| BufferError::Overloaded)?;

                    sender.send(managed_envelope).ok();
                }
            }
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
        }

        if let Some(db) = &self.db {
            for key in keys {
                let result =
                    sqlx::query("DELETE FROM envelopes where own_key = ? AND sampling_key = ?")
                        .bind(key.own_key.to_string())
                        .bind(key.sampling_key.to_string())
                        .execute(db)
                        .await?;

                count += result.rows_affected();
            }
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
            if let Err(err) = self.try_enqueue() {
                relay_log::error!("failed to spool {} on shutdown: {}", count, LogError(&err));
            }
        }
    }
}
