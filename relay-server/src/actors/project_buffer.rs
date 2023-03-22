use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use futures::TryStreamExt;
use relay_common::ProjectKey;
use relay_config::PersistentBuffer;
use relay_log::LogError;
use relay_system::{FromMessage, Interface, Service};
use sqlx::migrate::MigrateError;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Pool, Row, Sqlite};
use tokio::fs;
use tokio::sync::mpsc;

use crate::envelope::{Envelope, EnvelopeError};
use crate::utils::{BufferGuard, ManagedEnvelope};

/// The set of errors which can happend while working the the buffer.
#[derive(Debug, thiserror::Error)]
pub enum BufferError {
    #[error("failed to store the envelope in the buffer")]
    Full,

    #[error("failed to issue a permit, too many envelopes in-flight processing")]
    Overloaded,

    #[error("failed to get the size of the buffer file")]
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

/// The envelopes [`MemoryBufferService`].
///
/// Buffer maintaince internal storage (internal buffer) of the envelopes, which keep accumilating
/// till the request to dequeue them again comes in.
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

/// In-memory implementation of the [`Buffer`] interface.
#[derive(Debug)]
pub struct MemoryBufferService {
    /// Contains the cache of the incoming envelopes.
    buffer: BTreeMap<QueueKey, Vec<ManagedEnvelope>>,
}

impl MemoryBufferService {
    /// Creates a new [`MemoryBufferService`].
    pub fn new() -> Self {
        Self {
            buffer: BTreeMap::new(),
        }
    }

    /// Handles the enqueueing messages into the internal buffer.
    fn handle_enqueue(&mut self, message: Enqueue) {
        self.buffer
            .entry(message.key)
            .or_default()
            .push(message.value);
    }

    /// Handles the dequeueing messages from the internal buffer.
    ///
    /// This method removes the envelopes from the buffer and stream them to the sender.
    fn handle_dequeue(&mut self, message: DequeueMany) {
        let DequeueMany { keys, sender } = message;
        for key in keys {
            for value in self.buffer.remove(&key).unwrap_or_default() {
                sender.send(value).ok();
            }
        }
    }

    /// Handles the remove request.
    ///
    /// This remove all the envelopes from the internal buffer for the provided keys.
    /// If any of the provided keys are still have the envelopes, the error will be logged with the
    /// number of envelopes dropped for the specific project key.
    fn handle_remove(&mut self, message: RemoveMany) {
        let RemoveMany { project_key, keys } = message;
        let mut count = 0;
        for key in keys {
            count += self.buffer.remove(&key).map_or(0, |k| k.len());
        }
        if count > 0 {
            relay_log::with_scope(
                |scope| scope.set_tag("project_key", project_key),
                || relay_log::error!("evicted project with {} envelopes", count),
            );
        }
    }

    /// Handles all the incoming messages from the [`Buffer`] interface.
    fn handle_message(&mut self, message: Buffer) {
        match message {
            Buffer::Enqueue(message) => self.handle_enqueue(message),
            Buffer::DequeueMany(message) => self.handle_dequeue(message),
            Buffer::RemoveMany(message) => self.handle_remove(message),
        }
    }
}

impl Service for MemoryBufferService {
    type Interface = Buffer;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                self.handle_message(message);
            }
        });
    }
}

impl Drop for MemoryBufferService {
    fn drop(&mut self) {
        let count: usize = self.buffer.values().map(|v| v.len()).sum();
        if count > 0 {
            relay_log::error!("dropped queue with {} envelopes", count);
        }
    }
}

/// [`Buffer`] interface implementation backed by SQLite.
#[derive(Debug)]
pub struct SqliteBufferService {
    buffer: Arc<BufferGuard>,
    db: Pool<Sqlite>,
    path: PathBuf,
    db_size_limit: u64,
}

impl SqliteBufferService {
    /// Creates a new [`SqliteBufferService`] from the provided path to the SQLite database file.
    pub async fn from_path(
        buffer: Arc<BufferGuard>,
        config: &PersistentBuffer,
    ) -> Result<Self, BufferError> {
        let path = config.buffer_path().to_owned();
        let options = SqliteConnectOptions::new()
            .filename(PathBuf::from("sqlite://").join(&path))
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true);

        let db = SqlitePoolOptions::new()
            .max_connections(config.max_connections())
            .min_connections(config.min_connections())
            .connect_with(options)
            .await?;
        Ok(Self {
            buffer,
            db,
            path,
            db_size_limit: config.max_buffer_size(),
        })
    }

    /// Returns the size of the buffer.
    async fn buffer_size(&self) -> Result<u64, BufferError> {
        Ok(fs::metadata(&self.path).await?.len())
    }

    /// Handles the enqueueing messages into the internal buffer.
    async fn handle_enqueue(&self, message: Enqueue) -> Result<(), BufferError> {
        // Reject all the enqueue requests if we exceed the max size of the buffer.
        if self.buffer_size().await? >= self.db_size_limit {
            return Err(BufferError::Full);
        }

        let Enqueue {
            key,
            value: managed_envelope,
        } = message;

        let envelope_bytes = managed_envelope.into_envelope().to_vec()?;
        sqlx::query("INSERT INTO envelopes (own_key, sampling_key, envelope) VALUES (?, ?, ?)")
            .bind(key.own_key.to_string())
            .bind(key.sampling_key.to_string())
            .bind(envelope_bytes)
            .execute(&self.db)
            .await?;
        Ok(())
    }

    /// Handles the dequeueing messages from the internal buffer.
    ///
    /// This method removes the envelopes from the buffer and stream them to the sender.
    async fn handle_dequeue(&self, message: DequeueMany) -> Result<(), BufferError> {
        let DequeueMany { keys, sender } = message;

        // TODO: we also want to limit the number of fetched envelopes from the DB.
        // DO NOT fetch all the envelopes at once.
        for key in keys {
            let mut envelopes = sqlx::query(
                "DELETE FROM envelopes WHERE own_key = ? AND sampling_key = ? RETURNING envelope",
            )
            .bind(key.own_key.to_string())
            .bind(key.sampling_key.to_string())
            .bind(self.buffer.available() as i64)
            .fetch(&self.db);

            while let Some(row) = envelopes.try_next().await? {
                let envelope_bytes_slice: &[u8] = row.try_get("envelope")?;
                let envelope_bytes = bytes::Bytes::from(envelope_bytes_slice);
                let envelope = Envelope::parse_bytes(envelope_bytes)?;
                let managed_envelope = self
                    .buffer
                    .enter(envelope)
                    .map_err(|_| BufferError::Overloaded)?;
                sender.send(managed_envelope).ok();
            }
        }

        Ok(())
    }

    /// Handles the remove request.
    ///
    /// This removes all the envelopes from the internal buffer for the provided keys.
    /// If any of the provided keys still have the envelopes, the error will be logged with the
    /// number of envelopes dropped for the specific project key.
    async fn handle_remove(&self, message: RemoveMany) -> Result<(), BufferError> {
        let RemoveMany { project_key, keys } = message;

        let mut count = 0;
        for key in keys {
            let result =
                sqlx::query("DELETE FROM envelopes where own_key = ? AND sampling_key = ?")
                    .bind(key.own_key.to_string())
                    .bind(key.sampling_key.to_string())
                    .execute(&self.db)
                    .await?;

            count += result.rows_affected();
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

impl Service for SqliteBufferService {
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
