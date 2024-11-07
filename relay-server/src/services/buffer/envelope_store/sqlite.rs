use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::envelope::EnvelopeError;

use crate::services::buffer::common::ProjectKeyPair;
use crate::statsd::{RelayGauges, RelayHistograms, RelayTimers};
use crate::Envelope;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::StreamExt;
use hashbrown::HashSet;
use relay_base_schema::project::{ParseProjectKeyError, ProjectKey};
use relay_config::Config;
use serde::{Deserialize, Serialize};
use sqlx::migrate::MigrateError;
use sqlx::query::Query;
use sqlx::sqlite::{
    SqliteArguments, SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions,
    SqliteRow, SqliteSynchronous,
};
use sqlx::{Pool, Row, Sqlite};
use tokio::fs::DirBuilder;
use tokio::time::sleep;

/// Fixed first 4 bytes for zstd compressed envelopes.
///
/// Used for backward compatibility to check whether an envelope on disk is zstd-encoded.
const ZSTD_MAGIC_WORD: &[u8] = &[40, 181, 47, 253];

/// Struct that contains all the fields of an [`Envelope`] that are mapped to the database columns.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatabaseEnvelope {
    received_at: i64,
    own_key: ProjectKey,
    sampling_key: ProjectKey,
    encoded_envelope: Box<[u8]>,
}

#[derive(Clone, Debug)]
pub struct DatabaseBatch {
    received_at: i64,
    own_key: ProjectKey,
    sampling_key: ProjectKey,
    envelopes: Vec<DatabaseEnvelope>,
}

impl DatabaseBatch {
    pub fn len(&self) -> usize {
        self.envelopes.len()
    }
}

impl TryFrom<Vec<DatabaseEnvelope>> for DatabaseBatch {
    type Error = ();

    fn try_from(envelopes: Vec<DatabaseEnvelope>) -> Result<Self, Self::Error> {
        let Some(newest) = envelopes.last() else {
            return Err(());
        };
        Ok(Self {
            received_at: newest.received_at,
            own_key: newest.own_key,
            sampling_key: newest.sampling_key,
            envelopes,
        })
    }
}

impl From<DatabaseBatch> for Vec<DatabaseEnvelope> {
    fn from(value: DatabaseBatch) -> Self {
        value.envelopes
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InsertEnvelopeError {
    #[error("envelope conversion error: {0}")]
    Envelope(#[from] EnvelopeError),
    #[error("compression error: {0}")]
    Zstd(#[from] std::io::Error),
}

impl DatabaseEnvelope {
    // Use the lowest level of compression.
    //
    // Experiments showed that level 3 is significantly slower than level 1 while offering
    // no significant size reduction for our use case.
    const COMPRESSION_LEVEL: i32 = 1;

    pub fn len(&self) -> usize {
        self.encoded_envelope.len()
    }

    pub fn received_at(&self) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(self.received_at).unwrap_or(Utc::now())
    }
}

impl TryFrom<DatabaseEnvelope> for Box<Envelope> {
    type Error = InsertEnvelopeError;

    fn try_from(value: DatabaseEnvelope) -> Result<Self, Self::Error> {
        let received_at = value.received_at();
        let DatabaseEnvelope {
            received_at: _,
            own_key,
            sampling_key,
            mut encoded_envelope,
        } = value;

        if encoded_envelope.starts_with(ZSTD_MAGIC_WORD) {
            relay_statsd::metric!(timer(RelayTimers::BufferEnvelopeDecompression), {
                encoded_envelope = zstd::decode_all(&*encoded_envelope)?.into_boxed_slice();
            });
        }

        let mut envelope = Envelope::parse_bytes(Bytes::from(encoded_envelope))?;
        debug_assert_eq!(envelope.meta().public_key(), own_key);
        debug_assert!(envelope
            .sampling_key()
            .map_or(true, |key| key == sampling_key));

        envelope.set_received_at(received_at);

        Ok(envelope)
    }
}

impl<'a> TryFrom<&'a Envelope> for DatabaseEnvelope {
    type Error = InsertEnvelopeError;

    fn try_from(value: &'a Envelope) -> Result<Self, Self::Error> {
        let own_key = value.meta().public_key();
        let sampling_key = value.sampling_key().unwrap_or(own_key);

        let serialized_envelope = value.to_vec()?;
        relay_statsd::metric!(
            histogram(RelayHistograms::BufferEnvelopeSize) = serialized_envelope.len() as u64
        );

        let encoded_envelope =
            relay_statsd::metric!(timer(RelayTimers::BufferEnvelopeCompression), {
                zstd::encode_all(serialized_envelope.as_slice(), Self::COMPRESSION_LEVEL)?
            });
        relay_statsd::metric!(
            histogram(RelayHistograms::BufferEnvelopeSizeCompressed) =
                encoded_envelope.len() as u64
        );

        Ok(DatabaseEnvelope {
            received_at: value.received_at().timestamp_millis(),
            own_key,
            sampling_key,
            encoded_envelope: encoded_envelope.into_boxed_slice(),
        })
    }
}

/// An error returned when doing an operation on [`SqliteEnvelopeStore`].
#[derive(Debug, thiserror::Error)]
pub enum SqliteEnvelopeStoreError {
    #[error("failed to setup the database: {0}")]
    SqlxSetupFailed(sqlx::Error),

    #[error("failed to create the spool file: {0}")]
    FileSetupError(std::io::Error),

    #[error("msgpack error: {0}")]
    MsgPackEncode(#[from] rmp_serde::encode::Error),

    #[error("msgpack error: {0}")]
    MsgPackDecode(#[from] rmp_serde::decode::Error),

    #[error("failed to write to disk: {0}")]
    WriteError(sqlx::Error),

    #[error("failed to read from disk: {0}")]
    FetchError(sqlx::Error),

    #[error("no file path for the spool was provided")]
    NoFilePath,

    #[error("failed to migrate the database: {0}")]
    MigrationError(MigrateError),

    #[error("failed to extract the envelope from the database")]
    EnvelopeExtractionError,

    #[error("failed to extract a project key from the database")]
    ProjectKeyExtractionError(#[from] ParseProjectKeyError),

    #[error("failed to get database file size: {0}")]
    FileSizeReadFailed(sqlx::Error),
}

#[derive(Debug, Clone)]
struct DiskUsage {
    db: Pool<Sqlite>,
    last_known_usage: Arc<AtomicU64>,
    refresh_frequency: Duration,
}

impl DiskUsage {
    /// Creates a new empty [`DiskUsage`].
    fn new(db: Pool<Sqlite>, refresh_frequency: Duration) -> Self {
        Self {
            db,
            last_known_usage: Arc::new(AtomicU64::new(0)),
            refresh_frequency,
        }
    }

    /// Prepares a [`DiskUsage`] instance with an initial reading of the database usage and fails
    /// if not reading can be made.
    pub async fn prepare(
        db: Pool<Sqlite>,
        refresh_frequency: Duration,
    ) -> Result<Self, SqliteEnvelopeStoreError> {
        let usage = Self::estimate_usage(&db).await?;

        let disk_usage = Self::new(db, refresh_frequency);
        disk_usage.last_known_usage.store(usage, Ordering::Relaxed);
        disk_usage.start_background_refresh();

        Ok(disk_usage)
    }

    /// Returns the disk usage and asynchronously updates it in case a `refresh_frequency_ms`
    /// elapsed.
    fn usage(&self) -> u64 {
        self.last_known_usage.load(Ordering::Relaxed)
    }

    /// Starts a background tokio task to update the database usage.
    fn start_background_refresh(&self) {
        let db = self.db.clone();
        // We get a weak reference, to make sure that if `DiskUsage` is dropped, the reference can't
        // be upgraded, causing the loop in the tokio task to exit.
        let last_known_usage_weak = Arc::downgrade(&self.last_known_usage);
        let refresh_frequency = self.refresh_frequency;

        tokio::spawn(async move {
            loop {
                // When our `Weak` reference can't be upgraded to an `Arc`, it means that the value
                // is not referenced anymore by self, meaning that `DiskUsage` was dropped.
                let Some(last_known_usage) = last_known_usage_weak.upgrade() else {
                    break;
                };

                let usage = Self::estimate_usage(&db).await;
                let Ok(usage) = usage else {
                    relay_log::error!("failed to update the disk usage asynchronously");
                    return;
                };

                let current = last_known_usage.load(Ordering::Relaxed);
                if last_known_usage
                    .compare_exchange_weak(current, usage, Ordering::Relaxed, Ordering::Relaxed)
                    .is_err()
                {
                    relay_log::error!("failed to update the disk usage asynchronously");
                };

                sleep(refresh_frequency).await;
            }
        });
    }

    /// Estimates the disk usage of the SQLite database.
    async fn estimate_usage(db: &Pool<Sqlite>) -> Result<u64, SqliteEnvelopeStoreError> {
        let usage: i64 = build_estimate_size()
            .fetch_one(db)
            .await
            .and_then(|r| r.try_get(0))
            .map_err(SqliteEnvelopeStoreError::FileSizeReadFailed)?;

        relay_statsd::metric!(gauge(RelayGauges::BufferDiskUsed) = usage as u64);

        Ok(usage as u64)
    }
}

/// Struct that offers access to a SQLite-based store of [`Envelope`]s.
///
/// The goal of this struct is to hide away all the complexity of dealing with the database for
/// reading and writing envelopes.
#[derive(Debug, Clone)]
pub struct SqliteEnvelopeStore {
    db: Pool<Sqlite>,
    disk_usage: DiskUsage,
}

impl SqliteEnvelopeStore {
    /// Initializes the [`SqliteEnvelopeStore`] with a supplied [`Pool`].
    pub fn new(db: Pool<Sqlite>, refresh_frequency: Duration) -> Self {
        Self {
            db: db.clone(),
            disk_usage: DiskUsage::new(db, refresh_frequency),
        }
    }

    /// Prepares the [`SqliteEnvelopeStore`] by running all the necessary migrations and preparing
    /// the folders where data will be stored.
    pub async fn prepare(config: &Config) -> Result<SqliteEnvelopeStore, SqliteEnvelopeStoreError> {
        // If no path is provided, we can't do disk spooling.
        let Some(path) = config.spool_envelopes_path() else {
            return Err(SqliteEnvelopeStoreError::NoFilePath);
        };

        relay_log::info!("buffer file {}", path.to_string_lossy());

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
            // Auto-vacuum does not de-fragment the database nor repack individual database pages the way that the VACUUM command does.
            //
            // This will help us to keep the file size under some control.
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
            .map_err(SqliteEnvelopeStoreError::SqlxSetupFailed)?;

        Ok(SqliteEnvelopeStore {
            db: db.clone(),
            disk_usage: DiskUsage::prepare(db, config.spool_disk_usage_refresh_frequency_ms())
                .await?,
        })
    }

    /// Set up the database and return the current number of envelopes.
    ///
    /// The directories and spool file will be created if they don't already
    /// exist.
    async fn setup(path: &Path) -> Result<(), SqliteEnvelopeStoreError> {
        Self::create_spool_directory(path).await?;

        let options = SqliteConnectOptions::new()
            .filename(path)
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true);

        let db = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .map_err(SqliteEnvelopeStoreError::SqlxSetupFailed)?;

        sqlx::migrate!("../migrations")
            .run(&db)
            .await
            .map_err(SqliteEnvelopeStoreError::MigrationError)?;

        Ok(())
    }

    /// Creates the directories for the spool file.
    async fn create_spool_directory(path: &Path) -> Result<(), SqliteEnvelopeStoreError> {
        let Some(parent) = path.parent() else {
            return Ok(());
        };

        if !parent.as_os_str().is_empty() && !parent.exists() {
            relay_log::debug!("creating directory for spooling file: {}", parent.display());
            DirBuilder::new()
                .recursive(true)
                .create(&parent)
                .await
                .map_err(SqliteEnvelopeStoreError::FileSetupError)?;
        }

        Ok(())
    }

    /// Inserts one or more envelopes into the database.
    pub async fn insert_batch(
        &mut self,
        envelopes: DatabaseBatch,
    ) -> Result<(), SqliteEnvelopeStoreError> {
        let DatabaseBatch {
            received_at,
            own_key,
            sampling_key,
            envelopes,
        } = envelopes;

        let packed = rmp_serde::to_vec(&envelopes)?;

        let query = sqlx::query("INSERT INTO envelopes (received_at, own_key, sampling_key, envelope) VALUES (?, ?, ?, ?);")
            .bind(received_at)
            .bind(own_key.as_str())
            .bind(sampling_key.as_str())
            .bind(packed);

        query
            .execute(&self.db)
            .await
            .map_err(SqliteEnvelopeStoreError::WriteError)?;
        Ok(())
    }

    /// Deletes and returns at most `limit` [`Envelope`]s from the database.
    pub async fn delete_batch(
        &mut self,
        own_key: ProjectKey,
        sampling_key: ProjectKey,
    ) -> Result<Option<DatabaseBatch>, SqliteEnvelopeStoreError> {
        let mut rows = build_delete_and_fetch_many_envelopes(own_key, sampling_key).fetch(&self.db);
        let Some(row) = rows.as_mut().next().await else {
            return Ok(None);
        };
        let row = row.map_err(SqliteEnvelopeStoreError::FetchError)?;

        Ok(Some(extract_batch(own_key, sampling_key, row)?))
    }

    /// Returns a set of project key pairs, representing all the unique combinations of
    /// `own_key` and `project_key` that are found in the database.
    pub async fn project_key_pairs(
        &self,
    ) -> Result<HashSet<ProjectKeyPair>, SqliteEnvelopeStoreError> {
        let project_key_pairs = build_get_project_key_pairs()
            .fetch_all(&self.db)
            .await
            .map_err(SqliteEnvelopeStoreError::FetchError)?;

        let project_key_pairs = project_key_pairs
            .into_iter()
            // Collect only keys we can extract.
            .filter_map(|project_key_pair| extract_project_key_pair(project_key_pair).ok())
            .collect();

        Ok(project_key_pairs)
    }

    /// Returns an approximate measure of the used size of the database.
    pub fn usage(&self) -> u64 {
        self.disk_usage.usage()
    }

    /// Returns the total count of envelopes stored in the database.
    pub async fn total_count(&self) -> Result<u64, SqliteEnvelopeStoreError> {
        let row = build_count_all()
            .fetch_one(&self.db)
            .await
            .map_err(SqliteEnvelopeStoreError::FetchError)?;

        let total_count: i64 = row.get(0);
        Ok(total_count as u64)
    }
}

/// Loads a [`DatabaseEnvelope`] from a database row.
fn extract_batch(
    own_key: ProjectKey,
    sampling_key: ProjectKey,
    row: SqliteRow,
) -> Result<DatabaseBatch, SqliteEnvelopeStoreError> {
    let received_at: i64 = row
        .try_get("received_at")
        .map_err(SqliteEnvelopeStoreError::FetchError)?;
    let data: Box<[u8]> = row
        .try_get("envelope")
        .map_err(SqliteEnvelopeStoreError::FetchError)?;

    let envelopes = match rmp_serde::from_slice::<Vec<DatabaseEnvelope>>(&*data) {
        Ok(envelopes) => {
            #[cfg(debug_assertions)]
            for envelope in &envelopes {
                debug_assert_eq!(envelope.received_at, received_at);
            }
            envelopes
        }
        Err(_) => {
            // legacy: one envelope per row
            vec![DatabaseEnvelope {
                received_at,
                own_key,
                sampling_key,
                encoded_envelope: data,
            }]
        }
    };

    Ok(DatabaseBatch {
        received_at,
        own_key,
        sampling_key,
        envelopes,
    })
}

/// Deserializes a pair of [`ProjectKey`] from the database.
fn extract_project_key_pair(row: SqliteRow) -> Result<ProjectKeyPair, SqliteEnvelopeStoreError> {
    let own_key = row
        .try_get("own_key")
        .map_err(SqliteEnvelopeStoreError::FetchError)
        .and_then(|key| {
            ProjectKey::parse(key).map_err(SqliteEnvelopeStoreError::ProjectKeyExtractionError)
        });
    let sampling_key = row
        .try_get("sampling_key")
        .map_err(SqliteEnvelopeStoreError::FetchError)
        .and_then(|key| {
            ProjectKey::parse(key).map_err(SqliteEnvelopeStoreError::ProjectKeyExtractionError)
        });

    match (own_key, sampling_key) {
        (Ok(own_key), Ok(sampling_key)) => Ok(ProjectKeyPair::new(own_key, sampling_key)),
        // Report the first found error.
        (Err(err), _) | (_, Err(err)) => {
            relay_log::error!("failed to extract a queue key from the spool record: {err}");

            Err(err)
        }
    }
}

/// Builds a query that deletes many [`Envelope`] from the database.
pub fn build_delete_and_fetch_many_envelopes<'a>(
    own_key: ProjectKey,
    project_key: ProjectKey,
) -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query(
        "DELETE FROM
            envelopes
         WHERE id IN (SELECT id FROM envelopes WHERE own_key = ? AND sampling_key = ?
            ORDER BY received_at DESC LIMIT 1)
         RETURNING
            received_at, own_key, sampling_key, envelope",
    )
    .bind(own_key.to_string())
    .bind(project_key.to_string())
}

/// Creates a query which fetches the number of used database pages multiplied by the page size.
///
/// This info used to estimate the current allocated database size.
pub fn build_estimate_size<'a>() -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query(
        r#"SELECT (page_count - freelist_count) * page_size as size FROM pragma_page_count(), pragma_freelist_count(), pragma_page_size();"#,
    )
}

/// Returns the query to select all the unique combinations of own and sampling keys.
pub fn build_get_project_key_pairs<'a>() -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query("SELECT DISTINCT own_key, sampling_key FROM envelopes;")
}

/// Returns the query to count the number of envelopes on disk.
///
/// Please note that this query is SLOW because SQLite doesn't use any metadata to satisfy it,
/// meaning that it has to scan through all the rows and count them.
pub fn build_count_all<'a>() -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query("SELECT COUNT(1) FROM envelopes;")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tokio::time::sleep;

    use relay_base_schema::project::ProjectKey;

    use super::*;
    use crate::services::buffer::testutils::utils::{mock_envelopes, setup_db};

    #[tokio::test]
    async fn test_insert_and_delete_envelopes() {
        let db = setup_db(true).await;
        let mut envelope_store = SqliteEnvelopeStore::new(db, Duration::from_millis(100));

        let own_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let sampling_key = ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap();

        // We insert 10 envelopes.
        let batches = [mock_envelopes(5), mock_envelopes(5)];
        for batch in &batches {
            assert!(envelope_store
                .insert_batch(
                    batch
                        .iter()
                        .map(|e| DatabaseEnvelope::try_from(e.as_ref()).unwrap())
                        .collect::<Vec<_>>()
                        .try_into()
                        .unwrap()
                )
                .await
                .is_ok());
        }

        // We check that if we load 5, we get the newest 5.
        let batch = envelope_store
            .delete_batch(own_key, sampling_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.len(), 5);
        for (i, extracted_envelope) in batch.envelopes.iter().enumerate() {
            assert_eq!(
                extracted_envelope.received_at().timestamp_millis(),
                (&batches[1])[i].received_at().timestamp_millis()
            );
        }

        // We check that if we load 5 more, we get the oldest 5.
        let batch = envelope_store
            .delete_batch(own_key, sampling_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.len(), 5);
        for (i, extracted_envelope) in batch.envelopes.iter().enumerate() {
            assert_eq!(
                extracted_envelope.received_at().timestamp_millis(),
                (&batches[0])[i].received_at().timestamp_millis()
            );
        }

        // Store is empty.
        assert!(envelope_store
            .delete_batch(own_key, sampling_key)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_insert_and_get_project_keys_pairs() {
        let db = setup_db(true).await;
        let mut envelope_store = SqliteEnvelopeStore::new(db, Duration::from_millis(100));

        let own_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let sampling_key = ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap();

        // We insert 10 envelopes.
        let envelopes = mock_envelopes(2);
        envelope_store
            .insert_batch(
                envelopes
                    .into_iter()
                    .map(|e| DatabaseEnvelope::try_from(e.as_ref()).unwrap())
                    .collect::<Vec<_>>()
                    .try_into()
                    .unwrap(),
            )
            .await
            .unwrap();

        // We check that we get back only one pair of project keys, since all envelopes have the
        // same pair.
        let project_key_pairs = envelope_store.project_key_pairs().await.unwrap();
        assert_eq!(project_key_pairs.len(), 1);
        assert_eq!(
            project_key_pairs.into_iter().last().unwrap(),
            ProjectKeyPair::new(own_key, sampling_key)
        );
    }

    #[tokio::test]
    async fn test_estimate_disk_usage() {
        let db = setup_db(true).await;
        let mut store = SqliteEnvelopeStore::new(db.clone(), Duration::from_millis(1));
        let disk_usage = DiskUsage::prepare(db, Duration::from_millis(1))
            .await
            .unwrap();

        // We read the disk usage without envelopes stored.
        let usage_1 = disk_usage.usage();
        assert!(usage_1 > 0);

        // We write 10 envelopes to increase the disk usage.
        let envelopes = mock_envelopes(10);
        store
            .insert_batch(
                envelopes
                    .into_iter()
                    .map(|e| DatabaseEnvelope::try_from(e.as_ref()).unwrap())
                    .collect::<Vec<_>>()
                    .try_into()
                    .unwrap(),
            )
            .await
            .unwrap();

        // We wait for the refresh timeout of the disk usage task.
        sleep(Duration::from_millis(2)).await;

        // We now expect to read more disk usage because of the 10 elements.
        let usage_2 = disk_usage.usage();
        assert!(usage_2 >= usage_1);
    }

    #[tokio::test]
    async fn test_total_count() {
        let db = setup_db(true).await;
        let mut store = SqliteEnvelopeStore::new(db.clone(), Duration::from_millis(1));

        let envelopes = mock_envelopes(10);
        store
            .insert_batch(
                envelopes
                    .iter()
                    .map(|e| DatabaseEnvelope::try_from(e.as_ref()).unwrap())
                    .collect::<Vec<_>>()
                    .try_into()
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(store.total_count().await.unwrap(), envelopes.len() as u64);
    }
}
