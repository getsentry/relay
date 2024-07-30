use std::error::Error;
use std::iter;
use std::path::Path;
use std::pin::pin;

use futures::stream::StreamExt;
use sqlx::migrate::MigrateError;
use sqlx::query::Query;
use sqlx::sqlite::{
    SqliteArguments, SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions,
    SqliteRow, SqliteSynchronous,
};
use sqlx::{Pool, QueryBuilder, Row, Sqlite};
use tokio::fs::DirBuilder;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;

use crate::extractors::StartTime;
use crate::services::buffer::envelope_stack::sqlite::SqliteEnvelopeStackError;
use crate::Envelope;

struct InsertEnvelope {
    received_at: i64,
    own_key: ProjectKey,
    sampling_key: ProjectKey,
    encoded_envelope: Vec<u8>,
}

impl<'a> From<&'a Envelope> for InsertEnvelope {
    fn from(value: &'a Envelope) -> Self {
        let own_key = value.meta().public_key();
        let sampling_key = value.sampling_key().unwrap_or(own_key);

        InsertEnvelope {
            received_at: value.received_at().timestamp_millis(),
            own_key,
            sampling_key,
            encoded_envelope: value.to_vec().unwrap(),
        }
    }
}

/// An error returned when doing an operation on [`SqliteEnvelopeStore`].
#[derive(Debug, thiserror::Error)]
pub enum SqliteEnvelopeStoreError {
    #[error("failed to setup the database: {0}")]
    SqlxSetupFailed(sqlx::Error),

    #[error("failed to create the spool file: {0}")]
    FileSetupError(std::io::Error),

    #[error("an error occurred while spooling envelopes: {0}")]
    SpoolingError(sqlx::Error),

    #[error("no file path for the spool was provided")]
    NoFilePath,

    #[error("error during the migration of the database: {0}")]
    MigrationError(MigrateError),
}

#[derive(Debug, Clone)]
pub struct SqliteEnvelopeStore {
    db: Pool<Sqlite>,
    max_disk_size: usize,
}

impl SqliteEnvelopeStore {
    pub fn new(db: Pool<Sqlite>, max_disk_size: usize) -> Self {
        Self { db, max_disk_size }
    }

    /// Prepares the [`SqliteEnvelopeStore`] by running all the necessary migrations and preparing
    /// the folders where data will be stored.
    pub async fn prepare(config: &Config) -> Result<SqliteEnvelopeStore, SqliteEnvelopeStoreError> {
        // If no path is provided, we can't do disk spooling.
        let Some(path) = config.spool_envelopes_path() else {
            return Err(SqliteEnvelopeStoreError::NoFilePath);
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
            .map_err(SqliteEnvelopeStoreError::SqlxSetupFailed)?;

        Ok(SqliteEnvelopeStore {
            db,
            max_disk_size: config.spool_envelopes_max_disk_size(),
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

    pub async fn insert_many(
        &mut self,
        envelopes: impl Iterator<Item = InsertEnvelope>,
    ) -> Result<(), SqliteEnvelopeStoreError> {
        if let Err(err) = build_insert_many_envelopes(envelopes)
            .build()
            .execute(&self.db)
            .await
        {
            relay_log::error!(
                error = &err as &dyn Error,
                "failed to spool envelopes to disk",
            );

            return Err(SqliteEnvelopeStoreError::SpoolingError(err));
        }

        Ok(())
    }

    pub async fn delete_many(
        &mut self,
        own_key: ProjectKey,
        sampling_key: ProjectKey,
        limit: i64,
    ) -> Result<Vec<Box<Envelope>>, SqliteEnvelopeStoreError> {
        let envelopes = build_delete_and_fetch_many_envelopes(own_key, sampling_key, limit)
            .fetch(&self.db)
            .peekable();

        let mut envelopes = pin!(envelopes);
        if envelopes.as_mut().peek().await.is_none() {
            return Ok(vec![]);
        }

        let mut extracted_envelopes = Vec::with_capacity(limit as usize);
        while let Some(envelope) = envelopes.as_mut().next().await {
            let envelope = match envelope {
                Ok(envelope) => envelope,
                Err(err) => {
                    relay_log::error!(
                        error = &err as &dyn Error,
                        "failed to unspool the envelopes from the disk",
                    );
                    continue;
                }
            };

            match extract_envelope(envelope) {
                Ok(envelope) => {
                    extracted_envelopes.push(envelope);
                }
                Err(err) => {
                    relay_log::error!(
                        error = &err as &dyn Error,
                        "failed to extract the envelope unspooled from disk",
                    )
                }
            }
        }

        // We sort envelopes by `received_at`.
        // Unfortunately we have to do this because SQLite `DELETE` with `RETURNING` doesn't
        // return deleted rows in a specific order.
        extracted_envelopes.sort_by_key(|a| a.received_at());

        Ok(extracted_envelopes)
    }

    pub async fn project_keys_pairs(
        &self,
    ) -> Result<impl Iterator<Item = (String, String)>, SqliteEnvelopeStoreError> {
        // TODO: implement.
        Ok(iter::empty())
    }

    pub async fn used_size(&self) -> Result<i64, SqliteEnvelopeStoreError> {
        // TODO: implement.
        Ok(10)
    }
}

/// Deserializes an [`Envelope`] from a database row.
fn extract_envelope(row: SqliteRow) -> Result<Box<Envelope>, SqliteEnvelopeStackError> {
    let envelope_row: Vec<u8> = row
        .try_get("envelope")
        .map_err(|_| SqliteEnvelopeStackError::Empty)?;
    let envelope_bytes = bytes::Bytes::from(envelope_row);
    let mut envelope =
        Envelope::parse_bytes(envelope_bytes).map_err(|_| SqliteEnvelopeStackError::Empty)?;

    let received_at: i64 = row
        .try_get("received_at")
        .map_err(|_| SqliteEnvelopeStackError::Empty)?;
    let start_time = StartTime::from_timestamp_millis(received_at as u64);

    envelope.set_start_time(start_time.into_inner());

    Ok(envelope)
}

/// Builds a query that inserts many [`Envelope`]s in the database.
fn build_insert_many_envelopes<'a>(
    envelopes: impl Iterator<Item = InsertEnvelope>,
) -> QueryBuilder<'a, Sqlite> {
    let mut builder: QueryBuilder<Sqlite> =
        QueryBuilder::new("INSERT INTO envelopes (received_at, own_key, sampling_key, envelope) ");

    builder.push_values(envelopes, |mut b, envelope| {
        b.push_bind(envelope.received_at)
            .push_bind(envelope.own_key.to_string())
            .push_bind(envelope.sampling_key.to_string())
            .push_bind(envelope.encoded_envelope);
    });

    builder
}

/// Builds a query that deletes many [`Envelope`] from the database.
pub fn build_delete_and_fetch_many_envelopes<'a>(
    own_key: ProjectKey,
    project_key: ProjectKey,
    batch_size: i64,
) -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query(
        "DELETE FROM
            envelopes
         WHERE id IN (SELECT id FROM envelopes WHERE own_key = ? AND sampling_key = ?
            ORDER BY received_at DESC LIMIT ?)
         RETURNING
            received_at, own_key, sampling_key, envelope",
    )
    .bind(own_key.to_string())
    .bind(project_key.to_string())
    .bind(batch_size)
}

/// Creates a query which fetches the number of used database pages multiplied by the page size.
///
/// This info used to estimate the current allocated database size.
pub fn estimate_size<'a>() -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query(
        r#"SELECT (page_count - freelist_count) * page_size as size FROM pragma_page_count(), pragma_freelist_count(), pragma_page_size();"#,
    )
}

/// Returns the query to select all the unique combinations of own and sampling keys.
pub fn get_keys<'a>() -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query("SELECT DISTINCT own_key, sampling_key FROM envelopes;")
}
