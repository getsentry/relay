use std::error::Error;
use std::path::Path;
use std::pin::pin;

use futures::stream::StreamExt;
use hashbrown::HashSet;
use relay_base_schema::project::{ParseProjectKeyError, ProjectKey};
use relay_config::Config;
use sqlx::migrate::MigrateError;
use sqlx::query::Query;
use sqlx::sqlite::{
    SqliteArguments, SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions,
    SqliteRow, SqliteSynchronous,
};
use sqlx::{Pool, QueryBuilder, Row, Sqlite};
use tokio::fs::DirBuilder;

use crate::envelope::EnvelopeError;
use crate::extractors::StartTime;
use crate::Envelope;

/// Struct that contains all the fields of an [`Envelope`] that are mapped to the database columns.
pub struct InsertEnvelope {
    received_at: i64,
    own_key: ProjectKey,
    sampling_key: ProjectKey,
    encoded_envelope: Vec<u8>,
}

impl<'a> TryFrom<&'a Envelope> for InsertEnvelope {
    type Error = EnvelopeError;

    fn try_from(value: &'a Envelope) -> Result<Self, Self::Error> {
        let own_key = value.meta().public_key();
        let sampling_key = value.sampling_key().unwrap_or(own_key);

        let encoded_envelope = match value.to_vec() {
            Ok(encoded_envelope) => encoded_envelope,
            Err(err) => {
                relay_log::error!(
                    error = &err as &dyn Error,
                    own_key = own_key.to_string(),
                    sampling_key = sampling_key.to_string(),
                    "failed to serialize envelope",
                );

                return Err(err);
            }
        };

        Ok(InsertEnvelope {
            received_at: value.received_at().timestamp_millis(),
            own_key,
            sampling_key,
            encoded_envelope,
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

/// Enum representing the order in which [`Envelope`]s are fetched or deleted from the
/// database.
#[derive(Debug, Copy, Clone)]
pub enum EnvelopesOrder {
    MostRecent,
    Oldest,
}

/// Struct that offers access to a SQLite-based store of [`Envelope`]s.
///
/// The goal of this struct is to hide away all the complexity of dealing with the database for
/// reading and writing envelopes.
#[derive(Debug, Clone)]
pub struct SqliteEnvelopeStore {
    db: Pool<Sqlite>,
}

impl SqliteEnvelopeStore {
    /// Initializes the [`SqliteEnvelopeStore`] with a supplied [`Pool`].
    pub fn new(db: Pool<Sqlite>) -> Self {
        Self { db }
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
            // Auto-vacuum does not defragment the database nor repack individual database pages the way that the VACUUM command does.
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

        Ok(SqliteEnvelopeStore { db })
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
    pub async fn insert_many(
        &self,
        envelopes: impl IntoIterator<Item = InsertEnvelope>,
    ) -> Result<(), SqliteEnvelopeStoreError> {
        if let Err(err) = build_insert_many_envelopes(envelopes.into_iter())
            .build()
            .execute(&self.db)
            .await
        {
            relay_log::error!(
                error = &err as &dyn Error,
                "failed to spool envelopes to disk",
            );

            return Err(SqliteEnvelopeStoreError::WriteError(err));
        }

        Ok(())
    }

    /// Deletes and returns at most `limit` [`Envelope`]s from the database.
    pub async fn delete_many(
        &self,
        own_key: ProjectKey,
        sampling_key: ProjectKey,
        limit: i64,
        envelopes_order: EnvelopesOrder,
    ) -> Result<Vec<Box<Envelope>>, SqliteEnvelopeStoreError> {
        let query = match envelopes_order {
            EnvelopesOrder::MostRecent => {
                build_delete_and_fetch_many_recent_envelopes(own_key, sampling_key, limit)
            }
            EnvelopesOrder::Oldest => {
                build_delete_and_fetch_many_old_envelopes(own_key, sampling_key, limit)
            }
        };
        let envelopes = query.fetch(&self.db).peekable();

        let mut envelopes = pin!(envelopes);
        if envelopes.as_mut().peek().await.is_none() {
            return Ok(vec![]);
        }

        let mut extracted_envelopes = Vec::with_capacity(limit as usize);
        let mut db_error = None;
        while let Some(envelope) = envelopes.as_mut().next().await {
            let envelope = match envelope {
                Ok(envelope) => envelope,
                Err(err) => {
                    relay_log::error!(
                        error = &err as &dyn Error,
                        "failed to unspool the envelopes from the disk",
                    );
                    db_error = Some(err);

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

        // If we have no envelopes and there was at least one error, we signal total failure to the
        // caller. We do this under the assumption that if there are envelopes and failures, we are
        // fine with just logging the failure and not failing completely.
        if extracted_envelopes.is_empty() {
            if let Some(db_error) = db_error {
                return Err(SqliteEnvelopeStoreError::FetchError(db_error));
            }
        }

        // We sort envelopes by `received_at`.
        // Unfortunately we have to do this because SQLite `DELETE` with `RETURNING` doesn't
        // return deleted rows in a specific order.
        extracted_envelopes.sort_by_key(|a| a.received_at());

        Ok(extracted_envelopes)
    }

    /// Returns a set of project key pairs, representing all the unique combinations of
    /// `own_key` and `project_key` that are found in the database.
    pub async fn project_key_pairs(
        &self,
    ) -> Result<HashSet<(ProjectKey, ProjectKey)>, SqliteEnvelopeStoreError> {
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

    /// Returns an approximate measure of the size of the database.
    pub async fn used_size(&self) -> Result<i64, SqliteEnvelopeStoreError> {
        build_estimate_size()
            .fetch_one(&self.db)
            .await
            .and_then(|r| r.try_get(0))
            .map_err(SqliteEnvelopeStoreError::FileSizeReadFailed)
    }
}

/// Deserializes an [`Envelope`] from a database row.
fn extract_envelope(row: SqliteRow) -> Result<Box<Envelope>, SqliteEnvelopeStoreError> {
    let envelope_row: Vec<u8> = row
        .try_get("envelope")
        .map_err(SqliteEnvelopeStoreError::FetchError)?;
    let envelope_bytes = bytes::Bytes::from(envelope_row);
    let mut envelope = Envelope::parse_bytes(envelope_bytes)
        .map_err(|_| SqliteEnvelopeStoreError::EnvelopeExtractionError)?;

    let received_at: i64 = row
        .try_get("received_at")
        .map_err(SqliteEnvelopeStoreError::FetchError)?;
    let start_time = StartTime::from_timestamp_millis(received_at as u64);

    envelope.set_start_time(start_time.into_inner());

    Ok(envelope)
}

/// Deserializes a pair of [`ProjectKey`] from the database.
fn extract_project_key_pair(
    row: SqliteRow,
) -> Result<(ProjectKey, ProjectKey), SqliteEnvelopeStoreError> {
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
        (Ok(own_key), Ok(sampling_key)) => Ok((own_key, sampling_key)),
        // Report the first found error.
        (Err(err), _) | (_, Err(err)) => {
            relay_log::error!("failed to extract a queue key from the spool record: {err}");

            Err(err)
        }
    }
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

/// Builds a query that deletes many new [`Envelope`]s from the database.
pub fn build_delete_and_fetch_many_recent_envelopes<'a>(
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

/// Builds a query that deletes many old [`Envelope`]s from the database.
pub fn build_delete_and_fetch_many_old_envelopes<'a>(
    own_key: ProjectKey,
    project_key: ProjectKey,
    batch_size: i64,
) -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query(
        "DELETE FROM
            envelopes
         WHERE id IN (SELECT id FROM envelopes WHERE own_key = ? AND sampling_key = ?
            ORDER BY received_at ASC LIMIT ?)
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
pub fn build_estimate_size<'a>() -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query(
        r#"SELECT (page_count - freelist_count) * page_size as size FROM pragma_page_count(), pragma_freelist_count(), pragma_page_size();"#,
    )
}

/// Returns the query to select all the unique combinations of own and sampling keys.
pub fn build_get_project_key_pairs<'a>() -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query("SELECT DISTINCT own_key, sampling_key FROM envelopes;")
}

#[cfg(test)]
mod tests {

    use hashbrown::HashSet;
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};
    use uuid::Uuid;

    use relay_base_schema::project::ProjectKey;
    use relay_event_schema::protocol::EventId;
    use relay_sampling::DynamicSamplingContext;

    use super::*;
    use crate::envelope::{Envelope, Item, ItemType};
    use crate::extractors::RequestMeta;
    use crate::services::buffer::testutils::utils::setup_db;

    fn request_meta() -> RequestMeta {
        let dsn = "https://a94ae32be2584e0bbd7a4cbb95971fee:@sentry.io/42"
            .parse()
            .unwrap();

        RequestMeta::new(dsn)
    }

    fn mock_envelope(instant: Instant) -> Box<Envelope> {
        let event_id = EventId::new();
        let mut envelope = Envelope::from_request(Some(event_id), request_meta());

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
            release: Some("1.1.1".to_string()),
            user: Default::default(),
            replay_id: None,
            environment: None,
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: Some(true),
            other: BTreeMap::new(),
        };

        envelope.set_dsc(dsc);
        envelope.set_start_time(instant);

        envelope.add_item(Item::new(ItemType::Transaction));

        envelope
    }

    #[allow(clippy::vec_box)]
    fn mock_envelopes(count: usize) -> Vec<Box<Envelope>> {
        let instant = Instant::now();
        (0..count)
            .map(|i| mock_envelope(instant - Duration::from_secs((count - i) as u64)))
            .collect()
    }

    #[tokio::test]
    async fn test_insert_and_delete_envelopes() {
        let db = setup_db(true).await;
        let envelope_store = SqliteEnvelopeStore::new(db);

        let own_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let sampling_key = ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap();

        // We insert 10 envelopes.
        let envelopes = mock_envelopes(10);
        let envelope_ids: HashSet<EventId> =
            envelopes.iter().filter_map(|e| e.event_id()).collect();
        assert!(envelope_store
            .insert_many(envelopes.iter().map(|e| e.as_ref().try_into().unwrap()))
            .await
            .is_ok());

        // We check that if we load more than the limit, we still get back at most 10.
        let extracted_envelopes = envelope_store
            .delete_many(own_key, sampling_key, 15, EnvelopesOrder::MostRecent)
            .await
            .unwrap();
        assert_eq!(envelopes.len(), 10);
        for envelope in extracted_envelopes {
            assert!(envelope_ids.contains(&envelope.event_id().unwrap()));
        }
    }

    #[tokio::test]
    async fn test_insert_and_get_project_keys_pairs() {
        let db = setup_db(true).await;
        let envelope_store = SqliteEnvelopeStore::new(db);

        let own_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let sampling_key = ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap();

        // We insert 10 envelopes.
        let envelopes = mock_envelopes(2);
        assert!(envelope_store
            .insert_many(envelopes.iter().map(|e| e.as_ref().try_into().unwrap()))
            .await
            .is_ok());

        // We check that we get back only one pair of project keys, since all envelopes have the
        // same pair.
        let project_key_pairs = envelope_store.project_key_pairs().await.unwrap();
        assert_eq!(project_key_pairs.len(), 1);
        assert_eq!(
            project_key_pairs.into_iter().last().unwrap(),
            (own_key, sampling_key)
        );
    }
}
