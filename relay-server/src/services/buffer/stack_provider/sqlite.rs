use crate::services::buffer::envelope_stack::StackProvider;
use crate::{Envelope, EnvelopeStack, SqliteEnvelopeStack};
use relay_config::Config;
use sqlx::sqlite::{
    SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
};
use sqlx::{Pool, Sqlite};
use std::path::Path;
use std::sync::Arc;
use tokio::fs::DirBuilder;

#[derive(Debug, thiserror::Error)]
pub enum SqliteStackProviderError {
    #[error("failed to setup the database: {0}")]
    SqlxSetupFailed(sqlx::Error),

    #[error("failed to create the spool file: {0}")]
    FileSetupError(std::io::Error),

    #[error("the path to which the database is configured doesn't exist")]
    MissingPath,
}

pub struct SqliteStackProvider {
    db: Pool<Sqlite>,
    disk_batch_size: usize,
    max_batches: usize,
}

impl SqliteStackProvider {
    /// Creates a new [`SqliteStackProvider`] from the provided path to the SQLite database file.
    pub async fn create(config: Arc<Config>) -> Result<Self, SqliteStackProviderError> {
        // TODO: error handling
        let db = Self::prepare_disk(config.clone()).await?;
        Ok(Self {
            db,
            disk_batch_size: 100, // TODO: put in config
            max_batches: 2,       // TODO: put in config
        })
    }

    /// Set up the database and return the current number of envelopes.
    ///
    /// The directories and spool file will be created if they don't already
    /// exist.
    async fn setup(path: &Path) -> Result<(), SqliteStackProviderError> {
        Self::create_spool_directory(path).await?;

        let options = SqliteConnectOptions::new()
            .filename(path)
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true);

        let db = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .map_err(SqliteStackProviderError::SqlxSetupFailed)?;

        sqlx::migrate!("../migrations").run(&db).await.unwrap();

        Ok(())
    }

    /// Creates the directories for the spool file.
    async fn create_spool_directory(path: &Path) -> Result<(), SqliteStackProviderError> {
        let Some(parent) = path.parent() else {
            return Ok(());
        };

        if !parent.as_os_str().is_empty() && !parent.exists() {
            relay_log::debug!("creating directory for spooling file: {}", parent.display());
            DirBuilder::new()
                .recursive(true)
                .create(&parent)
                .await
                .map_err(SqliteStackProviderError::FileSetupError)?;
        }

        Ok(())
    }

    /// Prepares the disk for reading and writing data.
    async fn prepare_disk(config: Arc<Config>) -> Result<Pool<Sqlite>, SqliteStackProviderError> {
        let Some(path) = config.spool_envelopes_path() else {
            return Err(SqliteStackProviderError::MissingPath);
        };

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

        SqlitePoolOptions::new()
            .max_connections(config.spool_envelopes_max_connections())
            .min_connections(config.spool_envelopes_min_connections())
            .connect_with(options)
            .await
            .map_err(SqliteStackProviderError::SqlxSetupFailed)
    }
}

impl StackProvider for SqliteStackProvider {
    type Stack = SqliteEnvelopeStack;

    fn create_stack(&self, envelope: Box<Envelope>) -> Self::Stack {
        let own_key = envelope.meta().public_key();
        // TODO: start loading from disk the initial batch of envelopes.
        SqliteEnvelopeStack::new(
            self.db.clone(),
            self.disk_batch_size,
            self.max_batches,
            own_key,
            envelope.sampling_key().unwrap_or(own_key),
        )
    }
}
