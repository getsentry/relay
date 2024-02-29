//! Contains helper utils which help to manage the spooler and spooled data.

use std::path::PathBuf;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};

use crate::service::create_runtime;
use crate::services::spooler::{sql, BufferError};

/// Truncates the spool file deleting all the persisted on-disk data.
pub fn truncate(path: PathBuf) -> Result<(), BufferError> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .journal_mode(SqliteJournalMode::Wal)
        .create_if_missing(true);

    let rt = create_runtime("truncator", 1);

    rt.block_on(async move {
        let db = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .map_err(BufferError::SqlxSetupFailed)?;

        sql::truncate()
            .execute(&db)
            .await
            .map_err(BufferError::DeleteFailed)?;

        Ok::<(), BufferError>(())
    })?;

    Ok(())
}
