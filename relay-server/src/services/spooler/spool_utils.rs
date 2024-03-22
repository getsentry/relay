//! Contains helper utils which help to manage the spooler and spooled data.

use std::path::PathBuf;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};

use crate::service::create_runtime;
use crate::services::spooler::{sql, BufferError};

/// Truncates the spool file deleting all the persisted on-disk data.
///
/// Returns the number of deleted envelopes when run successfully.
pub fn truncate(path: &PathBuf) -> Result<u64, BufferError> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .journal_mode(SqliteJournalMode::Wal);

    let rt = create_runtime("truncator", 1);

    let result = rt.block_on(async move {
        let db = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .map_err(BufferError::SqlxSetupFailed)?;

        let result = sql::truncate()
            .execute(&db)
            .await
            .map_err(BufferError::DeleteFailed)?;

        Ok::<u64, BufferError>(result.rows_affected())
    })?;

    Ok(result)
}
