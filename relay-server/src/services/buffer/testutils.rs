#[cfg(test)]
pub mod utils {
    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
    use sqlx::{Pool, Sqlite};
    use tokio::fs::DirBuilder;
    use uuid::Uuid;

    /// Sets up a temporary SQLite database for testing purposes.
    pub async fn setup_db(run_migrations: bool) -> Pool<Sqlite> {
        let path = std::env::temp_dir().join(Uuid::new_v4().to_string());

        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                relay_log::debug!("creating directory for spooling file: {}", parent.display());
                DirBuilder::new()
                    .recursive(true)
                    .create(&parent)
                    .await
                    .unwrap();
            }
        }

        let options = SqliteConnectOptions::new()
            .filename(&path)
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true);

        let db = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .unwrap();

        if run_migrations {
            sqlx::migrate!("../migrations").run(&db).await.unwrap();
        }

        db
    }
}
