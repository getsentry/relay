#[cfg(test)]
pub mod utils {
    use crate::envelope::{Item, ItemType};
    use crate::extractors::RequestMeta;
    use crate::Envelope;
    use chrono::{DateTime, Utc};
    use relay_base_schema::project::ProjectKey;
    use relay_event_schema::protocol::{EventId, TraceId};
    use relay_sampling::DynamicSamplingContext;
    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
    use sqlx::{Pool, Sqlite};
    use std::collections::BTreeMap;
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

    pub fn request_meta() -> RequestMeta {
        let dsn = "https://a94ae32be2584e0bbd7a4cbb95971fee:@sentry.io/42"
            .parse()
            .unwrap();

        RequestMeta::new(dsn)
    }

    pub fn mock_envelope(received_at: DateTime<Utc>) -> Box<Envelope> {
        let event_id = EventId::new();
        let mut envelope = Envelope::from_request(Some(event_id), request_meta());

        let dsc = DynamicSamplingContext {
            trace_id: TraceId("67e5504410b1426f9247bb680e5fe0c8".into()),
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
        envelope.set_received_at(received_at);

        envelope.add_item(Item::new(ItemType::Transaction));

        envelope
    }

    #[allow(clippy::vec_box)]
    pub fn mock_envelopes(count: usize) -> Vec<Box<Envelope>> {
        let now = Utc::now();
        (0..count)
            .map(|i| mock_envelope(now - chrono::Duration::seconds((count - i) as i64)))
            .collect()
    }
}
