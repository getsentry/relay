use crate::envelope::Envelope;
use crate::extractors::StartTime;
use crate::services::spooler::envelope_stack::EnvelopeStack;
use futures::StreamExt;
use relay_base_schema::project::ProjectKey;
use sqlx::query::Query;
use sqlx::sqlite::{SqliteArguments, SqliteRow};
use sqlx::{Pool, QueryBuilder, Row, Sqlite};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::error::Error;
use std::pin::pin;

struct OrderedEnvelope(Box<Envelope>);

impl Eq for OrderedEnvelope {}

impl PartialEq<Self> for OrderedEnvelope {
    fn eq(&self, other: &Self) -> bool {
        matches!(self.cmp(other), Ordering::Equal)
    }
}

impl PartialOrd<Self> for OrderedEnvelope {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedEnvelope {
    fn cmp(&self, other: &Self) -> Ordering {
        received_at(&other.0).cmp(&received_at(&self.0))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SQLiteEnvelopeStackError {
    #[error("the stack is empty")]
    Empty,

    #[error("a database error occurred")]
    DatabaseError(#[from] sqlx::Error),
}

pub struct SQLiteEnvelopeStack {
    db: Pool<Sqlite>,
    spool_threshold: usize,
    own_key: ProjectKey,
    sampling_key: ProjectKey,
    buffer: VecDeque<Box<Envelope>>,
}

impl SQLiteEnvelopeStack {
    #[allow(dead_code)]
    pub fn new(
        db: Pool<Sqlite>,
        spool_threshold: usize,
        own_key: ProjectKey,
        sampling_key: ProjectKey,
    ) -> Self {
        Self {
            db,
            spool_threshold,
            own_key,
            sampling_key,
            buffer: VecDeque::with_capacity(spool_threshold),
        }
    }

    #[allow(dead_code)]
    pub async fn prepare(
        db: Pool<Sqlite>,
        spool_threshold: usize,
        own_key: ProjectKey,
        sampling_key: ProjectKey,
    ) -> Result<(), SQLiteEnvelopeStackError> {
        let mut stack = Self::new(db, spool_threshold, own_key, sampling_key);
        stack.load_from_disk().await?;

        Ok(())
    }

    fn above_spool_threshold(&self) -> bool {
        self.buffer.len() + 1 > self.spool_threshold
    }

    fn below_unspool_threshold(&self) -> bool {
        self.buffer.is_empty()
    }

    fn disk_batch_size(&self) -> usize {
        self.spool_threshold / 2
    }

    async fn spool_to_disk(&mut self) -> Result<(), SQLiteEnvelopeStackError> {
        if self.disk_batch_size() == 0 {
            return Ok(());
        }

        // TODO: we can make a custom iterator to consume back elements until threshold to avoid
        //  allocating a vector.
        let mut envelopes = Vec::with_capacity(self.disk_batch_size());
        for _ in 0..self.disk_batch_size() {
            let Some(value) = self.buffer.pop_back() else {
                break;
            };

            envelopes.push(value);
        }
        if envelopes.is_empty() {
            return Ok(());
        }

        let insert_envelopes = envelopes.iter().map(|e| InsertEnvelope {
            received_at: received_at(e),
            own_key: self.own_key,
            sampling_key: self.sampling_key,
            encoded_envelope: e.to_vec().unwrap(),
        });

        if let Err(err) = build_insert_many_envelopes(insert_envelopes)
            .build()
            .execute(&self.db)
            .await
        {
            relay_log::error!(
                error = &err as &dyn Error,
                "failed to spool envelopes to disk",
            );

            // When early return here, we are acknowledging that the elements that we popped from
            // the buffer are lost. We are doing this on purposes, since if we were to have a
            // database corruption during runtime, and we were to put the values back into the buffer
            // we will end up with an infinite cycle.
            return Err(SQLiteEnvelopeStackError::DatabaseError(err));
        }

        Ok(())
    }

    async fn load_from_disk(&mut self) -> Result<(), SQLiteEnvelopeStackError> {
        let envelopes = build_delete_and_fetch_many_envelopes(
            self.own_key,
            self.sampling_key,
            self.disk_batch_size() as i64,
        )
        .fetch(&self.db)
        .peekable();

        let mut envelopes = pin!(envelopes);
        if envelopes.as_mut().peek().await.is_none() {
            return Ok(());
        }

        // We use a priority map to order envelopes that are deleted from the database.
        // Unfortunately we have to do this because SQLite `DELETE` with `RETURNING` doesn't
        // return deleted rows in a specific order.
        let mut ordered_envelopes = BinaryHeap::with_capacity(self.disk_batch_size());
        while let Some(envelope) = envelopes.as_mut().next().await {
            let envelope = match envelope {
                Ok(envelope) => envelope,
                Err(err) => {
                    relay_log::error!(
                        error = &err as &dyn Error,
                        "failed to unspool the envelopes from the disk",
                    );

                    // We early return under the assumption that the stream, if it contains an
                    // error, it means that the query failed.
                    return Err(SQLiteEnvelopeStackError::DatabaseError(err));
                }
            };

            match self.extract_envelope(envelope) {
                Ok(envelope) => {
                    ordered_envelopes.push(OrderedEnvelope(envelope));
                }
                Err(err) => {
                    relay_log::error!(
                        error = &err as &dyn Error,
                        "failed to extract the envelope unspooled from disk",
                    )
                }
            }
        }

        for envelope in ordered_envelopes.into_sorted_vec() {
            // We push in the back of the buffer, since we still want to give priority to
            // incoming envelopes that have a more recent timestamp.
            self.buffer.push_back(envelope.0)
        }

        Ok(())
    }

    fn extract_envelope(&self, row: SqliteRow) -> Result<Box<Envelope>, SQLiteEnvelopeStackError> {
        let envelope_row: Vec<u8> = row
            .try_get("envelope")
            .map_err(|_| SQLiteEnvelopeStackError::Empty)?;
        let envelope_bytes = bytes::Bytes::from(envelope_row);
        let mut envelope =
            Envelope::parse_bytes(envelope_bytes).map_err(|_| SQLiteEnvelopeStackError::Empty)?;

        let received_at: i64 = row
            .try_get("received_at")
            .map_err(|_| SQLiteEnvelopeStackError::Empty)?;
        let start_time = StartTime::from_timestamp_millis(received_at as u64);

        envelope.set_start_time(start_time.into_inner());

        Ok(envelope)
    }
}

impl EnvelopeStack for SQLiteEnvelopeStack {
    type Error = SQLiteEnvelopeStackError;

    async fn push(&mut self, envelope: Box<Envelope>) -> Result<(), Self::Error> {
        if self.above_spool_threshold() {
            self.spool_to_disk().await?;
        }

        self.buffer.push_front(envelope);

        Ok(())
    }

    async fn peek(&mut self) -> Result<&Box<Envelope>, Self::Error> {
        if self.below_unspool_threshold() {
            self.load_from_disk().await?
        }

        self.buffer.front().ok_or(Self::Error::Empty)
    }

    async fn pop(&mut self) -> Result<Box<Envelope>, Self::Error> {
        if self.below_unspool_threshold() {
            self.load_from_disk().await?
        }

        self.buffer.pop_front().ok_or(Self::Error::Empty)
    }
}

struct InsertEnvelope {
    received_at: i64,
    own_key: ProjectKey,
    sampling_key: ProjectKey,
    encoded_envelope: Vec<u8>,
}

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

fn received_at(envelope: &Envelope) -> i64 {
    relay_common::time::instant_to_date_time(envelope.meta().start_time()).timestamp_millis()
}

#[cfg(test)]
mod tests {
    use crate::envelope::{Envelope, Item, ItemType};
    use crate::extractors::RequestMeta;
    use crate::services::spooler::envelope_stack::sqlite::{
        SQLiteEnvelopeStack, SQLiteEnvelopeStackError,
    };
    use crate::services::spooler::envelope_stack::EnvelopeStack;
    use relay_base_schema::project::ProjectKey;
    use relay_event_schema::protocol::EventId;
    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
    use sqlx::{Pool, Sqlite};
    use std::path::Path;
    use std::time::{Duration, Instant};
    use tokio::fs::DirBuilder;
    use uuid::Uuid;

    fn request_meta() -> RequestMeta {
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        RequestMeta::new(dsn)
    }

    fn mock_envelope(instant: Instant) -> Box<Envelope> {
        let event_id = EventId::new();
        let mut envelope = Envelope::from_request(Some(event_id), request_meta());
        envelope.set_start_time(instant);

        let mut item = Item::new(ItemType::Attachment);
        item.set_filename("item");
        envelope.add_item(item);

        envelope
    }

    #[allow(clippy::vec_box)]
    fn mock_envelopes(count: usize) -> Vec<Box<Envelope>> {
        let instant = Instant::now();
        (0..count)
            .map(|i| mock_envelope(instant - Duration::from_secs((count - i) as u64)))
            .collect()
    }

    async fn setup_db(run_migrations: bool) -> Pool<Sqlite> {
        let path = std::env::temp_dir().join(Uuid::new_v4().to_string());

        create_spool_directory(&path).await;

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

    async fn create_spool_directory(path: &Path) {
        let Some(parent) = path.parent() else {
            return;
        };

        if !parent.as_os_str().is_empty() && !parent.exists() {
            relay_log::debug!("creating directory for spooling file: {}", parent.display());
            DirBuilder::new()
                .recursive(true)
                .create(&parent)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn test_push_when_db_is_not_valid() {
        let db = setup_db(false).await;
        let mut stack = SQLiteEnvelopeStack::new(
            db,
            3,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        let envelopes = mock_envelopes(3);

        // We push the 3 envelopes without errors because they are below the threshold.
        for envelope in envelopes.clone() {
            assert!(stack.push(envelope).await.is_ok());
        }

        // We push 1 more envelope which results in spooling, which fails because of a database
        // problem.
        let envelope = mock_envelope(Instant::now());
        assert!(matches!(
            stack.push(envelope).await,
            Err(SQLiteEnvelopeStackError::DatabaseError(_))
        ));

        // Now one element should have been popped because the stack tried to spool it and the
        // previous, insertion failed, so we have only 2 elements in the stack, we can now add a
        // new one and we will succeed.
        let envelope = mock_envelope(Instant::now());
        assert!(stack.push(envelope).await.is_ok());
        assert_eq!(stack.buffer.len(), 3);
    }

    #[tokio::test]
    async fn test_pop_when_db_is_not_valid() {
        let db = setup_db(false).await;
        let mut stack = SQLiteEnvelopeStack::new(
            db,
            3,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        // We pop with an invalid db.
        assert!(matches!(
            stack.pop().await,
            Err(SQLiteEnvelopeStackError::DatabaseError(_))
        ));
    }

    #[tokio::test]
    async fn test_pop_when_stack_is_empty() {
        let db = setup_db(true).await;
        let mut stack = SQLiteEnvelopeStack::new(
            db,
            3,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        // We pop with no elements.
        assert!(matches!(
            stack.pop().await,
            Err(SQLiteEnvelopeStackError::Empty)
        ));
    }

    #[tokio::test]
    async fn test_push_below_threshold_and_pop() {
        let db = setup_db(true).await;
        let mut stack = SQLiteEnvelopeStack::new(
            db,
            10,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        let envelopes = mock_envelopes(5);

        // We push 5 envelopes.
        for envelope in envelopes.clone() {
            assert!(stack.push(envelope).await.is_ok());
        }
        assert_eq!(stack.buffer.len(), 5);

        // We peek the top element.
        let peeked_envelope = stack.peek().await.unwrap();
        assert_eq!(
            peeked_envelope.event_id().unwrap(),
            envelopes.clone()[4].event_id().unwrap()
        );

        // We pop 5 envelopes.
        for envelope in envelopes.iter().rev() {
            let popped_envelope = stack.pop().await.unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap()
            );
        }
    }

    #[tokio::test]
    async fn test_push_above_threshold_and_pop() {
        let db = setup_db(true).await;
        let mut stack = SQLiteEnvelopeStack::new(
            db,
            10,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        let envelopes = mock_envelopes(15);

        // We push 15 envelopes.
        for envelope in envelopes.clone() {
            assert!(stack.push(envelope).await.is_ok());
        }
        assert_eq!(stack.buffer.len(), 10);

        // We peek the top element.
        let peeked_envelope = stack.peek().await.unwrap();
        assert_eq!(
            peeked_envelope.event_id().unwrap(),
            envelopes.clone()[14].event_id().unwrap()
        );

        // We pop 10 envelopes, and we expect that the last 10 are in memory, since the first 5
        // should have been spooled to disk.
        for envelope in envelopes[5..15].iter().rev() {
            let popped_envelope = stack.pop().await.unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap()
            );
        }
        assert!(stack.buffer.is_empty());

        // We peek the top element, which since the buffer is empty should result in a disk load.
        let peeked_envelope = stack.peek().await.unwrap();
        assert_eq!(
            peeked_envelope.event_id().unwrap(),
            envelopes.clone()[4].event_id().unwrap()
        );

        // We insert a new envelope, to test the load from disk happening during `peek()` gives
        // priority to this envelope in the stack.
        let envelope = mock_envelope(Instant::now());
        assert!(stack.push(envelope.clone()).await.is_ok());

        // We pop and expect the newly inserted element.
        let popped_envelope = stack.pop().await.unwrap();
        assert_eq!(
            popped_envelope.event_id().unwrap(),
            envelope.event_id().unwrap()
        );

        // We pop 5 envelopes, which should not result in a disk load since `peek()` already should
        // have caused it.
        for envelope in envelopes[0..5].iter().rev() {
            let popped_envelope = stack.pop().await.unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap()
            );
        }
        assert!(stack.buffer.is_empty());
    }
}
