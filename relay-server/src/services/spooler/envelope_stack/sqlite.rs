use crate::envelope::Envelope;
use crate::extractors::StartTime;
use crate::services::spooler::envelope_stack::EnvelopeStack;
use futures::StreamExt;
use relay_base_schema::project::ProjectKey;
use sqlx::query::Query;
use sqlx::sqlite::{SqliteArguments, SqliteRow};
use sqlx::{Pool, QueryBuilder, Row, Sqlite};
use std::collections::VecDeque;
use std::error::Error;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::pin::pin;

/// An error returned when doing an operation on [`SQLiteEnvelopeStack`].
#[derive(Debug, thiserror::Error)]
pub enum SQLiteEnvelopeStackError {
    /// The stack is empty.
    #[error("the stack is empty")]
    Empty,

    /// The database encountered an unexpected error.
    #[error("a database error occurred")]
    DatabaseError(#[from] sqlx::Error),
}

/// An [`EnvelopeStack`] that is implemented on an SQLite database.
///
/// For efficiency reasons, the implementation has an in-memory buffer that is periodically spooled
/// to disk in a batched way.
pub struct SQLiteEnvelopeStack {
    /// Shared SQLite database pool which will be used to read and write from disk.
    db: Pool<Sqlite>,
    /// Threshold defining the maximum number of envelopes in the `batches_buffer` before spooling
    /// to disk will take place.
    spool_threshold: NonZeroUsize,
    /// Size of a batch of envelopes that is written to disk.
    batch_size: NonZeroUsize,
    /// The project key of the project to which all the envelopes belong.
    own_key: ProjectKey,
    /// The project key of the root project of the trace to which all the envelopes belong.
    sampling_key: ProjectKey,
    /// In-memory stack containing all the batches of envelopes that either have not been written to disk yet, or have been read from disk recently.
    #[allow(clippy::vec_box)]
    batches_buffer: VecDeque<Vec<Box<Envelope>>>,
    /// The total number of envelopes inside the `batches_buffer`.
    batches_buffer_size: usize,
    /// Boolean representing whether calls to `push()` and `peek()` check disk in case not enough
    /// elements are available in the `batches_buffer`.
    check_disk: bool,
}

impl SQLiteEnvelopeStack {
    /// Creates a new empty [`SQLiteEnvelopeStack`].
    #[allow(dead_code)]
    pub fn new(
        db: Pool<Sqlite>,
        disk_batch_size: usize,
        max_batches: usize,
        own_key: ProjectKey,
        sampling_key: ProjectKey,
    ) -> Self {
        Self {
            db,
            spool_threshold: NonZeroUsize::new(disk_batch_size * max_batches)
                .expect("the spool threshold must be > 0"),
            batch_size: NonZeroUsize::new(disk_batch_size)
                .expect("the disk batch size must be > 0"),
            own_key,
            sampling_key,
            batches_buffer: VecDeque::with_capacity(max_batches),
            batches_buffer_size: 0,
            check_disk: true,
        }
    }

    /// Threshold above which the [`SQLiteEnvelopeStack`] will spool data from the `buffer` to disk.
    fn above_spool_threshold(&self) -> bool {
        self.batches_buffer_size >= self.spool_threshold.get()
    }

    /// Threshold below which the [`SQLiteEnvelopeStack`] will unspool data from disk to the
    /// `buffer`.
    fn below_unspool_threshold(&self) -> bool {
        self.batches_buffer_size == 0
    }

    /// Spools to disk up to `disk_batch_size` envelopes from the `buffer`.
    ///
    /// In case there is a failure while writing envelopes, all the envelopes that were enqueued
    /// to be written to disk are lost. The explanation for this behavior can be found in the body
    /// of the method.
    fn background_spool_to_disk(&mut self) {
        let Some(envelopes) = self.batches_buffer.pop_front() else {
            return;
        };
        // We optimistically assume that such elements were removed from the buffer, so we update
        // the new size.
        self.batches_buffer_size -= envelopes.len();

        let insert_envelopes = envelopes
            .iter()
            .map(|e| InsertEnvelope {
                received_at: received_at(e),
                own_key: self.own_key,
                sampling_key: self.sampling_key,
                encoded_envelope: e.to_vec().unwrap(),
            })
            .collect();
        let db = self.db.clone();
        tokio::spawn(async move {
            // When spawning this task, we are acknowledging that the elements that we popped from
            // the buffer are lost in case of error during insertion.
            //
            // We are doing this on purposes, since if we were to have a database corruption during
            // runtime, and we were to put the values back into the buffer we will end up with an
            // infinite cycle.
            if let Err(err) = build_insert_many_envelopes(insert_envelopes)
                .build()
                .execute(&db)
                .await
            {
                relay_log::error!(
                    error = &err as &dyn Error,
                    "failed to spool envelopes to disk",
                );
            }
        });

        // We optimistically assume that data was written to disk and that in the next read/writes
        // we could try to check the disk. However, this doesn't guarantee data will be there, since
        // data might be written after this flag is set.
        self.check_disk = true;
    }

    /// Unspools from disk up to `disk_batch_size` envelopes and appends them to the `buffer`.
    ///
    /// In case a single deletion fails, the affected envelope will not be unspooled and unspooling
    /// will continue with the remaining envelopes.
    ///
    /// In case an envelope fails deserialization due to malformed data in the database, the affected
    /// envelope will not be unspooled and unspooling will continue with the remaining envelopes.
    async fn unspool_from_disk(&mut self) -> Result<(), SQLiteEnvelopeStackError> {
        let envelopes = build_delete_and_fetch_many_envelopes(
            self.own_key,
            self.sampling_key,
            self.batch_size.get() as i64,
        )
        .fetch(&self.db)
        .peekable();

        let mut envelopes = pin!(envelopes);
        if envelopes.as_mut().peek().await.is_none() {
            return Ok(());
        }

        // We use a sorted vector to order envelopes that are deleted from the database.
        // Unfortunately we have to do this because SQLite `DELETE` with `RETURNING` doesn't
        // return deleted rows in a specific order.
        let mut extracted_envelopes = Vec::with_capacity(self.batch_size.get());
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

            match self.extract_envelope(envelope) {
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

        if extracted_envelopes.is_empty() {
            // If there was a database error and no envelopes have been returned, we assume that we are
            // in a critical state, so we return an error.
            if let Some(db_error) = db_error {
                return Err(SQLiteEnvelopeStackError::DatabaseError(db_error));
            }

            // In case no envelopes were unspool, we will mark the disk as empty until another round
            // of spooling takes place.
            self.check_disk = false;

            return Ok(());
        }

        // We sort envelopes by `received_at`.
        extracted_envelopes.sort_by_key(|a| received_at(a));

        // We push in the back of the buffer, since we still want to give priority to
        // incoming envelopes that have a more recent timestamp.
        self.batches_buffer_size += extracted_envelopes.len();
        self.batches_buffer.push_front(extracted_envelopes);

        Ok(())
    }

    /// Deserializes an [`Envelope`] from a database row.
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

    /// Validates that the incoming [`Envelope`] has the same project keys at the
    /// [`SQLiteEnvelopeStack`].
    fn validate_envelope(&self, envelope: &Envelope) -> bool {
        let own_key = envelope.meta().public_key();
        let sampling_key = envelope.sampling_key().unwrap_or(own_key);

        self.own_key == own_key && self.sampling_key == sampling_key
    }
}

impl EnvelopeStack for SQLiteEnvelopeStack {
    type Error = SQLiteEnvelopeStackError;

    async fn push(&mut self, envelope: Box<Envelope>) -> Result<(), Self::Error> {
        debug_assert!(self.validate_envelope(&envelope));

        if self.above_spool_threshold() {
            self.background_spool_to_disk();
        }

        // We need to check if the topmost batch has space, if not we have to create a new batch and
        // push it in front.
        if let Some(last_batch) = self
            .batches_buffer
            .back_mut()
            .filter(|last_batch| last_batch.len() < self.batch_size.get())
        {
            last_batch.push(envelope);
        } else {
            let mut new_batch = Vec::with_capacity(self.batch_size.get());
            new_batch.push(envelope);
            self.batches_buffer.push_back(new_batch);
        }

        self.batches_buffer_size += 1;

        Ok(())
    }

    async fn peek(&mut self) -> Result<&Box<Envelope>, Self::Error> {
        if self.below_unspool_threshold() && self.check_disk {
            self.unspool_from_disk().await?
        }

        self.batches_buffer
            .back()
            .and_then(|last_batch| last_batch.last())
            .ok_or(Self::Error::Empty)
    }

    async fn pop(&mut self) -> Result<Box<Envelope>, Self::Error> {
        if self.below_unspool_threshold() && self.check_disk {
            self.unspool_from_disk().await?
        }

        let result = self
            .batches_buffer
            .back_mut()
            .and_then(|last_batch| {
                self.batches_buffer_size -= 1;
                last_batch.pop()
            })
            .ok_or(Self::Error::Empty);

        // Since we might leave a batch without elements, we want to pop it from the buffer.
        if self
            .batches_buffer
            .back()
            .map_or(false, |last_batch| last_batch.is_empty())
        {
            self.batches_buffer.pop_back();
        }

        result
    }
}

/// Struct which contains all the rows that have to be inserted in the database when storing an
/// [`Envelope`].
struct InsertEnvelope {
    received_at: i64,
    own_key: ProjectKey,
    sampling_key: ProjectKey,
    encoded_envelope: Vec<u8>,
}

/// Builds a query that inserts many [`Envelope`]s in the database.
fn build_insert_many_envelopes<'a>(envelopes: Vec<InsertEnvelope>) -> QueryBuilder<'a, Sqlite> {
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

/// Computes the `received_at` timestamps of an [`Envelope`] based on the `start_time` header.
///
/// This method has been copied from the `ManagedEnvelope.received_at()` method.
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
    use relay_sampling::DynamicSamplingContext;
    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
    use sqlx::{Pool, Sqlite};
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::time::{Duration, Instant};
    use tokio::fs::DirBuilder;
    use tokio::time::sleep;
    use uuid::Uuid;

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
    #[should_panic]
    async fn test_push_with_mismatching_project_keys() {
        let db = setup_db(false).await;
        let mut stack = SQLiteEnvelopeStack::new(
            db,
            2,
            2,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("c25ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        let envelope = mock_envelope(Instant::now());
        let _ = stack.push(envelope).await;
    }

    #[tokio::test]
    async fn test_push_when_db_is_not_valid() {
        let db = setup_db(false).await;
        let mut stack = SQLiteEnvelopeStack::new(
            db,
            2,
            2,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        let envelopes = mock_envelopes(4);

        // We push the 4 envelopes without errors because they are below the threshold.
        for envelope in envelopes.clone() {
            assert!(stack.push(envelope).await.is_ok());
        }

        // We push 1 more envelope that results in spooling, which returns ok but will fail in the
        // async task that writes to the db.
        let envelope_1 = mock_envelope(Instant::now());
        assert!(stack.push(envelope_1.clone()).await.is_ok());

        // We wait for the async spooling to take place.
        sleep(Duration::from_millis(100)).await;

        // The stack now contains 2 + 1 elements, since we have the remaining 2 elements of the 4
        // and the newly added element.
        let envelope_2 = mock_envelope(Instant::now());
        assert!(stack.push(envelope_2.clone()).await.is_ok());
        assert_eq!(stack.batches_buffer_size, 4);

        // We pop the remaining elements, expecting the last added envelope to be on top.
        let expected_envelopes = [
            envelope_2,
            envelope_1,
            envelopes[3].clone(),
            envelopes[2].clone(),
        ];
        for expected_envelope in expected_envelopes {
            let popped_envelope = stack.pop().await.unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                expected_envelope.event_id().unwrap()
            );
        }
        assert_eq!(stack.batches_buffer_size, 0);
    }

    #[tokio::test]
    async fn test_pop_when_db_is_not_valid() {
        let db = setup_db(false).await;
        let mut stack = SQLiteEnvelopeStack::new(
            db,
            2,
            2,
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
            2,
            2,
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
            5,
            2,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        let envelopes = mock_envelopes(5);

        // We push 5 envelopes.
        for envelope in envelopes.clone() {
            assert!(stack.push(envelope).await.is_ok());
        }
        assert_eq!(stack.batches_buffer_size, 5);

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
            5,
            2,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        let envelopes = mock_envelopes(15);

        // We push 15 envelopes.
        for envelope in envelopes.clone() {
            assert!(stack.push(envelope).await.is_ok());
        }
        assert_eq!(stack.batches_buffer_size, 10);

        // We wait for the async spooling to take place.
        sleep(Duration::from_millis(100)).await;

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
        assert_eq!(stack.batches_buffer_size, 0);

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
        assert_eq!(stack.batches_buffer_size, 0);
    }
}
