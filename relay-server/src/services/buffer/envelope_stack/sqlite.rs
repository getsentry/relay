use std::error::Error;
use std::fmt::Debug;
use std::num::NonZeroUsize;

use chrono::{DateTime, Utc};
use relay_base_schema::project::ProjectKey;
use tokio::task::JoinHandle;

use crate::envelope::Envelope;
use crate::services::buffer::envelope_stack::EnvelopeStack;
use crate::services::buffer::envelope_store::sqlite::{
    DatabaseEnvelope, InsertEnvelopeError, SqliteEnvelopeStore, SqliteEnvelopeStoreError,
};
use crate::statsd::{RelayCounters, RelayTimers};

/// An error returned when doing an operation on [`SqliteEnvelopeStack`].
#[derive(Debug, thiserror::Error)]
pub enum SqliteEnvelopeStackError {
    #[error("envelope store error: {0}")]
    EnvelopeStoreError(#[from] SqliteEnvelopeStoreError),
    #[error("envelope encode error: {0}")]
    Envelope(#[from] InsertEnvelopeError),
}

/// An [`EnvelopeStack`] that is implemented on an SQLite database.
///
/// For efficiency reasons, the implementation has an in-memory buffer that is periodically spooled
/// to disk in a batched way.
#[derive(Debug)]

pub struct SqliteEnvelopeStack {
    /// Shared SQLite database pool which will be used to read and write from disk.
    envelope_store: SqliteEnvelopeStore,
    /// Maximum number of envelopes to read from disk at once.
    read_batch_size: NonZeroUsize,
    /// Maximum number of bytes in the in-memory cache before we write to disk.
    write_batch_bytes: NonZeroUsize,
    /// The project key of the project to which all the envelopes belong.
    own_key: ProjectKey,
    /// The project key of the root project of the trace to which all the envelopes belong.
    sampling_key: ProjectKey,
    /// In-memory stack containing a batch of envelopes that either have not been written to disk yet, or have been read from disk recently.
    #[allow(clippy::vec_box)]
    batch: Vec<DatabaseEnvelope>,
    db_state: DbState,
}

impl SqliteEnvelopeStack {
    /// Creates a new empty [`SqliteEnvelopeStack`].
    pub fn new(
        envelope_store: SqliteEnvelopeStore,
        read_batch_size: usize,
        write_batch_bytes: usize,
        own_key: ProjectKey,
        sampling_key: ProjectKey,
        check_disk: bool,
    ) -> Self {
        Self {
            envelope_store,
            read_batch_size: NonZeroUsize::new(read_batch_size).expect("batch size should be > 0"),
            write_batch_bytes: NonZeroUsize::new(write_batch_bytes)
                .expect("batch bytes should be > 0"),
            own_key,
            sampling_key,
            batch: vec![],
            db_state: match check_disk {
                true => DbState::Idle,
                false => DbState::Empty,
            },
        }
    }

    /// Threshold above which the [`SqliteEnvelopeStack`] will spool data from the `buffer` to disk.
    fn above_spool_threshold(&self) -> bool {
        self.batch.iter().map(|e| e.len()).sum::<usize>() > self.write_batch_bytes.get()
    }

    /// Spools to disk up to `disk_batch_size` envelopes from the `buffer`.
    ///
    /// In case there is a failure while writing envelopes, all the envelopes that were enqueued
    /// to be written to disk are lost. The explanation for this behavior can be found in the body
    /// of the method.
    async fn spool_to_disk(&mut self) {
        let batch = std::mem::take(&mut self.batch);
        relay_statsd::metric!(counter(RelayCounters::BufferSpooledEnvelopes) += batch.len() as u64);

        self.previous_write().await;
        let mut store = self.envelope_store.clone();
        self.db_state = DbState::Busy(tokio::spawn(async move {
            relay_statsd::metric!(timer(RelayTimers::BufferSpool), {
                store
                    .insert_many(batch)
                    .await
                    .map_err(SqliteEnvelopeStackError::EnvelopeStoreError)
            })
        }));
    }

    /// Unspools from disk up to `disk_batch_size` envelopes and appends them to the `buffer`.
    ///
    /// In case a single deletion fails, the affected envelope will not be unspooled and unspooling
    /// will continue with the remaining envelopes.
    ///
    /// In case an envelope fails deserialization due to malformed data in the database, the affected
    /// envelope will not be unspooled and unspooling will continue with the remaining envelopes.
    async fn unspool_from_disk(&mut self) -> Result<(), SqliteEnvelopeStackError> {
        self.previous_write().await;

        debug_assert!(self.batch.is_empty());
        self.batch = relay_statsd::metric!(timer(RelayTimers::BufferUnspool), {
            self.envelope_store
                .delete_many(
                    self.own_key,
                    self.sampling_key,
                    self.read_batch_size.get() as i64,
                )
                .await
                .map_err(SqliteEnvelopeStackError::EnvelopeStoreError)?
        });

        if self.batch.is_empty() {
            // In case no envelopes were unspooled, we will mark the disk as empty until another
            // round of spooling takes place.
            self.db_state = DbState::Empty;
        }

        relay_statsd::metric!(
            counter(RelayCounters::BufferUnspooledEnvelopes) += self.batch.len() as u64
        );
        Ok(())
    }

    fn should_unspool(&self) -> bool {
        self.batch.is_empty() && !matches!(self.db_state, DbState::Empty)
    }

    async fn previous_write(&mut self) {
        if let DbState::Busy(ref mut join_handle) = self.db_state {
            match join_handle.await {
                Ok(res) => {
                    if let Err(e) = res {
                        relay_log::error!(error = &e as &dyn Error, "error on previous write");
                    }
                }
                Err(e) => {
                    relay_log::error!(error = &e as &dyn Error, "join error");
                }
            }
            self.db_state = DbState::Idle;
        }
    }

    /// Validates that the incoming [`Envelope`] has the same project keys at the
    /// [`SqliteEnvelopeStack`].
    fn validate_envelope(&self, envelope: &Envelope) -> bool {
        let own_key = envelope.meta().public_key();
        let sampling_key = envelope.sampling_key().unwrap_or(own_key);

        self.own_key == own_key && self.sampling_key == sampling_key
    }
}

impl EnvelopeStack for SqliteEnvelopeStack {
    type Error = SqliteEnvelopeStackError;

    async fn push(&mut self, envelope: Box<Envelope>) -> Result<(), Self::Error> {
        debug_assert!(self.validate_envelope(&envelope));

        if self.above_spool_threshold() {
            self.spool_to_disk().await;
        }

        let encoded_envelope =
            relay_statsd::metric!(timer(RelayTimers::BufferEnvelopesSerialization), {
                DatabaseEnvelope::try_from(envelope.as_ref())?
            });
        self.batch.push(encoded_envelope);

        Ok(())
    }

    async fn peek(&mut self) -> Result<Option<DateTime<Utc>>, Self::Error> {
        if self.should_unspool() {
            self.unspool_from_disk().await?
        }

        let Some(envelope) = self.batch.last() else {
            return Ok(None);
        };

        Ok(Some(envelope.received_at()))
    }

    async fn pop(&mut self) -> Result<Option<Box<Envelope>>, Self::Error> {
        if self.should_unspool() {
            self.unspool_from_disk().await?
        }

        let Some(envelope) = self.batch.pop() else {
            return Ok(None);
        };
        let envelope = envelope.try_into()?;

        Ok(Some(envelope))
    }

    fn flush(self) -> Vec<Box<Envelope>> {
        self.batch
            .into_iter()
            .filter_map(|e| e.try_into().ok())
            .collect()
    }
}

#[derive(Debug)]
enum DbState {
    /// No other tokio task is writing to the db, we can schedule a new one.
    Idle,
    /// Another task is currently writing to the db.
    Busy(JoinHandle<Result<(), SqliteEnvelopeStackError>>),
    /// We assume that there are no envelopes in the db for this project pair.
    ///
    /// This is the case when the last read operation returned empty.
    Empty,
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use relay_base_schema::project::ProjectKey;
    use std::time::Duration;

    use super::*;
    use crate::services::buffer::testutils::utils::{mock_envelope, mock_envelopes, setup_db};

    /// Helper function to calculate the total size of a slice of envelopes after compression
    fn calculate_compressed_size(envelopes: &[Box<Envelope>]) -> usize {
        envelopes
            .iter()
            .map(|e| DatabaseEnvelope::try_from(e.as_ref()).unwrap().len())
            .sum()
    }

    #[tokio::test]
    #[should_panic]
    async fn test_push_with_mismatching_project_keys() {
        let db = setup_db(false).await;
        let envelope_store = SqliteEnvelopeStore::new(db, Duration::from_millis(100));
        let mut stack = SqliteEnvelopeStack::new(
            envelope_store,
            10,
            2,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("c25ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
            true,
        );

        let envelope = mock_envelope(Utc::now());
        let _ = stack.push(envelope).await;
    }

    const COMPRESSED_ENVELOPE_SIZE: usize = 313;

    #[tokio::test]
    async fn test_push_when_db_is_not_valid() {
        let db = setup_db(false).await;
        let envelope_store = SqliteEnvelopeStore::new(db, Duration::from_millis(100));

        // Create envelopes first so we can calculate actual size
        let envelopes = mock_envelopes(4);
        let threshold_size = calculate_compressed_size(&envelopes) - 1;

        let mut stack = SqliteEnvelopeStack::new(
            envelope_store,
            10,
            threshold_size,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
            true,
        );

        // We push the 4 envelopes without errors because they are below the threshold.
        for envelope in envelopes.clone() {
            assert!(stack.push(envelope).await.is_ok());
        }

        // We push 1 more envelope which results in spooling, which fails because of a database
        // problem.
        let envelope = mock_envelope(Utc::now());
        assert!(matches!(
            stack.push(envelope).await,
            Err(SqliteEnvelopeStackError::EnvelopeStoreError(_))
        ));

        // The stack now contains the last of the 1 elements that were added. If we add a new one
        // we will end up with 2.
        let envelope = mock_envelope(Utc::now());
        assert!(stack.push(envelope.clone()).await.is_ok());
        assert_eq!(stack.batch.len(), 1);

        // We pop the remaining elements, expecting the last added envelope to be on top.
        let popped_envelope_1 = stack.pop().await.unwrap().unwrap();
        assert_eq!(
            popped_envelope_1.event_id().unwrap(),
            envelope.event_id().unwrap()
        );
        assert_eq!(stack.batch.len(), 0);
    }

    #[tokio::test]
    async fn test_pop_when_db_is_not_valid() {
        let db = setup_db(false).await;
        let envelope_store = SqliteEnvelopeStore::new(db, Duration::from_millis(100));
        let mut stack = SqliteEnvelopeStack::new(
            envelope_store,
            10,
            2,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
            true,
        );

        // We pop with an invalid db.
        assert!(matches!(
            stack.pop().await,
            Err(SqliteEnvelopeStackError::EnvelopeStoreError(_))
        ));
    }

    #[tokio::test]
    async fn test_pop_when_stack_is_empty() {
        let db = setup_db(true).await;
        let envelope_store = SqliteEnvelopeStore::new(db, Duration::from_millis(100));
        let mut stack = SqliteEnvelopeStack::new(
            envelope_store,
            10,
            2,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
            true,
        );

        // We pop with no elements.
        // We pop with no elements.
        assert!(stack.pop().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_push_below_threshold_and_pop() {
        let db = setup_db(true).await;
        let envelope_store = SqliteEnvelopeStore::new(db, Duration::from_millis(100));
        let mut stack = SqliteEnvelopeStack::new(
            envelope_store,
            10,
            9999,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
            true,
        );

        let envelopes = mock_envelopes(5);

        // We push 5 envelopes.
        for envelope in envelopes.clone() {
            assert!(stack.push(envelope).await.is_ok());
        }
        assert_eq!(stack.batch.len(), 5);

        // We peek the top element.
        let peeked = stack.peek().await.unwrap().unwrap();
        assert_eq!(
            peeked.timestamp_millis(),
            envelopes.clone()[4].received_at().timestamp_millis()
        );

        // We pop 5 envelopes.
        for envelope in envelopes.iter().rev() {
            let popped_envelope = stack.pop().await.unwrap().unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap()
            );
        }

        assert_eq!(stack.batch.len(), 0);
    }

    #[tokio::test]
    async fn test_push_above_threshold_and_pop() {
        let db = setup_db(true).await;
        let envelope_store = SqliteEnvelopeStore::new(db, Duration::from_millis(100));

        // Create envelopes first so we can calculate actual size
        let envelopes = mock_envelopes(7);
        let threshold_size = calculate_compressed_size(&envelopes[..5]) - 1;

        // Create stack with threshold just below the size of first 5 envelopes
        let mut stack = SqliteEnvelopeStack::new(
            envelope_store,
            10,
            threshold_size,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
            true,
        );

        // We push 7 envelopes.
        for envelope in envelopes.clone() {
            assert!(stack.push(envelope).await.is_ok());
        }
        assert_eq!(stack.batch.len(), 2);

        // We peek the top element.
        let peeked = stack.peek().await.unwrap().unwrap();
        assert_eq!(
            peeked.timestamp_millis(),
            envelopes[6].received_at().timestamp_millis()
        );

        // We pop envelopes, and we expect that the last 2 are in memory, since the first 5
        // should have been spooled to disk.
        for envelope in envelopes[5..7].iter().rev() {
            let popped_envelope = stack.pop().await.unwrap().unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap()
            );
        }
        assert_eq!(stack.batch.len(), 0);

        // We peek the top element, which since the buffer is empty should result in a disk load.
        let peeked = stack.peek().await.unwrap().unwrap();
        assert_eq!(
            peeked.timestamp_millis(),
            envelopes[4].received_at().timestamp_millis()
        );

        // We insert a new envelope, to test the load from disk happening during `peek()` gives
        // priority to this envelope in the stack.
        let envelope = mock_envelope(Utc::now());
        assert!(stack.push(envelope.clone()).await.is_ok());

        // We pop and expect the newly inserted element.
        let popped_envelope = stack.pop().await.unwrap().unwrap();
        assert_eq!(
            popped_envelope.event_id().unwrap(),
            envelope.event_id().unwrap()
        );

        // We pop 5 envelopes, which should not result in a disk load since `peek()` already should
        // have caused it.
        for envelope in envelopes[0..5].iter().rev() {
            let popped_envelope = stack.pop().await.unwrap().unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap()
            );
        }
        assert_eq!(stack.batch.len(), 0);
    }

    #[tokio::test]
    async fn test_drain() {
        let db = setup_db(true).await;
        let envelope_store = SqliteEnvelopeStore::new(db, Duration::from_millis(100));
        let mut stack = SqliteEnvelopeStack::new(
            envelope_store.clone(),
            10,
            10 * COMPRESSED_ENVELOPE_SIZE,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
            true,
        );

        let envelopes = mock_envelopes(5);

        // We push 5 envelopes and check that there is nothing on disk.
        for envelope in envelopes.clone() {
            assert!(stack.push(envelope).await.is_ok());
        }
        assert_eq!(stack.batch.len(), 5);
        assert_eq!(envelope_store.total_count().await.unwrap(), 0);

        // We drain the stack and make sure nothing was spooled to disk.
        let drained_envelopes = stack.flush();
        assert_eq!(drained_envelopes.into_iter().collect::<Vec<_>>().len(), 5);
        assert_eq!(envelope_store.total_count().await.unwrap(), 0);
    }
}
