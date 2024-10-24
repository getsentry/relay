use std::fmt::Debug;
use std::num::NonZeroUsize;

use relay_base_schema::project::ProjectKey;
use tokio::time::Instant;

use crate::envelope::Envelope;
use crate::services::buffer::envelope_stack::EnvelopeStack;
use crate::services::buffer::envelope_store::sqlite::{
    InsertEnvelope, InsertEnvelopeError, SqliteEnvelopeStore, SqliteEnvelopeStoreError,
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

#[derive(Debug)]
/// An [`EnvelopeStack`] that is implemented on an SQLite database.
///
/// For efficiency reasons, the implementation has an in-memory buffer that is periodically spooled
/// to disk in a batched way.
pub struct SqliteEnvelopeStack {
    /// Shared SQLite database pool which will be used to read and write from disk.
    envelope_store: SqliteEnvelopeStore,
    /// Maximum number of bytes in the in-memory cache before we write to disk.
    max_batch_bytes: NonZeroUsize,
    /// The project key of the project to which all the envelopes belong.
    own_key: ProjectKey,
    /// The project key of the root project of the trace to which all the envelopes belong.
    sampling_key: ProjectKey,
    /// In-memory stack containing a batch of envelopes that either have not been written to disk yet, or have been read from disk recently.
    #[allow(clippy::vec_box)]
    batch: Vec<InsertEnvelope>,
    /// Boolean representing whether calls to `push()` and `peek()` check disk in case not enough
    /// elements are available in the `batches_buffer`.
    check_disk: bool,
}

impl SqliteEnvelopeStack {
    /// Creates a new empty [`SqliteEnvelopeStack`].
    pub fn new(
        envelope_store: SqliteEnvelopeStore,
        max_batch_bytes: usize,
        own_key: ProjectKey,
        sampling_key: ProjectKey,
        check_disk: bool,
    ) -> Self {
        Self {
            envelope_store,
            max_batch_bytes: NonZeroUsize::new(max_batch_bytes)
                .expect("the spool threshold must be > 0"),
            own_key,
            sampling_key,
            batch: vec![],
            check_disk,
        }
    }

    /// Threshold above which the [`SqliteEnvelopeStack`] will spool data from the `buffer` to disk.
    fn above_spool_threshold(&self) -> bool {
        self.batch.iter().map(|e| e.len()).sum::<usize>() > self.max_batch_bytes.get()
    }

    /// Spools to disk up to `disk_batch_size` envelopes from the `buffer`.
    ///
    /// In case there is a failure while writing envelopes, all the envelopes that were enqueued
    /// to be written to disk are lost. The explanation for this behavior can be found in the body
    /// of the method.
    async fn spool_to_disk(&mut self) -> Result<(), SqliteEnvelopeStackError> {
        let batch = std::mem::take(&mut self.batch);
        relay_statsd::metric!(counter(RelayCounters::BufferSpooledEnvelopes) += batch.len() as u64);

        // TODO: log serialization metric

        // // We convert envelopes into a format which simplifies insertion in the store. If an
        // // envelope can't be serialized, we will not insert it.
        // let envelopes = relay_statsd::metric!(timer(RelayTimers::BufferEnvelopesSerialization), {
        //     envelopes.iter().filter_map(|e| e.as_ref().try_into().ok())
        // });

        // When early return here, we are acknowledging that the elements that we popped from
        // the buffer are lost in case of failure. We are doing this on purposes, since if we were
        // to have a database corruption during runtime, and we were to put the values back into
        // the buffer we will end up with an infinite cycle.
        relay_statsd::metric!(timer(RelayTimers::BufferSpool), {
            self.envelope_store
                .insert_many(batch)
                .await
                .map_err(SqliteEnvelopeStackError::EnvelopeStoreError)?;
        });

        // If we successfully spooled to disk, we know that data should be there.
        self.check_disk = true;

        Ok(())
    }

    /// Unspools from disk up to `disk_batch_size` envelopes and appends them to the `buffer`.
    ///
    /// In case a single deletion fails, the affected envelope will not be unspooled and unspooling
    /// will continue with the remaining envelopes.
    ///
    /// In case an envelope fails deserialization due to malformed data in the database, the affected
    /// envelope will not be unspooled and unspooling will continue with the remaining envelopes.
    async fn unspool_from_disk(&mut self) -> Result<(), SqliteEnvelopeStackError> {
        debug_assert!(self.batch.is_empty());
        self.batch = relay_statsd::metric!(timer(RelayTimers::BufferUnspool), {
            self.envelope_store
                .delete_many(
                    self.own_key,
                    self.sampling_key,
                    10, // TODO
                )
                .await
                .map_err(SqliteEnvelopeStackError::EnvelopeStoreError)?
        });

        if self.batch.is_empty() {
            // In case no envelopes were unspooled, we will mark the disk as empty until another
            // round of spooling takes place.
            self.check_disk = false;
        }

        relay_statsd::metric!(
            counter(RelayCounters::BufferUnspooledEnvelopes) += self.batch.len() as u64
        );
        Ok(())
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
            self.spool_to_disk().await?;
        }

        let encoded_envelope =
            relay_statsd::metric!(timer(RelayTimers::BufferEnvelopesSerialization), {
                InsertEnvelope::try_from(envelope.as_ref())?
            });
        self.batch.push(encoded_envelope);

        Ok(())
    }

    async fn peek(&mut self) -> Result<Option<Instant>, Self::Error> {
        if self.batch.is_empty() && self.check_disk {
            self.unspool_from_disk().await?
        }

        let Some(envelope) = self.batch.last() else {
            return Ok(None);
        };
        Ok(Some(envelope.received_at().into()))
    }

    async fn pop(&mut self) -> Result<Option<Box<Envelope>>, Self::Error> {
        if self.batch.is_empty() && self.check_disk {
            self.unspool_from_disk().await?
        }

        // FIXME: inefficient peek
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

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use relay_base_schema::project::ProjectKey;

    use super::*;
    use crate::services::buffer::testutils::utils::{mock_envelope, mock_envelopes, setup_db};

    #[tokio::test]
    #[should_panic]
    async fn test_push_with_mismatching_project_keys() {
        let db = setup_db(false).await;
        let envelope_store = SqliteEnvelopeStore::new(db, Duration::from_millis(100));
        let mut stack = SqliteEnvelopeStack::new(
            envelope_store,
            // 2,
            2,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("c25ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
            true,
        );

        let envelope = mock_envelope(Instant::now());
        let _ = stack.push(envelope).await;
    }

    #[tokio::test]
    async fn test_push_when_db_is_not_valid() {
        let db = setup_db(false).await;
        let envelope_store = SqliteEnvelopeStore::new(db, Duration::from_millis(100));
        let mut stack = SqliteEnvelopeStack::new(
            envelope_store,
            471 * 4 - 1,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
            true,
        );

        let envelopes = mock_envelopes(4);

        // We push the 4 envelopes without errors because they are below the threshold.
        for envelope in envelopes.clone() {
            assert!(stack.push(envelope).await.is_ok());
        }

        // We push 1 more envelope which results in spooling, which fails because of a database
        // problem.
        let envelope = mock_envelope(Instant::now());
        assert!(matches!(
            stack.push(envelope).await,
            Err(SqliteEnvelopeStackError::EnvelopeStoreError(_))
        ));

        // The stack now contains the last of the 1 elements that were added. If we add a new one
        // we will end up with 2.
        let envelope = mock_envelope(Instant::now());
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
        assert!(
            peeked.into_std() - envelopes.clone()[4].meta().start_time() < Duration::from_millis(1)
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
        let mut stack = SqliteEnvelopeStack::new(
            envelope_store,
            5 * 471 - 1,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
            true,
        );

        let envelopes = mock_envelopes(7);

        // We push 7 envelopes.
        for envelope in envelopes.clone() {
            assert!(stack.push(envelope).await.is_ok());
        }
        assert_eq!(stack.batch.len(), 2);

        // We peek the top element.
        let peeked = stack.peek().await.unwrap().unwrap();
        assert!(peeked.into_std() - envelopes[6].meta().start_time() < Duration::from_millis(1));

        // We pop envelopes, and we expect that the last 10 are in memory, since the first 5
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
        assert!(peeked.into_std() - envelopes[4].meta().start_time() < Duration::from_millis(1));

        // We insert a new envelope, to test the load from disk happening during `peek()` gives
        // priority to this envelope in the stack.
        let envelope = mock_envelope(Instant::now());
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
            4710,
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
