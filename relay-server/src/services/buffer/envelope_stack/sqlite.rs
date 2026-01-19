use std::fmt::Debug;
use std::num::NonZeroUsize;

use chrono::{DateTime, Utc};
use relay_base_schema::project::ProjectKey;

use crate::envelope::Envelope;
use crate::managed::Managed;
use crate::services::buffer::envelope_stack::EnvelopeStack;
use crate::services::buffer::envelope_store::sqlite::{
    DatabaseBatch, DatabaseEnvelope, InsertEnvelopeError, SqliteEnvelopeStore,
    SqliteEnvelopeStoreError,
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
    batch_size_bytes: NonZeroUsize,
    /// The project key of the project to which all the envelopes belong.
    own_key: ProjectKey,
    /// The project key of the root project of the trace to which all the envelopes belong.
    sampling_key: ProjectKey,
    /// In-memory stack containing a batch of envelopes that either have not been written to disk yet, or have been read from disk recently.
    batch: Vec<DatabaseEnvelope>,
    /// Boolean representing whether calls to `push()` and `peek()` check disk in case not enough
    /// elements are available in the `batches_buffer`.
    check_disk: bool,
    /// The tag value of this partition which is used for reporting purposes.
    partition_tag: String,
}

impl SqliteEnvelopeStack {
    /// Creates a new empty [`SqliteEnvelopeStack`].
    pub fn new(
        partition_id: u8,
        envelope_store: SqliteEnvelopeStore,
        batch_size_bytes: usize,
        own_key: ProjectKey,
        sampling_key: ProjectKey,
        check_disk: bool,
    ) -> Self {
        Self {
            envelope_store,
            batch_size_bytes: NonZeroUsize::new(batch_size_bytes)
                .expect("batch bytes should be > 0"),
            own_key,
            sampling_key,
            batch: vec![],
            check_disk,
            partition_tag: partition_id.to_string(),
        }
    }

    /// Threshold above which the [`SqliteEnvelopeStack`] will spool data from the `buffer` to disk.
    fn above_spool_threshold(&self) -> bool {
        self.batch.iter().map(|e| e.len()).sum::<usize>() > self.batch_size_bytes.get()
    }

    /// Spools to disk a batch of envelopes from the `batch`.
    ///
    /// In case there is a failure while writing envelopes, all the envelopes that were enqueued
    /// to be written to disk are lost. The explanation for this behavior can be found in the body
    /// of the method.
    async fn spool_to_disk(&mut self) -> Result<(), SqliteEnvelopeStackError> {
        let batch = std::mem::take(&mut self.batch);
        let Ok(batch) = DatabaseBatch::try_from(batch) else {
            return Ok(());
        };

        relay_statsd::metric!(
            counter(RelayCounters::BufferSpooledEnvelopes) += batch.len() as u64,
            partition_id = &self.partition_tag
        );

        // When early return here, we are acknowledging that the elements that we popped from
        // the buffer are lost in case of failure. We are doing this on purposes, since if we were
        // to have a database corruption during runtime, and we were to put the values back into
        // the buffer we will end up with an infinite cycle.
        relay_statsd::metric!(
            timer(RelayTimers::BufferSpool),
            partition_id = &self.partition_tag,
            {
                self.envelope_store
                    .insert_batch(batch)
                    .await
                    .map_err(SqliteEnvelopeStackError::EnvelopeStoreError)?;
            }
        );

        // If we successfully spooled to disk, we know that data should be there.
        self.check_disk = true;

        Ok(())
    }

    /// Unspools from disk a batch of envelopes and appends them to the `batch`.
    ///
    /// In case there is a failure while deleting envelopes, the envelopes will be lost.
    async fn unspool_from_disk(&mut self) -> Result<(), SqliteEnvelopeStackError> {
        debug_assert!(self.batch.is_empty());
        let batch = relay_statsd::metric!(
            timer(RelayTimers::BufferUnspool),
            partition_id = &self.partition_tag,
            {
                self.envelope_store
                    .delete_batch(self.own_key, self.sampling_key)
                    .await
                    .map_err(SqliteEnvelopeStackError::EnvelopeStoreError)?
            }
        );

        match batch {
            Some(batch) => {
                self.batch = batch.into();
            }
            None => self.check_disk = false,
        }

        relay_statsd::metric!(
            counter(RelayCounters::BufferUnspooledEnvelopes) += self.batch.len() as u64,
            partition_id = &self.partition_tag
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

    async fn push(&mut self, envelope: Managed<Box<Envelope>>) -> Result<(), Self::Error> {
        debug_assert!(self.validate_envelope(&envelope));

        if self.above_spool_threshold() {
            self.spool_to_disk().await?;
        }

        // Try to encode the envelope before accepting it. If encoding fails,
        // the Managed wrapper will be dropped and automatically reject the envelope.
        let encoded_envelope = relay_statsd::metric!(
            timer(RelayTimers::BufferEnvelopesSerialization),
            partition_id = &self.partition_tag,
            { DatabaseEnvelope::try_from(&**envelope)? }
        );

        // Only accept the envelope after successful encoding
        envelope.accept(|_| ());

        self.batch.push(encoded_envelope);

        Ok(())
    }

    async fn peek(&mut self) -> Result<Option<DateTime<Utc>>, Self::Error> {
        if self.batch.is_empty() && self.check_disk {
            self.unspool_from_disk().await?
        }

        let Some(envelope) = self.batch.last() else {
            return Ok(None);
        };

        Ok(Some(envelope.received_at()))
    }

    async fn pop(&mut self) -> Result<Option<Box<Envelope>>, Self::Error> {
        if self.batch.is_empty() && self.check_disk {
            self.unspool_from_disk().await?
        }

        let Some(envelope) = self.batch.pop() else {
            return Ok(None);
        };
        let envelope = envelope.try_into()?;

        Ok(Some(envelope))
    }

    async fn flush(mut self) {
        if let Err(e) = self.spool_to_disk().await {
            relay_log::error!(error = &e as &dyn std::error::Error, "flush error");
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use relay_base_schema::project::ProjectKey;
    use std::time::Duration;

    use super::*;
    use crate::services::buffer::testutils::utils::{
        managed_envelope, managed_envelopes, mock_envelopes, setup_db,
    };

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
        let envelope_store = SqliteEnvelopeStore::new(0, db, Duration::from_millis(100));
        let mut stack = SqliteEnvelopeStack::new(
            0,
            envelope_store,
            10,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("c25ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
            true,
        );

        let envelope = managed_envelope(Utc::now());
        let _ = stack.push(envelope).await;
    }

    const COMPRESSED_ENVELOPE_SIZE: usize = 313;

    #[tokio::test]
    async fn test_push_when_db_is_not_valid() {
        let db = setup_db(false).await;
        let envelope_store = SqliteEnvelopeStore::new(0, db, Duration::from_millis(100));

        // Create envelopes first so we can calculate actual size
        let envelopes = mock_envelopes(4);
        let threshold_size = calculate_compressed_size(&envelopes) - 1;

        let mut stack = SqliteEnvelopeStack::new(
            0,
            envelope_store,
            threshold_size,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
            true,
        );

        // We push the 4 envelopes without errors because they are below the threshold.
        let managed_envs = managed_envelopes(4);
        for envelope in managed_envs {
            assert!(stack.push(envelope).await.is_ok());
        }

        // We push 1 more envelope which results in spooling, which fails because of a database
        // problem. The managed envelope will be dropped and automatically rejected.
        let envelope = managed_envelope(Utc::now());
        assert!(matches!(
            stack.push(envelope).await,
            Err(SqliteEnvelopeStackError::EnvelopeStoreError(_))
        ));

        // The stack now contains no elements since the batch was taken for spooling and failed.
        // If we add a new one we will end up with 1.
        let envelope = managed_envelope(Utc::now());
        let expected_event_id = envelope.event_id().unwrap();
        assert!(stack.push(envelope).await.is_ok());
        assert_eq!(stack.batch.len(), 1);

        // We pop the remaining elements, expecting the last added envelope to be on top.
        let popped_envelope_1 = stack.pop().await.unwrap().unwrap();
        assert_eq!(popped_envelope_1.event_id().unwrap(), expected_event_id);
        assert_eq!(stack.batch.len(), 0);
    }

    #[tokio::test]
    async fn test_pop_when_db_is_not_valid() {
        let db = setup_db(false).await;
        let envelope_store = SqliteEnvelopeStore::new(0, db, Duration::from_millis(100));
        let mut stack = SqliteEnvelopeStack::new(
            0,
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
        let envelope_store = SqliteEnvelopeStore::new(0, db, Duration::from_millis(100));
        let mut stack = SqliteEnvelopeStack::new(
            0,
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
        let envelope_store = SqliteEnvelopeStore::new(0, db, Duration::from_millis(100));
        let mut stack = SqliteEnvelopeStack::new(
            0,
            envelope_store,
            9999,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
            true,
        );

        let managed_envs = managed_envelopes(5);

        // Collect event IDs and timestamps before pushing (since envelopes are consumed)
        let event_ids: Vec<_> = managed_envs.iter().map(|e| e.event_id().unwrap()).collect();
        let timestamps: Vec<_> = managed_envs.iter().map(|e| e.received_at()).collect();

        // We push 5 envelopes.
        for envelope in managed_envs {
            assert!(stack.push(envelope).await.is_ok());
        }
        assert_eq!(stack.batch.len(), 5);

        // We peek the top element.
        let peeked = stack.peek().await.unwrap().unwrap();
        assert_eq!(peeked.timestamp_millis(), timestamps[4].timestamp_millis());

        // We pop 5 envelopes (in reverse order - LIFO).
        for (_i, expected_event_id) in event_ids.iter().enumerate().rev() {
            let popped_envelope = stack.pop().await.unwrap().unwrap();
            assert_eq!(popped_envelope.event_id().unwrap(), *expected_event_id);
        }

        assert_eq!(stack.batch.len(), 0);
    }

    #[tokio::test]
    async fn test_push_above_threshold_and_pop() {
        let db = setup_db(true).await;
        let envelope_store = SqliteEnvelopeStore::new(0, db, Duration::from_millis(100));

        // Create envelopes first so we can calculate actual size
        let envelopes = mock_envelopes(7);
        let threshold_size = calculate_compressed_size(&envelopes[..5]) - 1;

        // Create stack with threshold just below the size of first 5 envelopes
        let mut stack = SqliteEnvelopeStack::new(
            0,
            envelope_store,
            threshold_size,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
            true,
        );

        // We push 7 envelopes.
        let managed_envs = managed_envelopes(7);

        // Collect event IDs and timestamps before pushing
        let event_ids: Vec<_> = managed_envs.iter().map(|e| e.event_id().unwrap()).collect();
        let timestamps: Vec<_> = managed_envs.iter().map(|e| e.received_at()).collect();

        for envelope in managed_envs {
            assert!(stack.push(envelope).await.is_ok());
        }
        assert_eq!(stack.batch.len(), 2);

        // We peek the top element.
        let peeked = stack.peek().await.unwrap().unwrap();
        assert_eq!(peeked.timestamp_millis(), timestamps[6].timestamp_millis());

        // We pop envelopes, and we expect that the last 2 are in memory, since the first 5
        // should have been spooled to disk.
        for i in (5..7).rev() {
            let popped_envelope = stack.pop().await.unwrap().unwrap();
            assert_eq!(popped_envelope.event_id().unwrap(), event_ids[i]);
        }
        assert_eq!(stack.batch.len(), 0);

        // We peek the top element, which since the buffer is empty should result in a disk load.
        let peeked = stack.peek().await.unwrap().unwrap();
        assert_eq!(peeked.timestamp_millis(), timestamps[4].timestamp_millis());

        // We insert a new envelope, to test the load from disk happening during `peek()` gives
        // priority to this envelope in the stack.
        let envelope = managed_envelope(Utc::now());
        let expected_event_id = envelope.event_id().unwrap();
        assert!(stack.push(envelope).await.is_ok());

        // We pop and expect the newly inserted element.
        let popped_envelope = stack.pop().await.unwrap().unwrap();
        assert_eq!(popped_envelope.event_id().unwrap(), expected_event_id);

        // We pop 5 envelopes, which should not result in a disk load since `peek()` already should
        // have caused it.
        for i in (0..5).rev() {
            let popped_envelope = stack.pop().await.unwrap().unwrap();
            assert_eq!(popped_envelope.event_id().unwrap(), event_ids[i]);
        }
        assert_eq!(stack.batch.len(), 0);
    }

    #[tokio::test]
    async fn test_drain() {
        let db = setup_db(true).await;
        let envelope_store = SqliteEnvelopeStore::new(0, db, Duration::from_millis(100));
        let mut stack = SqliteEnvelopeStack::new(
            0,
            envelope_store.clone(),
            10 * COMPRESSED_ENVELOPE_SIZE,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
            true,
        );

        let managed_envs = managed_envelopes(5);

        // We push 5 envelopes and check that there is nothing on disk.
        for envelope in managed_envs {
            assert!(stack.push(envelope).await.is_ok());
        }
        assert_eq!(stack.batch.len(), 5);
        assert_eq!(envelope_store.total_count().await.unwrap(), 0);

        // We drain the stack and make sure everything was spooled to disk.
        stack.flush().await;
        assert_eq!(envelope_store.total_count().await.unwrap(), 5);
    }
}
