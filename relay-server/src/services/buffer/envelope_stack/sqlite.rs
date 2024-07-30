use std::collections::VecDeque;
use std::fmt::Debug;
use std::num::NonZeroUsize;

use relay_base_schema::project::ProjectKey;

use crate::envelope::Envelope;
use crate::services::buffer::envelope_stack::EnvelopeStack;
use crate::services::buffer::sqlite_envelope_store::{
    SqliteEnvelopeStore, SqliteEnvelopeStoreError,
};

/// An error returned when doing an operation on [`SQLiteEnvelopeStack`].
#[derive(Debug, thiserror::Error)]
pub enum SqliteEnvelopeStackError {
    #[error("an error occurred in the envelope store: {0}")]
    EnvelopeStoreError(#[from] SqliteEnvelopeStoreError),
}

#[derive(Debug)]
/// An [`EnvelopeStack`] that is implemented on an SQLite database.
///
/// For efficiency reasons, the implementation has an in-memory buffer that is periodically spooled
/// to disk in a batched way.
pub struct SqliteEnvelopeStack {
    /// Shared SQLite database pool which will be used to read and write from disk.
    envelope_store: SqliteEnvelopeStore,
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

impl SqliteEnvelopeStack {
    /// Creates a new empty [`SQLiteEnvelopeStack`].
    pub fn new(
        envelope_store: SqliteEnvelopeStore,
        disk_batch_size: usize,
        max_batches: usize,
        own_key: ProjectKey,
        sampling_key: ProjectKey,
    ) -> Self {
        Self {
            envelope_store,
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
    async fn spool_to_disk(&mut self) -> Result<(), SqliteEnvelopeStackError> {
        let Some(envelopes) = self.batches_buffer.pop_front() else {
            return Ok(());
        };
        self.batches_buffer_size -= envelopes.len();

        // We convert envelopes into a format which simplifies insertion in the store.
        let envelopes = envelopes.iter().map(|e| e.as_ref().into());

        // When early return here, we are acknowledging that the elements that we popped from
        // the buffer are lost in case of failure. We are doing this on purposes, since if we were
        // to have a database corruption during runtime, and we were to put the values back into
        // the buffer we will end up with an infinite cycle.
        self.envelope_store
            .insert_many(envelopes)
            .await
            .map_err(SqliteEnvelopeStackError::EnvelopeStoreError)?;

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
        let envelopes = self
            .envelope_store
            .delete_many(
                self.own_key,
                self.sampling_key,
                self.batch_size.get() as i64,
            )
            .await
            .map_err(SqliteEnvelopeStackError::EnvelopeStoreError)?;

        if envelopes.is_empty() {
            // In case no envelopes were unspooled, we will mark the disk as empty until another
            // round of spooling takes place.
            self.check_disk = false;

            return Ok(());
        }

        // We push in the back of the buffer, since we still want to give priority to
        // incoming envelopes that have a more recent timestamp.
        self.batches_buffer_size += envelopes.len();
        self.batches_buffer.push_front(envelopes);

        Ok(())
    }

    /// Validates that the incoming [`Envelope`] has the same project keys at the
    /// [`SQLiteEnvelopeStack`].
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

    async fn peek(&mut self) -> Result<Option<&Envelope>, Self::Error> {
        if self.below_unspool_threshold() && self.check_disk {
            self.unspool_from_disk().await?
        }

        let last = self
            .batches_buffer
            .back()
            .and_then(|last_batch| last_batch.last())
            .map(|last_batch| last_batch.as_ref());

        Ok(last)
    }

    async fn pop(&mut self) -> Result<Option<Box<Envelope>>, Self::Error> {
        if self.below_unspool_threshold() && self.check_disk {
            self.unspool_from_disk().await?
        }

        let result = self.batches_buffer.back_mut().and_then(|last_batch| {
            self.batches_buffer_size -= 1;
            last_batch.pop()
        });
        if result.is_none() {
            return Ok(None);
        }

        // Since we might leave a batch without elements, we want to pop it from the buffer.
        if self
            .batches_buffer
            .back()
            .map_or(false, |last_batch| last_batch.is_empty())
        {
            self.batches_buffer.pop_back();
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};

    use uuid::Uuid;

    use relay_base_schema::project::ProjectKey;
    use relay_event_schema::protocol::EventId;
    use relay_sampling::DynamicSamplingContext;

    use super::*;
    use crate::envelope::{Envelope, Item, ItemType};
    use crate::extractors::RequestMeta;
    use crate::services::buffer::testutils::setup_db;

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

    #[tokio::test]
    #[should_panic]
    async fn test_push_with_mismatching_project_keys() {
        let db = setup_db(false).await;
        let envelope_store = SqliteEnvelopeStore::new(db);
        let mut stack = SqliteEnvelopeStack::new(
            envelope_store,
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
        let envelope_store = SqliteEnvelopeStore::new(db);
        let mut stack = SqliteEnvelopeStack::new(
            envelope_store,
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

        // We push 1 more envelope which results in spooling, which fails because of a database
        // problem.
        let envelope = mock_envelope(Instant::now());
        assert!(matches!(
            stack.push(envelope).await,
            Err(SqliteEnvelopeStackError::EnvelopeStoreError(_))
        ));

        // The stack now contains the last of the 3 elements that were added. If we add a new one
        // we will end up with 2.
        let envelope = mock_envelope(Instant::now());
        assert!(stack.push(envelope.clone()).await.is_ok());
        assert_eq!(stack.batches_buffer_size, 3);

        // We pop the remaining elements, expecting the last added envelope to be on top.
        let popped_envelope_1 = stack.pop().await.unwrap().unwrap();
        let popped_envelope_2 = stack.pop().await.unwrap().unwrap();
        let popped_envelope_3 = stack.pop().await.unwrap().unwrap();
        assert_eq!(
            popped_envelope_1.event_id().unwrap(),
            envelope.event_id().unwrap()
        );
        assert_eq!(
            popped_envelope_2.event_id().unwrap(),
            envelopes.clone()[3].event_id().unwrap()
        );
        assert_eq!(
            popped_envelope_3.event_id().unwrap(),
            envelopes.clone()[2].event_id().unwrap()
        );
        assert_eq!(stack.batches_buffer_size, 0);
    }

    #[tokio::test]
    async fn test_pop_when_db_is_not_valid() {
        let db = setup_db(false).await;
        let envelope_store = SqliteEnvelopeStore::new(db);
        let mut stack = SqliteEnvelopeStack::new(
            envelope_store,
            2,
            2,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
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
        let envelope_store = SqliteEnvelopeStore::new(db);
        let mut stack = SqliteEnvelopeStack::new(
            envelope_store,
            2,
            2,
            ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ProjectKey::parse("b81ae32be2584e0bbd7a4cbb95971fe1").unwrap(),
        );

        // We pop with no elements.
        // We pop with no elements.
        assert!(stack.pop().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_push_below_threshold_and_pop() {
        let db = setup_db(true).await;
        let envelope_store = SqliteEnvelopeStore::new(db);
        let mut stack = SqliteEnvelopeStack::new(
            envelope_store,
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
        let peeked_envelope = stack.peek().await.unwrap().unwrap();
        assert_eq!(
            peeked_envelope.event_id().unwrap(),
            envelopes.clone()[4].event_id().unwrap()
        );

        // We pop 5 envelopes.
        for envelope in envelopes.iter().rev() {
            let popped_envelope = stack.pop().await.unwrap().unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap()
            );
        }
    }

    #[tokio::test]
    async fn test_push_above_threshold_and_pop() {
        let db = setup_db(true).await;
        let envelope_store = SqliteEnvelopeStore::new(db);
        let mut stack = SqliteEnvelopeStack::new(
            envelope_store,
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

        // We peek the top element.
        let peeked_envelope = stack.peek().await.unwrap().unwrap();
        assert_eq!(
            peeked_envelope.event_id().unwrap(),
            envelopes.clone()[14].event_id().unwrap()
        );

        // We pop 10 envelopes, and we expect that the last 10 are in memory, since the first 5
        // should have been spooled to disk.
        for envelope in envelopes[5..15].iter().rev() {
            let popped_envelope = stack.pop().await.unwrap().unwrap();
            assert_eq!(
                popped_envelope.event_id().unwrap(),
                envelope.event_id().unwrap()
            );
        }
        assert_eq!(stack.batches_buffer_size, 0);

        // We peek the top element, which since the buffer is empty should result in a disk load.
        let peeked_envelope = stack.peek().await.unwrap().unwrap();
        assert_eq!(
            peeked_envelope.event_id().unwrap(),
            envelopes.clone()[4].event_id().unwrap()
        );

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
        assert_eq!(stack.batches_buffer_size, 0);
    }
}
