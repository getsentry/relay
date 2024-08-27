use futures::FutureExt;
use hashbrown::HashSet;
use itertools::Itertools;
use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::time::Instant;

use crate::envelope::Envelope;
use crate::services::buffer::envelope_stack::sqlite::SqliteEnvelopeStackError;
use crate::services::buffer::envelope_stack::EnvelopeStack;
use crate::services::buffer::envelope_store::sqlite::SqliteEnvelopeStoreError;
use crate::services::buffer::envelope_store::EnvelopeProjectKeys;
use crate::services::buffer::stack_provider::memory::MemoryStackProvider;
use crate::services::buffer::stack_provider::sqlite::SqliteStackProvider;
use crate::services::buffer::stack_provider::{InitializationState, StackProvider};
use crate::statsd::{RelayCounters, RelayGauges};
use crate::utils::MemoryChecker;

/// Polymorphic envelope buffering interface.
///
/// The underlying buffer can either be disk-based or memory-based,
/// depending on the given configuration.
///
/// NOTE: This is implemented as an enum because a trait object with async methods would not be
/// object safe.
#[derive(Debug)]
#[allow(private_interfaces)]
pub enum PolymorphicEnvelopeBuffer {
    /// An enveloper buffer that uses in-memory envelopes stacks.
    InMemory(EnvelopeBuffer<MemoryStackProvider>),
    /// An enveloper buffer that uses sqlite envelopes stacks.
    #[allow(dead_code)]
    Sqlite(EnvelopeBuffer<SqliteStackProvider>),
}

impl PolymorphicEnvelopeBuffer {
    /// Creates either a memory-based or a disk-based envelope buffer,
    /// depending on the given configuration.
    pub async fn from_config(
        config: &Config,
        memory_checker: MemoryChecker,
    ) -> Result<Self, EnvelopeBufferError> {
        if config.spool_envelopes_path().is_some() {
            panic!("Disk backend not yet supported for spool V2");
        }

        let mut buffer = Self::InMemory(EnvelopeBuffer::<MemoryStackProvider>::new(memory_checker));
        buffer.initialize().await;

        Ok(buffer)
    }

    /// Initializes the envelope buffer.
    pub async fn initialize(&mut self) {
        match self {
            PolymorphicEnvelopeBuffer::InMemory(buffer) => buffer.initialize().await,
            PolymorphicEnvelopeBuffer::Sqlite(buffer) => buffer.initialize().await,
        }
    }

    /// Adds an envelope to the buffer.
    pub async fn push(&mut self, envelope: Box<Envelope>) -> Result<(), EnvelopeBufferError> {
        match self {
            Self::Sqlite(buffer) => buffer.push(envelope).await,
            Self::InMemory(buffer) => buffer.push(envelope).await,
        }?;
        relay_statsd::metric!(counter(RelayCounters::BufferEnvelopesWritten) += 1);
        Ok(())
    }

    /// Returns a reference to the next-in-line envelope.
    pub async fn peek(&mut self) -> Result<Option<&Envelope>, EnvelopeBufferError> {
        match self {
            Self::Sqlite(buffer) => buffer.peek().await,
            Self::InMemory(buffer) => buffer.peek().await,
        }
    }

    /// Pops the next-in-line envelope.
    pub async fn pop(&mut self) -> Result<Option<Box<Envelope>>, EnvelopeBufferError> {
        let envelope = match self {
            Self::Sqlite(buffer) => buffer.pop().await,
            Self::InMemory(buffer) => buffer.pop().await,
        }?;
        relay_statsd::metric!(counter(RelayCounters::BufferEnvelopesRead) += 1);
        Ok(envelope)
    }

    /// Marks a project as ready or not ready.
    ///
    /// The buffer re-prioritizes its envelopes based on this information.
    pub fn mark_ready(&mut self, project: &ProjectKey, is_ready: bool) -> bool {
        match self {
            Self::Sqlite(buffer) => buffer.mark_ready(project, is_ready),
            Self::InMemory(buffer) => buffer.mark_ready(project, is_ready),
        }
    }

    /// Returns `true` whether the buffer has capacity to accept new [`Envelope`]s.
    pub fn has_capacity(&self) -> bool {
        match self {
            Self::Sqlite(buffer) => buffer.has_capacity(),
            Self::InMemory(buffer) => buffer.has_capacity(),
        }
    }
}

/// Error that occurs while interacting with the envelope buffer.
#[derive(Debug, thiserror::Error)]
pub enum EnvelopeBufferError {
    #[error("sqlite")]
    SqliteStore(#[from] SqliteEnvelopeStoreError),

    #[error("sqlite")]
    SqliteStack(#[from] SqliteEnvelopeStackError),

    #[error("impossible")]
    Impossible(#[from] Infallible),

    #[error("failed to push envelope to the buffer")]
    PushFailed,
}

/// An envelope buffer that holds an individual stack for each project/sampling project combination.
///
/// Envelope stacks are organized in a priority queue, and are reprioritized every time an envelope
/// is pushed, popped, or when a project becomes ready.
#[derive(Debug)]
struct EnvelopeBuffer<P: StackProvider> {
    /// The central priority queue.
    priority_queue:
        priority_queue::PriorityQueue<QueueItem<EnvelopeProjectKeys, P::Stack>, Priority>,
    /// A lookup table to find all stacks involving a project.
    stacks_by_project: hashbrown::HashMap<ProjectKey, BTreeSet<EnvelopeProjectKeys>>,
    /// A provider of stacks that provides utilities to create stacks, check their capacity...
    ///
    /// This indirection is needed because different stack implementations might need different
    /// initialization (e.g. a database connection).
    stack_provider: P,
}

impl EnvelopeBuffer<MemoryStackProvider> {
    /// Creates an empty memory-based buffer.
    pub fn new(memory_checker: MemoryChecker) -> Self {
        let stack_provider = MemoryStackProvider::new(memory_checker);

        Self {
            stacks_by_project: Default::default(),
            priority_queue: Default::default(),
            stack_provider,
        }
    }
}

#[allow(dead_code)]
impl EnvelopeBuffer<SqliteStackProvider> {
    /// Creates an empty sqlite-based buffer.
    pub async fn new(config: &Config) -> Result<Self, EnvelopeBufferError> {
        let stack_provider = SqliteStackProvider::new(config).await?;

        Ok(Self {
            stacks_by_project: Default::default(),
            priority_queue: Default::default(),
            stack_provider,
        })
    }
}

impl<P: StackProvider> EnvelopeBuffer<P>
where
    EnvelopeBufferError: From<<P::Stack as EnvelopeStack>::Error>,
{
    /// Initializes the [`EnvelopeBuffer`] given the [`InitializationState`] from the
    /// [`StackProvider`].
    pub async fn initialize(&mut self) {
        let initialization_state = self.stack_provider.initialize().await;
        self.load_stacks(initialization_state.envelopes_projects_keys)
            .await;
    }

    /// Pushes an envelope to the appropriate envelope stack and re-prioritizes the stack.
    ///
    /// If the envelope stack does not exist, a new stack is pushed to the priority queue.
    /// The priority of the stack is updated with the envelope's received_at time.
    pub async fn push(&mut self, envelope: Box<Envelope>) -> Result<(), EnvelopeBufferError> {
        let received_at = envelope.meta().start_time();
        let envelope_project_keys = EnvelopeProjectKeys::from_envelope(&envelope);
        if let Some((
            QueueItem {
                key: _,
                value: stack,
            },
            _,
        )) = self.priority_queue.get_mut(&envelope_project_keys)
        {
            stack.push(envelope).await?;
        } else {
            self.push_stack(envelope).await?;
        }
        self.priority_queue
            .change_priority_by(&envelope_project_keys, |prio| {
                prio.received_at = received_at;
            });

        Ok(())
    }

    /// Returns a reference to the next-in-line envelope, if one exists.
    pub async fn peek(&mut self) -> Result<Option<&Envelope>, EnvelopeBufferError> {
        let Some((
            QueueItem {
                key: envelope_project_keys,
                value: _,
            },
            _,
        )) = self.priority_queue.peek()
        else {
            return Ok(None);
        };

        let envelope_project_keys = *envelope_project_keys;

        self.priority_queue
            .change_priority_by(&envelope_project_keys, |prio| {
                prio.last_peek = Instant::now();
            });

        let Some((
            QueueItem {
                key: _,
                value: stack,
            },
            _,
        )) = self.priority_queue.get_mut(&envelope_project_keys)
        else {
            return Ok(None);
        };

        Ok(stack.peek().await?)
    }

    /// Returns the next-in-line envelope, if one exists.
    ///
    /// The priority of the envelope's stack is updated with the next envelope's received_at
    /// time. If the stack is empty after popping, it is removed from the priority queue.
    pub async fn pop(&mut self) -> Result<Option<Box<Envelope>>, EnvelopeBufferError> {
        let Some((QueueItem { key, value: stack }, _)) = self.priority_queue.peek_mut() else {
            return Ok(None);
        };
        let envelope_project_keys = *key;
        let envelope = stack.pop().await.unwrap().expect("found an empty stack");

        let next_received_at = stack
            .peek()
            .await?
            .map(|next_envelope| next_envelope.meta().start_time());
        match next_received_at {
            None => {
                self.pop_stack(envelope_project_keys);
            }
            Some(next_received_at) => {
                self.priority_queue
                    .change_priority_by(&envelope_project_keys, |prio| {
                        prio.received_at = next_received_at;
                    });
            }
        }
        Ok(Some(envelope))
    }

    /// Re-prioritizes all stacks that involve the given project key by setting it to "ready".
    pub fn mark_ready(&mut self, project: &ProjectKey, is_ready: bool) -> bool {
        let mut changed = false;
        if let Some(envelope_project_keyss) = self.stacks_by_project.get(project) {
            for envelope_project_keys in envelope_project_keyss {
                self.priority_queue
                    .change_priority_by(envelope_project_keys, |stack| {
                        let mut found = false;
                        for (subkey, readiness) in [
                            (
                                envelope_project_keys.own_key,
                                &mut stack.readiness.own_project_ready,
                            ),
                            (
                                envelope_project_keys.sampling_key,
                                &mut stack.readiness.sampling_project_ready,
                            ),
                        ] {
                            if subkey == *project {
                                found = true;
                                if *readiness != is_ready {
                                    changed = true;
                                    *readiness = is_ready;
                                }
                            }
                        }
                        debug_assert!(found);
                    });
            }
        }
        changed
    }

    /// Returns `true` if the underlying storage has the capacity to store more envelopes.
    pub fn has_capacity(&self) -> bool {
        self.stack_provider.has_store_capacity()
    }

    async fn push_stack(&mut self, envelope: Box<Envelope>) -> Result<(), EnvelopeBufferError> {
        let received_at = envelope.meta().start_time();

        let envelope_project_keys = EnvelopeProjectKeys::from_envelope(&envelope);
        let mut stack = self.stack_provider.create_stack(envelope_project_keys);
        stack.push(envelope).await?;

        let previous_entry = self.priority_queue.push(
            QueueItem {
                key: envelope_project_keys,
                value: stack,
            },
            Priority::new(received_at),
        );

        debug_assert!(previous_entry.is_none());

        for project_key in envelope_project_keys.iter() {
            self.stacks_by_project
                .entry(project_key)
                .or_default()
                .insert(envelope_project_keys);
        }
        relay_statsd::metric!(
            gauge(RelayGauges::BufferStackCount) = self.priority_queue.len() as u64
        );

        Ok(())
    }

    fn pop_stack(&mut self, envelope_project_keys: EnvelopeProjectKeys) {
        for project_key in envelope_project_keys.iter() {
            self.stacks_by_project
                .get_mut(&project_key)
                .expect("project_key is missing from lookup")
                .remove(&envelope_project_keys);
        }
        self.priority_queue.remove(&envelope_project_keys);

        relay_statsd::metric!(
            gauge(RelayGauges::BufferStackCount) = self.priority_queue.len() as u64
        );
    }

    async fn load_stacks(&mut self, envelopes_project_keys: HashSet<EnvelopeProjectKeys>) {
        let received_at = Instant::now();

        for envelope_project_keys in envelopes_project_keys {
            let previous_entry = self.priority_queue.push(
                QueueItem {
                    key: envelope_project_keys,
                    value: self.stack_provider.create_stack(envelope_project_keys),
                },
                // We don't set the readiness of the projects, because we will eventually poll
                // the envelopes for this project keys pair and load the projects if not in cache,
                // which, in turn, will mark the projects as ready via the `mark_ready` method.
                Priority::new(received_at),
            );

            debug_assert!(previous_entry.is_none());

            for project_key in envelope_project_keys.iter() {
                self.stacks_by_project
                    .entry(project_key)
                    .or_default()
                    .insert(envelope_project_keys);
            }
        }
    }
}

#[derive(Debug)]
struct QueueItem<K, V> {
    key: K,
    value: V,
}

impl<K, V> std::borrow::Borrow<K> for QueueItem<K, V> {
    fn borrow(&self) -> &K {
        &self.key
    }
}

impl<K: std::hash::Hash, V> std::hash::Hash for QueueItem<K, V> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

impl<K: PartialEq, V> PartialEq for QueueItem<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<K: PartialEq, V> Eq for QueueItem<K, V> {}

#[derive(Debug)]
struct Priority {
    readiness: Readiness,
    received_at: Instant,
    last_peek: Instant,
}

impl Priority {
    fn new(received_at: Instant) -> Self {
        Self {
            readiness: Readiness::new(),
            received_at,
            last_peek: Instant::now(),
        }
    }
}

impl PartialEq for Priority {
    fn eq(&self, other: &Self) -> bool {
        self.readiness.ready() == other.readiness.ready()
            && self.received_at == other.received_at
            && self.last_peek == other.last_peek
    }
}

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Priority {}

impl Ord for Priority {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.readiness.ready(), other.readiness.ready()) {
            // Assuming that two priorities differ only w.r.t. the `last_peek`, we want to prioritize
            // stacks that were the least recently peeked. The rationale behind this is that we want
            // to keep cycling through different stacks while peeking.
            (true, true) => self
                .received_at
                .cmp(&other.received_at)
                .then(self.last_peek.cmp(&other.last_peek).reverse()),
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            // For non-ready stacks, we invert the priority, such that projects that are not
            // ready and did not receive envelopes recently can be evicted.
            (false, false) => self
                .received_at
                .cmp(&other.received_at)
                .reverse()
                .then(self.last_peek.cmp(&other.last_peek).reverse()),
        }
    }
}

#[derive(Debug)]
struct Readiness {
    own_project_ready: bool,
    sampling_project_ready: bool,
}

impl Readiness {
    fn new() -> Self {
        Self {
            own_project_ready: false,
            sampling_project_ready: false,
        }
    }

    fn ready(&self) -> bool {
        self.own_project_ready && self.sampling_project_ready
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use relay_common::Dsn;
    use relay_event_schema::protocol::EventId;
    use relay_sampling::DynamicSamplingContext;
    use uuid::Uuid;

    use crate::envelope::{Item, ItemType};
    use crate::extractors::RequestMeta;
    use crate::utils::MemoryStat;

    use super::*;

    fn new_envelope(
        project_key: ProjectKey,
        sampling_key: Option<ProjectKey>,
        event_id: Option<EventId>,
    ) -> Box<Envelope> {
        let mut envelope = Envelope::from_request(
            None,
            RequestMeta::new(Dsn::from_str(&format!("http://{project_key}@localhost/1")).unwrap()),
        );
        if let Some(sampling_key) = sampling_key {
            envelope.set_dsc(DynamicSamplingContext {
                public_key: sampling_key,
                trace_id: Uuid::new_v4(),
                release: None,
                user: Default::default(),
                replay_id: None,
                environment: None,
                transaction: None,
                sample_rate: None,
                sampled: None,
                other: Default::default(),
            });
            envelope.add_item(Item::new(ItemType::Transaction));
        }
        if let Some(event_id) = event_id {
            envelope.set_event_id(event_id);
        }
        envelope
    }

    fn mock_memory_checker() -> MemoryChecker {
        let config: Arc<_> = Config::from_json_value(serde_json::json!({
            "spool": {
                "health": {
                    "max_memory_percent": 1.0
                }
            }
        }))
        .unwrap()
        .into();

        MemoryChecker::new(MemoryStat::default(), config.clone())
    }

    #[tokio::test]
    async fn test_insert_pop() {
        let mut buffer = EnvelopeBuffer::<MemoryStackProvider>::new(mock_memory_checker());

        let project_key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let project_key2 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let project_key3 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fef").unwrap();

        assert!(buffer.pop().await.unwrap().is_none());
        assert!(buffer.peek().await.unwrap().is_none());

        buffer
            .push(new_envelope(project_key1, None, None))
            .await
            .unwrap();
        assert_eq!(
            buffer.peek().await.unwrap().unwrap().meta().public_key(),
            project_key1
        );

        buffer
            .push(new_envelope(project_key2, None, None))
            .await
            .unwrap();
        // Both projects are not ready, so project 1 is on top (has the oldest envelopes):
        assert_eq!(
            buffer.peek().await.unwrap().unwrap().meta().public_key(),
            project_key1
        );

        buffer
            .push(new_envelope(project_key3, None, None))
            .await
            .unwrap();
        // All projects are not ready, so project 1 is on top (has the oldest envelopes):
        assert_eq!(
            buffer.peek().await.unwrap().unwrap().meta().public_key(),
            project_key1
        );

        // After marking a project ready, it goes to the top:
        buffer.mark_ready(&project_key3, true);
        assert_eq!(
            buffer.peek().await.unwrap().unwrap().meta().public_key(),
            project_key3
        );
        assert_eq!(
            buffer.pop().await.unwrap().unwrap().meta().public_key(),
            project_key3
        );

        // After popping, project 1 is on top again:
        assert_eq!(
            buffer.peek().await.unwrap().unwrap().meta().public_key(),
            project_key1
        );

        // Mark project 1 as ready (still on top):
        buffer.mark_ready(&project_key1, true);
        assert_eq!(
            buffer.peek().await.unwrap().unwrap().meta().public_key(),
            project_key1
        );

        // Mark project 2 as ready as well (now on top because most recent):
        buffer.mark_ready(&project_key2, true);
        assert_eq!(
            buffer.peek().await.unwrap().unwrap().meta().public_key(),
            project_key2
        );
        assert_eq!(
            buffer.pop().await.unwrap().unwrap().meta().public_key(),
            project_key2
        );

        // Pop last element:
        assert_eq!(
            buffer.pop().await.unwrap().unwrap().meta().public_key(),
            project_key1
        );
        assert!(buffer.pop().await.unwrap().is_none());
        assert!(buffer.peek().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_project_internal_order() {
        let mut buffer = EnvelopeBuffer::<MemoryStackProvider>::new(mock_memory_checker());

        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();

        let envelope1 = new_envelope(project_key, None, None);
        let instant1 = envelope1.meta().start_time();
        let envelope2 = new_envelope(project_key, None, None);
        let instant2 = envelope2.meta().start_time();

        assert!(instant2 > instant1);

        buffer.push(envelope1).await.unwrap();
        buffer.push(envelope2).await.unwrap();

        assert_eq!(
            buffer.pop().await.unwrap().unwrap().meta().start_time(),
            instant2
        );
        assert_eq!(
            buffer.pop().await.unwrap().unwrap().meta().start_time(),
            instant1
        );
        assert!(buffer.pop().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_sampling_projects() {
        let mut buffer = EnvelopeBuffer::<MemoryStackProvider>::new(mock_memory_checker());

        let project_key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let project_key2 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fef").unwrap();

        let envelope1 = new_envelope(project_key1, None, None);
        let instant1 = envelope1.meta().start_time();
        buffer.push(envelope1).await.unwrap();

        let envelope2 = new_envelope(project_key2, None, None);
        let instant2 = envelope2.meta().start_time();
        buffer.push(envelope2).await.unwrap();

        let envelope3 = new_envelope(project_key1, Some(project_key2), None);
        let instant3 = envelope3.meta().start_time();
        buffer.push(envelope3).await.unwrap();

        // Nothing is ready, instant1 is on top:
        assert_eq!(
            buffer.peek().await.unwrap().unwrap().meta().start_time(),
            instant1
        );

        // Mark project 2 ready, gets on top:
        buffer.mark_ready(&project_key2, true);
        assert_eq!(
            buffer.peek().await.unwrap().unwrap().meta().start_time(),
            instant2
        );

        // Revert
        buffer.mark_ready(&project_key2, false);
        assert_eq!(
            buffer.peek().await.unwrap().unwrap().meta().start_time(),
            instant1
        );

        // Project 1 ready:
        buffer.mark_ready(&project_key1, true);
        assert_eq!(
            buffer.peek().await.unwrap().unwrap().meta().start_time(),
            instant1
        );

        // when both projects are ready, event no 3 ends up on top:
        buffer.mark_ready(&project_key2, true);
        assert_eq!(
            buffer.pop().await.unwrap().unwrap().meta().start_time(),
            instant3
        );
        assert_eq!(
            buffer.peek().await.unwrap().unwrap().meta().start_time(),
            instant2
        );

        buffer.mark_ready(&project_key2, false);
        assert_eq!(
            buffer.pop().await.unwrap().unwrap().meta().start_time(),
            instant1
        );
        assert_eq!(
            buffer.pop().await.unwrap().unwrap().meta().start_time(),
            instant2
        );

        assert!(buffer.pop().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_project_keys_distinct() {
        let project_key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let project_key2 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fef").unwrap();

        let envelope_project_keys1 = EnvelopeProjectKeys::new(project_key1, project_key2);
        let envelope_project_keys2 = EnvelopeProjectKeys::new(project_key2, project_key1);

        assert_ne!(envelope_project_keys1, envelope_project_keys2);

        let mut buffer = EnvelopeBuffer::<MemoryStackProvider>::new(mock_memory_checker());
        buffer
            .push(new_envelope(project_key1, Some(project_key2), None))
            .await
            .unwrap();
        buffer
            .push(new_envelope(project_key2, Some(project_key1), None))
            .await
            .unwrap();
        assert_eq!(buffer.priority_queue.len(), 2);
    }

    #[tokio::test]
    async fn test_last_peek_internal_order() {
        let mut buffer = EnvelopeBuffer::<MemoryStackProvider>::new(mock_memory_checker());

        let project_key_1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let event_id_1 = EventId::new();
        let envelope1 = new_envelope(project_key_1, None, Some(event_id_1));

        let project_key_2 = ProjectKey::parse("b56ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let event_id_2 = EventId::new();
        let mut envelope2 = new_envelope(project_key_2, None, Some(event_id_2));
        envelope2.set_start_time(envelope1.meta().start_time());

        buffer.push(envelope1).await.unwrap();
        buffer.push(envelope2).await.unwrap();

        assert_eq!(
            buffer.peek().await.unwrap().unwrap().event_id().unwrap(),
            event_id_1
        );
        assert_eq!(
            buffer.peek().await.unwrap().unwrap().event_id().unwrap(),
            event_id_2
        );
        assert_eq!(
            buffer.peek().await.unwrap().unwrap().event_id().unwrap(),
            event_id_1
        );
    }
}
