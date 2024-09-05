use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::error::Error;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hashbrown::HashSet;
use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use tokio::time::timeout;

use crate::envelope::Envelope;
use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_stack::sqlite::SqliteEnvelopeStackError;
use crate::services::buffer::envelope_stack::EnvelopeStack;
use crate::services::buffer::envelope_store::sqlite::SqliteEnvelopeStoreError;
use crate::services::buffer::stack_provider::memory::MemoryStackProvider;
use crate::services::buffer::stack_provider::sqlite::SqliteStackProvider;
use crate::services::buffer::stack_provider::{StackCreationType, StackProvider};
use crate::statsd::{RelayCounters, RelayGauges, RelayHistograms, RelayTimers};
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
    Sqlite(EnvelopeBuffer<SqliteStackProvider>),
}

impl PolymorphicEnvelopeBuffer {
    /// Returns true if the implementation stores envelopes on external storage (e.g. disk).
    pub fn is_external(&self) -> bool {
        match self {
            PolymorphicEnvelopeBuffer::InMemory(_) => false,
            PolymorphicEnvelopeBuffer::Sqlite(_) => true,
        }
    }

    /// Creates either a memory-based or a disk-based envelope buffer,
    /// depending on the given configuration.
    pub async fn from_config(
        config: &Config,
        memory_checker: MemoryChecker,
    ) -> Result<Self, EnvelopeBufferError> {
        let buffer = if config.spool_envelopes_path().is_some() {
            let buffer = EnvelopeBuffer::<SqliteStackProvider>::new(config).await?;
            Self::Sqlite(buffer)
        } else {
            let buffer = EnvelopeBuffer::<MemoryStackProvider>::new(memory_checker);
            Self::InMemory(buffer)
        };

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
    pub async fn peek(&mut self) -> Result<Peek, EnvelopeBufferError> {
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
    /// Returns `true` if at least one priority was changed.
    pub fn mark_ready(&mut self, project: &ProjectKey, is_ready: bool) -> bool {
        match self {
            Self::Sqlite(buffer) => buffer.mark_ready(project, is_ready),
            Self::InMemory(buffer) => buffer.mark_ready(project, is_ready),
        }
    }

    /// Marks a stack as seen.
    ///
    /// Non-ready stacks are deprioritized when they are marked as seen, such that
    /// the next call to `.peek()` will look at a different stack. This prevents
    /// head-of-line blocking.
    pub fn mark_seen(&mut self, project_key_pair: &ProjectKeyPair) {
        match self {
            Self::Sqlite(buffer) => buffer.mark_seen(project_key_pair),
            Self::InMemory(buffer) => buffer.mark_seen(project_key_pair),
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

    #[error("failed to push envelope to the buffer")]
    PushFailed,

    #[error("impossible")]
    Impossible(#[from] Infallible),
}

/// An envelope buffer that holds an individual stack for each project/sampling project combination.
///
/// Envelope stacks are organized in a priority queue, and are re-prioritized every time an envelope
/// is pushed, popped, or when a project becomes ready.
#[derive(Debug)]
struct EnvelopeBuffer<P: StackProvider> {
    /// The central priority queue.
    priority_queue: priority_queue::PriorityQueue<QueueItem<ProjectKeyPair, P::Stack>, Priority>,
    /// A lookup table to find all stacks involving a project.
    stacks_by_project: hashbrown::HashMap<ProjectKey, BTreeSet<ProjectKeyPair>>,
    /// A provider of stacks that provides utilities to create stacks, check their capacity...
    ///
    /// This indirection is needed because different stack implementations might need different
    /// initialization (e.g. a database connection).
    stack_provider: P,
    /// The total count of envelopes that the buffer is working with.
    ///
    /// Note that this count is not meant to be perfectly accurate since the initialization of the
    /// count might not succeed if it takes more than a set timeout. For example, if we load the
    /// count of all envelopes from disk, and it takes more than the time we set, we will mark the
    /// initial count as 0 and just count incoming and outgoing envelopes from the buffer.
    total_count: Arc<AtomicI64>,
    /// Whether the count initialization succeeded or not.
    ///
    /// This boolean is just used for tagging the metric that tracks the total count of envelopes
    /// in the buffer.
    total_count_initialized: bool,
}

impl EnvelopeBuffer<MemoryStackProvider> {
    /// Creates an empty memory-based buffer.
    pub fn new(memory_checker: MemoryChecker) -> Self {
        Self {
            stacks_by_project: Default::default(),
            priority_queue: Default::default(),
            stack_provider: MemoryStackProvider::new(memory_checker),
            total_count: Arc::new(AtomicI64::new(0)),
            total_count_initialized: false,
        }
    }
}

#[allow(dead_code)]
impl EnvelopeBuffer<SqliteStackProvider> {
    /// Creates an empty sqlite-based buffer.
    pub async fn new(config: &Config) -> Result<Self, EnvelopeBufferError> {
        Ok(Self {
            stacks_by_project: Default::default(),
            priority_queue: Default::default(),
            stack_provider: SqliteStackProvider::new(config).await?,
            total_count: Arc::new(AtomicI64::new(0)),
            total_count_initialized: false,
        })
    }
}

impl<P: StackProvider> EnvelopeBuffer<P>
where
    EnvelopeBufferError: From<<P::Stack as EnvelopeStack>::Error>,
{
    /// Initializes the [`EnvelopeBuffer`] given the initialization state from the
    /// [`StackProvider`].
    pub async fn initialize(&mut self) {
        relay_statsd::metric!(timer(RelayTimers::BufferInitialization), {
            let initialization_state = self.stack_provider.initialize().await;
            self.load_stacks(initialization_state.project_key_pairs)
                .await;
            self.load_store_total_count().await;
        });
    }

    /// Pushes an envelope to the appropriate envelope stack and re-prioritizes the stack.
    ///
    /// If the envelope stack does not exist, a new stack is pushed to the priority queue.
    /// The priority of the stack is updated with the envelope's received_at time.
    pub async fn push(&mut self, envelope: Box<Envelope>) -> Result<(), EnvelopeBufferError> {
        let received_at = envelope.meta().start_time();
        let project_key_pair = ProjectKeyPair::from_envelope(&envelope);
        if let Some((
            QueueItem {
                key: _,
                value: stack,
            },
            _,
        )) = self.priority_queue.get_mut(&project_key_pair)
        {
            stack.push(envelope).await?;
        } else {
            // Since we have initialization code that creates all the necessary stacks, we assume
            // that any new stack that is added during the envelope buffer's lifecycle, is recreated.
            self.push_stack(
                StackCreationType::New,
                ProjectKeyPair::from_envelope(&envelope),
                Some(envelope),
            )
            .await?;
        }
        self.priority_queue
            .change_priority_by(&project_key_pair, |prio| {
                prio.received_at = received_at;
            });

        self.total_count.fetch_add(1, AtomicOrdering::SeqCst);
        self.track_total_count();

        Ok(())
    }

    /// Returns a reference to the next-in-line envelope, if one exists.
    pub async fn peek(&mut self) -> Result<Peek, EnvelopeBufferError> {
        let Some((
            QueueItem {
                key: stack_key,
                value: stack,
            },
            Priority { readiness, .. },
        )) = self.priority_queue.peek_mut()
        else {
            return Ok(Peek::Empty);
        };

        let ready = readiness.ready();

        Ok(match (stack.peek().await?, ready) {
            (None, _) => Peek::Empty,
            (Some(envelope), true) => Peek::Ready(envelope),
            (Some(envelope), false) => Peek::NotReady(*stack_key, envelope),
        })
    }

    /// Returns the next-in-line envelope, if one exists.
    ///
    /// The priority of the envelope's stack is updated with the next envelope's received_at
    /// time. If the stack is empty after popping, it is removed from the priority queue.
    pub async fn pop(&mut self) -> Result<Option<Box<Envelope>>, EnvelopeBufferError> {
        let Some((QueueItem { key, value: stack }, _)) = self.priority_queue.peek_mut() else {
            return Ok(None);
        };
        let project_key_pair = *key;
        let envelope = stack.pop().await.unwrap().expect("found an empty stack");

        let next_received_at = stack
            .peek()
            .await?
            .map(|next_envelope| next_envelope.meta().start_time());

        match next_received_at {
            None => {
                relay_statsd::metric!(counter(RelayCounters::BufferEnvelopeStacksPopped) += 1);
                self.pop_stack(project_key_pair);
            }
            Some(next_received_at) => {
                self.priority_queue
                    .change_priority_by(&project_key_pair, |prio| {
                        prio.received_at = next_received_at;
                    });
            }
        }

        // We are fine with the count going negative, since it represents that more data was popped,
        // than it was initially counted, meaning that we had a wrong total count from
        // initialization.
        self.total_count.fetch_sub(1, AtomicOrdering::SeqCst);
        self.track_total_count();

        Ok(Some(envelope))
    }

    /// Re-prioritizes all stacks that involve the given project key by setting it to "ready".
    ///
    /// Returns `true` if at least one priority was changed.
    pub fn mark_ready(&mut self, project: &ProjectKey, is_ready: bool) -> bool {
        let mut changed = false;
        if let Some(project_key_pair) = self.stacks_by_project.get(project) {
            for project_key_pair in project_key_pair {
                self.priority_queue
                    .change_priority_by(project_key_pair, |stack| {
                        let mut found = false;
                        for (subkey, readiness) in [
                            (
                                project_key_pair.own_key,
                                &mut stack.readiness.own_project_ready,
                            ),
                            (
                                project_key_pair.sampling_key,
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

    /// Marks a stack as seen.
    ///
    /// Non-ready stacks are deprioritized when they are marked as seen, such that
    /// the next call to `.peek()` will look at a different stack. This prevents
    /// head-of-line blocking.
    pub fn mark_seen(&mut self, project_key_pair: &ProjectKeyPair) {
        self.priority_queue
            .change_priority_by(project_key_pair, |stack| {
                stack.last_peek = Instant::now();
            });
    }

    /// Pushes a new [`EnvelopeStack`] with the given [`Envelope`] inserted.
    async fn push_stack(
        &mut self,
        stack_creation_type: StackCreationType,
        project_key_pair: ProjectKeyPair,
        envelope: Option<Box<Envelope>>,
    ) -> Result<(), EnvelopeBufferError> {
        let received_at = envelope
            .as_ref()
            .map_or(Instant::now(), |e| e.meta().start_time());

        let mut stack = self
            .stack_provider
            .create_stack(stack_creation_type, project_key_pair);
        if let Some(envelope) = envelope {
            stack.push(envelope).await?;
        }

        let previous_entry = self.priority_queue.push(
            QueueItem {
                key: project_key_pair,
                value: stack,
            },
            Priority::new(received_at),
        );
        debug_assert!(previous_entry.is_none());
        for project_key in project_key_pair.iter() {
            self.stacks_by_project
                .entry(project_key)
                .or_default()
                .insert(project_key_pair);
        }
        relay_statsd::metric!(
            gauge(RelayGauges::BufferStackCount) = self.priority_queue.len() as u64
        );

        Ok(())
    }

    /// Returns `true` if the underlying storage has the capacity to store more envelopes.
    pub fn has_capacity(&self) -> bool {
        self.stack_provider.has_store_capacity()
    }

    /// Pops an [`EnvelopeStack`] with the supplied [`EnvelopeBufferError`].
    fn pop_stack(&mut self, project_key_pair: ProjectKeyPair) {
        for project_key in project_key_pair.iter() {
            self.stacks_by_project
                .get_mut(&project_key)
                .expect("project_key is missing from lookup")
                .remove(&project_key_pair);
        }
        self.priority_queue.remove(&project_key_pair);

        relay_statsd::metric!(
            gauge(RelayGauges::BufferStackCount) = self.priority_queue.len() as u64
        );
    }

    /// Creates all the [`EnvelopeStack`]s with no data given a set of [`ProjectKeyPair`].
    async fn load_stacks(&mut self, project_key_pairs: HashSet<ProjectKeyPair>) {
        for project_key_pair in project_key_pairs {
            self.push_stack(StackCreationType::Initialization, project_key_pair, None)
                .await
                .expect("Pushing an empty stack raised an error");
        }
    }

    /// Loads the total count from the store if it takes less than a specified duration.
    ///
    /// The total count returned by the store is related to the count of elements that the buffer
    /// will process, besides the count of elements that will be added and removed during its
    /// lifecycle
    async fn load_store_total_count(&mut self) {
        let total_count = timeout(Duration::from_secs(1), async {
            self.stack_provider.store_total_count().await
        })
        .await;
        match total_count {
            Ok(total_count) => {
                self.total_count
                    .store(total_count as i64, AtomicOrdering::SeqCst);
                self.total_count_initialized = true;
            }
            Err(error) => {
                self.total_count_initialized = false;
                relay_log::error!(
                    error = &error as &dyn Error,
                    "failed to load the total envelope count of the store",
                );
            }
        };
        self.track_total_count();
    }

    /// Emits a metric to track the total count of envelopes that are in the envelope buffer.
    fn track_total_count(&self) {
        let total_count = self.total_count.load(AtomicOrdering::SeqCst) as f64;
        let initialized = match self.total_count_initialized {
            true => "true",
            false => "false",
        };
        relay_statsd::metric!(
            histogram(RelayHistograms::BufferEnvelopesCount) = total_count,
            initialized = initialized,
            stack_type = self.stack_provider.stack_type()
        );
    }
}

/// Contains a reference to the first element in the buffer, together with its stack's ready state.
pub enum Peek<'a> {
    Empty,
    Ready(&'a Envelope),
    NotReady(ProjectKeyPair, &'a Envelope),
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
    // FIXME(jjbayer): `last_peek` is currently never updated, see https://github.com/getsentry/relay/pull/3960.
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
            (true, true) => self.received_at.cmp(&other.received_at),
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            // For non-ready stacks, we invert the priority, such that projects that are not
            // ready and did not receive envelopes recently can be evicted.
            (false, false) => self
                .last_peek
                .cmp(&other.last_peek)
                .reverse()
                .then(self.received_at.cmp(&other.received_at).reverse()),
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
    use relay_common::Dsn;
    use relay_event_schema::protocol::EventId;
    use relay_sampling::DynamicSamplingContext;
    use std::str::FromStr;
    use std::sync::Arc;
    use uuid::Uuid;

    use crate::envelope::{Item, ItemType};
    use crate::extractors::RequestMeta;
    use crate::services::buffer::common::ProjectKeyPair;
    use crate::services::buffer::testutils::utils::mock_envelopes;
    use crate::utils::MemoryStat;
    use crate::SqliteEnvelopeStore;

    use super::*;

    impl Peek<'_> {
        fn is_empty(&self) -> bool {
            matches!(self, Peek::Empty)
        }

        fn envelope(&self) -> Option<&Envelope> {
            match self {
                Peek::Empty => None,
                Peek::Ready(envelope) | Peek::NotReady(_, envelope) => Some(envelope),
            }
        }
    }

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

    fn mock_config(path: &str) -> Arc<Config> {
        Config::from_json_value(serde_json::json!({
            "spool": {
                "envelopes": {
                    "path": path
                }
            }
        }))
        .unwrap()
        .into()
    }

    fn mock_memory_checker() -> MemoryChecker {
        MemoryChecker::new(MemoryStat::default(), mock_config("my/db/path").clone())
    }

    #[tokio::test]
    async fn test_insert_pop() {
        let mut buffer = EnvelopeBuffer::<MemoryStackProvider>::new(mock_memory_checker());

        let project_key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let project_key2 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let project_key3 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fef").unwrap();

        assert!(buffer.pop().await.unwrap().is_none());
        assert!(buffer.peek().await.unwrap().is_empty());

        buffer
            .push(new_envelope(project_key1, None, None))
            .await
            .unwrap();
        assert_eq!(
            buffer
                .peek()
                .await
                .unwrap()
                .envelope()
                .unwrap()
                .meta()
                .public_key(),
            project_key1
        );

        buffer
            .push(new_envelope(project_key2, None, None))
            .await
            .unwrap();
        // Both projects are not ready, so project 1 is on top (has the oldest envelopes):
        assert_eq!(
            buffer
                .peek()
                .await
                .unwrap()
                .envelope()
                .unwrap()
                .meta()
                .public_key(),
            project_key1
        );

        buffer
            .push(new_envelope(project_key3, None, None))
            .await
            .unwrap();
        // All projects are not ready, so project 1 is on top (has the oldest envelopes):
        assert_eq!(
            buffer
                .peek()
                .await
                .unwrap()
                .envelope()
                .unwrap()
                .meta()
                .public_key(),
            project_key1
        );

        // After marking a project ready, it goes to the top:
        buffer.mark_ready(&project_key3, true);
        assert_eq!(
            buffer
                .peek()
                .await
                .unwrap()
                .envelope()
                .unwrap()
                .meta()
                .public_key(),
            project_key3
        );
        assert_eq!(
            buffer.pop().await.unwrap().unwrap().meta().public_key(),
            project_key3
        );

        // After popping, project 1 is on top again:
        assert_eq!(
            buffer
                .peek()
                .await
                .unwrap()
                .envelope()
                .unwrap()
                .meta()
                .public_key(),
            project_key1
        );

        // Mark project 1 as ready (still on top):
        buffer.mark_ready(&project_key1, true);
        assert_eq!(
            buffer
                .peek()
                .await
                .unwrap()
                .envelope()
                .unwrap()
                .meta()
                .public_key(),
            project_key1
        );

        // Mark project 2 as ready as well (now on top because most recent):
        buffer.mark_ready(&project_key2, true);
        assert_eq!(
            buffer
                .peek()
                .await
                .unwrap()
                .envelope()
                .unwrap()
                .meta()
                .public_key(),
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
        assert!(buffer.peek().await.unwrap().is_empty());
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
            buffer
                .peek()
                .await
                .unwrap()
                .envelope()
                .unwrap()
                .meta()
                .start_time(),
            instant1
        );

        // Mark project 2 ready, gets on top:
        buffer.mark_ready(&project_key2, true);
        assert_eq!(
            buffer
                .peek()
                .await
                .unwrap()
                .envelope()
                .unwrap()
                .meta()
                .start_time(),
            instant2
        );

        // Revert
        buffer.mark_ready(&project_key2, false);
        assert_eq!(
            buffer
                .peek()
                .await
                .unwrap()
                .envelope()
                .unwrap()
                .meta()
                .start_time(),
            instant1
        );

        // Project 1 ready:
        buffer.mark_ready(&project_key1, true);
        assert_eq!(
            buffer
                .peek()
                .await
                .unwrap()
                .envelope()
                .unwrap()
                .meta()
                .start_time(),
            instant1
        );

        // when both projects are ready, event no 3 ends up on top:
        buffer.mark_ready(&project_key2, true);
        assert_eq!(
            buffer.pop().await.unwrap().unwrap().meta().start_time(),
            instant3
        );
        assert_eq!(
            buffer
                .peek()
                .await
                .unwrap()
                .envelope()
                .unwrap()
                .meta()
                .start_time(),
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

        let project_key_pair1 = ProjectKeyPair::new(project_key1, project_key2);
        let project_key_pair2 = ProjectKeyPair::new(project_key2, project_key1);

        assert_ne!(project_key_pair1, project_key_pair2);

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
        let envelope2 = new_envelope(project_key_2, None, Some(event_id_2));

        buffer.push(envelope1).await.unwrap();
        buffer.push(envelope2).await.unwrap();

        // event_id_1 is first element:
        let Peek::NotReady(_, envelope) = buffer.peek().await.unwrap() else {
            panic!();
        };
        assert_eq!(envelope.event_id(), Some(event_id_1));

        // Second peek returns same element:
        let Peek::NotReady(stack_key, envelope) = buffer.peek().await.unwrap() else {
            panic!();
        };
        assert_eq!(envelope.event_id(), Some(event_id_1));

        buffer.mark_seen(&stack_key);

        // After mark_seen, event 2 is on top:
        let Peek::NotReady(_, envelope) = buffer.peek().await.unwrap() else {
            panic!();
        };
        assert_eq!(envelope.event_id(), Some(event_id_2));

        let Peek::NotReady(stack_key, envelope) = buffer.peek().await.unwrap() else {
            panic!();
        };
        assert_eq!(envelope.event_id(), Some(event_id_2));

        buffer.mark_seen(&stack_key);

        // After another mark_seen, cycle back to event 1:
        let Peek::NotReady(_, envelope) = buffer.peek().await.unwrap() else {
            panic!();
        };
        assert_eq!(envelope.event_id(), Some(event_id_1));
    }

    #[tokio::test]
    async fn test_initialize_buffer() {
        let path = std::env::temp_dir()
            .join(Uuid::new_v4().to_string())
            .into_os_string()
            .into_string()
            .unwrap();
        let config = mock_config(&path);
        let mut store = SqliteEnvelopeStore::prepare(&config).await.unwrap();
        let mut buffer = EnvelopeBuffer::<SqliteStackProvider>::new(&config)
            .await
            .unwrap();

        // We write 5 envelopes to disk so that we can check if they are loaded. These envelopes
        // belong to the same project keys, so they belong to the same envelope stack.
        let envelopes = mock_envelopes(10);
        assert!(store
            .insert_many(envelopes.iter().map(|e| e.as_ref().try_into().unwrap()))
            .await
            .is_ok());

        // We assume that the buffer is empty.
        assert!(buffer.priority_queue.is_empty());
        assert!(buffer.stacks_by_project.is_empty());

        buffer.initialize().await;

        // We assume that we loaded only 1 envelope stack, because of the project keys combinations
        // of the envelopes we inserted above.
        assert_eq!(buffer.priority_queue.len(), 1);
        // We expect to have an entry per project key, since we have 1 pair, the total entries
        // should be 2.
        assert_eq!(buffer.stacks_by_project.len(), 2);
    }
}
