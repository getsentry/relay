use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::error::Error;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::Arc;
use std::time::Duration;

use crate::envelope::Envelope;
use crate::services::buffer::common::{EnvelopeBufferError, ProjectKeyPair};
use crate::services::buffer::envelope_repository::EnvelopeRepository;
use crate::statsd::{RelayCounters, RelayGauges, RelayHistograms, RelayTimers};
use crate::MemoryChecker;
use hashbrown::{HashMap, HashSet};
use priority_queue::PriorityQueue;
use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use tokio::time::{timeout, Instant};

/// An envelope buffer that holds an individual stack for each project/sampling project combination.
///
/// Envelope stacks are organized in a priority queue, and are re-prioritized every time an envelope
/// is pushed, popped, or when a project becomes ready.
#[derive(Debug)]
pub struct EnvelopeBuffer {
    /// The central priority queue.
    priority_queue: PriorityQueue<ProjectKeyPair, Priority>,
    /// A lookup table to find all project key pairs for a given project.
    project_to_pairs: HashMap<ProjectKey, BTreeSet<ProjectKeyPair>>,
    /// Provider of envelopes that can provide envelopes via different implementations.
    envelope_repository: EnvelopeRepository,
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

impl EnvelopeBuffer {
    /// Creates either a memory-based or a disk-based envelope buffer,
    /// depending on the given configuration.
    pub async fn from_config(
        config: &Config,
        memory_checker: MemoryChecker,
    ) -> Result<Self, EnvelopeBufferError> {
        let buffer = if config.spool_envelopes_path().is_some() {
            relay_log::trace!("EnvelopeBuffer: initializing sqlite envelope buffer");
            Self::sqlite(config).await?
        } else {
            relay_log::trace!("EnvelopeBuffer: initializing memory envelope buffer");
            Self::memory(memory_checker)?
        };

        Ok(buffer)
    }

    /// Creates a memory-based [`EnvelopeBuffer`].
    fn memory(memory_checker: MemoryChecker) -> Result<Self, EnvelopeBufferError> {
        Ok(Self {
            project_to_pairs: Default::default(),
            priority_queue: Default::default(),
            envelope_repository: EnvelopeRepository::memory(memory_checker)?,
            total_count: Arc::new(AtomicI64::new(0)),
            total_count_initialized: false,
        })
    }

    /// Creates a sqlite-based [`EnvelopeBuffer`].
    async fn sqlite(config: &Config) -> Result<Self, EnvelopeBufferError> {
        Ok(Self {
            project_to_pairs: Default::default(),
            priority_queue: Default::default(),
            envelope_repository: EnvelopeRepository::sqlite(config).await?,
            total_count: Arc::new(AtomicI64::new(0)),
            total_count_initialized: false,
        })
    }

    /// Initializes the [`EnvelopeBuffer`] given the initialization state from the
    /// [`EnvelopeRepository`].
    pub async fn initialize(&mut self) {
        relay_statsd::metric!(timer(RelayTimers::BufferInitialization), {
            let initialization_state = match &mut self.envelope_repository {
                EnvelopeRepository::Memory(provider) => provider.initialize().await,
                EnvelopeRepository::SQLite(provider) => provider.initialize().await,
            };
            self.load_project_key_pairs(initialization_state.project_key_pairs)
                .await;
            self.load_store_total_count().await;
        });
    }

    /// Pushes an envelope to the [`EnvelopeRepository`] and updates the priority queue accordingly.
    pub async fn push(&mut self, envelope: Box<Envelope>) -> Result<(), EnvelopeBufferError> {
        relay_statsd::metric!(timer(RelayTimers::BufferPush), {
            let received_at = envelope.meta().start_time().into();
            let project_key_pair = ProjectKeyPair::from_envelope(&envelope);

            // If we haven't seen this project key pair, we will add it to the priority queue, otherwise
            // we just update its priority.
            if self.priority_queue.get_mut(&project_key_pair).is_none() {
                self.add(project_key_pair, Some(envelope.as_ref()))
            } else {
                self.priority_queue
                    .change_priority_by(&project_key_pair, |prio| {
                        prio.received_at = received_at;
                    });
            }

            self.envelope_repository
                .push(project_key_pair, envelope)
                .await?;

            self.total_count.fetch_add(1, AtomicOrdering::SeqCst);
            self.track_total_count();

            Ok(())
        })
    }

    /// Returns a reference to the next-in-line envelope, if one exists.
    pub async fn peek(&mut self) -> Result<Peek, EnvelopeBufferError> {
        relay_statsd::metric!(timer(RelayTimers::BufferPeek), {
            let Some((&project_key_pair, priority)) = self.priority_queue.peek() else {
                return Ok(Peek::Empty);
            };

            let envelope = self.envelope_repository.peek(project_key_pair).await?;

            Ok(match (envelope, priority.readiness.ready()) {
                (None, _) => Peek::Empty,
                (Some(envelope), true) => Peek::Ready(envelope),
                (Some(envelope), false) => {
                    Peek::NotReady(project_key_pair, priority.next_project_fetch, envelope)
                }
            })
        })
    }

    /// Returns the next-in-line envelope, if one exists.
    ///
    /// The priority of the [`ProjectKeyPair`] is updated with the next envelope's received_at
    /// time.
    pub async fn pop(&mut self) -> Result<Option<Box<Envelope>>, EnvelopeBufferError> {
        relay_statsd::metric!(timer(RelayTimers::BufferPop), {
            let Some((&project_key_pair, _)) = self.priority_queue.peek() else {
                return Ok(None);
            };

            let Some(envelope) = self.envelope_repository.pop(project_key_pair).await? else {
                // If we have no data from the envelope repository, we remove this project key pair
                // from the priority queue to free some memory up.
                relay_statsd::metric!(counter(RelayCounters::BufferEnvelopeStacksPopped) += 1);
                self.remove(project_key_pair);
                return Ok(None);
            };

            let next_received_at = self
                .envelope_repository
                .peek(project_key_pair)
                .await?
                .map(|next_envelope| next_envelope.meta().start_time().into());
            match next_received_at {
                None => {
                    self.remove(project_key_pair);
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
        })
    }

    /// Re-prioritizes all [`ProjectKeyPair`]s that involve the given project key by setting it to
    /// "ready".
    ///
    /// Returns `true` if at least one priority was changed.
    pub fn mark_ready(&mut self, project: &ProjectKey, is_ready: bool) -> bool {
        let mut changed = false;
        if let Some(project_key_pairs) = self.project_to_pairs.get(project) {
            for project_key_pair in project_key_pairs {
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

    /// Marks a [`ProjectKeyPair`] as seen.
    ///
    /// Non-ready stacks are deprioritized when they are marked as seen, such that
    /// the next call to `.peek()` will look at a different stack. This prevents
    /// head-of-line blocking.
    pub fn mark_seen(&mut self, project_key_pair: &ProjectKeyPair, next_fetch: Duration) {
        self.priority_queue
            .change_priority_by(project_key_pair, |stack| {
                // We use the next project fetch to debounce project fetching and avoid head of
                // line blocking of non-ready stacks.
                stack.next_project_fetch = Instant::now() + next_fetch;
            });
    }

    /// Returns `true` if the underlying storage has the capacity to store more envelopes, false
    /// otherwise.
    pub fn has_capacity(&self) -> bool {
        match &self.envelope_repository {
            EnvelopeRepository::Memory(provider) => provider.has_store_capacity(),
            EnvelopeRepository::SQLite(provider) => provider.has_store_capacity(),
        }
    }

    /// Flushes the envelope buffer.
    ///
    /// Returns `true` in case after the flushing it is safe to destroy the buffer, `false`
    /// otherwise. This is done because we want to make sure we know whether it is safe to drop the
    /// [`EnvelopeBuffer`] after flushing is performed.
    pub async fn flush(&mut self) -> bool {
        match &mut self.envelope_repository {
            EnvelopeRepository::Memory(provider) => provider.flush().await,
            EnvelopeRepository::SQLite(provider) => provider.flush().await,
        }
    }

    /// Returns `true` if the [`EnvelopeBuffer`] is using an in-memory strategy, false otherwise.
    pub fn is_memory(&self) -> bool {
        matches!(self.envelope_repository, EnvelopeRepository::Memory(_))
    }

    /// Adds a new [`ProjectKeyPair`] to the `priority_queue` and `project_to_pairs`.
    fn add(&mut self, project_key_pair: ProjectKeyPair, envelope: Option<&Envelope>) {
        let received_at = envelope
            .as_ref()
            .map_or(Instant::now(), |e| e.meta().start_time().into());

        let previous_entry = self
            .priority_queue
            .push(project_key_pair, Priority::new(received_at));
        debug_assert!(previous_entry.is_none());

        for project_key in project_key_pair.iter() {
            self.project_to_pairs
                .entry(project_key)
                .or_default()
                .insert(project_key_pair);
        }
        relay_statsd::metric!(
            gauge(RelayGauges::BufferStackCount) = self.priority_queue.len() as u64
        );
    }

    /// Removes a [`ProjectKeyPair`] from the `priority_queue` and `project_to_pairs`.
    fn remove(&mut self, project_key_pair: ProjectKeyPair) {
        for project_key in project_key_pair.iter() {
            self.project_to_pairs
                .get_mut(&project_key)
                .expect("project_key is missing from lookup")
                .remove(&project_key_pair);
        }
        self.priority_queue.remove(&project_key_pair);

        relay_statsd::metric!(
            gauge(RelayGauges::BufferStackCount) = self.priority_queue.len() as u64
        );
    }

    /// Creates all the priority queue entries given the supplied [`ProjectKeyPair`]s.
    async fn load_project_key_pairs(&mut self, project_key_pairs: HashSet<ProjectKeyPair>) {
        for project_key_pair in project_key_pairs {
            self.add(project_key_pair, None);
        }
    }

    /// Loads the total count from the store if it takes less than a specified duration.
    ///
    /// The total count returned by the store is related to the count of elements that the buffer
    /// will process, besides the count of elements that will be added and removed during its
    /// lifecycle
    async fn load_store_total_count(&mut self) {
        let total_count = timeout(Duration::from_secs(1), async {
            match &self.envelope_repository {
                EnvelopeRepository::Memory(_) => 0,
                EnvelopeRepository::SQLite(provider) => provider.store_total_count().await,
            }
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
            stack_type = &self.envelope_repository.name()
        );
    }
}

/// Contains a reference to the first element in the buffer, together with its stack's ready state.
pub enum Peek<'a> {
    Empty,
    Ready(&'a Envelope),
    NotReady(ProjectKeyPair, Instant, &'a Envelope),
}

#[derive(Debug, Clone)]
struct Priority {
    readiness: Readiness,
    received_at: Instant,
    next_project_fetch: Instant,
}

impl Priority {
    fn new(received_at: Instant) -> Self {
        Self {
            readiness: Readiness::new(),
            received_at,
            next_project_fetch: Instant::now(),
        }
    }
}

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
                .next_project_fetch
                .cmp(&other.next_project_fetch)
                .reverse()
                .then(self.received_at.cmp(&other.received_at).reverse()),
        }
    }
}

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Priority {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Priority {}

#[derive(Debug, Clone, Copy)]
struct Readiness {
    own_project_ready: bool,
    sampling_project_ready: bool,
}

impl Readiness {
    fn new() -> Self {
        // Optimistically set ready state to true.
        // The large majority of stack creations are re-creations after a stack was emptied.
        Self {
            own_project_ready: true,
            sampling_project_ready: true,
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
                Peek::Ready(envelope) | Peek::NotReady(_, _, envelope) => Some(envelope),
            }
        }
    }

    fn new_envelope(
        own_key: ProjectKey,
        sampling_key: Option<ProjectKey>,
        event_id: Option<EventId>,
    ) -> Box<Envelope> {
        let mut envelope = Envelope::from_request(
            None,
            RequestMeta::new(Dsn::from_str(&format!("http://{own_key}@localhost/1")).unwrap()),
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

    async fn peek_project_key(buffer: &mut EnvelopeBuffer) -> ProjectKey {
        buffer
            .peek()
            .await
            .unwrap()
            .envelope()
            .unwrap()
            .meta()
            .public_key()
    }

    #[tokio::test]
    async fn test_insert_pop() {
        let mut buffer = EnvelopeBuffer::memory(mock_memory_checker()).unwrap();

        let project_key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let project_key2 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let project_key3 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fef").unwrap();

        assert!(buffer.pop().await.unwrap().is_none());
        assert!(buffer.peek().await.unwrap().is_empty());

        buffer
            .push(new_envelope(project_key1, None, None))
            .await
            .unwrap();

        buffer
            .push(new_envelope(project_key2, None, None))
            .await
            .unwrap();

        // Both projects are ready, so project 2 is on top (has the newest envelopes):
        assert_eq!(peek_project_key(&mut buffer).await, project_key2);

        buffer.mark_ready(&project_key1, false);
        buffer.mark_ready(&project_key2, false);

        // Both projects are not ready, so project 1 is on top (has the oldest envelopes):
        assert_eq!(peek_project_key(&mut buffer).await, project_key1);

        buffer
            .push(new_envelope(project_key3, None, None))
            .await
            .unwrap();
        buffer.mark_ready(&project_key3, false);

        // All projects are not ready, so project 1 is on top (has the oldest envelopes):
        assert_eq!(peek_project_key(&mut buffer).await, project_key1);

        // After marking a project ready, it goes to the top:
        buffer.mark_ready(&project_key3, true);
        assert_eq!(peek_project_key(&mut buffer).await, project_key3);
        assert_eq!(
            buffer.pop().await.unwrap().unwrap().meta().public_key(),
            project_key3
        );

        // After popping, project 1 is on top again:
        assert_eq!(peek_project_key(&mut buffer).await, project_key1);

        // Mark project 1 as ready (still on top):
        buffer.mark_ready(&project_key1, true);
        assert_eq!(peek_project_key(&mut buffer).await, project_key1);

        // Mark project 2 as ready as well (now on top because most recent):
        buffer.mark_ready(&project_key2, true);
        assert_eq!(peek_project_key(&mut buffer).await, project_key2);
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
        let mut buffer = EnvelopeBuffer::memory(mock_memory_checker()).unwrap();

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
        let mut buffer = EnvelopeBuffer::memory(mock_memory_checker()).unwrap();

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

        buffer.mark_ready(&project_key1, false);
        buffer.mark_ready(&project_key2, false);

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

        let mut buffer = EnvelopeBuffer::memory(mock_memory_checker()).unwrap();
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

    #[test]
    fn test_total_order() {
        let p1 = Priority {
            readiness: Readiness {
                own_project_ready: true,
                sampling_project_ready: true,
            },
            received_at: Instant::now(),
            next_project_fetch: Instant::now(),
        };
        let mut p2 = p1.clone();
        p2.next_project_fetch += Duration::from_millis(1);

        // Last peek does not matter because project is ready:
        assert_eq!(p1.cmp(&p2), Ordering::Equal);
        assert_eq!(p1, p2);
    }

    #[tokio::test]
    async fn test_last_peek_internal_order() {
        let mut buffer = EnvelopeBuffer::memory(mock_memory_checker()).unwrap();

        let project_key_1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let event_id_1 = EventId::new();
        let envelope1 = new_envelope(project_key_1, None, Some(event_id_1));

        let project_key_2 = ProjectKey::parse("b56ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let event_id_2 = EventId::new();
        let envelope2 = new_envelope(project_key_2, None, Some(event_id_2));

        buffer.push(envelope1).await.unwrap();
        buffer.push(envelope2).await.unwrap();

        buffer.mark_ready(&project_key_1, false);
        buffer.mark_ready(&project_key_2, false);

        // event_id_1 is first element:
        let Peek::NotReady(_, _, envelope) = buffer.peek().await.unwrap() else {
            panic!();
        };
        assert_eq!(envelope.event_id(), Some(event_id_1));

        // Second peek returns same element:
        let Peek::NotReady(stack_key, _, envelope) = buffer.peek().await.unwrap() else {
            panic!();
        };
        assert_eq!(envelope.event_id(), Some(event_id_1));

        buffer.mark_seen(&stack_key, Duration::ZERO);

        // After mark_seen, event 2 is on top:
        let Peek::NotReady(_, _, envelope) = buffer.peek().await.unwrap() else {
            panic!();
        };
        assert_eq!(envelope.event_id(), Some(event_id_2));

        let Peek::NotReady(stack_key, _, envelope) = buffer.peek().await.unwrap() else {
            panic!();
        };
        assert_eq!(envelope.event_id(), Some(event_id_2));

        buffer.mark_seen(&stack_key, Duration::ZERO);

        // After another mark_seen, cycle back to event 1:
        let Peek::NotReady(_, _, envelope) = buffer.peek().await.unwrap() else {
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
        let mut buffer = EnvelopeBuffer::sqlite(&config).await.unwrap();

        // We write 5 envelopes to disk so that we can check if they are loaded. These envelopes
        // belong to the same project keys, so they belong to the same envelope stack.
        let envelopes = mock_envelopes(10);
        assert!(store
            .insert_many(envelopes.iter().map(|e| e.as_ref().try_into().unwrap()))
            .await
            .is_ok());

        // We assume that the buffer is empty.
        assert!(buffer.priority_queue.is_empty());
        assert!(buffer.project_to_pairs.is_empty());

        buffer.initialize().await;

        // We assume that we loaded only 1 envelope stack, because of the project keys combinations
        // of the envelopes we inserted above.
        assert_eq!(buffer.priority_queue.len(), 1);
        // We expect to have an entry per project key, since we have 1 pair, the total entries
        // should be 2.
        assert_eq!(buffer.project_to_pairs.len(), 2);
    }
}
