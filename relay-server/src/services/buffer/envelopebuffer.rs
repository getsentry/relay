use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::time::Instant;

use relay_base_schema::project::ProjectKey;

use crate::envelope::Envelope;
use crate::services::buffer::envelopestack::EnvelopeStack;

pub trait EnvelopeBuffer {
    fn push(&mut self, envelope: Box<Envelope>);
    fn pop(&mut self) -> Option<Box<Envelope>>;
    fn mark_ready(&mut self, project: &ProjectKey, is_ready: bool);
}

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord)]
struct StackKey {
    own_key: ProjectKey,
    sampling_key: ProjectKey,
}

impl StackKey {
    fn from_envelope(envelope: &Envelope) -> Self {}
}

struct PriorityEnvelopeBuffer<S: EnvelopeStack> {
    own_keys: hashbrown::HashMap<ProjectKey, BTreeSet<StackKey>>,
    sampling_keys: hashbrown::HashMap<ProjectKey, BTreeSet<StackKey>>,
    stacks: priority_queue::PriorityQueue<QueueItem<StackKey, S>, Priority>,
}

impl<S: EnvelopeStack> EnvelopeBuffer for PriorityEnvelopeBuffer<S> {
    fn push(&mut self, envelope: Box<Envelope>) {
        let received_at = envelope.received_at();
        let stack_key = StackKey::from_envelope(&envelope);
        if let Some(qi) = self.stacks.get_mut(&stack_key) {
            qi.0.value.push(envelope);
        } else {
            self.stacks.push(
                QueueItem {
                    key: stack_key,
                    value: S::new(envelope),
                },
                Priority::new(received_at),
            );
            self.own_keys
                .entry(stack_key.own_key)
                .or_default()
                .insert(stack_key);
            self.sampling_keys
                .entry(stack_key.sampling_key)
                .or_default()
                .insert(stack_key);
        }
        self.stacks.change_priority_by(stack_key, |prio| {
            prio.received_at = received_at;
        });
    }

    fn pop(&mut self) -> Option<Box<Envelope>> {
        let (
            QueueItem {
                key: stack_key,
                value: stack,
            },
            priority,
        ) = self.stacks.peek_mut()?;
        let envelope = stack.pop();
        debug_assert!(envelope.is_some());
        match stack.peek() {
            None => {
                self.own_keys
                    .get_mut(&stack_key.own_key)
                    .expect("own_keys")
                    .remove(&stack_key);
                self.sampling_keys
                    .get_mut(&stack_key.sampling_key)
                    .expect("sampling_keys")
                    .remove(&stack_key);
                self.stacks.remove(stack_key);
            }
            Some(next_envelope) => {
                self.stacks.change_priority_by(stack_key, |prio| {
                    prio.received_at = next_envelope.received_at();
                });
            }
        }
        envelope
    }

    fn mark_ready(&mut self, project: &ProjectKey, is_ready: bool) {
        if let Some(stack_keys) = self.own_keys.get(project) {
            for stack_key in stack_keys {
                self.stacks.change_priority_by(stack_key, |stack| {
                    stack.own_ready = is_ready;
                });
            }
        }
        if let Some(stack_keys) = self.sampling_keys.get(project) {
            for stack_key in stack_keys {
                self.stacks.change_priority_by(stack_key, |stack| {
                    stack.sampling_ready = is_ready;
                });
            }
        }
    }
}

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

struct Priority {
    own_ready: bool,
    sampling_ready: bool,
    received_at: Instant,
}

impl Priority {
    fn ready(&self) -> bool {
        self.own_ready && self.sampling_ready
    }
}

impl Priority {
    fn new(received_at: Instant) -> Self {
        Self {
            own_ready: false,
            sampling_ready: false,
            received_at,
        }
    }
}

impl PartialEq for Priority {
    fn eq(&self, other: &Self) -> bool {
        self.ready() == other.ready() && self.received_at == other.received_at
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
        match (self.ready(), other.ready()) {
            (true, true) => self.received_at.cmp(&other.received_at),
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            // For non-ready stacks, we invert the priority, such that projects that are not
            // ready and did not receive envelopes recently can be evicted.
            (false, false) => self.received_at.cmp(&other.received_at).reverse(),
        }
    }
}
