use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::time::Instant;

use relay_base_schema::project::ProjectKey;

use crate::envelope::Envelope;
use crate::services::buffer::envelopebuffer::EnvelopeBuffer;
use crate::services::buffer::envelopestack::EnvelopeStack;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct StackKey {
    own_key: ProjectKey,
    sampling_key: ProjectKey,
}

impl StackKey {
    fn from_envelope(envelope: &Envelope) -> Self {
        let own_key = envelope.meta().public_key();
        Self {
            own_key,
            sampling_key: envelope.sampling_key().unwrap_or(own_key),
        }
    }
}

#[derive(Debug)]
pub struct PriorityEnvelopeBuffer<S: EnvelopeStack> {
    own_keys: hashbrown::HashMap<ProjectKey, BTreeSet<StackKey>>,
    sampling_keys: hashbrown::HashMap<ProjectKey, BTreeSet<StackKey>>,
    priority_queue: priority_queue::PriorityQueue<QueueItem<StackKey, S>, Priority>,
}

impl<S: EnvelopeStack> PriorityEnvelopeBuffer<S> {
    pub fn new() -> Self {
        Self {
            own_keys: Default::default(),
            sampling_keys: Default::default(),
            priority_queue: Default::default(),
        }
    }
}

impl<S: EnvelopeStack> PriorityEnvelopeBuffer<S> {
    fn push_stack(&mut self, envelope: Box<Envelope>) {
        relay_log::trace!("PriorityEnvelopeBuffer: push_stack");
        let received_at = envelope.meta().start_time();
        let stack_key = StackKey::from_envelope(&envelope);
        let previous_entry = self.priority_queue.push(
            QueueItem {
                key: stack_key,
                value: S::new(envelope),
            },
            Priority::new(received_at),
        );
        debug_assert!(previous_entry.is_none());
        self.own_keys
            .entry(stack_key.own_key)
            .or_default()
            .insert(stack_key);
        self.sampling_keys
            .entry(stack_key.sampling_key)
            .or_default()
            .insert(stack_key);
    }

    fn pop_stack(&mut self, stack_key: StackKey) {
        relay_log::trace!("PriorityEnvelopeBuffer: pop_stack");
        self.own_keys
            .get_mut(&stack_key.own_key)
            .expect("own_keys")
            .remove(&stack_key);
        self.sampling_keys
            .get_mut(&stack_key.sampling_key)
            .expect("sampling_keys")
            .remove(&stack_key);
        self.priority_queue.remove(&stack_key);
    }
}

impl<S: EnvelopeStack + std::fmt::Debug> EnvelopeBuffer for PriorityEnvelopeBuffer<S> {
    fn push(&mut self, envelope: Box<Envelope>) {
        relay_log::trace!("PriorityEnvelopeBuffer: push");
        let received_at = envelope.meta().start_time();
        let stack_key = StackKey::from_envelope(&envelope);
        if let Some((
            QueueItem {
                key: _,
                value: stack,
            },
            _,
        )) = self.priority_queue.get_mut(&stack_key)
        {
            relay_log::trace!("PriorityEnvelopeBuffer: pushing to existing stack");
            stack.push(envelope);
        } else {
            relay_log::trace!("PriorityEnvelopeBuffer: pushing new stack with one element");
            self.push_stack(envelope);
        }
        self.priority_queue.change_priority_by(&stack_key, |prio| {
            prio.received_at = received_at;
        });
    }

    fn peek(&mut self) -> Option<&Envelope> {
        relay_log::trace!("PriorityEnvelopeBuffer: peek");
        let (
            QueueItem {
                key: _,
                value: stack,
            },
            _,
        ) = self.priority_queue.peek_mut()?;
        stack.peek()
    }

    fn pop(&mut self) -> Option<Box<Envelope>> {
        relay_log::trace!("PriorityEnvelopeBuffer: pop");
        let (QueueItem { key, value: stack }, _) = self.priority_queue.peek_mut()?;
        let stack_key = *key;
        let envelope = stack.pop().expect("found an empty stack");

        let next_received_at = stack
            .peek()
            .map(|next_envelope| next_envelope.meta().start_time());
        match next_received_at {
            None => {
                self.pop_stack(stack_key);
            }
            Some(next_received_at) => {
                self.priority_queue.change_priority_by(&stack_key, |prio| {
                    prio.received_at = next_received_at;
                });
            }
        }
        Some(envelope)
    }

    fn mark_ready(&mut self, project: &ProjectKey, is_ready: bool) -> bool {
        relay_log::trace!("PriorityEnvelopeBuffer: mark_ready");
        let mut changed = false;
        if let Some(stack_keys) = self.own_keys.get(project) {
            for stack_key in stack_keys {
                self.priority_queue.change_priority_by(stack_key, |stack| {
                    if is_ready != stack.own_ready {
                        stack.own_ready = is_ready;
                        changed = true;
                    }
                });
            }
        }
        if let Some(stack_keys) = self.sampling_keys.get(project) {
            for stack_key in stack_keys {
                self.priority_queue.change_priority_by(stack_key, |stack| {
                    if is_ready != stack.sampling_ready {
                        stack.sampling_ready = is_ready;
                        changed = true;
                    }
                });
            }
        }
        changed
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use relay_common::Dsn;
    use relay_sampling::DynamicSamplingContext;
    use uuid::Uuid;

    use crate::envelope::{Item, ItemType};
    use crate::extractors::RequestMeta;
    use crate::services::buffer::envelopestack::InMemoryEnvelopeStack;

    use super::*;

    fn new_envelope(project_key: ProjectKey, sampling_key: Option<ProjectKey>) -> Box<Envelope> {
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
        envelope
    }

    #[test]
    fn insert_pop() {
        let mut buffer = PriorityEnvelopeBuffer::<InMemoryEnvelopeStack>::new();

        let project_key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let project_key2 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let project_key3 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fef").unwrap();

        assert!(buffer.pop().is_none());
        assert!(buffer.peek().is_none());

        buffer.push(new_envelope(project_key1, None));
        assert_eq!(buffer.peek().unwrap().meta().public_key(), project_key1);

        buffer.push(new_envelope(project_key2, None));
        // Both projects are not ready, so project 1 is on top (has the oldest envelopes):
        assert_eq!(buffer.peek().unwrap().meta().public_key(), project_key1);

        buffer.push(new_envelope(project_key3, None));
        // All projects are not ready, so project 1 is on top (has the oldest envelopes):
        assert_eq!(buffer.peek().unwrap().meta().public_key(), project_key1);

        // After marking a project ready, it goes to the top:
        buffer.mark_ready(&project_key3, true);
        assert_eq!(buffer.peek().unwrap().meta().public_key(), project_key3);
        assert_eq!(buffer.pop().unwrap().meta().public_key(), project_key3);

        // After popping, project 1 is on top again:
        assert_eq!(buffer.peek().unwrap().meta().public_key(), project_key1);

        // Mark project 1 as ready (still on top):
        buffer.mark_ready(&project_key1, true);
        assert_eq!(buffer.peek().unwrap().meta().public_key(), project_key1);

        // Mark project 2 as ready as well (now on top because most recent):
        buffer.mark_ready(&project_key2, true);
        assert_eq!(buffer.peek().unwrap().meta().public_key(), project_key2);
        assert_eq!(buffer.pop().unwrap().meta().public_key(), project_key2);

        // Pop last element:
        assert_eq!(buffer.pop().unwrap().meta().public_key(), project_key1);
        assert!(buffer.pop().is_none());
        assert!(buffer.peek().is_none());
    }

    #[test]
    fn project_internal_order() {
        let mut buffer = PriorityEnvelopeBuffer::<InMemoryEnvelopeStack>::new();

        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();

        let envelope1 = new_envelope(project_key, None);
        let instant1 = envelope1.meta().start_time();
        let envelope2 = new_envelope(project_key, None);
        let instant2 = envelope2.meta().start_time();

        assert!(instant2 > instant1);

        buffer.push(envelope1);
        buffer.push(envelope2);

        assert_eq!(buffer.pop().unwrap().meta().start_time(), instant2);
        assert_eq!(buffer.pop().unwrap().meta().start_time(), instant1);
        assert!(buffer.pop().is_none());
    }

    #[test]
    fn sampling_projects() {
        let mut buffer = PriorityEnvelopeBuffer::<InMemoryEnvelopeStack>::new();

        let project_key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fed").unwrap();
        let project_key2 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fef").unwrap();

        let envelope1 = new_envelope(project_key1, None);
        let instant1 = envelope1.meta().start_time();
        buffer.push(envelope1);

        let envelope2 = new_envelope(project_key2, None);
        let instant2 = envelope2.meta().start_time();
        buffer.push(envelope2);

        let envelope3 = new_envelope(project_key1, Some(project_key2));
        let instant3 = envelope3.meta().start_time();
        buffer.push(envelope3);

        // Nothing is ready, instant1 is on top:
        assert_eq!(buffer.peek().unwrap().meta().start_time(), instant1);

        // Mark project 2 ready, gets on top:
        buffer.mark_ready(&project_key2, true);
        assert_eq!(buffer.peek().unwrap().meta().start_time(), instant2);

        // Revert
        buffer.mark_ready(&project_key2, false);
        assert_eq!(buffer.peek().unwrap().meta().start_time(), instant1);

        // Project 1 ready:
        buffer.mark_ready(&project_key1, true);
        assert_eq!(buffer.peek().unwrap().meta().start_time(), instant1);

        // when both projects are ready, event no 3 ends up on top:
        buffer.mark_ready(&project_key2, true);
        assert_eq!(buffer.pop().unwrap().meta().start_time(), instant3);
        assert_eq!(buffer.peek().unwrap().meta().start_time(), instant2);

        buffer.mark_ready(&project_key2, false);
        assert_eq!(buffer.pop().unwrap().meta().start_time(), instant1);
        assert_eq!(buffer.pop().unwrap().meta().start_time(), instant2);

        assert!(buffer.pop().is_none());
    }
}
