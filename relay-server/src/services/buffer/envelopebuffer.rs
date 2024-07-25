use std::cmp::Ordering;
use std::time::Instant;

use relay_base_schema::project::ProjectKey;

use crate::envelope::Envelope;
use crate::services::buffer::envelopestack::EnvelopeStack;

pub trait EnvelopeBuffer {
    fn push(&mut self, envelope: Box<Envelope>);
    fn pop(&mut self) -> Option<Box<Envelope>>;
    fn mark_ready(&mut self, project: &ProjectKey, is_ready: bool);
}

#[derive(Hash, PartialEq, Eq)]
struct StackKey {
    own_key: ProjectKey,
    sampling_key: ProjectKey,
}

impl StackKey {
    fn from_envelope(envelope: &Envelope) -> Self {}
}

struct PriorityEnvelopeBuffer<S: EnvelopeStack> {
    own_keys: hashbrown::HashMap<ProjectKey, Vec<StackKey>>,
    sampling_keys: hashbrown::HashMap<ProjectKey, Vec<StackKey>>,
    stacks: priority_queue::PriorityQueue<StackKey, PrioritizedStack<S>>,
}

impl<S: EnvelopeStack> EnvelopeBuffer for PriorityEnvelopeBuffer<S> {
    fn push(&mut self, envelope: Box<Envelope>) {
        let stack_key = StackKey::from_envelope(&envelope);
        let updated = self.stacks.change_priority_by(&stack_key, |stack| {});
        if !updated {
            let old = self.stacks.push(stack_key, PrioritizedStack::new(envelope));
            debug_assert!(old.is_none());
        }
        self.own_keys
            .entry(stack_key.own_key)
            .or_default()
            .push(stack_key);
        self.sampling_keys
            .entry(stack_key.sampling_key)
            .or_default()
            .push(stack_key);
    }

    fn pop(&mut self) -> Option<Box<Envelope>> {
        let (stack_key, stack) = self.stacks.peek_mut()?;
        let entry = self
            .own_keys
            .entry(stack_key.own_key)
            .or_default()
            .push(stack_key);
        self.sampling_keys
            .entry(stack_key.sampling_key)
            .or_default()
            .push(stack_key);
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

struct PrioritizedStack<S> {
    own_ready: bool,
    sampling_ready: bool,
    received_at: Instant,
    stack: S,
}

impl<S> PrioritizedStack<S> {
    fn ready(&self) -> bool {
        self.own_ready && self.sampling_ready
    }
}

impl<S: Default> PrioritizedStack<S> {
    fn new(received_at: Instant) -> Self {
        Self {
            own_ready: false,
            sampling_ready: false,
            received_at,
            stack: S::default(),
        }
    }
}

impl<S> PartialEq for PrioritizedStack<S> {
    fn eq(&self, other: &Self) -> bool {
        self.ready() == other.ready() && self.received_at == other.received_at
    }
}

impl<S> PartialOrd for PrioritizedStack<S> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<S> Eq for PrioritizedStack<S> {}

impl<S> Ord for PrioritizedStack<S> {
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
