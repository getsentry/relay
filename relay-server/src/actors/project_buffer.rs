use std::collections::{BTreeMap, BTreeSet};

use tokio::sync::mpsc;

use relay_common::ProjectKey;
use relay_system::{FromMessage, Interface, Service};

use crate::envelope::Envelope;
use crate::utils::EnvelopeContext;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueueKey {
    pub own_key: ProjectKey,
    pub sampling_key: ProjectKey,
}

impl QueueKey {
    pub fn new(own_key: ProjectKey, sampling_key: ProjectKey) -> Self {
        Self {
            own_key,
            sampling_key,
        }
    }
}

/// Adds the envelope and the envelope context to the internal buffer.
#[derive(Debug)]
pub struct Enqueue {
    key: QueueKey,
    value: (Box<Envelope>, EnvelopeContext),
}

impl Enqueue {
    pub fn new(key: QueueKey, value: (Box<Envelope>, EnvelopeContext)) -> Self {
        Self { key, value }
    }
}

/// Removes messages from the internal buffer and streams them to the sender.
#[derive(Debug)]
pub struct DequeueMany {
    keys: Vec<QueueKey>,
    sender: mpsc::UnboundedSender<(Box<Envelope>, EnvelopeContext)>,
}

impl DequeueMany {
    pub fn new(
        keys: Vec<QueueKey>,
        sender: mpsc::UnboundedSender<(Box<Envelope>, EnvelopeContext)>,
    ) -> Self {
        Self { keys, sender }
    }
}

/// Removes the provided keys from the internal buffer.
#[derive(Debug)]
pub struct RemoveMany {
    project_key: ProjectKey,
    keys: BTreeSet<QueueKey>,
}

impl RemoveMany {
    pub fn new(project_key: ProjectKey, keys: BTreeSet<QueueKey>) -> Self {
        Self { project_key, keys }
    }
}

/// The Interface for [`BufferService`] service.
#[derive(Debug)]
pub enum Buffer {
    Enqueue(Enqueue),
    DequeueMany(DequeueMany),
    RemoveMany(RemoveMany),
}

impl Interface for Buffer {}

impl FromMessage<Enqueue> for Buffer {
    type Response = relay_system::NoResponse;

    fn from_message(message: Enqueue, _: ()) -> Self {
        Self::Enqueue(message)
    }
}

impl FromMessage<DequeueMany> for Buffer {
    type Response = relay_system::NoResponse;

    fn from_message(message: DequeueMany, _: ()) -> Self {
        Self::DequeueMany(message)
    }
}

impl FromMessage<RemoveMany> for Buffer {
    type Response = relay_system::NoResponse;

    fn from_message(message: RemoveMany, _: ()) -> Self {
        Self::RemoveMany(message)
    }
}

#[derive(Debug)]
pub struct BufferService {
    /// Contains the cache of the incoming envelopes.
    buffer: BTreeMap<QueueKey, Vec<(Box<Envelope>, EnvelopeContext)>>,
}

impl BufferService {
    pub fn new() -> Self {
        Self {
            buffer: BTreeMap::new(),
        }
    }

    /// Handles the enqueueing messages into the internal buffer.
    fn handle_enqueue(&mut self, message: Enqueue) {
        self.buffer
            .entry(message.key)
            .or_default()
            .push(message.value);
    }

    /// Handles the dequeueing messages from the internal buffer.
    ///
    /// This method removes the envelopes from the buffer and stream them to the sender.
    fn handle_dequeue(&mut self, message: DequeueMany) {
        let DequeueMany { keys, sender } = message;
        for key in keys {
            for value in self.buffer.remove(&key).unwrap_or_default() {
                sender.send(value).ok();
            }
        }
    }

    /// Handles the remove request.
    ///
    /// This remove all the envelopes from the internal buffer for the provided keys.
    fn handle_remove(&mut self, message: RemoveMany) {
        let RemoveMany { project_key, keys } = message;
        let mut count = 0;
        for key in keys {
            count += self.buffer.remove(&key).map_or(0, |k| k.len());
        }
        if count > 0 {
            relay_log::with_scope(
                |scope| scope.set_tag("project_key", project_key),
                || relay_log::error!("evicted project with {} envelopes", count),
            );
        }
    }

    /// Handles all the incoming messages from the [`Buffer`] interface.
    fn handle_message(&mut self, message: Buffer) {
        match message {
            Buffer::Enqueue(message) => self.handle_enqueue(message),
            Buffer::DequeueMany(message) => self.handle_dequeue(message),
            Buffer::RemoveMany(message) => self.handle_remove(message),
        }
    }
}

impl Service for BufferService {
    type Interface = Buffer;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                self.handle_message(message);
            }
        });
    }
}

impl Drop for BufferService {
    fn drop(&mut self) {
        let count: usize = self.buffer.values().map(|v| v.len()).sum();
        if count > 0 {
            relay_log::error!("dropped queue with {} envelopes", count);
        }
    }
}
