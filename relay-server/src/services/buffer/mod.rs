//! Types for buffering envelopes.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use tokio::sync::MutexGuard;

use crate::envelope::Envelope;

mod envelopebuffer;
mod envelopestack;

/// Async envelope buffering interface.
///
/// Access to the buffer is synchronized by a tokio lock.
#[derive(Debug, Clone)]
pub struct EnvelopeBuffer {
    /// TODO: Reconsider synchronization mechanism.
    /// We can either
    /// - keep the interface sync and use a std Mutex. In this case, we create a queue of threads.
    /// - use an async interface with a tokio mutex. In this case, we create a queue of futures.
    /// - use message passing (service or channel). In this case, we create a queue of messages.
    ///
    /// From the tokio docs:
    ///
    /// >  The primary use case for the async mutex is to provide shared mutable access to IO resources such as a database connection.
    /// > [...] when you do want shared access to an IO resource, it is often better to spawn a task to manage the IO resource,
    /// > and to use message passing to communicate with that task.
    backend: Arc<tokio::sync::Mutex<dyn envelopebuffer::EnvelopeBuffer>>,
    notify: Arc<tokio::sync::Notify>,
    changed: Arc<AtomicBool>,
}

impl EnvelopeBuffer {
    /// Creates a memory or disk based [`EnvelopeBuffer`], depending on the given config.
    ///
    /// NOTE: until the V1 spooler implementation is removed, this function returns `None`
    /// if V2 spooling is not configured.
    pub fn from_config(config: &Config) -> Option<Self> {
        // TODO: create a disk-based backend if db config is given (loads stacks from db).
        config.spool_v2().then(|| Self {
            backend: envelopebuffer::create(config),
            notify: Arc::new(tokio::sync::Notify::new()),
            changed: Arc::new(AtomicBool::new(true)),
        })
    }

    /// Adds an envelope to the buffer and wakes any waiting consumers.
    pub async fn push(&self, envelope: Box<Envelope>) {
        let mut guard = self.backend.lock().await;
        guard.push(envelope);
        self.notify();
    }

    /// Returns a reference to the next-in-line envelope.
    ///
    /// If the buffer is empty or has not changed since the last peek, this function will sleep
    /// until something changes in the buffer.
    pub async fn peek(&self) -> Peek {
        loop {
            {
                let mut guard = self.backend.lock().await;
                if self.changed.load(Ordering::Relaxed) && guard.peek().is_some() {
                    self.changed.store(false, Ordering::Relaxed);
                    return Peek {
                        guard,
                        changed: &self.changed,
                        notify: &self.notify,
                    };
                }
            }
            self.notify.notified().await;
        }
    }

    /// Marks a project as ready or not ready.
    ///
    /// The buffer reprioritizes its envelopes based on this information.
    pub async fn mark_ready(&self, project_key: &ProjectKey, ready: bool) {
        let mut guard = self.backend.lock().await;
        let changed = guard.mark_ready(project_key, ready);
        if changed {
            self.notify();
        }
    }

    fn notify(&self) {
        self.changed.store(true, Ordering::Relaxed);
        self.notify.notify_waiters();
    }
}

/// A view onto the next envelope in the buffer.
///
/// Objects of this type can only exist if the buffer is not empty.
pub struct Peek<'a> {
    guard: MutexGuard<'a, dyn envelopebuffer::EnvelopeBuffer>,
    notify: &'a tokio::sync::Notify,
    changed: &'a AtomicBool,
}

impl Peek<'_> {
    /// Returns a reference to the next envelope.
    pub fn get(&mut self) -> &Envelope {
        self.guard
            .peek()
            .expect("element disappeared while holding lock")
    }

    /// Pops the next envelope from the buffer.
    ///
    /// This functions consumes the [`Peek`].
    pub fn remove(mut self) -> Box<Envelope> {
        self.notify();
        self.guard
            .pop()
            .expect("element disappeared while holding lock")
    }

    /// Sync version of [`EnvelopeBuffer::mark_ready`].
    ///
    /// Since [`Peek`] already has exclusive access to the buffer, it can mark projects as ready
    /// without awaiting the lock.
    pub fn mark_ready(&mut self, project_key: &ProjectKey, ready: bool) {
        let changed = self.guard.mark_ready(project_key, ready);
        if changed {
            self.notify();
        }
    }

    fn notify(&self) {
        self.changed.store(true, Ordering::Relaxed);
        self.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use relay_common::Dsn;

    use crate::extractors::RequestMeta;

    use super::*;

    #[tokio::test]
    async fn no_busy_loop_when_empty() {
        let buffer = new_buffer();
        let call_count = Arc::new(AtomicUsize::new(0));

        tokio::time::pause();

        let cloned_buffer = buffer.clone();
        let cloned_call_count = call_count.clone();
        tokio::spawn(async move {
            loop {
                cloned_buffer.peek().await.remove();
                cloned_call_count.fetch_add(1, Ordering::Relaxed);
            }
        });

        // Initial state: no calls
        assert_eq!(call_count.load(Ordering::Relaxed), 0);
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert_eq!(call_count.load(Ordering::Relaxed), 0);

        // State after push: one call
        buffer.push(new_envelope()).await;
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert_eq!(call_count.load(Ordering::Relaxed), 1);

        // State after second push: two calls
        buffer.push(new_envelope()).await;
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert_eq!(call_count.load(Ordering::Relaxed), 2);
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert_eq!(call_count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn no_busy_loop_when_unchanged() {
        let buffer = new_buffer();
        let call_count = Arc::new(AtomicUsize::new(0));

        tokio::time::pause();

        let cloned_buffer = buffer.clone();
        let cloned_call_count = call_count.clone();
        tokio::spawn(async move {
            loop {
                cloned_buffer.peek().await;
                cloned_call_count.fetch_add(1, Ordering::Relaxed);
            }
        });

        buffer.push(new_envelope()).await;

        // Initial state: no calls
        assert_eq!(call_count.load(Ordering::Relaxed), 0);

        // After first advance: got one call
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert_eq!(call_count.load(Ordering::Relaxed), 1);

        // After second advance: still only one call (no change)
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert_eq!(call_count.load(Ordering::Relaxed), 1);

        // State after second push: two calls
        buffer.push(new_envelope()).await;
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert_eq!(call_count.load(Ordering::Relaxed), 2);
    }

    fn new_buffer() -> EnvelopeBuffer {
        EnvelopeBuffer::from_config(
            &Config::from_json_value(serde_json::json!({
                "spool": {
                    "envelopes": {
                        "version": "2"
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap()
    }

    fn new_envelope() -> Box<Envelope> {
        Envelope::from_request(
            None,
            RequestMeta::new(
                Dsn::from_str("http://a94ae32be2584e0bbd7a4cbb95971fed@localhost/1").unwrap(),
            ),
        )
    }
}
