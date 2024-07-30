//! Types for buffering envelopes.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use tokio::sync::MutexGuard;

use crate::envelope::Envelope;
use crate::services::buffer::envelope_buffer::PolymorphicEnvelopeBuffer;
use crate::utils::ManagedEnvelope;

pub use envelope_buffer::EnvelopeBufferError;
pub use envelope_stack::sqlite::SqliteEnvelopeStack; // pub for benchmarks
pub use envelope_stack::EnvelopeStack; // pub for benchmarks
pub use sqlite_envelope_store::SqliteEnvelopeStore; // pub for benchmarks

mod envelope_buffer;
mod envelope_stack;
mod sqlite_envelope_store;
mod stack_provider;
mod testutils;

/// Async envelope buffering interface.
///
/// Access to the buffer is synchronized by a tokio lock.
#[derive(Debug)]
pub struct GuardedEnvelopeBuffer {
    /// TODO: Reconsider synchronization mechanism.
    /// We can either
    /// - make the interface sync and use a std Mutex. In this case, we create a queue of threads.
    /// - use an async interface with a tokio mutex. In this case, we create a queue of futures.
    /// - use message passing (service or channel). In this case, we create a queue of messages.
    ///
    /// From the tokio docs:
    ///
    /// >  The primary use case for the async mutex is to provide shared mutable access to IO resources such as a database connection.
    /// > [...] when you do want shared access to an IO resource, it is often better to spawn a task to manage the IO resource,
    /// > and to use message passing to communicate with that task.
    backend: tokio::sync::Mutex<PolymorphicEnvelopeBuffer>,
    notify: tokio::sync::Notify,
    changed: AtomicBool,
    inflight_push_count: AtomicUsize,
}

impl GuardedEnvelopeBuffer {
    /// Creates a memory or disk based [`GuardedEnvelopeBuffer`], depending on the given config.
    ///
    /// NOTE: until the V1 spooler implementation is removed, this function returns `None`
    /// if V2 spooling is not configured.
    pub fn from_config(config: &Config) -> Option<Self> {
        if config.spool_v2() {
            Some(Self {
                backend: tokio::sync::Mutex::new(PolymorphicEnvelopeBuffer::from_config(config)),
                notify: tokio::sync::Notify::new(),
                changed: AtomicBool::new(true),
                inflight_push_count: AtomicUsize::new(0),
            })
        } else {
            None
        }
    }

    /// Schedules a task to push an envelope to the buffer.
    ///
    /// Once the envelope is pushed, waiters will be notified.
    pub fn defer_push(self: Arc<Self>, envelope: ManagedEnvelope) {
        self.inflight_push_count.fetch_add(1, Ordering::Relaxed);
        let this = self.clone();
        tokio::spawn(async move {
            if let Err(e) = this.push(envelope.into_envelope()).await {
                relay_log::error!(
                    error = &e as &dyn std::error::Error,
                    "failed to push envelope"
                );
            }
            this.inflight_push_count.fetch_sub(1, Ordering::Relaxed);
        });
    }

    /// Returns a reference to the next-in-line envelope.
    ///
    /// If the buffer is empty or has not changed since the last peek, this function will sleep
    /// until something changes in the buffer.
    pub async fn peek(&self) -> Peek {
        loop {
            {
                let mut guard = self.backend.lock().await;
                if self.changed.load(Ordering::Relaxed) {
                    match guard.peek().await {
                        Ok(envelope) => {
                            if envelope.is_some() {
                                self.changed.store(false, Ordering::Relaxed);
                                return Peek {
                                    guard,
                                    changed: &self.changed,
                                    notify: &self.notify,
                                };
                            }
                        }
                        Err(error) => {
                            relay_log::error!(
                                error = &error as &dyn std::error::Error,
                                "failed to peek envelope"
                            );
                        }
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

    /// Adds an envelope to the buffer and wakes any waiting consumers.
    async fn push(&self, envelope: Box<Envelope>) -> Result<(), EnvelopeBufferError> {
        let mut guard = self.backend.lock().await;
        guard.push(envelope).await?;
        self.notify();
        Ok(())
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
    guard: MutexGuard<'a, PolymorphicEnvelopeBuffer>,
    notify: &'a tokio::sync::Notify,
    changed: &'a AtomicBool,
}

impl Peek<'_> {
    /// Returns a reference to the next envelope.
    pub async fn get(&mut self) -> Result<&Envelope, EnvelopeBufferError> {
        Ok(self
            .guard
            .peek()
            .await?
            .expect("element disappeared while holding lock"))
    }

    /// Pops the next envelope from the buffer.
    ///
    /// This functions consumes the [`Peek`].
    pub async fn remove(mut self) -> Result<Box<Envelope>, EnvelopeBufferError> {
        self.notify();
        Ok(self
            .guard
            .pop()
            .await?
            .expect("element disappeared while holding lock"))
    }

    /// Sync version of [`GuardedEnvelopeBuffer::mark_ready`].
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
    use std::sync::Arc;
    use std::time::Duration;

    use relay_common::Dsn;

    use crate::extractors::RequestMeta;

    use super::*;

    fn new_buffer() -> Arc<GuardedEnvelopeBuffer> {
        GuardedEnvelopeBuffer::from_config(
            &Config::from_json_value(serde_json::json!({
                "spool": {
                    "envelopes": {
                        "version": "experimental"
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap()
        .into()
    }

    fn new_envelope() -> Box<Envelope> {
        Envelope::from_request(
            None,
            RequestMeta::new(
                Dsn::from_str("http://a94ae32be2584e0bbd7a4cbb95971fed@localhost/1").unwrap(),
            ),
        )
    }

    #[tokio::test]
    async fn no_busy_loop_when_empty() {
        let buffer = new_buffer();
        let call_count = Arc::new(AtomicUsize::new(0));

        tokio::time::pause();

        let cloned_buffer = buffer.clone();
        let cloned_call_count = call_count.clone();
        tokio::spawn(async move {
            loop {
                cloned_buffer.peek().await.remove().await.unwrap();
                cloned_call_count.fetch_add(1, Ordering::Relaxed);
            }
        });

        // Initial state: no calls
        assert_eq!(call_count.load(Ordering::Relaxed), 0);
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert_eq!(call_count.load(Ordering::Relaxed), 0);

        // State after push: one call
        buffer.push(new_envelope()).await.unwrap();
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert_eq!(call_count.load(Ordering::Relaxed), 1);

        // State after second push: two calls
        buffer.push(new_envelope()).await.unwrap();
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

        buffer.push(new_envelope()).await.unwrap();

        // Initial state: no calls
        assert_eq!(call_count.load(Ordering::Relaxed), 0);

        // After first advance: got one call
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert_eq!(call_count.load(Ordering::Relaxed), 1);

        // After second advance: still only one call (no change)
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert_eq!(call_count.load(Ordering::Relaxed), 1);

        // State after second push: two calls
        buffer.push(new_envelope()).await.unwrap();
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert_eq!(call_count.load(Ordering::Relaxed), 2);
    }
}
