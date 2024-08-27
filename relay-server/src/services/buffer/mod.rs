//! Types for buffering envelopes.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use tokio::sync::MutexGuard;

use crate::envelope::Envelope;
use crate::utils::{ManagedEnvelope, MemoryChecker};

use crate::statsd::RelayCounters;
pub use envelope_buffer::EnvelopeBufferError;
pub use envelope_buffer::PolymorphicEnvelopeBuffer;
pub use envelope_stack::sqlite::SqliteEnvelopeStack; // pub for benchmarks
pub use envelope_stack::EnvelopeStack; // pub for benchmarks
pub use envelope_store::sqlite::SqliteEnvelopeStore;
// pub for benchmarks // pub for benchmarks

mod envelope_buffer;
mod envelope_stack;
mod envelope_store;
mod stack_provider;
mod testutils;

/// Struct that wraps the envelope buffer backend with a boolean flag to signal whether
/// the contents of the buffer have changed.
#[derive(Debug)]
struct Inner {
    /// The buffer that we are writing to and reading from.
    backend: PolymorphicEnvelopeBuffer,
    /// Used to notify callers of `peek()` of any changes in the buffer.
    should_peek: bool,
}

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
    inner: tokio::sync::Mutex<Inner>,
    /// Used to notify callers of `peek()` of any changes in the buffer.
    notify: tokio::sync::Notify,
    /// Metric that counts how many push operations are waiting.
    inflight_push_count: AtomicU64,
    /// Last known capacity check result.
    cached_capacity: AtomicBool,
}

impl GuardedEnvelopeBuffer {
    /// Creates a memory or disk based [`GuardedEnvelopeBuffer`], depending on the given config.
    ///
    /// NOTE: until the V1 spooler implementation is removed, this function returns `None`
    /// if V2 spooling is not configured.
    pub fn from_config(config: &Config, memory_checker: MemoryChecker) -> Option<Self> {
        if config.spool_v2() {
            Some(Self {
                inner: tokio::sync::Mutex::new(Inner {
                    backend: PolymorphicEnvelopeBuffer::from_config(config, memory_checker),
                    should_peek: true,
                }),
                notify: tokio::sync::Notify::new(),
                inflight_push_count: AtomicU64::new(0),
                cached_capacity: AtomicBool::new(true),
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
    pub async fn peek(&self) -> EnvelopeBufferGuard {
        loop {
            let mut guard = self.inner.lock().await;
            if guard.should_peek {
                match guard.backend.peek().await {
                    Ok(envelope) => {
                        if envelope.is_some() {
                            guard.should_peek = false;
                            return EnvelopeBufferGuard {
                                guard,
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
            // Release the lock before waiting for new notifications, otherwise we will indefinitely
            // block.
            drop(guard);
            // We wait to get notified for any changes in the buffer.
            self.notify.notified().await;
        }
    }

    /// Marks a project as ready or not ready.
    ///
    /// The buffer re-prioritizes its envelopes based on this information.
    pub async fn mark_ready(&self, project_key: &ProjectKey, ready: bool) {
        let mut guard = self.inner.lock().await;
        let changed = guard.backend.mark_ready(project_key, ready);
        if changed {
            self.notify(&mut guard);
        }
    }

    /// Returns `true` if the buffer has capacity to accept more [`Envelope`]s.
    ///
    /// This method tries to acquire the lock and read the latest capacity, but doesn't
    /// guarantee that the returned value will be up to date, since lock contention could lead to
    /// this method never acquiring the lock, thus returning the last known capacity value.
    pub fn has_capacity(&self) -> bool {
        match self.inner.try_lock() {
            Ok(guard) => {
                relay_statsd::metric!(
                    counter(RelayCounters::BufferCapacityCheck) += 1,
                    lock_aquired = "true"
                );

                let has_capacity = guard.backend.has_capacity();
                self.cached_capacity.store(has_capacity, Ordering::Relaxed);
                has_capacity
            }
            Err(_) => {
                relay_statsd::metric!(
                    counter(RelayCounters::BufferCapacityCheck) += 1,
                    lock_aquired = "false"
                );

                self.cached_capacity.load(Ordering::Relaxed)
            }
        }
    }

    /// Returns the count of how many pushes are in flight and not been finished.
    pub fn inflight_push_count(&self) -> u64 {
        self.inflight_push_count.load(Ordering::Relaxed)
    }

    /// Adds an envelope to the buffer and wakes any waiting consumers.
    async fn push(&self, envelope: Box<Envelope>) -> Result<(), EnvelopeBufferError> {
        let mut guard = self.inner.lock().await;
        guard.backend.push(envelope).await?;
        self.notify(&mut guard);
        Ok(())
    }

    /// Notifies the waiting tasks that a change has happened in the buffer.
    fn notify(&self, guard: &mut MutexGuard<Inner>) {
        guard.should_peek = true;
        self.notify.notify_waiters();
    }
}

/// A view onto the next envelope in the buffer.
///
/// Objects of this type can only exist if the buffer is not empty.
pub struct EnvelopeBufferGuard<'a> {
    guard: MutexGuard<'a, Inner>,
    notify: &'a tokio::sync::Notify,
}

impl EnvelopeBufferGuard<'_> {
    /// Returns a reference to the next envelope.
    pub async fn get(&mut self) -> Result<&Envelope, EnvelopeBufferError> {
        Ok(self
            .guard
            .backend
            .peek()
            .await?
            .expect("element disappeared while holding lock"))
    }

    /// Pops the next envelope from the buffer.
    ///
    /// This functions consumes the [`EnvelopeBufferGuard`].
    pub async fn remove(mut self) -> Result<Box<Envelope>, EnvelopeBufferError> {
        self.notify();
        Ok(self
            .guard
            .backend
            .pop()
            .await?
            .expect("element disappeared while holding lock"))
    }

    /// Sync version of [`GuardedEnvelopeBuffer::mark_ready`].
    ///
    /// Since [`EnvelopeBufferGuard`] already has exclusive access to the buffer, it can mark projects as ready
    /// without awaiting the lock.
    pub fn mark_ready(&mut self, project_key: &ProjectKey, ready: bool) {
        let changed = self.guard.backend.mark_ready(project_key, ready);
        if changed {
            self.notify();
        }
    }

    /// Notifies the waiting tasks that a change has happened in the buffer.
    fn notify(&mut self) {
        self.guard.should_peek = true;
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
    use crate::utils::MemoryStat;

    use super::*;

    fn new_buffer() -> Arc<GuardedEnvelopeBuffer> {
        let config = Arc::new(
            Config::from_json_value(serde_json::json!({
                "spool": {
                    "envelopes": {
                        "version": "experimental",
                        "max_memory_percent": 1.0
                    }
                }
            }))
            .unwrap(),
        );

        let memory_checker = MemoryChecker::new(MemoryStat::default(), config.clone());

        GuardedEnvelopeBuffer::from_config(&config, memory_checker)
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
