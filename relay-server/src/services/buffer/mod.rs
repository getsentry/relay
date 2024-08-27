//! Types for buffering envelopes.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Receiver, Service};
use tokio::sync::MutexGuard;

use crate::envelope::Envelope;
use crate::services::project_cache::ProjectCache;
use crate::utils::ManagedEnvelope;

pub use envelope_buffer::EnvelopeBufferError;
pub use envelope_buffer::PolymorphicEnvelopeBuffer;
pub use envelope_stack::sqlite::SqliteEnvelopeStack; // pub for benchmarks
pub use envelope_stack::EnvelopeStack; // pub for benchmarks
pub use sqlite_envelope_store::SqliteEnvelopeStore; // pub for benchmarks // pub for benchmarks

mod envelope_buffer;
mod envelope_stack;
mod sqlite_envelope_store;
mod stack_provider;
mod testutils;

/// TODO: docs
#[derive(Debug)]
pub struct EnvelopeBuffer(Box<Envelope>);

impl Interface for EnvelopeBuffer {}

impl FromMessage<Box<Envelope>> for EnvelopeBuffer {
    type Response = NoResponse;

    fn from_message(message: Box<Envelope>, _: ()) -> Self {
        Self(message)
    }
}

struct EnvelopeBufferService {
    buffer: GuardedEnvelopeBuffer,
    project_cache: Addr<ProjectCache>,
}

impl Service for EnvelopeBufferService {
    type Interface = EnvelopeBuffer;

    fn spawn_handler(self, mut rx: Receiver<Self::Interface>) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;

                    // Prefer dequeing over enqueing so we do not exceed the buffer capacity
                    // by starving the dequeue.
                    guard = self.buffer.peek() => {
                        // TODO: get stuff from project cache here, then send envelope to project cache
                        todo!();
                    }
                    Some(EnvelopeBuffer(envelope)) = rx.recv() => {
                        self.buffer.push(envelope).await;
                    }

                    else => break,

                }
            }
        });
    }
}

/// TODO: docs
// TODO: rename
#[derive(Debug)]
struct GuardedEnvelopeBuffer {
    /// The buffer that we are writing to and reading from.
    backend: PolymorphicEnvelopeBuffer,
    /// Used to notify callers of `peek()` of any changes in the buffer.
    should_peek: bool,
    /// Used to notify callers of `peek()` of any changes in the buffer.
    notify: tokio::sync::Notify,
}

impl GuardedEnvelopeBuffer {
    /// Creates a memory or disk based [`GuardedEnvelopeBuffer`], depending on the given config.
    ///
    /// NOTE: until the V1 spooler implementation is removed, this function returns `None`
    /// if V2 spooling is not configured.
    fn from_config(config: &Config) -> Option<Self> {
        if config.spool_v2() {
            Some(Self {
                backend: PolymorphicEnvelopeBuffer::from_config(config),
                should_peek: true,
                notify: tokio::sync::Notify::new(),
            })
        } else {
            None
        }
    }

    /// Schedules a task to push an envelope to the buffer.
    ///
    /// Once the envelope is pushed, waiters will be notified.
    async fn push(&mut self, envelope: Envelope) {
        if let Err(e) = self.backend.push(envelope).await {
            relay_log::error!(
                error = &e as &dyn std::error::Error,
                "failed to push envelope"
            );
        }
        self.notify.notify_waiters();
    }

    /// Returns a reference to the next-in-line envelope.
    ///
    /// If the buffer is empty or has not changed since the last peek, this function will sleep
    /// until something changes in the buffer.
    async fn peek(&mut self) -> EnvelopeBufferGuard {
        loop {
            if self.should_peek {
                match self.backend.peek().await {
                    Ok(envelope) => {
                        if envelope.is_some() {
                            self.should_peek = false;
                            return EnvelopeBufferGuard(self);
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
            // We wait to get notified for any changes in the buffer.
            self.notify.notified().await;
        }
    }

    /// Marks a project as ready or not ready.
    ///
    /// The buffer re-prioritizes its envelopes based on this information.
    async fn mark_ready(&mut self, project_key: &ProjectKey, ready: bool) {
        let changed = self.backend.mark_ready(project_key, ready);
        if changed {
            self.notify.notify_waiters();
        }
    }
}

/// A view onto the next envelope in the buffer.
///
/// Objects of this type can only exist if the buffer is not empty.
pub struct EnvelopeBufferGuard<'a>(&'a mut GuardedEnvelopeBuffer);

impl EnvelopeBufferGuard<'_> {
    /// Returns a reference to the next envelope.
    pub async fn get(&mut self) -> Result<&Envelope, EnvelopeBufferError> {
        Ok(self
            .0
            .peek()
            .await
            .expect("element disappeared during exclusive access"))
    }

    /// Pops the next envelope from the buffer.
    ///
    /// This functions consumes the [`EnvelopeBufferGuard`].
    pub async fn remove(mut self) -> Result<Box<Envelope>, EnvelopeBufferError> {
        self.0.notify.notify_waiters();
        Ok(self
            .0
            .backend
            .pop()
            .await?
            .expect("element disappeared during exclusive access"))
    }

    /// Sync version of [`GuardedEnvelopeBuffer::mark_ready`].
    ///
    /// Since [`EnvelopeBufferGuard`] already has exclusive access to the buffer, it can mark projects as ready
    /// without awaiting the lock.
    pub fn mark_ready(&mut self, project_key: &ProjectKey, ready: bool) {
        self.0.mark_ready(project_key, ready);
    }

    // /// Notifies the waiting tasks that a change has happened in the buffer.
    // fn notify(&mut self) {
    //     self.guard.should_peek = true;
    //     self.notify.notify_waiters();
    // }
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
