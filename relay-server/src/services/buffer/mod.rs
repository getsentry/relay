//! Types for buffering envelopes.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Receiver, Service};
use tokio::sync::MutexGuard;

use crate::envelope::Envelope;
use crate::services::project_cache::{FetchProjectState, ProjectCache};
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
pub struct Readiness {
    project_key: ProjectKey,
    ready: bool,
}

/// TODO: docs
#[derive(Debug)]
pub enum EnvelopeBuffer {
    Push(Box<Envelope>),
    MarkReady(Readiness),
}

impl Interface for EnvelopeBuffer {}

impl FromMessage<Box<Envelope>> for EnvelopeBuffer {
    type Response = NoResponse;

    fn from_message(message: Box<Envelope>, _: ()) -> Self {
        Self::Push(message)
    }
}

impl FromMessage<Readiness> for EnvelopeBuffer {
    type Response = NoResponse;

    fn from_message(message: Readiness, _: ()) -> Self {
        Self::MarkReady(message)
    }
}

/// TODO: docs
pub struct EnvelopeBufferService {
    buffer: PolymorphicEnvelopeBuffer,
    changes: tokio::sync::Notify,
    project_cache: Addr<ProjectCache>,
}

impl EnvelopeBufferService {
    async fn try_pop(&mut self) {
        match self.buffer.peek().await {
            Ok(Some(envelope, assume_ready)) => match assume_ready {
                true => {
                    let envelope = peek.remove();
                    self.project_cache.send(ProcessEnvelope2(envelope));
                    self.changes.notify_waiters();
                }
                false => self
                    .project_cache
                    .send(Prefetch(envelope.project_key(), envelope.sampling_key())),
            },
            Ok(None) => {
                // There's nothing in the buffer.
            }
            Err(error) => {
                relay_log::error!(
                    error = &error as &dyn std::error::Error,
                    "failed to peek at envelope"
                );
            }
        };
    }

    async fn handle_message(&mut self, message: EnvelopeBuffer) {
        let changed;
        match message {
            EnvelopeBuffer::Push(envelope) => {
                // NOTE: This function assumes that a project state update for the relevant
                // projects was already triggered (see XXX).
                // For better separation of concerns, this prefetch should be triggered from here
                // once buffer V1 has been removed.
                if let Err(e) = self.buffer.push(envelope).await {
                    relay_log::error!(
                        error = &e as &dyn std::error::Error,
                        "failed to push envelope"
                    );
                }
                changed = true;
            }
            EnvelopeBuffer::MarkReady(Readiness { project_key, ready }) => {
                changed = self.buffer.mark_ready(&project_key, ready);
            }
        };
        if changed {
            self.changes.notify_waiters();
        }
    }
}

impl Service for EnvelopeBufferService {
    type Interface = EnvelopeBuffer;

    fn spawn_handler(mut self, mut rx: Receiver<Self::Interface>) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    // Prefer dequeing over enqueing so we do not exceed the buffer capacity
                    // by starving the dequeue.
                    () = self.changes.notified() => {
                        self.try_pop().await;
                    }
                    Some(message) = rx.recv() => {
                        self.handle_message(message).await;
                    }

                    else => break,
                }
            }
        });
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
