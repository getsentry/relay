//! Types for buffering envelopes.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Receiver, Service};

use crate::envelope::Envelope;
use crate::services::buffer::envelope_buffer::Peek;
use crate::services::project_cache::DequeuedEnvelope;
use crate::services::project_cache::GetProjectState;
use crate::services::project_cache::ProjectCache;
use crate::utils::MemoryChecker;

pub use envelope_buffer::EnvelopeBufferError;
pub use envelope_buffer::PolymorphicEnvelopeBuffer;
pub use envelope_stack::sqlite::SqliteEnvelopeStack; // pub for benchmarks
pub use envelope_stack::EnvelopeStack; // pub for benchmarks
pub use envelope_store::sqlite::SqliteEnvelopeStore; // pub for benchmarks

mod envelope_buffer;
mod envelope_stack;
mod envelope_store;
mod stack_provider;
mod testutils;

/// Message interface for [`EnvelopeBufferService`].
#[derive(Debug)]
pub enum EnvelopeBuffer {
    /// An fresh envelope that gets pushed into the buffer by the request handler.
    Push(Box<Envelope>),
    /// Informs the service that a project has no valid project state and must be marked as not ready.
    ///
    /// This happens when an envelope was sent to the project cache, but one of the necessary project
    /// state has expired. The envelope is pushed back into the envelope buffer.
    NotReady(ProjectKey, Box<Envelope>),
    /// Informs the service that a project has a valid project state and can be marked as ready.
    Ready(ProjectKey),
}

impl Interface for EnvelopeBuffer {}

impl FromMessage<Self> for EnvelopeBuffer {
    type Response = NoResponse;

    fn from_message(message: Self, _: ()) -> Self {
        message
    }
}

/// Contains the services `Addr` and a watch channel to observe its state.
// NOTE: This pattern of combining an Addr with some observable state could be generalized into
// `Service` itself.
#[derive(Debug, Clone)]
pub struct ObservableEnvelopeBuffer {
    addr: Addr<EnvelopeBuffer>,
    has_capacity: Arc<AtomicBool>,
}

impl ObservableEnvelopeBuffer {
    pub fn addr(&self) -> Addr<EnvelopeBuffer> {
        self.addr.clone()
    }

    pub fn has_capacity(&self) -> bool {
        self.has_capacity.load(Ordering::Relaxed)
    }
}

/// Spool V2 service which buffers envelopes and forwards them to the project cache when a project
/// becomes ready.
pub struct EnvelopeBufferService {
    buffer: PolymorphicEnvelopeBuffer,
    changes: tokio::sync::Notify,
    project_cache: Addr<ProjectCache>,
    has_capacity: Arc<AtomicBool>,
}

impl EnvelopeBufferService {
    /// Creates a memory or disk based [`EnvelopeBufferService`], depending on the given config.
    ///
    /// NOTE: until the V1 spooler implementation is removed, this function returns `None`
    /// if V2 spooling is not configured.
    pub fn new(
        config: &Config,
        memory_checker: MemoryChecker,
        project_cache: Addr<ProjectCache>,
    ) -> Option<Self> {
        config.spool_v2().then(|| Self {
            buffer: PolymorphicEnvelopeBuffer::from_config(config, memory_checker),
            changes: tokio::sync::Notify::new(),
            project_cache,
            has_capacity: Arc::new(AtomicBool::new(true)),
        })
    }

    pub fn start_observable(self) -> ObservableEnvelopeBuffer {
        let has_capacity = self.has_capacity.clone();
        ObservableEnvelopeBuffer {
            addr: self.start(),
            has_capacity,
        }
    }

    async fn try_pop(&mut self) {
        if let Err(e) = self.try_pop_inner().await {
            relay_log::error!(
                error = &e as &dyn std::error::Error,
                "failed to pop envelope"
            );
        }
    }

    async fn try_pop_inner(&mut self) -> Result<(), EnvelopeBufferError> {
        match self.buffer.peek().await? {
            Peek::Empty => {
                // There's nothing in the buffer.
            }
            Peek::Ready(_) => {
                // FIXME(jjbayer): Requires https://github.com/getsentry/relay/pull/3960
                // in order to work.
                let envelope = self
                    .buffer
                    .pop()
                    .await?
                    .expect("Element disappeared despite exclusive excess");
                self.project_cache.send(DequeuedEnvelope(envelope));
            }
            Peek::NotReady(envelope) => {
                let project_key = envelope.meta().public_key();
                self.project_cache.send(GetProjectState::new(project_key));
                match envelope.sampling_key() {
                    None => {}
                    Some(sampling_key) if sampling_key == project_key => {} // already sent.
                    Some(sampling_key) => {
                        self.project_cache.send(GetProjectState::new(sampling_key));
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_message(&mut self, message: EnvelopeBuffer) {
        let changed;
        match message {
            EnvelopeBuffer::Push(envelope) => {
                // NOTE: This function assumes that a project state update for the relevant
                // projects was already triggered (see XXX).
                // For better separation of concerns, this prefetch should be triggered from here
                // once buffer V1 has been removed.
                self.push(envelope).await;
                changed = true;
            }
            EnvelopeBuffer::NotReady(project_key, envelope) => {
                self.buffer.mark_ready(&project_key, false);
                // TODO: metric
                self.push(envelope).await;
                changed = true;
            }
            EnvelopeBuffer::Ready(project_key) => {
                changed = self.buffer.mark_ready(&project_key, true);
            }
        };
        if changed {
            self.changes.notify_waiters();
        }
    }

    async fn push(&mut self, envelope: Box<Envelope>) {
        if let Err(e) = self.buffer.push(envelope).await {
            relay_log::error!(
                error = &e as &dyn std::error::Error,
                "failed to push envelope"
            );
        }
    }

    fn update_observable_state(&self) {
        self.has_capacity
            .store(self.buffer.has_capacity(), Ordering::Relaxed);
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
                self.update_observable_state();
            }
        });
    }
}

// #[cfg(test)]
// mod tests {
//     use std::str::FromStr;
//     use std::sync::atomic::AtomicUsize;
//     use std::sync::atomic::Ordering;
//     use std::sync::Arc;
//     use std::time::Duration;

//     use relay_common::Dsn;

//     use crate::extractors::RequestMeta;

//     use super::*;

//     fn new_buffer() -> Arc<GuardedEnvelopeBuffer> {
//         GuardedEnvelopeBuffer::from_config(
//             &Config::from_json_value(serde_json::json!({
//                 "spool": {
//                     "envelopes": {
//                         "version": "experimental"
//                     }
//                 }
//             }))
//             .unwrap(),
//         )
//         .unwrap()
//         .into()
//     }

//     fn new_envelope() -> Box<Envelope> {
//         Envelope::from_request(
//             None,
//             RequestMeta::new(
//                 Dsn::from_str("http://a94ae32be2584e0bbd7a4cbb95971fed@localhost/1").unwrap(),
//             ),
//         )
//     }

//     #[tokio::test]
//     async fn no_busy_loop_when_empty() {
//         let buffer = new_buffer();
//         let call_count = Arc::new(AtomicUsize::new(0));

//         tokio::time::pause();

//         let cloned_buffer = buffer.clone();
//         let cloned_call_count = call_count.clone();
//         tokio::spawn(async move {
//             loop {
//                 cloned_buffer.peek().await.remove().await.unwrap();
//                 cloned_call_count.fetch_add(1, Ordering::Relaxed);
//             }
//         });

//         // Initial state: no calls
//         assert_eq!(call_count.load(Ordering::Relaxed), 0);
//         tokio::time::advance(Duration::from_nanos(1)).await;
//         assert_eq!(call_count.load(Ordering::Relaxed), 0);

//         // State after push: one call
//         buffer.push(new_envelope()).await.unwrap();
//         tokio::time::advance(Duration::from_nanos(1)).await;
//         assert_eq!(call_count.load(Ordering::Relaxed), 1);
//         tokio::time::advance(Duration::from_nanos(1)).await;
//         assert_eq!(call_count.load(Ordering::Relaxed), 1);

//         // State after second push: two calls
//         buffer.push(new_envelope()).await.unwrap();
//         tokio::time::advance(Duration::from_nanos(1)).await;
//         assert_eq!(call_count.load(Ordering::Relaxed), 2);
//         tokio::time::advance(Duration::from_nanos(1)).await;
//         assert_eq!(call_count.load(Ordering::Relaxed), 2);
//     }

//     #[tokio::test]
//     async fn no_busy_loop_when_unchanged() {
//         let buffer = new_buffer();
//         let call_count = Arc::new(AtomicUsize::new(0));

//         tokio::time::pause();

//         let cloned_buffer = buffer.clone();
//         let cloned_call_count = call_count.clone();
//         tokio::spawn(async move {
//             loop {
//                 cloned_buffer.peek().await;
//                 cloned_call_count.fetch_add(1, Ordering::Relaxed);
//             }
//         });

//         buffer.push(new_envelope()).await.unwrap();

//         // Initial state: no calls
//         assert_eq!(call_count.load(Ordering::Relaxed), 0);

//         // After first advance: got one call
//         tokio::time::advance(Duration::from_nanos(1)).await;
//         assert_eq!(call_count.load(Ordering::Relaxed), 1);

//         // After second advance: still only one call (no change)
//         tokio::time::advance(Duration::from_nanos(1)).await;
//         assert_eq!(call_count.load(Ordering::Relaxed), 1);

//         // State after second push: two calls
//         buffer.push(new_envelope()).await.unwrap();
//         tokio::time::advance(Duration::from_nanos(1)).await;
//         assert_eq!(call_count.load(Ordering::Relaxed), 2);
//     }
// }
