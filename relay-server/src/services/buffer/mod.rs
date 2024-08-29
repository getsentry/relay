//! Types for buffering envelopes.

use std::future;
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
use crate::statsd::RelayCounters;
use crate::utils::MemoryChecker;

pub use envelope_buffer::EnvelopeBufferError;
pub use envelope_buffer::PolymorphicEnvelopeBuffer;
pub use envelope_stack::sqlite::SqliteEnvelopeStack;
// pub for benchmarks
pub use envelope_stack::EnvelopeStack;
// pub for benchmarks
pub use envelope_store::sqlite::SqliteEnvelopeStore;
// pub for benchmarks

mod envelope_buffer;
mod envelope_stack;
mod envelope_store;
mod stack_provider;
mod testutils;

/// Message interface for [`EnvelopeBufferService`].
#[derive(Debug)]
pub enum EnvelopeBuffer {
    /// A fresh envelope that gets pushed into the buffer by the request handler.
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

/// Contains the services [`Addr`] and a watch channel to observe its state.
///
/// This allows outside observers to check the capacity without having to send a message.
///
// NOTE: This pattern of combining an Addr with some observable state could be generalized into
// `Service` itself.
#[derive(Debug, Clone)]
pub struct ObservableEnvelopeBuffer {
    addr: Addr<EnvelopeBuffer>,
    has_capacity: Arc<AtomicBool>,
}

impl ObservableEnvelopeBuffer {
    /// Returns the address of the buffer service.
    pub fn addr(&self) -> Addr<EnvelopeBuffer> {
        self.addr.clone()
    }

    /// Returns `true` if the buffer has the capacity to accept more elements.
    pub fn has_capacity(&self) -> bool {
        self.has_capacity.load(Ordering::Relaxed)
    }
}

/// Spool V2 service which buffers envelopes and forwards them to the project cache when a project
/// becomes ready.
pub struct EnvelopeBufferService {
    config: Arc<Config>,
    memory_checker: MemoryChecker,
    project_cache: Addr<ProjectCache>,
    has_capacity: Arc<AtomicBool>,
    changes: bool,
}

impl EnvelopeBufferService {
    /// Creates a memory or disk based [`EnvelopeBufferService`], depending on the given config.
    ///
    /// NOTE: until the V1 spooler implementation is removed, this function returns `None`
    /// if V2 spooling is not configured.
    pub fn new(
        config: Arc<Config>,
        memory_checker: MemoryChecker,
        project_cache: Addr<ProjectCache>,
    ) -> Option<Self> {
        config.spool_v2().then(|| Self {
            config,
            memory_checker,
            project_cache,
            has_capacity: Arc::new(AtomicBool::new(true)),
            changes: true,
        })
    }

    /// Returns both the [`Addr`] to this service, and a reference to the capacity flag.
    pub fn start_observable(self) -> ObservableEnvelopeBuffer {
        let has_capacity = self.has_capacity.clone();
        ObservableEnvelopeBuffer {
            addr: self.start(),
            has_capacity,
        }
    }

    /// Return immediately if changes were flagged, otherwise sleep forever.
    ///
    /// NOTE: This function sleeps indefinitely if no changes were flagged.
    /// Only use in combination with [`tokio::select!`].
    async fn wait_for_changes(&mut self) {
        if !self.changes {
            let _: () = future::pending().await; // wait until cancelled
        }
    }

    /// Tries to pop an envelope for a ready project.
    async fn try_pop(
        &mut self,
        buffer: &mut PolymorphicEnvelopeBuffer,
    ) -> Result<(), EnvelopeBufferError> {
        relay_log::trace!("EnvelopeBufferService peek");
        match buffer.peek().await? {
            Peek::Empty => {
                relay_log::trace!("EnvelopeBufferService empty");
                self.changes = false;
            }
            Peek::Ready(_) => {
                relay_log::trace!("EnvelopeBufferService pop");
                let envelope = buffer
                    .pop()
                    .await?
                    .expect("Element disappeared despite exclusive excess");
                self.project_cache.send(DequeuedEnvelope(envelope));
                self.changes = true;
            }
            Peek::NotReady(envelope) => {
                relay_log::trace!("EnvelopeBufferService request update");
                let project_key = envelope.meta().public_key();
                self.project_cache.send(GetProjectState::new(project_key));
                match envelope.sampling_key() {
                    None => {}
                    Some(sampling_key) if sampling_key == project_key => {} // already sent.
                    Some(sampling_key) => {
                        self.project_cache.send(GetProjectState::new(sampling_key));
                    }
                }
                self.changes = false;
            }
        }
        Ok(())
    }

    async fn handle_message(
        &mut self,
        buffer: &mut PolymorphicEnvelopeBuffer,
        message: EnvelopeBuffer,
    ) {
        match message {
            EnvelopeBuffer::Push(envelope) => {
                // NOTE: This function assumes that a project state update for the relevant
                // projects was already triggered (see XXX).
                // For better separation of concerns, this prefetch should be triggered from here
                // once buffer V1 has been removed.
                relay_log::trace!("EnvelopeBufferService push");
                self.push(buffer, envelope).await;
            }
            EnvelopeBuffer::NotReady(project_key, envelope) => {
                relay_log::trace!("EnvelopeBufferService project not ready");
                buffer.mark_ready(&project_key, false);
                relay_statsd::metric!(counter(RelayCounters::BufferEnvelopesReturned) += 1);
                self.push(buffer, envelope).await;
            }
            EnvelopeBuffer::Ready(project_key) => {
                relay_log::trace!("EnvelopeBufferService project ready {}", &project_key);
                buffer.mark_ready(&project_key, true);
            }
        };
        self.changes = true;
    }

    async fn push(&mut self, buffer: &mut PolymorphicEnvelopeBuffer, envelope: Box<Envelope>) {
        if let Err(e) = buffer.push(envelope).await {
            relay_log::error!(
                error = &e as &dyn std::error::Error,
                "failed to push envelope"
            );
        }
    }

    fn update_observable_state(&self, buffer: &mut PolymorphicEnvelopeBuffer) {
        self.has_capacity
            .store(buffer.has_capacity(), Ordering::Relaxed);
    }
}

impl Service for EnvelopeBufferService {
    type Interface = EnvelopeBuffer;

    fn spawn_handler(mut self, mut rx: Receiver<Self::Interface>) {
        let config = self.config.clone();
        let memory_checker = self.memory_checker.clone();
        tokio::spawn(async move {
            let mut buffer = PolymorphicEnvelopeBuffer::from_config(&config, memory_checker).await;

            relay_log::info!("EnvelopeBufferService start");
            loop {
                relay_log::trace!("EnvelopeBufferService loop");
                tokio::select! {
                    // NOTE: we do not select a bias here.
                    // On the one hand, we might want to prioritize dequeing over enqueing
                    // so we do not exceed the buffer capacity by starving the dequeue.
                    // on the other hand, prioritizing old messages violates the LIFO design.
                    () = self.wait_for_changes() => {
                        if let Err(e) = self.try_pop(&mut buffer).await {
                            relay_log::error!(
                                error = &e as &dyn std::error::Error,
                                "failed to pop envelope"
                            );
                        }
                    }
                    Some(message) = rx.recv() => {
                        self.handle_message(&mut buffer, message).await;
                    }

                    else => break,
                }
                self.update_observable_state(&mut buffer);
            }
            relay_log::info!("EnvelopeBufferService stop");
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::MemoryStat;

    use super::*;

    #[tokio::test]
    async fn capacity_is_updated() {
        tokio::time::pause();
        let config = Arc::new(
            Config::from_json_value(serde_json::json!({
                "spool": {
                    "envelopes": {
                        "version": "experimental"
                    }
                }
            }))
            .unwrap(),
        );
        let memory_checker = MemoryChecker::new(MemoryStat::default(), config.clone());
        let service = EnvelopeBufferService::new(config, memory_checker, Addr::dummy()).unwrap();

        // Set capacity to false:
        service.has_capacity.store(false, Ordering::Relaxed);

        // Observable has correct value:
        let ObservableEnvelopeBuffer { addr, has_capacity } = service.start_observable();
        assert!(!has_capacity.load(Ordering::Relaxed));

        // Send a message to trigger update of `has_capacity` flag:
        let some_project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        addr.send(EnvelopeBuffer::Ready(some_project_key));

        tokio::time::advance(Duration::from_millis(100)).await;

        // Observable has correct value:
        assert!(has_capacity.load(Ordering::Relaxed));
    }
}
