//! Types for buffering envelopes.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Receiver, Service};
use tokio::sync::mpsc;

use crate::envelope::Envelope;
use crate::services::buffer::envelope_buffer::Peek;
use crate::services::project_cache::ProjectCache;
use crate::services::project_cache::UpdateProject;
use crate::statsd::RelayCounters;
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
    buffer: PolymorphicEnvelopeBuffer,
    project_cache: Addr<ProjectCache>,
    has_capacity: Arc<AtomicBool>,
    sleep: Duration,
    output_tx: mpsc::Sender<Box<Envelope>>,
}

const DEFAULT_SLEEP: Duration = Duration::from_millis(100);

impl EnvelopeBufferService {
    /// Creates a memory or disk based [`EnvelopeBufferService`], depending on the given config.
    ///
    /// NOTE: until the V1 spooler implementation is removed, this function returns `None`
    /// if V2 spooling is not configured.
    pub fn new(
        config: &Config,
        memory_checker: MemoryChecker,
        project_cache: Addr<ProjectCache>,
        output_tx: mpsc::Sender<Box<Envelope>>,
    ) -> Option<Self> {
        config.spool_v2().then(|| Self {
            buffer: PolymorphicEnvelopeBuffer::from_config(config, memory_checker),
            project_cache,
            has_capacity: Arc::new(AtomicBool::new(true)),
            sleep: Duration::ZERO,
            output_tx,
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

    /// Tries to pop an envelope for a ready project.
    ///
    /// Returns the amount of time we should wait until next pop
    async fn try_pop(
        &mut self,
        permit: mpsc::Permit<'_, Box<Envelope>>,
    ) -> Result<(), EnvelopeBufferError> {
        relay_log::trace!("EnvelopeBufferService peek");
        match self.buffer.peek().await? {
            Peek::Empty => {
                relay_log::trace!("EnvelopeBufferService empty");
                self.sleep = Duration::MAX; // wait for reset by `handle_message`.
            }
            Peek::Ready(_) => {
                relay_log::trace!("EnvelopeBufferService pop");
                let envelope = self
                    .buffer
                    .pop()
                    .await?
                    .expect("Element disappeared despite exclusive excess");
                permit.send(envelope);
                self.sleep = Duration::ZERO; // try next pop immediately
            }
            Peek::NotReady(stack_key, envelope) => {
                relay_log::trace!("EnvelopeBufferService request update");
                let project_key = envelope.meta().public_key();
                self.project_cache.send(UpdateProject(project_key));
                match envelope.sampling_key() {
                    None => {}
                    Some(sampling_key) if sampling_key == project_key => {} // already sent.
                    Some(sampling_key) => {
                        self.project_cache.send(UpdateProject(sampling_key));
                    }
                }
                // deprioritize the stack to prevent head-of-line blocking
                self.buffer.mark_seen(&stack_key);
                self.sleep = DEFAULT_SLEEP;
            }
        }
        Ok(())
    }

    async fn handle_message(&mut self, message: EnvelopeBuffer) {
        match message {
            EnvelopeBuffer::Push(envelope) => {
                // NOTE: This function assumes that a project state update for the relevant
                // projects was already triggered (see XXX).
                // For better separation of concerns, this prefetch should be triggered from here
                // once buffer V1 has been removed.
                relay_log::trace!("EnvelopeBufferService push");
                self.push(envelope).await;
            }
            EnvelopeBuffer::NotReady(project_key, envelope) => {
                relay_log::trace!("EnvelopeBufferService project not ready");
                self.buffer.mark_ready(&project_key, false);
                relay_statsd::metric!(counter(RelayCounters::BufferEnvelopesReturned) += 1);
                self.push(envelope).await;
            }
            EnvelopeBuffer::Ready(project_key) => {
                relay_log::trace!("EnvelopeBufferService project ready {}", &project_key);
                self.buffer.mark_ready(&project_key, true);
            }
        };
        self.sleep = Duration::ZERO;
    }

    async fn push(&mut self, envelope: Box<Envelope>) {
        if let Err(e) = self.buffer.push(envelope).await {
            relay_log::error!(
                error = &e as &dyn std::error::Error,
                "failed to push envelope"
            );
        }
    }

    async fn next_pop_attempt(
        &mut self,
    ) -> Result<mpsc::Permit<Box<Envelope>>, mpsc::error::SendError<()>> {
        tokio::time::sleep(self.sleep).await;
        self.output_tx.reserve().await
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
            relay_log::info!("EnvelopeBufferService start");
            loop {
                relay_log::trace!("EnvelopeBufferService loop");
                tokio::select! {
                    // NOTE: we do not select a bias here.
                    // On the one hand, we might want to prioritize dequeing over enqueing
                    // so we do not exceed the buffer capacity by starving the dequeue.
                    // on the other hand, prioritizing old messages violates the LIFO design.
                    permit = self.next_pop_attempt() => {
                        match permit {
                            Ok(permit) => {
                                if let Err(e) = self.try_pop(permit).await {
                                    relay_log::error!(
                                        error = &e as &dyn std::error::Error,
                                        "failed to pop envelope"
                                    );
                                }
                            },
                            Err(e) => {
                                relay_log::error!(
                                        error = &e as &dyn std::error::Error,
                                        "failed to acquire permit"
                                    );
                            }
                        }
                    }
                    Some(message) = rx.recv() => {
                        self.handle_message(message).await;
                    }

                    else => break,
                }
                self.update_observable_state();
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
        let service = EnvelopeBufferService::new(&config, memory_checker, Addr::dummy()).unwrap();

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
