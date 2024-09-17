//! Types for buffering envelopes.

use std::error::Error;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_system::SendError;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Receiver, Service};
use relay_system::{Controller, Shutdown};
use tokio::sync::watch;
use tokio::time::timeout;

use crate::envelope::Envelope;
use crate::services::buffer::envelope_buffer::Peek;
use crate::services::global_config;
use crate::services::outcome::DiscardReason;
use crate::services::outcome::Outcome;
use crate::services::outcome::TrackOutcome;
use crate::services::processor::ProcessingGroup;
use crate::services::project_cache::DequeuedEnvelope;
use crate::services::project_cache::ProjectCache;
use crate::services::project_cache::UpdateProject;
use crate::services::test_store::TestStore;
use crate::statsd::RelayCounters;
use crate::utils::ManagedEnvelope;
use crate::utils::MemoryChecker;

pub use envelope_buffer::EnvelopeBufferError;
// pub for benchmarks
pub use envelope_buffer::PolymorphicEnvelopeBuffer;
// pub for benchmarks
pub use envelope_stack::sqlite::SqliteEnvelopeStack;
// pub for benchmarks
pub use envelope_stack::EnvelopeStack;
// pub for benchmarks
pub use envelope_store::sqlite::SqliteEnvelopeStore;

mod common;
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

/// Services that the buffer service communicates with.
pub struct Services {
    pub project_cache: Addr<ProjectCache>,
    pub outcome_aggregator: Addr<TrackOutcome>,
    pub test_store: Addr<TestStore>,
}

/// Spool V2 service which buffers envelopes and forwards them to the project cache when a project
/// becomes ready.
pub struct EnvelopeBufferService {
    config: Arc<Config>,
    memory_checker: MemoryChecker,
    global_config_rx: watch::Receiver<global_config::Status>,
    services: Services,
    has_capacity: Arc<AtomicBool>,
    sleep: Duration,
}

/// The maximum amount of time between evaluations of dequeue conditions.
///
/// Some condition checks are sync (`has_capacity`), so cannot be awaited. The sleep in cancelled
/// whenever a new message or a global config update comes in.
const DEFAULT_SLEEP: Duration = Duration::from_secs(1);

impl EnvelopeBufferService {
    /// Creates a memory or disk based [`EnvelopeBufferService`], depending on the given config.
    ///
    /// NOTE: until the V1 spooler implementation is removed, this function returns `None`
    /// if V2 spooling is not configured.
    pub fn new(
        config: Arc<Config>,
        memory_checker: MemoryChecker,
        global_config_rx: watch::Receiver<global_config::Status>,
        services: Services,
    ) -> Option<Self> {
        config.spool_v2().then(|| Self {
            config,
            memory_checker,
            global_config_rx,
            services,
            has_capacity: Arc::new(AtomicBool::new(true)),
            sleep: Duration::ZERO,
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

    /// Wait for the configured amount of time and make sure the project cache is ready to receive.
    async fn ready_to_pop(
        &mut self,
        buffer: &mut PolymorphicEnvelopeBuffer,
    ) -> Result<(), SendError> {
        relay_statsd::metric!(
            counter(RelayCounters::BufferReadyToPop) += 1,
            status = "checking"
        );

        self.system_ready(buffer).await;

        relay_statsd::metric!(
            counter(RelayCounters::BufferReadyToPop) += 1,
            status = "system_ready"
        );

        if self.sleep > Duration::ZERO {
            tokio::time::sleep(self.sleep).await;
        }

        relay_statsd::metric!(
            counter(RelayCounters::BufferReadyToPop) += 1,
            status = "slept"
        );

        relay_statsd::metric!(
            counter(RelayCounters::BufferReadyToPop) += 1,
            status = "checked"
        );

        Ok(())
    }

    /// Waits until preconditions for unspooling are met.
    ///
    /// - We should not pop from disk into memory when relay's overall memory capacity
    ///   has been reached.
    /// - We need a valid global config to unspool.
    async fn system_ready(&self, buffer: &PolymorphicEnvelopeBuffer) {
        loop {
            // We should not unspool from external storage if memory capacity has been reached.
            // But if buffer storage is in memory, unspooling can reduce memory usage.
            let memory_ready =
                !buffer.is_external() || self.memory_checker.check_memory().has_capacity();
            let global_config_ready = self.global_config_rx.borrow().is_ready();

            if memory_ready && global_config_ready {
                return;
            }
            tokio::time::sleep(DEFAULT_SLEEP).await;
        }
    }

    /// Tries to pop an envelope for a ready project.
    async fn try_pop(
        &mut self,
        buffer: &mut PolymorphicEnvelopeBuffer,
    ) -> Result<(), EnvelopeBufferError> {
        relay_log::trace!("EnvelopeBufferService: peeking the buffer");
        match buffer.peek().await? {
            Peek::Empty => {
                relay_log::trace!("EnvelopeBufferService: peek returned empty");
                relay_statsd::metric!(
                    counter(RelayCounters::BufferTryPop) += 1,
                    peek_result = "empty"
                );
                self.sleep = Duration::MAX; // wait for reset by `handle_message`.
            }
            Peek::Ready(envelope) | Peek::NotReady(.., envelope) if self.expired(envelope) => {
                let envelope = buffer
                    .pop()
                    .await?
                    .expect("Element disappeared despite exclusive excess");
                self.drop_expired(envelope);
            }
            Peek::Ready(_) => {
                relay_log::trace!("EnvelopeBufferService: popping envelope");
                relay_statsd::metric!(
                    counter(RelayCounters::BufferTryPop) += 1,
                    peek_result = "ready"
                );
                let envelope = buffer
                    .pop()
                    .await?
                    .expect("Element disappeared despite exclusive excess");
                self.services.project_cache.send(DequeuedEnvelope(envelope));
                self.sleep = Duration::ZERO; // try next pop immediately
            }
            Peek::NotReady(stack_key, envelope) => {
                relay_log::trace!("EnvelopeBufferService: project(s) of envelope not ready, requesting project update");
                relay_statsd::metric!(
                    counter(RelayCounters::BufferTryPop) += 1,
                    peek_result = "not_ready"
                );
                let project_key = envelope.meta().public_key();
                self.services.project_cache.send(UpdateProject(project_key));
                match envelope.sampling_key() {
                    None => {}
                    Some(sampling_key) if sampling_key == project_key => {} // already sent.
                    Some(sampling_key) => {
                        self.services
                            .project_cache
                            .send(UpdateProject(sampling_key));
                    }
                }
                // deprioritize the stack to prevent head-of-line blocking
                buffer.mark_seen(&stack_key);
                self.sleep = DEFAULT_SLEEP;
            }
        }

        Ok(())
    }

    fn expired(&self, envelope: &Envelope) -> bool {
        envelope.meta().start_time().elapsed() > self.config.spool_envelopes_max_age()
    }

    fn drop_expired(&self, envelope: Box<Envelope>) {
        let mut managed_envelope = ManagedEnvelope::new(
            envelope,
            self.services.outcome_aggregator.clone(),
            self.services.test_store.clone(),
            ProcessingGroup::Ungrouped,
        );
        managed_envelope.reject(Outcome::Invalid(DiscardReason::Timestamp));
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
                relay_log::trace!("EnvelopeBufferService: received push message");
                self.push(buffer, envelope).await;
            }
            EnvelopeBuffer::NotReady(project_key, envelope) => {
                relay_log::trace!(
                    "EnvelopeBufferService: received project not ready message for project key {}",
                    &project_key
                );
                buffer.mark_ready(&project_key, false);
                relay_statsd::metric!(counter(RelayCounters::BufferEnvelopesReturned) += 1);
                self.push(buffer, envelope).await;
            }
            EnvelopeBuffer::Ready(project_key) => {
                relay_log::trace!(
                    "EnvelopeBufferService: received project ready message for project key {}",
                    &project_key
                );
                buffer.mark_ready(&project_key, true);
            }
        };
        self.sleep = Duration::ZERO;
    }

    async fn handle_shutdown(
        &mut self,
        buffer: &mut PolymorphicEnvelopeBuffer,
        message: Shutdown,
    ) -> bool {
        // We gracefully shut down only if the shutdown has a timeout.
        if let Some(shutdown_timeout) = message.timeout {
            relay_log::trace!("EnvelopeBufferService: shutting down gracefully");

            let shutdown_result = timeout(shutdown_timeout, buffer.shutdown()).await;
            match shutdown_result {
                Ok(shutdown_result) => {
                    return shutdown_result;
                }
                Err(error) => {
                    relay_log::error!(
                    error = &error as &dyn Error,
                    "the envelope buffer didn't shut down in time, some envelopes might be lost",
                );
                }
            }
        }

        false
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
        let mut global_config_rx = self.global_config_rx.clone();
        tokio::spawn(async move {
            let buffer = PolymorphicEnvelopeBuffer::from_config(&config, memory_checker).await;

            let mut buffer = match buffer {
                Ok(buffer) => buffer,
                Err(error) => {
                    relay_log::error!(
                        error = &error as &dyn std::error::Error,
                        "failed to start the envelope buffer service",
                    );
                    std::process::exit(1);
                }
            };
            buffer.initialize().await;

            let mut shutdown = Controller::shutdown_handle();

            relay_log::info!("EnvelopeBufferService: starting");
            let mut iteration = 0;
            loop {
                iteration += 1;
                relay_log::trace!("EnvelopeBufferService: loop iteration {iteration}");

                tokio::select! {
                    // NOTE: we do not select a bias here.
                    // On the one hand, we might want to prioritize dequeuing over enqueuing
                    // so we do not exceed the buffer capacity by starving the dequeue.
                    // on the other hand, prioritizing old messages violates the LIFO design.
                    Ok(()) = self.ready_to_pop(&mut buffer) => {
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
                    shutdown = shutdown.notified() => {
                        // In case the shutdown was handled, we break out of the loop signaling that
                        // there is no need to process anymore envelopes.
                        if self.handle_shutdown(&mut buffer, shutdown).await {
                            break;
                        }
                    }
                    _ = global_config_rx.changed() => {
                        relay_log::trace!("EnvelopeBufferService: received global config");
                        self.sleep = Duration::ZERO; // Try to pop
                    }
                    else => break,
                }

                self.update_observable_state(&mut buffer);
            }

            relay_log::info!("EnvelopeBufferService: stopping");
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use relay_dynamic_config::GlobalConfig;
    use relay_quotas::DataCategory;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use crate::testutils::new_envelope;
    use crate::MemoryStat;

    use super::*;

    fn buffer_service() -> (
        EnvelopeBufferService,
        watch::Sender<global_config::Status>,
        mpsc::UnboundedReceiver<ProjectCache>,
        mpsc::UnboundedReceiver<TrackOutcome>,
    ) {
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
        let (global_tx, global_rx) = watch::channel(global_config::Status::Pending);
        let (project_cache, project_cache_rx) = Addr::custom();
        let (outcome_aggregator, outcome_aggregator_rx) = Addr::custom();
        (
            EnvelopeBufferService::new(
                config,
                memory_checker,
                global_rx,
                Services {
                    project_cache,
                    outcome_aggregator,
                    test_store: Addr::dummy(),
                },
            )
            .unwrap(),
            global_tx,
            project_cache_rx,
            outcome_aggregator_rx,
        )
    }

    #[tokio::test]
    async fn capacity_is_updated() {
        tokio::time::pause();
        let (service, _global_rx, _project_cache_tx, _) = buffer_service();

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

    #[tokio::test]
    async fn pop_requires_global_config() {
        tokio::time::pause();
        let (service, global_tx, project_cache_rx, _) = buffer_service();

        let addr = service.start();

        // Send five messages:
        let envelope = new_envelope(false, "foo");
        let project_key = envelope.meta().public_key();
        addr.send(EnvelopeBuffer::Push(envelope.clone()));
        addr.send(EnvelopeBuffer::Ready(project_key));

        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Nothing was dequeued, global config not ready:
        assert_eq!(project_cache_rx.len(), 0);

        global_tx.send_replace(global_config::Status::Ready(Arc::new(
            GlobalConfig::default(),
        )));

        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Dequeued, global config ready:
        assert_eq!(project_cache_rx.len(), 1);
    }

    #[tokio::test]
    async fn pop_requires_memory_capacity() {
        tokio::time::pause();

        let config = Arc::new(
            Config::from_json_value(serde_json::json!({
                "spool": {
                    "envelopes": {
                        "version": "experimental",
                        "path": std::env::temp_dir().join(Uuid::new_v4().to_string()),
                    }
                },
                "health": {
                    "max_memory_bytes": 0,
                }
            }))
            .unwrap(),
        );
        let memory_checker = MemoryChecker::new(MemoryStat::default(), config.clone());
        let (_, global_rx) = watch::channel(global_config::Status::Ready(Arc::new(
            GlobalConfig::default(),
        )));

        let (project_cache, project_cache_rx) = Addr::custom();
        let service = EnvelopeBufferService::new(
            config,
            memory_checker,
            global_rx,
            Services {
                project_cache,
                outcome_aggregator: Addr::dummy(),
                test_store: Addr::dummy(),
            },
        )
        .unwrap();
        let addr = service.start();

        // Send five messages:
        let envelope = new_envelope(false, "foo");
        let project_key = envelope.meta().public_key();
        addr.send(EnvelopeBuffer::Push(envelope.clone()));
        addr.send(EnvelopeBuffer::Ready(project_key));

        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Nothing was dequeued, memory not ready:
        assert_eq!(project_cache_rx.len(), 0);
    }

    #[tokio::test]
    async fn old_envelope_is_dropped() {
        tokio::time::pause();
        let (service, global_tx, project_cache_rx, mut outcome_rx) = buffer_service();
        global_tx.send_replace(global_config::Status::Ready(Arc::new(
            GlobalConfig::default(),
        )));

        let config = service.config.clone();
        let addr = service.start();

        // Send five messages:
        let mut envelope = new_envelope(false, "foo");
        envelope
            .meta_mut()
            .set_start_time(Instant::now() - 2 * config.spool_envelopes_max_age());
        addr.send(EnvelopeBuffer::Push(envelope));

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(project_cache_rx.is_empty());
        let outcome = outcome_rx.try_recv().unwrap();
        assert_eq!(outcome.category, DataCategory::TransactionIndexed);
        assert_eq!(outcome.quantity, 1);
    }
}
