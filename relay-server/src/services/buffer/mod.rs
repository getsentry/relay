//! Types for buffering envelopes.

use std::error::Error;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Receiver, Service};
use relay_system::{Controller, Shutdown};
use tokio::sync::mpsc::Permit;
use tokio::sync::{mpsc, watch};
use tokio::time::{timeout, Instant};

use crate::envelope::Envelope;
use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_buffer::Peek;
use crate::services::global_config;
use crate::services::outcome::DiscardReason;
use crate::services::outcome::Outcome;
use crate::services::outcome::TrackOutcome;
use crate::services::processor::ProcessingGroup;
use crate::services::projects::cache::{DequeuedEnvelope, ProjectCache, UpdateProject};

use crate::services::test_store::TestStore;
use crate::statsd::{RelayCounters, RelayHistograms};
use crate::utils::ManagedEnvelope;
use crate::MemoryChecker;
use crate::MemoryStat;

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
#[derive(Clone)]
pub struct Services {
    /// Bounded channel used exclusively to handle backpressure when sending envelopes to the
    /// project cache.
    pub envelopes_tx: mpsc::Sender<DequeuedEnvelope>,
    pub project_cache: Addr<ProjectCache>,
    pub outcome_aggregator: Addr<TrackOutcome>,
    pub test_store: Addr<TestStore>,
}

/// Spool V2 service which buffers envelopes and forwards them to the project cache when a project
/// becomes ready.
pub struct EnvelopeBufferService {
    config: Arc<Config>,
    memory_stat: MemoryStat,
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
        memory_stat: MemoryStat,
        global_config_rx: watch::Receiver<global_config::Status>,
        services: Services,
    ) -> Option<Self> {
        config.spool_v2().then(|| Self {
            config,
            memory_stat,
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
        buffer: &PolymorphicEnvelopeBuffer,
        dequeue: bool,
    ) -> Option<Permit<DequeuedEnvelope>> {
        relay_statsd::metric!(
            counter(RelayCounters::BufferReadyToPop) += 1,
            status = "checking"
        );

        self.system_ready(buffer, dequeue).await;

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

        let permit = self.services.envelopes_tx.reserve().await.ok();

        relay_statsd::metric!(
            counter(RelayCounters::BufferReadyToPop) += 1,
            status = "checked"
        );

        permit
    }

    /// Waits until preconditions for unspooling are met.
    ///
    /// - We should not pop from disk into memory when relay's overall memory capacity
    ///   has been reached.
    /// - We need a valid global config to unspool.
    async fn system_ready(&self, buffer: &PolymorphicEnvelopeBuffer, dequeue: bool) {
        loop {
            // We should not unspool from external storage if memory capacity has been reached.
            // But if buffer storage is in memory, unspooling can reduce memory usage.
            let memory_ready = buffer.is_memory() || self.memory_ready();
            let global_config_ready = self.global_config_rx.borrow().is_ready();

            if memory_ready && global_config_ready && dequeue {
                return;
            }
            tokio::time::sleep(DEFAULT_SLEEP).await;
        }
    }

    fn memory_ready(&self) -> bool {
        self.memory_stat.memory().used_percent()
            <= self.config.spool_max_backpressure_memory_percent()
    }

    /// Tries to pop an envelope for a ready project.
    async fn try_pop<'a>(
        config: &Config,
        buffer: &mut PolymorphicEnvelopeBuffer,
        services: &Services,
        envelopes_tx_permit: Permit<'a, DequeuedEnvelope>,
    ) -> Result<Duration, EnvelopeBufferError> {
        let sleep = match buffer.peek() {
            None => {
                relay_statsd::metric!(
                    counter(RelayCounters::BufferTryPop) += 1,
                    peek_result = "empty"
                );

                Duration::MAX // wait for reset by `handle_message`.
            }
            Some(Peek {
                project_key_pair,
                received_at,
                ready,
                next_project_fetch,
            }) => {
                if received_at.elapsed() > config.spool_envelopes_max_age() {
                    relay_statsd::metric!(
                        counter(RelayCounters::BufferTryPop) += 1,
                        peek_result = "expired"
                    );
                    if let Some(envelope) = buffer.pop().await? {
                        Self::drop_expired(envelope, services);
                    }

                    Duration::ZERO // try next pop immediately
                } else if ready {
                    relay_statsd::metric!(
                        counter(RelayCounters::BufferTryPop) += 1,
                        peek_result = "ready"
                    );
                    if let Some(envelope) = buffer.pop().await? {
                        // The cached `received_at` time on the queue might be newer than
                        // the actual timestamp of the envelope, so check again here.
                        if envelope.meta().start_time().elapsed() > config.spool_envelopes_max_age()
                        {
                            Self::drop_expired(envelope, services);
                        } else {
                            envelopes_tx_permit.send(DequeuedEnvelope(envelope));
                        }
                    }

                    Duration::ZERO // try next pop immediately
                } else {
                    let ProjectKeyPair {
                        own_key,
                        sampling_key,
                    } = project_key_pair;
                    relay_statsd::metric!(
                        counter(RelayCounters::BufferTryPop) += 1,
                        peek_result = "not_ready"
                    );
                    if Instant::now() >= next_project_fetch {
                        relay_log::trace!("EnvelopeBufferService: requesting project(s) update");

                        services.project_cache.send(UpdateProject(own_key));
                        if sampling_key != own_key {
                            services.project_cache.send(UpdateProject(sampling_key));
                        }

                        // Deprioritize the stack to prevent head-of-line blocking and update the next fetch
                        // time.
                        buffer.mark_seen(&project_key_pair, DEFAULT_SLEEP);
                    }

                    DEFAULT_SLEEP // wait and prioritize handling new messages.
                }
            }
        };

        Ok(sleep)
    }

    fn drop_expired(envelope: Box<Envelope>, services: &Services) {
        let mut managed_envelope = ManagedEnvelope::new(
            envelope,
            services.outcome_aggregator.clone(),
            services.test_store.clone(),
            ProcessingGroup::Ungrouped,
        );
        managed_envelope.reject(Outcome::Invalid(DiscardReason::Timestamp));
    }

    async fn handle_message(buffer: &mut PolymorphicEnvelopeBuffer, message: EnvelopeBuffer) {
        match message {
            EnvelopeBuffer::Push(envelope) => {
                // NOTE: This function assumes that a project state update for the relevant
                // projects was already triggered (see XXX).
                // For better separation of concerns, this prefetch should be triggered from here
                // once buffer V1 has been removed.
                relay_log::trace!("EnvelopeBufferService: received push message");
                Self::push(buffer, envelope).await;
            }
            EnvelopeBuffer::NotReady(project_key, envelope) => {
                relay_log::trace!(
                    "EnvelopeBufferService: received project not ready message for project key {}",
                    &project_key
                );
                relay_statsd::metric!(counter(RelayCounters::BufferEnvelopesReturned) += 1);
                Self::push(buffer, envelope).await;
                buffer.mark_ready(&project_key, false);
            }
            EnvelopeBuffer::Ready(project_key) => {
                relay_log::trace!(
                    "EnvelopeBufferService: received project ready message for project key {}",
                    &project_key
                );
                buffer.mark_ready(&project_key, true);
            }
        };
    }

    async fn handle_shutdown(buffer: &mut PolymorphicEnvelopeBuffer, message: Shutdown) -> bool {
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

    async fn push(buffer: &mut PolymorphicEnvelopeBuffer, envelope: Box<Envelope>) {
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
        let memory_checker = MemoryChecker::new(self.memory_stat.clone(), config.clone());
        let mut global_config_rx = self.global_config_rx.clone();
        let services = self.services.clone();

        let dequeue = Arc::<AtomicBool>::new(true.into());
        let dequeue1 = dequeue.clone();

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
            loop {
                let used_capacity = self.services.envelopes_tx.max_capacity()
                    - self.services.envelopes_tx.capacity();
                relay_statsd::metric!(
                    histogram(RelayHistograms::BufferBackpressureEnvelopesCount) =
                        used_capacity as u64
                );

                let mut sleep = Duration::MAX;
                tokio::select! {
                    // NOTE: we do not select a bias here.
                    // On the one hand, we might want to prioritize dequeuing over enqueuing
                    // so we do not exceed the buffer capacity by starving the dequeue.
                    // on the other hand, prioritizing old messages violates the LIFO design.
                    Some(permit) = self.ready_to_pop(&buffer, dequeue.load(Ordering::Relaxed)) => {
                        match Self::try_pop(&config, &mut buffer, &services, permit).await {
                            Ok(new_sleep) => {
                                sleep = new_sleep;
                            }
                            Err(error) => {
                                relay_log::error!(
                                error = &error as &dyn std::error::Error,
                                "failed to pop envelope"
                            );
                            }
                        }
                    }
                    Some(message) = rx.recv() => {
                        Self::handle_message(&mut buffer, message).await;
                        sleep = Duration::ZERO;
                    }
                    shutdown = shutdown.notified() => {
                        // In case the shutdown was handled, we break out of the loop signaling that
                        // there is no need to process anymore envelopes.
                        if Self::handle_shutdown(&mut buffer, shutdown).await {
                            break;
                        }
                    }
                    Ok(()) = global_config_rx.changed() => {
                        sleep = Duration::ZERO;
                    }
                    else => break,
                }

                self.sleep = sleep;
                self.update_observable_state(&mut buffer);
            }

            relay_log::info!("EnvelopeBufferService: stopping");
        });

        #[cfg(unix)]
        tokio::spawn(async move {
            use tokio::signal::unix::{signal, SignalKind};
            let Ok(mut signal) = signal(SignalKind::user_defined1()) else {
                return;
            };
            while let Some(()) = signal.recv().await {
                let deq = !dequeue1.load(Ordering::Relaxed);
                dequeue1.store(deq, Ordering::Relaxed);
                relay_log::info!("SIGUSR1 receive, dequeue={}", deq);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use relay_dynamic_config::GlobalConfig;
    use relay_metrics::UnixTimestamp;
    use relay_quotas::DataCategory;
    use sqlx::Connection;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use crate::testutils::new_envelope;
    use crate::MemoryStat;

    use super::*;

    struct EnvelopeBufferServiceResult {
        service: EnvelopeBufferService,
        global_tx: watch::Sender<global_config::Status>,
        envelopes_rx: mpsc::Receiver<DequeuedEnvelope>,
        project_cache_rx: mpsc::UnboundedReceiver<ProjectCache>,
        outcome_aggregator_rx: mpsc::UnboundedReceiver<TrackOutcome>,
    }

    fn envelope_buffer_service(
        config_json: Option<serde_json::Value>,
        global_config_status: global_config::Status,
    ) -> EnvelopeBufferServiceResult {
        let config_json = config_json.unwrap_or(serde_json::json!({
            "spool": {
                "envelopes": {
                    "version": "experimental"
                }
            }
        }));
        let config = Arc::new(Config::from_json_value(config_json).unwrap());

        let memory_stat = MemoryStat::default();
        let (global_tx, global_rx) = watch::channel(global_config_status);
        let (envelopes_tx, envelopes_rx) = mpsc::channel(5);
        let (project_cache, project_cache_rx) = Addr::custom();
        let (outcome_aggregator, outcome_aggregator_rx) = Addr::custom();

        let envelope_buffer_service = EnvelopeBufferService::new(
            config,
            memory_stat,
            global_rx,
            Services {
                envelopes_tx,
                project_cache,
                outcome_aggregator,
                test_store: Addr::dummy(),
            },
        )
        .unwrap();

        EnvelopeBufferServiceResult {
            service: envelope_buffer_service,
            global_tx,
            envelopes_rx,
            project_cache_rx,
            outcome_aggregator_rx,
        }
    }

    #[tokio::test]
    async fn capacity_is_updated() {
        tokio::time::pause();

        let EnvelopeBufferServiceResult {
            service,
            global_tx: _global_tx,
            envelopes_rx: _envelopes_rx,
            project_cache_rx: _project_cache_rx,
            outcome_aggregator_rx: _outcome_aggregator_rx,
        } = envelope_buffer_service(None, global_config::Status::Pending);

        service.has_capacity.store(false, Ordering::Relaxed);

        let ObservableEnvelopeBuffer { addr, has_capacity } = service.start_observable();
        assert!(!has_capacity.load(Ordering::Relaxed));

        let some_project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        addr.send(EnvelopeBuffer::Ready(some_project_key));

        tokio::time::advance(Duration::from_millis(100)).await;

        assert!(has_capacity.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn pop_requires_global_config() {
        tokio::time::pause();

        let EnvelopeBufferServiceResult {
            service,
            global_tx,
            envelopes_rx,
            project_cache_rx,
            outcome_aggregator_rx: _outcome_aggregator_rx,
        } = envelope_buffer_service(None, global_config::Status::Pending);

        let addr = service.start();

        let envelope = new_envelope(false, "foo");
        let project_key = envelope.meta().public_key();
        addr.send(EnvelopeBuffer::Push(envelope.clone()));
        addr.send(EnvelopeBuffer::Ready(project_key));

        tokio::time::sleep(Duration::from_millis(1000)).await;

        assert_eq!(envelopes_rx.len(), 0);
        assert_eq!(project_cache_rx.len(), 0);

        global_tx.send_replace(global_config::Status::Ready(Arc::new(
            GlobalConfig::default(),
        )));

        tokio::time::sleep(Duration::from_millis(1000)).await;

        assert_eq!(envelopes_rx.len(), 1);
        assert_eq!(project_cache_rx.len(), 0);
    }

    #[tokio::test]
    async fn pop_requires_memory_capacity() {
        tokio::time::pause();

        let EnvelopeBufferServiceResult {
            service,
            envelopes_rx,
            project_cache_rx,
            outcome_aggregator_rx: _outcome_aggregator_rx,
            global_tx: _global_tx,
        } = envelope_buffer_service(
            Some(serde_json::json!({
                "spool": {
                    "envelopes": {
                        "version": "experimental",
                        "path": std::env::temp_dir().join(Uuid::new_v4().to_string()),
                    }
                },
                "health": {
                    "max_memory_bytes": 0,
                }
            })),
            global_config::Status::Ready(Arc::new(GlobalConfig::default())),
        );

        let addr = service.start();

        let envelope = new_envelope(false, "foo");
        let project_key = envelope.meta().public_key();
        addr.send(EnvelopeBuffer::Push(envelope.clone()));
        addr.send(EnvelopeBuffer::Ready(project_key));

        tokio::time::sleep(Duration::from_millis(1000)).await;

        assert_eq!(envelopes_rx.len(), 0);
        assert_eq!(project_cache_rx.len(), 0);
    }

    #[tokio::test]
    async fn old_envelope_is_dropped() {
        tokio::time::pause();

        let EnvelopeBufferServiceResult {
            service,
            envelopes_rx,
            project_cache_rx,
            mut outcome_aggregator_rx,
            global_tx: _global_tx,
        } = envelope_buffer_service(
            Some(serde_json::json!({
                "spool": {
                    "envelopes": {
                        "version": "experimental",
                        "max_envelope_delay_secs": 1,
                    }
                }
            })),
            global_config::Status::Ready(Arc::new(GlobalConfig::default())),
        );

        let config = service.config.clone();
        let addr = service.start();

        let mut envelope = new_envelope(false, "foo");
        envelope
            .meta_mut()
            .set_start_time(Instant::now() - 2 * config.spool_envelopes_max_age());
        addr.send(EnvelopeBuffer::Push(envelope));

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(envelopes_rx.len(), 0);
        assert_eq!(project_cache_rx.len(), 0);

        let outcome = outcome_aggregator_rx.try_recv().unwrap();
        assert_eq!(outcome.category, DataCategory::TransactionIndexed);
        assert_eq!(outcome.quantity, 1);
    }

    #[tokio::test]
    async fn old_envelope_from_disk_is_dropped() {
        relay_log::init_test!();

        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("envelopes.db");

        let buffer_service = || {
            envelope_buffer_service(
                Some(serde_json::json!({
                    "spool": {
                        "envelopes": {
                            "version": "experimental",
                            "path": path,
                            "max_envelope_delay_secs": 1,
                        }
                    }
                })),
                global_config::Status::Ready(Arc::new(GlobalConfig::default())),
            )
        };

        // Initialize once to migrate the database:
        let service = buffer_service().service;
        let config = service.config.clone();
        service.start();

        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Write an envelope to the db
        let envelope = new_envelope(false, "foo");
        let mut db = sqlx::SqliteConnection::connect(path.to_str().unwrap())
            .await
            .unwrap();

        let received_at =
            UnixTimestamp::now().as_datetime().unwrap() - 2 * config.spool_envelopes_max_age();

        let query = sqlx::query("INSERT INTO envelopes (received_at, own_key, sampling_key, envelope) VALUES ($1, $2, $3, $4);")
            .bind(received_at.timestamp_millis())
            .bind(envelope.meta().public_key().to_string())
            .bind(envelope.meta().public_key().to_string())
            .bind(envelope.to_vec().unwrap());
        query.execute(&mut db).await.unwrap();

        // Initialize again to read from db:
        let EnvelopeBufferServiceResult {
            service,
            envelopes_rx,
            project_cache_rx,
            mut outcome_aggregator_rx,
            global_tx,
        } = buffer_service();

        let _addr = service.start();
        global_tx
            .send(global_config::Status::Ready(Arc::new(
                GlobalConfig::default(),
            )))
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(envelopes_rx.len(), 0);
        assert_eq!(project_cache_rx.len(), 0);

        let outcome = outcome_aggregator_rx.try_recv().unwrap();
        assert_eq!(outcome.category, DataCategory::TransactionIndexed);
        assert_eq!(outcome.quantity, 1);
    }

    #[tokio::test]
    async fn test_update_project() {
        tokio::time::pause();

        let EnvelopeBufferServiceResult {
            service,
            mut envelopes_rx,
            mut project_cache_rx,
            global_tx: _global_tx,
            outcome_aggregator_rx: _outcome_aggregator_rx,
        } = envelope_buffer_service(
            None,
            global_config::Status::Ready(Arc::new(GlobalConfig::default())),
        );

        let addr = service.start();

        let envelope = new_envelope(false, "foo");
        let project_key = envelope.meta().public_key();

        addr.send(EnvelopeBuffer::Push(envelope.clone()));

        tokio::time::sleep(Duration::from_secs(1)).await;

        let Some(DequeuedEnvelope(envelope)) = envelopes_rx.recv().await else {
            panic!();
        };

        addr.send(EnvelopeBuffer::NotReady(project_key, envelope));

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(project_cache_rx.len(), 1);
        let message = project_cache_rx.recv().await;
        assert!(matches!(
            message,
            Some(ProjectCache::UpdateProject(key)) if key == project_key
        ));

        tokio::time::sleep(Duration::from_secs(1)).await;

        assert_eq!(project_cache_rx.len(), 1);
        assert!(matches!(
            message,
            Some(ProjectCache::UpdateProject(key)) if key == project_key
        ))
    }

    #[tokio::test]
    async fn output_is_throttled() {
        tokio::time::pause();

        let EnvelopeBufferServiceResult {
            service,
            mut envelopes_rx,
            global_tx: _global_tx,
            project_cache_rx: _project_cache_rx,
            outcome_aggregator_rx: _outcome_aggregator_rx,
        } = envelope_buffer_service(
            None,
            global_config::Status::Ready(Arc::new(GlobalConfig::default())),
        );

        let addr = service.start();

        let envelope = new_envelope(false, "foo");
        let project_key = envelope.meta().public_key();
        for _ in 0..10 {
            addr.send(EnvelopeBuffer::Push(envelope.clone()));
        }
        addr.send(EnvelopeBuffer::Ready(project_key));

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut messages = vec![];
        envelopes_rx.recv_many(&mut messages, 100).await;

        assert_eq!(
            messages
                .iter()
                .filter(|message| matches!(message, DequeuedEnvelope(..)))
                .count(),
            5
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut messages = vec![];
        envelopes_rx.recv_many(&mut messages, 100).await;

        assert_eq!(
            messages
                .iter()
                .filter(|message| matches!(message, DequeuedEnvelope(..)))
                .count(),
            5
        );
    }
}
