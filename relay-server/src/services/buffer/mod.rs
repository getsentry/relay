//! Types for buffering envelopes.

use std::error::Error;
use std::num::NonZeroU8;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use ahash::RandomState;
use chrono::DateTime;
use chrono::Utc;
use relay_config::Config;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Service};
use relay_system::{Controller, Shutdown};
use relay_system::{Receiver, ServiceRunner};
use tokio::sync::watch;
use tokio::time::{timeout, Instant};

use crate::envelope::Envelope;
use crate::services::buffer::envelope_buffer::Peek;
use crate::services::global_config;
use crate::services::outcome::DiscardReason;
use crate::services::outcome::Outcome;
use crate::services::outcome::TrackOutcome;
use crate::services::processor::{EnvelopeProcessor, ProcessEnvelope, ProcessingGroup};
use crate::services::projects::cache::{CheckedEnvelope, ProjectCacheHandle, ProjectChange};
use crate::services::test_store::TestStore;
use crate::statsd::RelayCounters;
use crate::statsd::RelayTimers;
use crate::utils::ManagedEnvelope;
use crate::MemoryChecker;
use crate::MemoryStat;

// pub for benchmarks
pub use envelope_buffer::EnvelopeBufferError;
// pub for benchmarks
pub use envelope_buffer::PolymorphicEnvelopeBuffer;
// pub for benchmarks
pub use envelope_stack::sqlite::SqliteEnvelopeStack;
// pub for benchmarks
pub use envelope_stack::EnvelopeStack;
// pub for benchmarks
pub use envelope_store::sqlite::SqliteEnvelopeStore;

use crate::services::projects::project::ProjectState;
pub use common::ProjectKeyPair;

mod common;
mod envelope_buffer;
mod envelope_stack;
mod envelope_store;
mod stack_provider;
mod testutils;

/// Seed used for hashing the project key pairs.
const PARTITIONING_HASHING_SEED: usize = 0;

/// Message interface for [`EnvelopeBufferService`].
#[derive(Debug)]
pub enum EnvelopeBuffer {
    /// A fresh envelope that gets pushed into the buffer by the request handler.
    Push(Box<Envelope>),
}

impl EnvelopeBuffer {
    fn name(&self) -> &'static str {
        match &self {
            EnvelopeBuffer::Push(_) => "push",
        }
    }
}

impl Interface for EnvelopeBuffer {}

impl FromMessage<Self> for EnvelopeBuffer {
    type Response = NoResponse;

    fn from_message(message: Self, _: ()) -> Self {
        message
    }
}

/// Abstraction that wraps a list of [`ObservableEnvelopeBuffer`]s to which [`Envelope`] are routed
/// based on their [`ProjectKeyPair`].
#[derive(Debug, Clone)]
pub struct PartitionedEnvelopeBuffer {
    buffers: Arc<Vec<ObservableEnvelopeBuffer>>,
}

impl PartitionedEnvelopeBuffer {
    /// Creates a new [`PartitionedEnvelopeBuffer`] by instantiating inside all the necessary
    /// [`ObservableEnvelopeBuffer`]s.
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        partitions: NonZeroU8,
        config: Arc<Config>,
        memory_stat: MemoryStat,
        global_config_rx: watch::Receiver<global_config::Status>,
        project_cache_handle: ProjectCacheHandle,
        envelope_processor: Addr<EnvelopeProcessor>,
        outcome_aggregator: Addr<TrackOutcome>,
        test_store: Addr<TestStore>,
        runner: &mut ServiceRunner,
    ) -> Self {
        let mut envelope_buffers = Vec::with_capacity(partitions.get() as usize);
        for partition_id in 0..partitions.get() {
            let envelope_buffer = EnvelopeBufferService::new(
                partition_id,
                config.clone(),
                memory_stat.clone(),
                global_config_rx.clone(),
                Services {
                    project_cache_handle: project_cache_handle.clone(),
                    envelope_processor: envelope_processor.clone(),
                    outcome_aggregator: outcome_aggregator.clone(),
                    test_store: test_store.clone(),
                },
            )
            .start_in(runner);

            envelope_buffers.push(envelope_buffer);
        }

        Self {
            buffers: Arc::new(envelope_buffers),
        }
    }

    /// Returns the [`ObservableEnvelopeBuffer`] to which [`Envelope`]s having the supplied
    /// [`ProjectKeyPair`] will be sent.
    ///
    /// The rationale of using this partitioning strategy is to reduce memory usage across buffers
    /// since each individual buffer will only take care of a subset of projects.
    pub fn buffer(&self, project_key_pair: ProjectKeyPair) -> &ObservableEnvelopeBuffer {
        let state = RandomState::with_seed(PARTITIONING_HASHING_SEED);
        let buffer_index = (state.hash_one(project_key_pair) % self.buffers.len() as u64) as usize;
        self.buffers
            .get(buffer_index)
            .expect("buffers should not be empty")
    }

    /// Returns `true` if all [`ObservableEnvelopeBuffer`]s have capacity to get new [`Envelope`]s.
    ///
    /// If no buffers are specified, the function returns `true`, assuming that there is capacity
    /// if the buffer is not setup.
    pub fn has_capacity(&self) -> bool {
        if self.buffers.is_empty() {
            return true;
        }

        self.buffers.iter().all(|buffer| buffer.has_capacity())
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
    pub project_cache_handle: ProjectCacheHandle,
    pub envelope_processor: Addr<EnvelopeProcessor>,
    pub outcome_aggregator: Addr<TrackOutcome>,
    pub test_store: Addr<TestStore>,
}

/// Spool V2 service which buffers envelopes and forwards them to the project cache when a project
/// becomes ready.
pub struct EnvelopeBufferService {
    partition_id: u8,
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
    pub fn new(
        partition_id: u8,
        config: Arc<Config>,
        memory_stat: MemoryStat,
        global_config_rx: watch::Receiver<global_config::Status>,
        services: Services,
    ) -> Self {
        Self {
            partition_id,
            config,
            memory_stat,
            global_config_rx,
            services,
            has_capacity: Arc::new(AtomicBool::new(true)),
            sleep: Duration::ZERO,
        }
    }

    /// Returns both the [`Addr`] to this service, and a reference to the capacity flag.
    pub fn start_in(self, runner: &mut ServiceRunner) -> ObservableEnvelopeBuffer {
        let has_capacity = self.has_capacity.clone();

        let addr = runner.start(self);
        ObservableEnvelopeBuffer { addr, has_capacity }
    }

    /// Wait for the configured amount of time and make sure the project cache is ready to receive.
    async fn ready_to_pop(&mut self, buffer: &PolymorphicEnvelopeBuffer, dequeue: bool) {
        self.system_ready(buffer, dequeue).await;

        if self.sleep > Duration::ZERO {
            tokio::time::sleep(self.sleep).await;
        }
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
        partition_tag: &str,
        config: &Config,
        buffer: &mut PolymorphicEnvelopeBuffer,
        services: &Services,
    ) -> Result<Duration, EnvelopeBufferError> {
        let sleep = match buffer.peek().await? {
            Peek::Empty => {
                relay_statsd::metric!(
                    counter(RelayCounters::BufferTryPop) += 1,
                    peek_result = "empty",
                    partition_id = partition_tag
                );

                DEFAULT_SLEEP // wait for reset by `handle_message`.
            }
            Peek::Ready {
                last_received_at, ..
            }
            | Peek::NotReady {
                last_received_at, ..
            } if is_expired(last_received_at, config) => {
                relay_statsd::metric!(
                    counter(RelayCounters::BufferTryPop) += 1,
                    peek_result = "expired",
                    partition_id = partition_tag
                );
                let envelope = buffer
                    .pop()
                    .await?
                    .expect("Element disappeared despite exclusive excess");

                Self::drop_expired(envelope, services);

                Duration::ZERO // try next pop immediately
            }
            Peek::Ready {
                project_key_pair, ..
            } => {
                relay_log::trace!("EnvelopeBufferService: popping envelope");
                relay_statsd::metric!(
                    counter(RelayCounters::BufferTryPop) += 1,
                    peek_result = "ready",
                    partition_id = partition_tag
                );

                Self::pop_and_forward(services, buffer, project_key_pair).await?;

                Duration::ZERO // try next pop immediately
            }
            Peek::NotReady {
                project_key_pair,
                next_project_fetch,
                last_received_at: _,
            } => {
                relay_log::trace!("EnvelopeBufferService: project(s) of envelope not ready");
                relay_statsd::metric!(
                    counter(RelayCounters::BufferTryPop) += 1,
                    peek_result = "not_ready",
                    partition_id = partition_tag
                );

                // We want to fetch the configs again, only if some time passed between the last
                // peek of this not ready project key pair and the current peek. This is done to
                // avoid flooding the project cache with `UpdateProject` messages.
                if Instant::now() >= next_project_fetch {
                    relay_log::trace!("EnvelopeBufferService: requesting project(s) update");

                    let own_key = project_key_pair.own_key();
                    let sampling_key = project_key_pair.sampling_key_unwrap();

                    services.project_cache_handle.fetch(own_key);
                    if sampling_key != own_key {
                        services.project_cache_handle.fetch(sampling_key);
                    }

                    // Deprioritize the stack to prevent head-of-line blocking and update the next fetch
                    // time.
                    buffer.mark_seen(&project_key_pair, DEFAULT_SLEEP);
                }

                DEFAULT_SLEEP // wait and prioritize handling new messages.
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

    async fn pop_and_forward<'a>(
        services: &Services,
        buffer: &mut PolymorphicEnvelopeBuffer,
        project_key_pair: ProjectKeyPair,
    ) -> Result<(), EnvelopeBufferError> {
        let mut at_least_one_pending = false;

        let own_key = project_key_pair.own_key();
        let own_project = services.project_cache_handle.get(own_key);
        if let ProjectState::Pending = own_project.state() {
            buffer.mark_ready(&own_key, false);
            services.project_cache_handle.fetch(own_key);
            at_least_one_pending = true;
        }

        let mut sampling_project = None;
        if let Some(sampling_key) = project_key_pair.sampling_key() {
            let inner_sampling_project = services.project_cache_handle.get(sampling_key);
            if let ProjectState::Pending = inner_sampling_project.state() {
                // If the sampling keys are identical, no need to perform duplicate work.
                if own_key != sampling_key {
                    buffer.mark_ready(&sampling_key, false);
                    services.project_cache_handle.fetch(sampling_key);
                }
                at_least_one_pending = true;
            }

            sampling_project = Some(inner_sampling_project);
        }

        // If we have at least one project which was pending, we don't want to pop the envelope and
        // early return.
        if at_least_one_pending {
            return Ok(());
        }

        // We know that both projects are available, so we pop the envelope.
        let envelope = buffer
            .pop()
            .await?
            .expect("Element disappeared despite exclusive excess");

        // We extract the project info for the main project of the envelope.
        let own_project_info = match own_project.state() {
            ProjectState::Enabled(info) => info,
            ProjectState::Disabled => {
                let mut managed_envelope = ManagedEnvelope::new(
                    envelope,
                    services.outcome_aggregator.clone(),
                    services.test_store.clone(),
                    ProcessingGroup::Ungrouped,
                );
                managed_envelope.reject(Outcome::Invalid(DiscardReason::ProjectId));
                return Ok(());
            }
            ProjectState::Pending => {
                unreachable!("The own project should not be pending after pop");
            }
        };

        // If we have different project keys, we want to extract the sampling project info.
        let sampling_project_info = sampling_project
            .as_ref()
            .map(|project| project.state())
            .and_then(|state| {
                match state {
                    ProjectState::Enabled(sampling_project_info) => {
                        // Only set if it matches the organization id. Otherwise, treat as if there is
                        // no sampling project.
                        (sampling_project_info.organization_id == own_project_info.organization_id)
                            .then_some(sampling_project_info)
                    }
                    ProjectState::Pending => {
                        unreachable!("The sampling project should not be pending after pop");
                    }
                    _ => {
                        // In any other case, we treat it as if there is no sampling project state.
                        None
                    }
                }
            });

        for (group, envelope) in ProcessingGroup::split_envelope(*envelope) {
            let managed_envelope = ManagedEnvelope::new(
                envelope,
                services.outcome_aggregator.clone(),
                services.test_store.clone(),
                group,
            );

            let Ok(CheckedEnvelope {
                envelope: Some(managed_envelope),
                ..
            }) = own_project.check_envelope(managed_envelope)
            else {
                continue; // Outcomes are emitted by `check_envelope`.
            };

            let reservoir_counters = own_project.reservoir_counters().clone();
            services.envelope_processor.send(ProcessEnvelope {
                envelope: managed_envelope,
                project_info: own_project_info.clone(),
                rate_limits: own_project.rate_limits().current_limits(),
                sampling_project_info: sampling_project_info.cloned(),
                reservoir_counters,
            });
        }

        Ok(())
    }

    fn update_observable_state(&self, buffer: &mut PolymorphicEnvelopeBuffer) {
        self.has_capacity
            .store(buffer.has_capacity(), Ordering::Relaxed);
    }
}

fn is_expired(last_received_at: DateTime<Utc>, config: &Config) -> bool {
    (Utc::now() - last_received_at)
        .to_std()
        .is_ok_and(|age| age > config.spool_envelopes_max_age())
}

impl Service for EnvelopeBufferService {
    type Interface = EnvelopeBuffer;

    async fn run(mut self, mut rx: Receiver<Self::Interface>) {
        let config = self.config.clone();
        let memory_checker = MemoryChecker::new(self.memory_stat.clone(), config.clone());
        let mut global_config_rx = self.global_config_rx.clone();
        let services = self.services.clone();

        let dequeue = Arc::<AtomicBool>::new(true.into());
        let dequeue1 = dequeue.clone();

        let mut buffer =
            PolymorphicEnvelopeBuffer::from_config(self.partition_id, &config, memory_checker)
                .await
                .expect("failed to start the envelope buffer service");

        buffer.initialize().await;

        // We convert the partition id to string to use it as a tag for all the metrics.
        let partition_tag = self.partition_id.to_string();

        let mut shutdown = Controller::shutdown_handle();
        let mut project_changes = self.services.project_cache_handle.changes();

        #[cfg(unix)]
        relay_system::spawn!(async move {
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

        relay_log::info!("EnvelopeBufferService {}: starting", self.partition_id);
        loop {
            let mut sleep = DEFAULT_SLEEP;
            let start = Instant::now();
            tokio::select! {
                // NOTE: we do not select a bias here.
                // On the one hand, we might want to prioritize dequeuing over enqueuing
                // so we do not exceed the buffer capacity by starving the dequeue.
                // on the other hand, prioritizing old messages violates the LIFO design.
                _ = self.ready_to_pop(&buffer, dequeue.load(Ordering::Relaxed)) => {
                    relay_statsd::metric!(timer(RelayTimers::BufferIdle) = start.elapsed(), input = "pop", partition_id = &partition_tag);
                    relay_statsd::metric!(timer(RelayTimers::BufferBusy), input = "pop", partition_id = &partition_tag, {
                        match Self::try_pop(&partition_tag, &config, &mut buffer, &services).await {
                            Ok(new_sleep) => {
                                sleep = new_sleep;
                            }
                            Err(error) => {
                                relay_log::error!(
                                error = &error as &dyn std::error::Error,
                                "failed to pop envelope"
                            );
                        }
                    }});
                }
                change = project_changes.recv() => {
                    relay_statsd::metric!(timer(RelayTimers::BufferIdle) = start.elapsed(), input = "project_change", partition_id = &partition_tag);
                    relay_statsd::metric!(timer(RelayTimers::BufferBusy), input = "project_change", partition_id = &partition_tag, {
                        match change {
                            Ok(ProjectChange::Ready(project_key)) => {
                                buffer.mark_ready(&project_key, true);
                            },
                            Ok(ProjectChange::Evicted(project_key)) => {
                                buffer.mark_ready(&project_key, false);
                            },
                            _ => {}
                        };
                        relay_statsd::metric!(counter(RelayCounters::BufferProjectChangedEvent) += 1);
                        sleep = Duration::ZERO;
                    });
                }
                Some(message) = rx.recv() => {
                    relay_statsd::metric!(timer(RelayTimers::BufferIdle) = start.elapsed(), input = "handle_message", partition_id = &partition_tag);
                    let message_name = message.name();
                    relay_statsd::metric!(timer(RelayTimers::BufferBusy), input = message_name, partition_id = &partition_tag, {
                        Self::handle_message(&mut buffer, message).await;
                        sleep = Duration::ZERO;
                    });
                }
                shutdown = shutdown.notified() => {
                    relay_statsd::metric!(timer(RelayTimers::BufferIdle) = start.elapsed(), input = "shutdown", partition_id = &partition_tag);
                    relay_statsd::metric!(timer(RelayTimers::BufferBusy), input = "shutdown", partition_id = &partition_tag, {
                        // In case the shutdown was handled, we break out of the loop signaling that
                        // there is no need to process anymore envelopes.
                        if Self::handle_shutdown(&mut buffer, shutdown).await {
                            break;
                        }
                    });
                }
                Ok(()) = global_config_rx.changed() => {
                    relay_statsd::metric!(timer(RelayTimers::BufferIdle) = start.elapsed(), input = "global_config_change", partition_id = &partition_tag);
                    sleep = Duration::ZERO;

                }
                else => break,
            }

            self.sleep = sleep;
            self.update_observable_state(&mut buffer);
        }

        relay_log::info!("EnvelopeBufferService {}: stopping", self.partition_id);
    }
}

#[cfg(test)]
mod tests {
    use crate::services::projects::project::{ProjectInfo, ProjectState};
    use crate::testutils::new_envelope;
    use crate::MemoryStat;
    use chrono::Utc;
    use relay_base_schema::project::ProjectKey;
    use relay_dynamic_config::GlobalConfig;
    use relay_quotas::DataCategory;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use super::*;

    struct EnvelopeBufferServiceResult {
        service: EnvelopeBufferService,
        global_tx: watch::Sender<global_config::Status>,
        envelope_processor_rx: mpsc::UnboundedReceiver<EnvelopeProcessor>,
        project_cache_handle: ProjectCacheHandle,
        outcome_aggregator_rx: mpsc::UnboundedReceiver<TrackOutcome>,
    }

    fn envelope_buffer_service(
        config_json: Option<serde_json::Value>,
        global_config_status: global_config::Status,
    ) -> EnvelopeBufferServiceResult {
        relay_log::init_test!();

        let config = Arc::new(
            config_json.map_or_else(Config::default, |c| Config::from_json_value(c).unwrap()),
        );

        let memory_stat = MemoryStat::default();
        let (global_tx, global_rx) = watch::channel(global_config_status);
        let (outcome_aggregator, outcome_aggregator_rx) = Addr::custom();
        let project_cache_handle = ProjectCacheHandle::for_test();
        let (envelope_processor, envelope_processor_rx) = Addr::custom();

        let envelope_buffer_service = EnvelopeBufferService::new(
            0,
            config,
            memory_stat,
            global_rx,
            Services {
                project_cache_handle: project_cache_handle.clone(),
                envelope_processor,
                outcome_aggregator,
                test_store: Addr::dummy(),
            },
        );

        EnvelopeBufferServiceResult {
            service: envelope_buffer_service,
            global_tx,
            envelope_processor_rx,
            project_cache_handle,
            outcome_aggregator_rx,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn capacity_is_updated() {
        let EnvelopeBufferServiceResult {
            service,
            global_tx: _global_tx,
            envelope_processor_rx: _envelope_processor_rx,
            project_cache_handle,
            outcome_aggregator_rx: _outcome_aggregator_rx,
        } = envelope_buffer_service(None, global_config::Status::Pending);

        service.has_capacity.store(false, Ordering::Relaxed);

        let ObservableEnvelopeBuffer { has_capacity, .. } =
            service.start_in(&mut ServiceRunner::new());
        assert!(!has_capacity.load(Ordering::Relaxed));

        tokio::time::advance(Duration::from_millis(100)).await;

        let some_project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        project_cache_handle.test_set_project_state(some_project_key, ProjectState::Disabled);

        tokio::time::advance(Duration::from_millis(100)).await;

        assert!(has_capacity.load(Ordering::Relaxed));
    }

    #[tokio::test(start_paused = true)]
    async fn pop_requires_global_config() {
        let EnvelopeBufferServiceResult {
            service,
            global_tx,
            envelope_processor_rx,
            project_cache_handle,
            outcome_aggregator_rx: _outcome_aggregator_rx,
            ..
        } = envelope_buffer_service(None, global_config::Status::Pending);

        let addr = service.start_detached();

        let envelope = new_envelope(false, "foo");
        let project_key = envelope.meta().public_key();
        let project_info = Arc::new(ProjectInfo::default());
        project_cache_handle
            .test_set_project_state(project_key, ProjectState::Enabled(project_info));
        addr.send(EnvelopeBuffer::Push(envelope.clone()));

        tokio::time::sleep(Duration::from_millis(1000)).await;

        assert_eq!(envelope_processor_rx.len(), 0);

        global_tx.send_replace(global_config::Status::Ready(Arc::new(
            GlobalConfig::default(),
        )));

        tokio::time::sleep(Duration::from_millis(1000)).await;

        assert_eq!(envelope_processor_rx.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn pop_with_pending_project() {
        let EnvelopeBufferServiceResult {
            service,
            global_tx: _global_tx,
            envelope_processor_rx,
            project_cache_handle,
            outcome_aggregator_rx: _outcome_aggregator_rx,
            ..
        } = envelope_buffer_service(
            None,
            global_config::Status::Ready(Arc::new(GlobalConfig::default())),
        );

        let addr = service.start_detached();

        let envelope = new_envelope(false, "foo");
        let project_key = envelope.meta().public_key();
        project_cache_handle.test_set_project_state(project_key, ProjectState::Pending);
        addr.send(EnvelopeBuffer::Push(envelope.clone()));

        tokio::time::sleep(Duration::from_millis(1000)).await;

        assert_eq!(envelope_processor_rx.len(), 0);

        let project_info = Arc::new(ProjectInfo::default());
        project_cache_handle
            .test_set_project_state(project_key, ProjectState::Enabled(project_info));

        tokio::time::sleep(Duration::from_millis(1000)).await;

        assert_eq!(envelope_processor_rx.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn pop_requires_memory_capacity() {
        let EnvelopeBufferServiceResult {
            service,
            envelope_processor_rx,
            project_cache_handle,
            outcome_aggregator_rx: _outcome_aggregator_rx,
            global_tx: _global_tx,
            ..
        } = envelope_buffer_service(
            Some(serde_json::json!({
                "spool": {
                    "envelopes": {
                        "path": std::env::temp_dir().join(Uuid::new_v4().to_string()),
                    }
                },
                "health": {
                    "max_memory_bytes": 0,
                }
            })),
            global_config::Status::Ready(Arc::new(GlobalConfig::default())),
        );

        let addr = service.start_detached();

        let envelope = new_envelope(false, "foo");
        let project_key = envelope.meta().public_key();
        addr.send(EnvelopeBuffer::Push(envelope.clone()));
        project_cache_handle.test_set_project_state(project_key, ProjectState::Disabled);

        tokio::time::sleep(Duration::from_millis(1000)).await;

        assert_eq!(envelope_processor_rx.len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn old_envelope_is_dropped() {
        let EnvelopeBufferServiceResult {
            service,
            envelope_processor_rx,
            project_cache_handle: _project_cache_handle,
            mut outcome_aggregator_rx,
            global_tx: _global_tx,
        } = envelope_buffer_service(
            Some(serde_json::json!({
                "spool": {
                    "envelopes": {
                        "max_envelope_delay_secs": 1,
                    }
                }
            })),
            global_config::Status::Ready(Arc::new(GlobalConfig::default())),
        );

        let config = service.config.clone();
        let addr = service.start_detached();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut envelope = new_envelope(false, "foo");
        envelope.meta_mut().set_received_at(
            Utc::now()
                - chrono::Duration::seconds(2 * config.spool_envelopes_max_age().as_secs() as i64),
        );
        addr.send(EnvelopeBuffer::Push(envelope));

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(envelope_processor_rx.len(), 0);

        let outcome = outcome_aggregator_rx.try_recv().unwrap();
        assert_eq!(outcome.category, DataCategory::TransactionIndexed);
        assert_eq!(outcome.quantity, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_partitioned_buffer() {
        let mut runner = ServiceRunner::new();
        let (_global_tx, global_rx) = watch::channel(global_config::Status::Ready(Arc::new(
            GlobalConfig::default(),
        )));
        let (outcome_aggregator, _outcome_rx) = Addr::custom();
        let project_cache_handle = ProjectCacheHandle::for_test();
        let (envelope_processor, mut envelope_processor_rx) = Addr::custom();

        // Create common services for both buffers
        let services = Services {
            envelope_processor,
            project_cache_handle: project_cache_handle.clone(),
            outcome_aggregator,
            test_store: Addr::dummy(),
        };

        // Create two buffer services
        let config = Arc::new(Config::default());

        let buffer1 = EnvelopeBufferService::new(
            0,
            config.clone(),
            MemoryStat::default(),
            global_rx.clone(),
            services.clone(),
        );

        let buffer2 = EnvelopeBufferService::new(
            1,
            config.clone(),
            MemoryStat::default(),
            global_rx,
            services,
        );

        // Start both services and create partitioned buffer
        let observable1 = buffer1.start_in(&mut runner);
        let observable2 = buffer2.start_in(&mut runner);

        let partitioned = PartitionedEnvelopeBuffer {
            buffers: Arc::new(vec![observable1, observable2]),
        };

        // Create two envelopes with different project keys
        let envelope1 = new_envelope(false, "foo");
        let project_key = envelope1.meta().public_key();
        let project_info = Arc::new(ProjectInfo::default());
        project_cache_handle
            .test_set_project_state(project_key, ProjectState::Enabled(project_info));

        let envelope2 = new_envelope(false, "bar");
        let project_key = envelope2.meta().public_key();
        let project_info = Arc::new(ProjectInfo::default());
        project_cache_handle
            .test_set_project_state(project_key, ProjectState::Enabled(project_info));

        // Send envelopes to their respective buffers
        let buffer1 = &partitioned.buffers[0];
        let buffer2 = &partitioned.buffers[1];

        buffer1.addr().send(EnvelopeBuffer::Push(envelope1));
        buffer2.addr().send(EnvelopeBuffer::Push(envelope2));

        // Verify both envelopes were received
        assert!(envelope_processor_rx.recv().await.is_some());
        assert!(envelope_processor_rx.recv().await.is_some());
        assert!(envelope_processor_rx.is_empty());
    }
}
