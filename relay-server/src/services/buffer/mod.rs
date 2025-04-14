//! Types for buffering envelopes.

use std::error::Error;
use std::num::NonZeroU8;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;

use ahash::RandomState;
use chrono::DateTime;
use chrono::Utc;
pub use common::ProjectKeyPair;
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
use relay_config::Config;
use relay_system::Receiver;
use relay_system::ServiceSpawn;
use relay_system::ServiceSpawnExt as _;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Service};
use relay_system::{Controller, Shutdown};
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
use crate::services::projects::project::ProjectState;
use crate::services::test_store::TestStore;
use crate::statsd::RelayCounters;
use crate::utils::ManagedEnvelope;
use crate::MemoryChecker;
use crate::MemoryStat;

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
    hasher: RandomState,
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
        services: &dyn ServiceSpawn,
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
            .start_in(services);

            envelope_buffers.push(envelope_buffer);
        }

        Self {
            buffers: Arc::new(envelope_buffers),
            hasher: Self::build_hasher(),
        }
    }

    /// Returns the [`ObservableEnvelopeBuffer`] to which [`Envelope`]s having the supplied
    /// [`ProjectKeyPair`] will be sent.
    ///
    /// The rationale of using this partitioning strategy is to reduce memory usage across buffers
    /// since each individual buffer will only take care of a subset of projects.
    pub fn buffer(&self, project_key_pair: ProjectKeyPair) -> &ObservableEnvelopeBuffer {
        let buffer_index =
            (self.hasher.hash_one(project_key_pair) % self.buffers.len() as u64) as usize;
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

    pub fn item_count(&self) -> u64 {
        self.buffers.iter().map(|buffer| buffer.item_count()).sum()
    }

    pub fn total_storage_size(&self) -> u64 {
        self.buffers
            .iter()
            .map(|buffer| buffer.storage_size())
            .sum()
    }

    /// Builds a hasher with fixed seeds for consistent partitioning across Relay instances.
    fn build_hasher() -> RandomState {
        const K0: u64 = 0xd34db33f11223344;
        const K1: u64 = 0xc0ffee0987654321;
        const K2: u64 = 0xdeadbeef55667788;
        const K3: u64 = 0xbadc0de901234567;

        RandomState::with_seeds(K0, K1, K2, K3)
    }
}

#[derive(Debug)]
pub struct EnvelopeBufferMetrics {
    has_capacity: AtomicBool,
    item_count: AtomicU64,
    storage_size: AtomicU64,
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
    metrics: Arc<EnvelopeBufferMetrics>,
}

impl ObservableEnvelopeBuffer {
    /// Returns the address of the buffer service.
    pub fn addr(&self) -> Addr<EnvelopeBuffer> {
        self.addr.clone()
    }

    /// Returns `true` if the buffer has the capacity to accept more elements.
    pub fn has_capacity(&self) -> bool {
        self.metrics.has_capacity.load(Ordering::Relaxed)
    }

    pub fn item_count(&self) -> u64 {
        self.metrics.item_count.load(Ordering::Relaxed)
    }

    pub fn storage_size(&self) -> u64 {
        self.metrics.storage_size.load(Ordering::Relaxed)
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
    metrics: Arc<EnvelopeBufferMetrics>,
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
            metrics: Arc::new(EnvelopeBufferMetrics {
                has_capacity: AtomicBool::new(true),
                item_count: AtomicU64::new(0),
                storage_size: AtomicU64::new(0),
            }),
            sleep: Duration::ZERO,
        }
    }

    /// Returns both the [`Addr`] to this service, and references to spooler metrics.
    pub fn start_in(self, services: &dyn ServiceSpawn) -> ObservableEnvelopeBuffer {
        let metrics = self.metrics.clone();

        let addr = services.start(self);

        ObservableEnvelopeBuffer { addr, metrics }
    }

    /// Wait for the configured amount of time and make sure the project cache is ready to receive.
    async fn ready_to_pop(&mut self, buffer: &PolymorphicEnvelopeBuffer, dequeue: bool) {
        self.system_ready(buffer, dequeue).await;

        if self.sleep > Duration::ZERO {
            tokio::time::sleep(self.sleep).await;
        }
    }

    /// Waits until the system is ready to unspool envelopes.
    ///
    /// This function ensures that unspooling only happens when it's safe and appropriate to do so.
    /// It continuously checks specific system conditions and only returns when they are all satisfied.
    ///
    /// # Preconditions for Unspooling
    ///
    /// 1. **Memory Availability**:
    ///    - If the system is using **disk spooling**, we must avoid unspooling when
    ///      memory usage has reached its configured capacity.
    ///      This prevents overloading memory and ensures that downstream services can process
    ///      in-flight envelopes at their own pace.
    ///    - If the system is using **memory spooling only** (i.e., no disk),
    ///      we *must not* block unspooling, even when memory has reached its configured capacity.
    ///      Blocking in this case would lead to a **deadlock**, since new envelopes
    ///      would continue accumulating in the buffer's memory and will never be unspooled and
    ///      processed downstream.
    ///
    /// 2. **Global Configuration Availability**:
    ///    - Unspooling requires a valid, ready-to-use global configuration.
    ///      If the configuration is not yet initialized or ready, unspooling must wait.
    async fn system_ready(&self, buffer: &PolymorphicEnvelopeBuffer, dequeue: bool) {
        loop {
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
    async fn try_pop(
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
                relay_log::trace!("EnvelopeBufferService: project(s) of envelope ready");
                relay_statsd::metric!(
                    counter(RelayCounters::BufferTryPop) += 1,
                    peek_result = "ready",
                    partition_id = partition_tag
                );

                Self::pop_and_forward(partition_tag, services, buffer, project_key_pair).await?;

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

                    let own_key = project_key_pair.own_key;
                    let sampling_key = project_key_pair.sampling_key;

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

    async fn pop_and_forward(
        partition_tag: &str,
        services: &Services,
        buffer: &mut PolymorphicEnvelopeBuffer,
        project_key_pair: ProjectKeyPair,
    ) -> Result<(), EnvelopeBufferError> {
        let own_key = project_key_pair.own_key;
        let own_project = services.project_cache_handle.get(own_key);
        // We try to load the own project state and bail in case it's pending.
        let own_project_info = match own_project.state() {
            ProjectState::Enabled(info) => Some(info.clone()),
            ProjectState::Disabled => None,
            ProjectState::Pending => {
                buffer.mark_ready(&own_key, false);
                relay_statsd::metric!(
                    counter(RelayCounters::BufferProjectPending) += 1,
                    partition_id = &partition_tag
                );

                return Ok(());
            }
        };

        let sampling_key = project_key_pair.sampling_key;
        // If the projects are different, we load the project key of the sampling project. On the
        // other hand, if they are the same, we just reuse the own project.
        let sampling_project_info = if project_key_pair.has_distinct_sampling_key() {
            // We try to load the sampling project state and bail in case it's pending.
            match services.project_cache_handle.get(sampling_key).state() {
                ProjectState::Enabled(info) => Some(info.clone()),
                ProjectState::Disabled => None,
                ProjectState::Pending => {
                    buffer.mark_ready(&sampling_key, false);
                    relay_statsd::metric!(
                        counter(RelayCounters::BufferProjectPending) += 1,
                        partition_id = &partition_tag
                    );

                    return Ok(());
                }
            }
        } else {
            own_project_info.clone()
        };

        relay_log::trace!("EnvelopeBufferService: popping envelope");

        // If we arrived here, know that both projects are available, so we pop the envelope.
        let envelope = buffer
            .pop()
            .await?
            .expect("Element disappeared despite exclusive excess");

        // If the own project state is disabled, we want to drop the envelope and early return since
        // we can't do much about it.
        let Some(own_project_info) = own_project_info else {
            let mut managed_envelope = ManagedEnvelope::new(
                envelope,
                services.outcome_aggregator.clone(),
                services.test_store.clone(),
                ProcessingGroup::Ungrouped,
            );
            managed_envelope.reject(Outcome::Invalid(DiscardReason::ProjectId));

            return Ok(());
        };

        // We only extract the sampling project info if both projects belong to the same org.
        let sampling_project_info = sampling_project_info
            .filter(|info| info.organization_id == own_project_info.organization_id);

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
            }) = own_project.check_envelope(managed_envelope).await
            else {
                continue; // Outcomes are emitted by `check_envelope`.
            };

            let reservoir_counters = own_project.reservoir_counters().clone();
            services.envelope_processor.send(ProcessEnvelope {
                envelope: managed_envelope,
                project_info: own_project_info.clone(),
                rate_limits: own_project.rate_limits().current_limits(),
                sampling_project_info: sampling_project_info.clone(),
                reservoir_counters,
            });
        }

        Ok(())
    }

    fn update_observable_state(&self, buffer: &mut PolymorphicEnvelopeBuffer) {
        self.metrics
            .has_capacity
            .store(buffer.has_capacity(), Ordering::Relaxed);
        self.metrics
            .storage_size
            .store(buffer.total_size().unwrap_or(0), Ordering::Relaxed);
        self.metrics
            .item_count
            .store(buffer.item_count(), Ordering::Relaxed);
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
        {
            let dequeue1 = dequeue.clone();
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
        }

        relay_log::info!("EnvelopeBufferService {}: starting", self.partition_id);
        loop {
            let mut sleep = DEFAULT_SLEEP;

            tokio::select! {
                // NOTE: we do not select a bias here.
                // On the one hand, we might want to prioritize dequeuing over enqueuing
                // so we do not exceed the buffer capacity by starving the dequeue.
                // on the other hand, prioritizing old messages violates the LIFO design.
                _ = self.ready_to_pop(&buffer, dequeue.load(Ordering::Relaxed)) => {
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
                    }
                }
                change = project_changes.recv() => {
                    match change {
                            Ok(ProjectChange::Ready(project_key)) => {
                                buffer.mark_ready(&project_key, true);
                            },
                            Ok(ProjectChange::Evicted(project_key)) => {
                                buffer.mark_ready(&project_key, false);
                            },
                            _ => {}
                        };
                        relay_statsd::metric!(counter(RelayCounters::BufferProjectChangedEvent) += 1, partition_id = &partition_tag);
                        sleep = Duration::ZERO;
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

        relay_log::info!("EnvelopeBufferService {}: stopping", self.partition_id);
    }
}

/// The spooler uses internal time based mechanics and to not make the tests actually wait
/// it's good to use `#[tokio::test(start_paused = true)]`. For memory based spooling, this will
/// just work.
///
/// However, testing the sqlite spooler will not behave correctly when using `start_paused`
/// because the sqlite pool uses the timeout provided by tokio for connection establishing but
/// the work that happens during connection will run outside of tokio in its own threadpool.
/// During connection the tokio runtime will have no work, triggering the [auto advance](https://docs.rs/tokio/latest/tokio/time/fn.pause.html#auto-advance)
/// feature of the runtime, which causes the timeout to resolve immediately, preventing
/// the connection to be established (`SqliteStore(SqlxSetupFailed(PoolTimedOut))`).
///
/// To test sqlite based spooling it is necessary to manually pause the time using
/// `tokio::time::pause` *after* the connection is established.
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::Utc;
    use relay_base_schema::project::ProjectKey;
    use relay_dynamic_config::GlobalConfig;
    use relay_quotas::DataCategory;
    use relay_system::TokioServiceSpawn;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use super::*;
    use crate::services::projects::project::{ProjectInfo, ProjectState};
    use crate::testutils::new_envelope;
    use crate::MemoryStat;

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

        service.metrics.has_capacity.store(false, Ordering::Relaxed);

        let ObservableEnvelopeBuffer { metrics, .. } = service.start_in(&TokioServiceSpawn);
        assert!(!metrics.has_capacity.load(Ordering::Relaxed));

        tokio::time::advance(Duration::from_millis(100)).await;

        let some_project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        project_cache_handle.test_set_project_state(some_project_key, ProjectState::Disabled);

        tokio::time::advance(Duration::from_millis(100)).await;

        assert!(metrics.has_capacity.load(Ordering::Relaxed));
    }

    #[tokio::test(start_paused = true)]
    async fn pop_with_global_config_changes() {
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
    async fn pop_with_project_state_changes() {
        let EnvelopeBufferServiceResult {
            service,
            global_tx: _global_tx,
            mut envelope_processor_rx,
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
        assert!(envelope_processor_rx.recv().await.is_some());

        let envelope = new_envelope(false, "foo");
        let project_key = envelope.meta().public_key();
        project_cache_handle.test_set_project_state(project_key, ProjectState::Disabled);
        addr.send(EnvelopeBuffer::Push(envelope.clone()));

        tokio::time::sleep(Duration::from_millis(1000)).await;

        assert_eq!(envelope_processor_rx.len(), 0);
    }

    #[tokio::test]
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
        tokio::time::sleep(Duration::from_millis(50)).await;
        tokio::time::pause();

        let envelope = new_envelope(false, "foo");
        let project_key = envelope.meta().public_key();
        addr.send(EnvelopeBuffer::Push(envelope.clone()));
        project_cache_handle.test_set_project_state(project_key, ProjectState::Disabled);

        tokio::time::sleep(Duration::from_millis(1000)).await;

        assert_eq!(envelope_processor_rx.len(), 0);
    }

    #[tokio::test]
    async fn test_sqlite_metrics() {
        let EnvelopeBufferServiceResult {
            service,
            envelope_processor_rx: _envelope_processor_rx,
            project_cache_handle: _project_cache_handle,
            outcome_aggregator_rx: _outcome_aggregator_rx,
            global_tx: _global_tx,
        } = envelope_buffer_service(
            Some(serde_json::json!({
                "spool": {
                    "envelopes": {
                        "path": std::env::temp_dir().join(Uuid::new_v4().to_string()),
                    }
                }
            })),
            global_config::Status::Pending,
        );

        let addr = service.start_in(&TokioServiceSpawn);
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(addr.metrics.item_count.load(Ordering::Relaxed), 0);

        for _ in 0..10 {
            let envelope = new_envelope(false, "foo");
            addr.addr().send(EnvelopeBuffer::Push(envelope.clone()));
        }

        tokio::time::sleep(Duration::from_millis(1100)).await;

        assert_eq!(addr.metrics.item_count.load(Ordering::Relaxed), 10);
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
        let observable1 = buffer1.start_in(&TokioServiceSpawn);
        let observable2 = buffer2.start_in(&TokioServiceSpawn);

        let partitioned = PartitionedEnvelopeBuffer {
            buffers: Arc::new(vec![observable1, observable2]),
            hasher: PartitionedEnvelopeBuffer::build_hasher(),
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
