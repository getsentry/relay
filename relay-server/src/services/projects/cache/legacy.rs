use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use crate::services::buffer::{EnvelopeBuffer, EnvelopeBufferError};
use crate::services::global_config;
use crate::services::processor::{
    EncodeMetrics, EnvelopeProcessor, ProcessEnvelope, ProcessingGroup, ProjectMetrics,
};
use crate::services::projects::cache::{CheckedEnvelope, ProjectCacheHandle, ProjectChange};
use crate::Envelope;
use hashbrown::HashSet;
use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, Interface, Service};
use tokio::sync::{mpsc, watch};

use crate::services::metrics::{Aggregator, FlushBuckets, MergeBuckets};
use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::services::projects::project::ProjectState;
use crate::services::spooler::{
    self, Buffer, BufferService, DequeueMany, Enqueue, QueueKey, RemoveMany, RestoreIndex,
    UnspooledEnvelope, BATCH_KEY_COUNT,
};
use crate::services::test_store::TestStore;

use crate::statsd::{RelayCounters, RelayGauges, RelayTimers};
use crate::utils::{ManagedEnvelope, MemoryChecker, RetryBackoff, SleepHandle};

/// Validates the envelope against project configuration and rate limits.
///
/// This ensures internally that the project state is up to date .
/// Once the envelope has been validated, remaining items are forwarded to the next stage:
///
///  - If the envelope needs dynamic sampling, and the project state is not cached or out of the
///    date, the envelopes is spooled and we continue when the state is fetched.
///  - Otherwise, the envelope is directly submitted to the [`EnvelopeProcessor`].
#[derive(Debug)]
pub struct ValidateEnvelope {
    envelope: ManagedEnvelope,
}

impl ValidateEnvelope {
    pub fn new(envelope: ManagedEnvelope) -> Self {
        Self { envelope }
    }
}

/// Updates the buffer index for [`ProjectKey`] with the [`QueueKey`] keys.
///
/// This message is sent from the project buffer in case of the error while fetching the data from
/// the persistent buffer, ensuring that we still have the index pointing to the keys, which could be found in the
/// persistent storage.
#[derive(Debug)]
pub struct UpdateSpoolIndex(pub HashSet<QueueKey>);

impl UpdateSpoolIndex {
    pub fn new(keys: HashSet<QueueKey>) -> Self {
        Self(keys)
    }
}

/// The current envelopes index fetched from the underlying buffer spool.
///
/// This index will be received only once shortly after startup and will trigger refresh for the
/// project states for the project keys returned in the message.
#[derive(Debug)]
pub struct RefreshIndexCache(pub HashSet<QueueKey>);

/// Handle an envelope that was popped from the envelope buffer.
#[derive(Debug)]
pub struct DequeuedEnvelope(pub Box<Envelope>);

/// The legacy project cache.
///
/// It manages spool v1 and some remaining messages which handle project state.
#[derive(Debug)]
pub enum ProjectCache {
    ValidateEnvelope(ValidateEnvelope),
    FlushBuckets(FlushBuckets),
    UpdateSpoolIndex(UpdateSpoolIndex),
    RefreshIndexCache(RefreshIndexCache),
}

impl ProjectCache {
    pub fn variant(&self) -> &'static str {
        match self {
            Self::ValidateEnvelope(_) => "ValidateEnvelope",
            Self::FlushBuckets(_) => "FlushBuckets",
            Self::UpdateSpoolIndex(_) => "UpdateSpoolIndex",
            Self::RefreshIndexCache(_) => "RefreshIndexCache",
        }
    }
}

impl Interface for ProjectCache {}

impl FromMessage<UpdateSpoolIndex> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: UpdateSpoolIndex, _: ()) -> Self {
        Self::UpdateSpoolIndex(message)
    }
}

impl FromMessage<RefreshIndexCache> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: RefreshIndexCache, _: ()) -> Self {
        Self::RefreshIndexCache(message)
    }
}

impl FromMessage<ValidateEnvelope> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: ValidateEnvelope, _: ()) -> Self {
        Self::ValidateEnvelope(message)
    }
}

impl FromMessage<FlushBuckets> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: FlushBuckets, _: ()) -> Self {
        Self::FlushBuckets(message)
    }
}

/// Holds the addresses of all services required for [`ProjectCache`].
#[derive(Debug, Clone)]
pub struct Services {
    pub envelope_buffer: Option<Addr<EnvelopeBuffer>>,
    pub aggregator: Addr<Aggregator>,
    pub envelope_processor: Addr<EnvelopeProcessor>,
    pub outcome_aggregator: Addr<TrackOutcome>,
    pub project_cache: Addr<ProjectCache>,
    pub test_store: Addr<TestStore>,
}

/// Main broker of the [`ProjectCacheService`].
///
/// This handles incoming public messages, merges resolved project states, and maintains the actual
/// cache of project states.
#[derive(Debug)]
struct ProjectCacheBroker {
    config: Arc<Config>,
    memory_checker: MemoryChecker,
    services: Services,
    projects: ProjectCacheHandle,

    /// Handle to schedule periodic unspooling of buffered envelopes (spool V1).
    spool_v1_unspool_handle: SleepHandle,
    spool_v1: Option<SpoolV1>,
    /// Status of the global configuration, used to determine readiness for processing.
    global_config: GlobalConfigStatus,
}

#[derive(Debug)]
struct SpoolV1 {
    /// Tx channel used by the [`BufferService`] to send back the requested dequeued elements.
    buffer_tx: mpsc::UnboundedSender<UnspooledEnvelope>,
    /// Index containing all the [`QueueKey`] that have been enqueued in the [`BufferService`].
    index: HashSet<QueueKey>,
    /// Backoff strategy for retrying unspool attempts.
    buffer_unspool_backoff: RetryBackoff,
    /// Address of the [`BufferService`] used for enqueuing and dequeuing envelopes that can't be
    /// immediately processed.
    buffer: Addr<Buffer>,
}

/// Describes the current status of the `GlobalConfig`.
///
/// Either it's ready to be used, or it contains the list of in-flight project keys,
/// to be processed once the config arrives.
#[derive(Debug)]
enum GlobalConfigStatus {
    /// Global config needed for envelope processing.
    Ready,
    /// The global config is not fetched yet.
    Pending,
}

impl GlobalConfigStatus {
    fn is_ready(&self) -> bool {
        matches!(self, GlobalConfigStatus::Ready)
    }
}

impl ProjectCacheBroker {
    fn set_global_config_ready(&mut self) {
        self.global_config = GlobalConfigStatus::Ready;
    }

    /// Adds the value to the queue for the provided key.
    fn enqueue(&mut self, key: QueueKey, value: ManagedEnvelope) {
        let spool_v1 = self.spool_v1.as_mut().expect("no V1 spool configured");
        spool_v1.index.insert(key);
        spool_v1.buffer.send(Enqueue::new(key, value));
    }

    /// Sends the message to the buffer service to dequeue the envelopes.
    ///
    /// All the found envelopes will be send back through the `buffer_tx` channel and directly
    /// forwarded to `handle_processing`.
    fn dequeue(&self, keys: HashSet<QueueKey>) {
        let spool_v1 = self.spool_v1.as_ref().expect("no V1 spool configured");
        spool_v1
            .buffer
            .send(DequeueMany::new(keys, spool_v1.buffer_tx.clone()))
    }

    fn evict_project(&mut self, project_key: ProjectKey) {
        let Some(ref mut spool_v1) = self.spool_v1 else {
            return;
        };

        let keys = spool_v1
            .index
            .extract_if(|key| key.own_key == project_key || key.sampling_key == project_key)
            .collect::<BTreeSet<_>>();

        if !keys.is_empty() {
            spool_v1.buffer.send(RemoveMany::new(project_key, keys))
        }
    }

    fn handle_project_change(&mut self, event: ProjectChange) {
        match event {
            ProjectChange::Ready(_) => self.schedule_unspool(),
            ProjectChange::Evicted(project_key) => self.evict_project(project_key),
        }
    }

    /// Handles the processing of the provided envelope.
    fn handle_processing(&mut self, mut managed_envelope: ManagedEnvelope) {
        let project_key = managed_envelope.envelope().meta().public_key();

        let project = self.projects.get(project_key);

        let project_info = match project.state() {
            ProjectState::Enabled(info) => info,
            ProjectState::Disabled => {
                managed_envelope.reject(Outcome::Invalid(DiscardReason::ProjectId));
                return;
            }
            ProjectState::Pending => {
                relay_log::error!(
                    tags.project_key = %project_key,
                    "project has no valid cached state",
                );
                return;
            }
        };

        // The `Envelope` and `EnvelopeContext` will be dropped if the `Project::check_envelope()`
        // function returns any error, which will also be ignored here.
        // TODO(jjbayer): check_envelope also makes sure the envelope has proper scoping.
        // If we don't call check_envelope in the same message handler as handle_processing,
        // there is a chance that the project is not ready yet and events are published with
        // `organization_id: 0`. We should eliminate this footgun by introducing a `ScopedEnvelope`
        // type which guarantees that the envelope has complete scoping.
        if let Ok(CheckedEnvelope {
            envelope: Some(managed_envelope),
            ..
        }) = project.check_envelope(managed_envelope)
        {
            let rate_limits = project.rate_limits().current_limits();
            let reservoir_counters = project.reservoir_counters().clone();

            let sampling_project_info = managed_envelope
                .envelope()
                .sampling_key()
                .map(|key| self.projects.get(key))
                .and_then(|p| p.state().clone().enabled())
                .filter(|info| info.organization_id == project_info.organization_id);

            let process = ProcessEnvelope {
                envelope: managed_envelope,
                project_info: Arc::clone(project_info),
                rate_limits,
                sampling_project_info,
                reservoir_counters,
            };

            self.services.envelope_processor.send(process);
        }
    }

    /// Checks an incoming envelope and decides either process it immediately or buffer it.
    ///
    /// Few conditions are checked here:
    /// - If there is no dynamic sampling key and the project is already cached, we do straight to
    ///   processing otherwise buffer the envelopes.
    /// - If the dynamic sampling key is provided and if the root and sampling projects
    ///   are cached - process the envelope, buffer otherwise.
    ///
    /// This means if the caches are hot we always process all the incoming envelopes without any
    /// delay. But in case the project state cannot be fetched, we keep buffering till the state
    /// is eventually updated.
    ///
    /// The flushing of the buffered envelopes happens in `update_state`.
    fn handle_validate_envelope(&mut self, message: ValidateEnvelope) {
        let ValidateEnvelope {
            envelope: mut managed_envelope,
        } = message;

        let envelope = managed_envelope.envelope();

        // Fetch the project state for our key and make sure it's not invalid.
        let own_key = envelope.meta().public_key();
        let project = self.projects.get(own_key);

        let project_state = match project.state() {
            ProjectState::Enabled(state) => Some(state),
            ProjectState::Disabled => {
                managed_envelope.reject(Outcome::Invalid(DiscardReason::ProjectId));
                return;
            }
            ProjectState::Pending => None,
        };

        // Also, fetch the project state for sampling key and make sure it's not invalid.
        let sampling_key = envelope.sampling_key();
        let mut requires_sampling_state = sampling_key.is_some();
        let sampling_info = if let Some(sampling_key) = sampling_key {
            let sampling_project = self.projects.get(sampling_key);
            match sampling_project.state() {
                ProjectState::Enabled(info) => Some(Arc::clone(info)),
                ProjectState::Disabled => {
                    relay_log::trace!("Sampling state is disabled ({sampling_key})");
                    // We accept events even if its root project has been disabled.
                    requires_sampling_state = false;
                    None
                }
                ProjectState::Pending => {
                    relay_log::trace!("Sampling state is pending ({sampling_key})");
                    None
                }
            }
        } else {
            None
        };

        let key = QueueKey::new(own_key, sampling_key.unwrap_or(own_key));

        // Trigger processing once we have a project state and we either have a sampling project
        // state or we do not need one.
        if project_state.is_some()
            && (sampling_info.is_some() || !requires_sampling_state)
            && self.memory_checker.check_memory().has_capacity()
            && self.global_config.is_ready()
        {
            // TODO: Add ready project infos to the processing message.
            relay_log::trace!("Sending envelope to processor");
            return self.handle_processing(managed_envelope);
        }

        relay_log::trace!("Enqueueing envelope");
        self.enqueue(key, managed_envelope);
    }

    fn handle_flush_buckets(&mut self, message: FlushBuckets) {
        let aggregator = self.services.aggregator.clone();

        let mut no_project = 0;
        let mut scoped_buckets = BTreeMap::new();
        for (project_key, buckets) in message.buckets {
            let project = self.projects.get(project_key);

            let project_info = match project.state() {
                ProjectState::Pending => {
                    no_project += 1;

                    // Return the buckets to the aggregator.
                    aggregator.send(MergeBuckets::new(project_key, buckets));
                    continue;
                }
                ProjectState::Disabled => {
                    // Project loaded and disabled, discard the buckets.
                    //
                    // Ideally we log outcomes for the metrics here, but currently for metric
                    // outcomes we need a valid scoping, which we cannot construct for disabled
                    // projects.
                    continue;
                }
                ProjectState::Enabled(project_info) => project_info,
            };

            let Some(scoping) = project_info.scoping(project_key) else {
                relay_log::error!(
                    tags.project_key = project_key.as_str(),
                    "there is no scoping: dropping {} buckets",
                    buckets.len(),
                );
                continue;
            };

            use std::collections::btree_map::Entry::*;
            match scoped_buckets.entry(scoping) {
                Vacant(entry) => {
                    entry.insert(ProjectMetrics {
                        project_info: Arc::clone(project_info),
                        rate_limits: project.rate_limits().current_limits(),
                        buckets,
                    });
                }
                Occupied(entry) => {
                    entry.into_mut().buckets.extend(buckets);
                }
            }
        }

        self.services.envelope_processor.send(EncodeMetrics {
            partition_key: message.partition_key,
            scopes: scoped_buckets,
        });

        relay_statsd::metric!(
            counter(RelayCounters::ProjectStateFlushMetricsNoProject) += no_project
        );
    }

    fn handle_buffer_index(&mut self, message: UpdateSpoolIndex) {
        let spool_v1 = self.spool_v1.as_mut().expect("no V1 spool configured");
        spool_v1.index.extend(message.0);
    }

    fn handle_refresh_index_cache(&mut self, message: RefreshIndexCache) {
        let RefreshIndexCache(index) = message;

        for key in index {
            let spool_v1 = self.spool_v1.as_mut().expect("no V1 spool configured");
            spool_v1.index.insert(key);

            self.projects.fetch(key.own_key);
            if key.own_key != key.sampling_key {
                self.projects.fetch(key.sampling_key);
            }
        }
    }

    fn handle_dequeued_envelope(
        &mut self,
        envelope: Box<Envelope>,
        envelope_buffer: Addr<EnvelopeBuffer>,
    ) -> Result<(), EnvelopeBufferError> {
        let sampling_key = envelope.sampling_key();
        let services = self.services.clone();

        let own_key = envelope.meta().public_key();
        let project = self.projects.get(own_key);

        // Check if project config is enabled.
        let project_info = match project.state() {
            ProjectState::Enabled(info) => info,
            ProjectState::Disabled => {
                let mut managed_envelope = ManagedEnvelope::new(
                    envelope,
                    self.services.outcome_aggregator.clone(),
                    self.services.test_store.clone(),
                    ProcessingGroup::Ungrouped,
                );
                managed_envelope.reject(Outcome::Invalid(DiscardReason::ProjectId));
                return Ok(());
            }
            ProjectState::Pending => {
                envelope_buffer.send(EnvelopeBuffer::NotReady(own_key, envelope));
                return Ok(());
            }
        };

        // Check if sampling config is enabled.
        let sampling_project_info = match sampling_key.map(|sampling_key| {
            (
                sampling_key,
                self.projects.get(sampling_key).state().clone(),
            )
        }) {
            Some((_, ProjectState::Enabled(info))) => {
                // Only set if it matches the organization ID. Otherwise treat as if there is
                // no sampling project.
                (info.organization_id == project_info.organization_id).then_some(info)
            }
            Some((_, ProjectState::Disabled)) => {
                // Accept envelope even if its sampling state is disabled:
                None
            }
            Some((sampling_key, ProjectState::Pending)) => {
                envelope_buffer.send(EnvelopeBuffer::NotReady(sampling_key, envelope));
                return Ok(());
            }
            None => None,
        };

        // Reassign processing groups and proceed to processing.
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
            }) = project.check_envelope(managed_envelope)
            else {
                continue; // Outcomes are emitted by check_envelope
            };

            let reservoir_counters = project.reservoir_counters().clone();
            services.envelope_processor.send(ProcessEnvelope {
                envelope: managed_envelope,
                project_info: project_info.clone(),
                rate_limits: project.rate_limits().current_limits(),
                sampling_project_info: sampling_project_info.clone(),
                reservoir_counters,
            });
        }

        Ok(())
    }

    /// Returns backoff timeout for an unspool attempt.
    fn next_unspool_attempt(&mut self) -> Duration {
        let spool_v1 = self.spool_v1.as_mut().expect("no V1 spool configured");
        self.config.spool_envelopes_unspool_interval()
            + spool_v1.buffer_unspool_backoff.next_backoff()
    }

    fn schedule_unspool(&mut self) {
        if self.spool_v1.is_none() {
            return;
        }

        if self.spool_v1_unspool_handle.is_idle() {
            // Set the time for the next attempt.
            let wait = self.next_unspool_attempt();
            self.spool_v1_unspool_handle.set(wait);
        }
    }

    /// Returns `true` if the project state valid for the [`QueueKey`].
    ///
    /// Which includes the own key and the sampling key for the project.
    /// Note: this function will trigger [`ProjectState`] refresh if it's already expired.
    fn is_state_cached(&mut self, key: &QueueKey) -> bool {
        key.unique_keys()
            .iter()
            .all(|key| !self.projects.get(*key).state().is_pending())
    }

    /// Iterates the buffer index and tries to unspool the envelopes for projects with a valid
    /// state.
    ///
    /// This makes sure we always moving the unspool forward, even if we do not fetch the project
    /// states updates, but still can process data based on the existing cache.
    fn handle_periodic_unspool(&mut self) {
        relay_log::trace!("handle_periodic_unspool");
        let (num_keys, reason) = self.handle_periodic_unspool_inner();
        relay_statsd::metric!(
            gauge(RelayGauges::BufferPeriodicUnspool) = num_keys as u64,
            reason = reason
        );
    }

    fn handle_periodic_unspool_inner(&mut self) -> (usize, &str) {
        let spool_v1 = self.spool_v1.as_mut().expect("no V1 spool configured");
        self.spool_v1_unspool_handle.reset();

        // If we don't yet have the global config, we will defer dequeuing until we do.
        if let GlobalConfigStatus::Pending = self.global_config {
            spool_v1.buffer_unspool_backoff.reset();
            self.schedule_unspool();
            return (0, "no_global_config");
        }
        // If there is nothing spooled, schedule the next check a little bit later.
        if spool_v1.index.is_empty() {
            self.schedule_unspool();
            return (0, "index_empty");
        }

        let mut index = std::mem::take(&mut spool_v1.index);
        let keys = index
            .extract_if(|key| self.is_state_cached(key))
            .take(BATCH_KEY_COUNT)
            .collect::<HashSet<_>>();
        let num_keys = keys.len();

        if !keys.is_empty() {
            self.dequeue(keys);
        }

        // Return all the un-used items to the index.
        let spool_v1 = self.spool_v1.as_mut().expect("no V1 spool configured");
        if !index.is_empty() {
            spool_v1.index.extend(index);
        }

        // Schedule unspool once we are done.
        spool_v1.buffer_unspool_backoff.reset();
        self.schedule_unspool();

        (num_keys, "found_keys")
    }

    fn handle_message(&mut self, message: ProjectCache) {
        let ty = message.variant();
        metric!(
            timer(RelayTimers::LegacyProjectCacheMessageDuration),
            message = ty,
            {
                match message {
                    ProjectCache::ValidateEnvelope(message) => {
                        self.handle_validate_envelope(message)
                    }
                    ProjectCache::FlushBuckets(message) => self.handle_flush_buckets(message),
                    ProjectCache::UpdateSpoolIndex(message) => self.handle_buffer_index(message),
                    ProjectCache::RefreshIndexCache(message) => {
                        self.handle_refresh_index_cache(message)
                    }
                }
            }
        )
    }

    fn handle_envelope(&mut self, dequeued_envelope: DequeuedEnvelope) {
        let envelope_buffer = self
            .services
            .envelope_buffer
            .clone()
            .expect("Called HandleDequeuedEnvelope without an envelope buffer");

        if let Err(e) = self.handle_dequeued_envelope(dequeued_envelope.0, envelope_buffer) {
            relay_log::error!(
                error = &e as &dyn std::error::Error,
                "Failed to handle popped envelope"
            );
        }
    }
}

/// Service implementing the [`ProjectCache`] interface.
#[derive(Debug)]
pub struct ProjectCacheService {
    config: Arc<Config>,
    memory_checker: MemoryChecker,
    project_cache_handle: ProjectCacheHandle,
    services: Services,
    global_config_rx: watch::Receiver<global_config::Status>,
    /// Bounded channel used exclusively to receive envelopes from the envelope buffer.
    envelopes_rx: mpsc::Receiver<DequeuedEnvelope>,
}

impl ProjectCacheService {
    /// Creates a new `ProjectCacheService`.
    pub fn new(
        config: Arc<Config>,
        memory_checker: MemoryChecker,
        project_cache_handle: ProjectCacheHandle,
        services: Services,
        global_config_rx: watch::Receiver<global_config::Status>,
        envelopes_rx: mpsc::Receiver<DequeuedEnvelope>,
    ) -> Self {
        Self {
            config,
            memory_checker,
            project_cache_handle,
            services,
            global_config_rx,
            envelopes_rx,
        }
    }
}

impl Service for ProjectCacheService {
    type Interface = ProjectCache;

    async fn run(self, mut rx: relay_system::Receiver<Self::Interface>) {
        let Self {
            config,
            memory_checker,
            project_cache_handle,
            services,
            mut global_config_rx,
            mut envelopes_rx,
        } = self;
        let mut project_changes = project_cache_handle.changes();
        let project_cache = services.project_cache.clone();
        let outcome_aggregator = services.outcome_aggregator.clone();
        let test_store = services.test_store.clone();

        relay_log::info!("project cache started");

        let global_config = match global_config_rx.borrow().clone() {
            global_config::Status::Ready(_) => {
                relay_log::info!("global config received");
                GlobalConfigStatus::Ready
            }
            global_config::Status::Pending => {
                relay_log::info!("waiting for global config");
                GlobalConfigStatus::Pending
            }
        };

        let (buffer_tx, mut buffer_rx) = mpsc::unbounded_channel();
        let spool_v1 = match config.spool_v2() {
            true => None,
            false => Some({
                // Channel for envelope buffering.
                let buffer_services = spooler::Services {
                    outcome_aggregator,
                    project_cache,
                    test_store,
                };
                let buffer = match BufferService::create(
                    memory_checker.clone(),
                    buffer_services,
                    config.clone(),
                )
                .await
                {
                    Ok(buffer) => {
                        // NOTE: This service is not monitored by the service runner.
                        buffer.start_detached()
                    }
                    Err(err) => {
                        relay_log::error!(
                            error = &err as &dyn Error,
                            "failed to start buffer service",
                        );
                        // NOTE: The process will exit with error if the buffer file could not be
                        // opened or the migrations could not be run.
                        std::process::exit(1);
                    }
                };

                // Request the existing index from the spooler.
                buffer.send(RestoreIndex);

                SpoolV1 {
                    buffer_tx,
                    index: HashSet::new(),
                    buffer_unspool_backoff: RetryBackoff::new(config.http_max_retry_interval()),
                    buffer,
                }
            }),
        };

        let mut broker = ProjectCacheBroker {
            config: config.clone(),
            memory_checker,
            projects: project_cache_handle,
            services,
            spool_v1_unspool_handle: SleepHandle::idle(),
            spool_v1,
            global_config,
        };

        loop {
            tokio::select! {
                biased;

                Ok(()) = global_config_rx.changed() => {
                    metric!(timer(RelayTimers::LegacyProjectCacheTaskDuration), task = "update_global_config", {
                        match global_config_rx.borrow().clone() {
                            global_config::Status::Ready(_) => broker.set_global_config_ready(),
                            // The watch should only be updated if it gets a new value.
                            // This would imply a logical bug.
                            global_config::Status::Pending => relay_log::error!("still waiting for the global config"),
                        }
                    })
                },
                project_change = project_changes.recv() => {
                    metric!(timer(RelayTimers::LegacyProjectCacheTaskDuration), task = "handle_project_change", {
                        if let Ok(project_change) = project_change {
                            broker.handle_project_change(project_change);
                        }
                    })
                }
                // Buffer will not dequeue the envelopes from the spool if there is not enough
                // permits in `BufferGuard` available. Currently this is 50%.
                Some(UnspooledEnvelope { managed_envelope, .. }) = buffer_rx.recv() => {
                    metric!(timer(RelayTimers::LegacyProjectCacheTaskDuration), task = "handle_processing", {
                        broker.handle_processing(managed_envelope)
                    })
                },
                () = &mut broker.spool_v1_unspool_handle => {
                    metric!(timer(RelayTimers::LegacyProjectCacheTaskDuration), task = "periodic_unspool", {
                        broker.handle_periodic_unspool()
                    })
                }
                Some(message) = rx.recv() => {
                    metric!(timer(RelayTimers::LegacyProjectCacheTaskDuration), task = "handle_message", {
                        broker.handle_message(message)
                    })
                }
                Some(message) = envelopes_rx.recv() => {
                    metric!(timer(RelayTimers::LegacyProjectCacheTaskDuration), task = "handle_envelope", {
                        broker.handle_envelope(message)
                    })
                }
                else => break,
            }
        }

        relay_log::info!("project cache stopped");
    }
}

#[cfg(test)]
mod tests {
    use relay_test::mock_service;
    use tokio::select;
    use uuid::Uuid;

    use crate::services::processor::ProcessingGroup;
    use crate::testutils::empty_envelope_with_dsn;
    use crate::utils::MemoryStat;

    use super::*;

    fn mocked_services() -> Services {
        let (aggregator, _) = mock_service("aggregator", (), |&mut (), _| {});
        let (envelope_processor, _) = mock_service("envelope_processor", (), |&mut (), _| {});
        let (outcome_aggregator, _) = mock_service("outcome_aggregator", (), |&mut (), _| {});
        let (project_cache, _) = mock_service("project_cache", (), |&mut (), _| {});
        let (test_store, _) = mock_service("test_store", (), |&mut (), _| {});

        Services {
            envelope_buffer: None,
            aggregator,
            envelope_processor,
            project_cache,
            outcome_aggregator,
            test_store,
        }
    }

    async fn project_cache_broker_setup(
        services: Services,
        buffer_tx: mpsc::UnboundedSender<UnspooledEnvelope>,
    ) -> (ProjectCacheBroker, Addr<Buffer>) {
        let config: Arc<_> = Config::from_json_value(serde_json::json!({
            "spool": {
                "envelopes": {
                    "path": std::env::temp_dir().join(Uuid::new_v4().to_string()),
                    "max_memory_size": 0, // 0 bytes, to force to spool to disk all the envelopes.
                },
                "health": {
                    "max_memory_percent": 0.0
                }
            }
        }))
        .unwrap()
        .into();
        let memory_checker = MemoryChecker::new(MemoryStat::default(), config.clone());
        let buffer_services = spooler::Services {
            outcome_aggregator: services.outcome_aggregator.clone(),
            project_cache: services.project_cache.clone(),
            test_store: services.test_store.clone(),
        };
        let buffer =
            match BufferService::create(memory_checker.clone(), buffer_services, config.clone())
                .await
            {
                Ok(buffer) => buffer.start_detached(),
                Err(err) => {
                    relay_log::error!(error = &err as &dyn Error, "failed to start buffer service");
                    // NOTE: The process will exit with error if the buffer file could not be
                    // opened or the migrations could not be run.
                    std::process::exit(1);
                }
            };

        (
            ProjectCacheBroker {
                config: config.clone(),
                memory_checker,
                projects: ProjectCacheHandle::for_test(),
                services,
                spool_v1_unspool_handle: SleepHandle::idle(),
                spool_v1: Some(SpoolV1 {
                    buffer_tx,
                    index: HashSet::new(),
                    buffer: buffer.clone(),
                    buffer_unspool_backoff: RetryBackoff::new(Duration::from_millis(100)),
                }),
                global_config: GlobalConfigStatus::Pending,
            },
            buffer,
        )
    }

    #[tokio::test]
    async fn periodic_unspool() {
        relay_log::init_test!();

        let services = mocked_services();
        let (buffer_tx, mut buffer_rx) = mpsc::unbounded_channel();
        let (mut broker, _buffer_svc) =
            project_cache_broker_setup(services.clone(), buffer_tx).await;
        let projects = broker.projects.clone();
        let mut project_events = projects.changes();

        broker.global_config = GlobalConfigStatus::Ready;
        let (tx_assert, mut rx_assert) = mpsc::unbounded_channel();

        let dsn1 = "111d836b15bb49d7bbf99e64295d995b";
        let dsn2 = "eeed836b15bb49d7bbf99e64295d995b";

        // Send and spool some envelopes.
        for dsn in [dsn1, dsn2] {
            let envelope = ManagedEnvelope::new(
                empty_envelope_with_dsn(dsn),
                services.outcome_aggregator.clone(),
                services.test_store.clone(),
                ProcessingGroup::Ungrouped,
            );

            let message = ValidateEnvelope { envelope };

            broker.handle_validate_envelope(message);
            tokio::time::sleep(Duration::from_millis(200)).await;
            // Nothing will be dequeued.
            assert!(buffer_rx.try_recv().is_err())
        }

        // Emulate the project cache service loop.
        tokio::task::spawn(async move {
            loop {
                select! {
                    Ok(project_event) = project_events.recv() => {
                        broker.handle_project_change(project_event);
                    }
                    Some(assert) = rx_assert.recv() => {
                        assert_eq!(broker.spool_v1.as_ref().unwrap().index.len(), assert);
                    },
                    () = &mut broker.spool_v1_unspool_handle => broker.handle_periodic_unspool(),
                }
            }
        });

        // Before updating any project states.
        tx_assert.send(2).unwrap();

        projects.test_set_project_state(
            ProjectKey::parse(dsn1).unwrap(),
            ProjectState::new_allowed(),
        );

        assert!(buffer_rx.recv().await.is_some());
        // One of the project should be unspooled.
        tx_assert.send(1).unwrap();

        // Schedule some work...
        tokio::time::sleep(Duration::from_secs(2)).await;

        projects.test_set_project_state(
            ProjectKey::parse(dsn2).unwrap(),
            ProjectState::new_allowed(),
        );

        assert!(buffer_rx.recv().await.is_some());
        // The last project should be unspooled.
        tx_assert.send(0).unwrap();
        // Make sure the last assert is tested.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
