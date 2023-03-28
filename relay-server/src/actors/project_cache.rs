use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use relay_common::ProjectKey;
use relay_config::{Config, RelayMode};
use relay_log::LogError;
use relay_metrics::{self, Aggregator, FlushBuckets, InsertMetrics, MergeBuckets};
use relay_quotas::RateLimits;
use relay_redis::RedisPool;
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, Interface, Sender, Service};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::actors::envelopes::EnvelopeManager;
use crate::actors::outcome::{DiscardReason, TrackOutcome};
use crate::actors::processor::{EnvelopeProcessor, ProcessEnvelope};
use crate::actors::project::{Project, ProjectSender, ProjectState};
use crate::actors::project_buffer::{
    Buffer, BufferService, DequeueMany, Enqueue, QueueKey, RemoveMany,
};
use crate::actors::project_local::{LocalProjectSource, LocalProjectSourceService};
#[cfg(feature = "processing")]
use crate::actors::project_redis::RedisProjectSource;
use crate::actors::project_upstream::{UpstreamProjectSource, UpstreamProjectSourceService};
use crate::actors::upstream::UpstreamRelay;

use crate::service::REGISTRY;
use crate::statsd::{RelayCounters, RelayGauges, RelayHistograms, RelayTimers};
use crate::utils::{self, BufferGuard, GarbageDisposal, ManagedEnvelope};

/// Requests a refresh of a project state from one of the available sources.
///
/// The project state is resolved in the following precedence:
///
///  1. Local file system
///  2. Redis cache (processing mode only)
///  3. Upstream (managed and processing mode only)
///
/// Requests to the upstream are performed via `UpstreamProjectSource`, which internally batches
/// individual requests.
#[derive(Clone, Debug)]
pub struct RequestUpdate {
    /// The public key to fetch the project by.
    project_key: ProjectKey,

    /// If true, all caches should be skipped and a fresh state should be computed.
    no_cache: bool,
}

impl RequestUpdate {
    pub fn new(project_key: ProjectKey, no_cache: bool) -> Self {
        Self {
            project_key,
            no_cache,
        }
    }
}

/// Returns the project state.
///
/// The project state is fetched if it is missing or outdated. If `no_cache` is specified, then the
/// state is always refreshed.
#[derive(Debug)]
pub struct GetProjectState {
    project_key: ProjectKey,
    no_cache: bool,
}

impl GetProjectState {
    /// Fetches the project state and uses the cached version if up-to-date.
    pub fn new(project_key: ProjectKey) -> Self {
        Self {
            project_key,
            no_cache: false,
        }
    }

    /// Fetches the project state and conditionally skips the cache.
    pub fn no_cache(mut self, no_cache: bool) -> Self {
        self.no_cache = no_cache;
        self
    }
}

/// Returns the project state if it is already cached.
///
/// This is used for cases when we only want to perform operations that do
/// not require waiting for network requests.
#[derive(Debug)]
pub struct GetCachedProjectState {
    project_key: ProjectKey,
}

impl GetCachedProjectState {
    pub fn new(project_key: ProjectKey) -> Self {
        Self { project_key }
    }
}

/// A checked envelope and associated rate limits.
///
/// Items violating the rate limits have been removed from the envelope. If all items are removed
/// from the envelope, `None` is returned in place of the envelope.
#[derive(Debug)]
pub struct CheckedEnvelope {
    pub envelope: Option<ManagedEnvelope>,
    pub rate_limits: RateLimits,
}

/// Checks the envelope against project configuration and rate limits.
///
/// When `fetched`, then the project state is ensured to be up to date. When `cached`, an outdated
/// project state may be used, or otherwise the envelope is passed through unaltered.
///
/// To check the envelope, this runs:
///  - Validate origins and public keys
///  - Quotas with a limit of `0`
///  - Cached rate limits
#[derive(Debug)]
pub struct CheckEnvelope {
    envelope: ManagedEnvelope,
}

impl CheckEnvelope {
    /// Uses a cached project state and checks the envelope.
    pub fn new(envelope: ManagedEnvelope) -> Self {
        Self { envelope }
    }
}

/// Validates the envelope against project configuration and rate limits.
///
/// This ensures internally that the project state is up to date and then runs the same checks as
/// [`CheckEnvelope`]. Once the envelope has been validated, remaining items are forwarded to the
/// next stage:
///
///  - If the envelope needs dynamic sampling, and the project state is not cached or out of the
///  date, the envelopes is spooled and we continue when the state is fetched.
///  - Otherwise, the envelope is directly submitted to the [`EnvelopeProcessor`].
///
/// [`EnvelopeProcessor`]: crate::actors::processor::EnvelopeProcessor
#[derive(Debug)]
pub struct ValidateEnvelope {
    envelope: ManagedEnvelope,
}

impl ValidateEnvelope {
    pub fn new(envelope: ManagedEnvelope) -> Self {
        Self { envelope }
    }
}

pub struct UpdateRateLimits {
    project_key: ProjectKey,
    rate_limits: RateLimits,
}

impl UpdateRateLimits {
    pub fn new(project_key: ProjectKey, rate_limits: RateLimits) -> UpdateRateLimits {
        Self {
            project_key,
            rate_limits,
        }
    }
}

/// Updates buffer index for [`ProjectKey`] with keys list of the [`QueueKey`]s.
///
/// This message is sent from the project buffer in case of the error while fetching the data from
/// the persistent buffer, ensuring that we still have the index pointing to the keys, which could be found in the
/// persistent storage.
pub struct UpdateBufferIndex {
    project_key: ProjectKey,
    keys: BTreeSet<QueueKey>,
}

impl UpdateBufferIndex {
    pub fn new(project_key: ProjectKey, keys: BTreeSet<QueueKey>) -> Self {
        Self { project_key, keys }
    }
}

/// A cache for [`ProjectState`]s.
///
/// The project maintains information about organizations, projects, and project keys along with
/// settings required to ingest traffic to Sentry. Internally, it tries to keep information
/// up-to-date and automatically retires unused old data.
///
/// To retrieve information from the cache, use [`GetProjectState`] for guaranteed up-to-date
/// information, or [`GetCachedProjectState`] for immediately available but potentially older
/// information.
///
/// There are also higher-level operations, such as [`CheckEnvelope`] and [`ValidateEnvelope`] that
/// inspect contents of envelopes for ingestion, as well as [`InsertMetrics`] and [`MergeBuckets`]
/// to aggregate metrics associated with a project.
///
/// See the enumerated variants for a full list of available messages for this service.
pub enum ProjectCache {
    RequestUpdate(RequestUpdate),
    Get(GetProjectState, ProjectSender),
    GetCached(GetCachedProjectState, Sender<Option<Arc<ProjectState>>>),
    CheckEnvelope(
        CheckEnvelope,
        Sender<Result<CheckedEnvelope, DiscardReason>>,
    ),
    ValidateEnvelope(ValidateEnvelope),
    UpdateRateLimits(UpdateRateLimits),
    InsertMetrics(InsertMetrics),
    MergeBuckets(MergeBuckets),
    FlushBuckets(FlushBuckets),
    UpdateBufferIndex(UpdateBufferIndex),
}

impl ProjectCache {
    pub fn from_registry() -> Addr<Self> {
        REGISTRY.get().unwrap().project_cache.clone()
    }
}

impl Interface for ProjectCache {}

impl FromMessage<UpdateBufferIndex> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: UpdateBufferIndex, _: ()) -> Self {
        Self::UpdateBufferIndex(message)
    }
}

impl FromMessage<RequestUpdate> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: RequestUpdate, _: ()) -> Self {
        Self::RequestUpdate(message)
    }
}

impl FromMessage<GetProjectState> for ProjectCache {
    type Response = relay_system::BroadcastResponse<Arc<ProjectState>>;

    fn from_message(message: GetProjectState, sender: ProjectSender) -> Self {
        Self::Get(message, sender)
    }
}

impl FromMessage<GetCachedProjectState> for ProjectCache {
    type Response = relay_system::AsyncResponse<Option<Arc<ProjectState>>>;

    fn from_message(
        message: GetCachedProjectState,
        sender: Sender<Option<Arc<ProjectState>>>,
    ) -> Self {
        Self::GetCached(message, sender)
    }
}

impl FromMessage<CheckEnvelope> for ProjectCache {
    type Response = relay_system::AsyncResponse<Result<CheckedEnvelope, DiscardReason>>;

    fn from_message(
        message: CheckEnvelope,
        sender: Sender<Result<CheckedEnvelope, DiscardReason>>,
    ) -> Self {
        Self::CheckEnvelope(message, sender)
    }
}

impl FromMessage<ValidateEnvelope> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: ValidateEnvelope, _: ()) -> Self {
        Self::ValidateEnvelope(message)
    }
}

impl FromMessage<UpdateRateLimits> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: UpdateRateLimits, _: ()) -> Self {
        Self::UpdateRateLimits(message)
    }
}

impl FromMessage<InsertMetrics> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: InsertMetrics, _: ()) -> Self {
        Self::InsertMetrics(message)
    }
}

impl FromMessage<MergeBuckets> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: MergeBuckets, _: ()) -> Self {
        Self::MergeBuckets(message)
    }
}

impl FromMessage<FlushBuckets> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: FlushBuckets, _: ()) -> Self {
        Self::FlushBuckets(message)
    }
}

/// Helper type that contains all configured sources for project cache fetching.
///
/// See [`RequestUpdate`] for a description on how project states are fetched.
#[derive(Clone, Debug)]
struct ProjectSource {
    config: Arc<Config>,
    local_source: Addr<LocalProjectSource>,
    upstream_source: Addr<UpstreamProjectSource>,
    #[cfg(feature = "processing")]
    redis_source: Option<RedisProjectSource>,
}

impl ProjectSource {
    /// Starts all project source services in the current runtime.
    pub fn start(
        config: Arc<Config>,
        upstream_relay: Addr<UpstreamRelay>,
        _redis: Option<RedisPool>,
    ) -> Self {
        let local_source = LocalProjectSourceService::new(config.clone()).start();
        let upstream_source =
            UpstreamProjectSourceService::new(config.clone(), upstream_relay).start();

        #[cfg(feature = "processing")]
        let redis_source = _redis.map(|pool| RedisProjectSource::new(config.clone(), pool));

        Self {
            config,
            local_source,
            upstream_source,
            #[cfg(feature = "processing")]
            redis_source,
        }
    }

    async fn fetch(self, project_key: ProjectKey, no_cache: bool) -> Result<Arc<ProjectState>, ()> {
        let state_opt = self
            .local_source
            .send(FetchOptionalProjectState { project_key })
            .await
            .map_err(|_| ())?;

        if let Some(state) = state_opt {
            return Ok(state);
        }

        match self.config.relay_mode() {
            RelayMode::Proxy => return Ok(Arc::new(ProjectState::allowed())),
            RelayMode::Static => return Ok(Arc::new(ProjectState::missing())),
            RelayMode::Capture => return Ok(Arc::new(ProjectState::allowed())),
            RelayMode::Managed => (), // Proceed with loading the config from redis or upstream
        }

        #[cfg(feature = "processing")]
        if let Some(redis_source) = self.redis_source {
            let state_fetch_result =
                tokio::task::spawn_blocking(move || redis_source.get_config(project_key))
                    .await
                    .map_err(|_| ())?;

            let state_opt = match state_fetch_result {
                Ok(x) => x.map(ProjectState::sanitize).map(Arc::new),
                Err(e) => {
                    relay_log::error!(
                        "Failed to fetch project from Redis: {}",
                        relay_log::LogError(&e)
                    );
                    None
                }
            };

            if let Some(state) = state_opt {
                return Ok(state);
            }
        };

        self.upstream_source
            .send(FetchProjectState {
                project_key,
                no_cache,
            })
            .await
            .map_err(|_| ())
    }
}

/// Updates the cache with new project state information.
struct UpdateProjectState {
    /// The public key to fetch the project by.
    project_key: ProjectKey,

    /// New project state information.
    state: Arc<ProjectState>,

    /// If true, all caches should be skipped and a fresh state should be computed.
    no_cache: bool,
}

/// Holds the addresses of all services required for [`ProjectCache`].
#[derive(Debug, Clone)]
pub struct Services {
    buffer: Arc<BufferGuard>,
    pub aggregator: Addr<Aggregator>,
    pub envelope_processor: Addr<EnvelopeProcessor>,
    pub envelope_manager: Addr<EnvelopeManager>,
    pub outcome_aggregator: Addr<TrackOutcome>,
    pub project_cache: Addr<ProjectCache>,
    pub upstream_relay: Addr<UpstreamRelay>,
}

impl Services {
    /// Creates new [`Services`] context.
    pub fn new(
        buffer: Arc<BufferGuard>,
        aggregator: Addr<Aggregator>,
        envelope_processor: Addr<EnvelopeProcessor>,
        envelope_manager: Addr<EnvelopeManager>,
        outcome_aggregator: Addr<TrackOutcome>,
        project_cache: Addr<ProjectCache>,
        upstream_relay: Addr<UpstreamRelay>,
    ) -> Self {
        Self {
            buffer,
            aggregator,
            envelope_processor,
            envelope_manager,
            outcome_aggregator,
            project_cache,
            upstream_relay,
        }
    }
}

/// Main broker of the [`ProjectCacheService`].
///
/// This handles incoming public messages, merges resolved project states, and maintains the actual
/// cache of project states.
#[derive(Debug)]
struct ProjectCacheBroker {
    config: Arc<Config>,
    services: Services,
    // Need hashbrown because drain_filter is not stable in std yet.
    projects: hashbrown::HashMap<ProjectKey, Project>,
    garbage_disposal: GarbageDisposal<Project>,
    source: ProjectSource,
    state_tx: mpsc::UnboundedSender<UpdateProjectState>,
    /// Index of the buffered project keys.
    buffer_tx: mpsc::UnboundedSender<ManagedEnvelope>,
    index: BTreeMap<ProjectKey, BTreeSet<QueueKey>>,
    buffer: Addr<Buffer>,
}

impl ProjectCacheBroker {
    /// Adds the value to the queue for the provided key.
    pub fn enqueue(&mut self, key: QueueKey, value: ManagedEnvelope) {
        self.index.entry(key.own_key).or_default().insert(key);
        self.index.entry(key.sampling_key).or_default().insert(key);
        self.buffer.send(Enqueue::new(key, value));
    }

    /// Sends the message to the buffer service to dequeue the envelopes.
    ///
    /// All the found envelopes will be send back through the `buffer_tx` channel and dirrectly
    /// forwarded to `handle_processing`.
    pub fn dequeue(&mut self, partial_key: ProjectKey) {
        let mut result = Vec::new();
        let mut queue_keys = self.index.remove(&partial_key).unwrap_or_default();
        let mut index = BTreeSet::new();

        while let Some(queue_key) = queue_keys.pop_first() {
            // We only have to check `other_key`, because we already know that the `partial_key`s `state`
            // is valid and loaded.
            let other_key = if queue_key.own_key == partial_key {
                queue_key.sampling_key
            } else {
                queue_key.own_key
            };

            if self
                .projects
                .get(&other_key)
                // Make sure we have only cached and valid state.
                .and_then(|p| p.valid_state())
                .map_or(false, |s| !s.invalid())
            {
                result.push(queue_key);
            } else {
                index.insert(queue_key);
            }
        }

        if !index.is_empty() {
            self.index.insert(partial_key, index);
        }

        if !result.is_empty() {
            self.buffer.send(DequeueMany::new(
                partial_key,
                result,
                self.buffer_tx.clone(),
            ))
        }
    }

    /// Evict projects that are over its expiry date.
    ///
    /// Ideally, we would use `check_expiry` to determine expiry here.
    /// However, for eviction, we want to add an additional delay, such that we do not delete
    /// a project that has expired recently and for which a fetch is already underway in
    /// [`super::project_upstream`].
    fn evict_stale_project_caches(&mut self) {
        let eviction_start = Instant::now();
        let delta = 2 * self.config.project_cache_expiry() + self.config.project_grace_period();

        let expired = self
            .projects
            .drain_filter(|_, entry| entry.last_updated_at() + delta <= eviction_start);

        // Defer dropping the projects to a dedicated thread:
        let mut count = 0;
        for (project_key, project) in expired {
            if let Some(keys) = self.index.remove(&project_key) {
                self.buffer.send(RemoveMany::new(project_key, keys))
            }

            self.garbage_disposal.dispose(project);
            count += 1;
        }
        metric!(counter(RelayCounters::EvictingStaleProjectCaches) += count);

        // Log garbage queue size:
        let queue_size = self.garbage_disposal.queue_size() as f64;
        metric!(gauge(RelayGauges::ProjectCacheGarbageQueueSize) = queue_size);

        metric!(timer(RelayTimers::ProjectStateEvictionDuration) = eviction_start.elapsed());
    }

    fn get_or_create_project(&mut self, project_key: ProjectKey) -> &mut Project {
        metric!(histogram(RelayHistograms::ProjectStateCacheSize) = self.projects.len() as u64);

        let config = self.config.clone();

        self.projects
            .entry(project_key)
            .and_modify(|_| {
                metric!(counter(RelayCounters::ProjectCacheHit) += 1);
            })
            .or_insert_with(move || {
                metric!(counter(RelayCounters::ProjectCacheMiss) += 1);
                Project::new(project_key, config)
            })
    }

    /// Updates the [`Project`] with received [`ProjectState`].
    ///
    /// If the project state is valid we also send the message to the buffer service to dequeue the
    /// envelopes for this project.
    fn merge_state(&mut self, message: UpdateProjectState) {
        let UpdateProjectState {
            project_key,
            state,
            no_cache,
        } = message;

        let project_cache = self.services.project_cache.clone();
        self.get_or_create_project(project_key).update_state(
            project_cache,
            state.clone(),
            no_cache,
        );

        if !state.invalid() {
            self.dequeue(project_key);
        }
    }

    fn handle_request_update(&mut self, message: RequestUpdate) {
        let RequestUpdate {
            project_key,
            no_cache,
        } = message;

        // Bump the update time of the project in our hashmap to evade eviction.
        let project = self.get_or_create_project(project_key);
        project.refresh_updated_timestamp();
        let next_attempt = project.next_fetch_attempt();

        let source = self.source.clone();
        let sender = self.state_tx.clone();

        tokio::spawn(async move {
            // Wait on the new attempt time when set.
            if let Some(next_attempt) = next_attempt {
                tokio::time::sleep_until(next_attempt).await;
            }
            let state = source
                .fetch(project_key, no_cache)
                .await
                .unwrap_or_else(|()| Arc::new(ProjectState::err()));

            let message = UpdateProjectState {
                project_key,
                state,
                no_cache,
            };

            sender.send(message).ok();
        });
    }

    fn handle_get(&mut self, message: GetProjectState, sender: ProjectSender) {
        let project_cache = self.services.project_cache.clone();
        self.get_or_create_project(message.project_key).get_state(
            project_cache,
            sender,
            message.no_cache,
        );
    }

    fn handle_get_cached(&mut self, message: GetCachedProjectState) -> Option<Arc<ProjectState>> {
        let project_cache = self.services.project_cache.clone();
        self.get_or_create_project(message.project_key)
            .get_cached_state(project_cache, false)
    }

    fn handle_check_envelope(
        &mut self,
        message: CheckEnvelope,
    ) -> Result<CheckedEnvelope, DiscardReason> {
        let CheckEnvelope { envelope: context } = message;
        let project_cache = self.services.project_cache.clone();
        let outcome_aggregator = self.services.outcome_aggregator.clone();
        let project = self.get_or_create_project(context.envelope().meta().public_key());

        // Preload the project cache so that it arrives a little earlier in processing. However,
        // do not pass `no_cache`. In case the project is rate limited, we do not want to force
        // a full reload. Fetching must not block the store request.
        project.prefetch(project_cache, false);
        project.check_envelope(context, outcome_aggregator)
    }

    /// Handles the processing of the provided envelope.
    ///
    /// The following pre-conditions must be met before calling this function:
    /// - Envelope's project state must be cached and valid.
    /// - If dynamic sampling key exists, the sampling project state must be cached and valid.
    ///
    /// Calling this function without envelope's project state available will cause the envelope to
    /// be dropped and outcome will be logged.
    fn handle_processing(&mut self, managed_envelope: ManagedEnvelope) {
        let project_key = managed_envelope.envelope().meta().public_key();

        let Some(project) = self.projects.get_mut(&project_key) else {
            relay_log::with_scope(
                |scope| scope.set_tag("project_key", project_key),
                || relay_log::error!("project could not be found in the cache"),
            );
            return;
        };

        let Some(own_project_state) = project.valid_state().filter(|s| !s.invalid()) else {
            relay_log::with_scope(
                |scope| scope.set_tag("project_key", project_key),
                || relay_log::error!("project has no valid cached state"),
            );
            return;
        };

        // The `Envelope` and `EnvelopeContext` will be dropped if the `Project::check_envelope()`
        // function returns any error, which will also be ignored here.
        if let Ok(CheckedEnvelope {
            envelope: Some(managed_envelope),
            ..
        }) = project.check_envelope(managed_envelope, self.services.outcome_aggregator.clone())
        {
            let sampling_state = utils::get_sampling_key(managed_envelope.envelope())
                .and_then(|key| self.projects.get(&key))
                .and_then(|p| p.valid_state());

            let mut process = ProcessEnvelope {
                envelope: managed_envelope,
                project_state: own_project_state.clone(),
                sampling_project_state: None,
            };

            if let Some(sampling_state) = sampling_state {
                if own_project_state.organization_id == sampling_state.organization_id {
                    process.sampling_project_state = Some(sampling_state)
                }
            }

            self.services.envelope_processor.send(process);
        }
    }

    /// Checks an incoming envelope and decides either process it immediately or buffer it.
    ///
    /// Few conditions are checked here:
    /// - If there is no dynamic sampling key and the project is already cached, we do straight to
    /// processing otherwise buffer the envelopes.
    /// - If the dynamic sampling key is provided and if the root and sampling projects
    /// are cached - process the envelope, buffer otherwise.
    ///
    /// This means if the caches are hot we always process all the incoming envelopes without any
    /// delay. But in case the project state cannot be fetched, we keep buffering till the state
    /// is eventually updated.
    ///
    /// The flushing of the buffered envelopes happens in `update_state`.
    fn handle_validate_envelope(&mut self, message: ValidateEnvelope) {
        let ValidateEnvelope { envelope: context } = message;
        let project_cache = self.services.project_cache.clone();
        let envelope = context.envelope();

        // Fetch the project state for our key and make sure it's not invalid.
        let own_key = envelope.meta().public_key();
        let project_state = self
            .get_or_create_project(own_key)
            .get_cached_state(project_cache.clone(), envelope.meta().no_cache())
            .filter(|st| !st.invalid());

        // Also, fetch the project state for sampling key and make sure it's not invalid.
        let sampling_key = utils::get_sampling_key(envelope);
        let sampling_state = sampling_key.and_then(|key| {
            self.get_or_create_project(key)
                .get_cached_state(project_cache, envelope.meta().no_cache())
                .filter(|st| !st.invalid())
        });

        // Trigger processing once we have a project state and we either have a sampling project
        // state or we do not need one.
        if project_state.is_some() && (sampling_state.is_some() || sampling_key.is_none()) {
            return self.handle_processing(context);
        }

        let key = QueueKey::new(own_key, sampling_key.unwrap_or(own_key));
        self.enqueue(key, context);
    }

    fn handle_rate_limits(&mut self, message: UpdateRateLimits) {
        self.get_or_create_project(message.project_key)
            .merge_rate_limits(message.rate_limits);
    }

    fn handle_insert_metrics(&mut self, message: InsertMetrics) {
        let aggregator = self.services.aggregator.clone();
        let outcome_aggregator = self.services.outcome_aggregator.clone();
        // Only keep if we have an aggregator, otherwise drop because we know that we were disabled.
        self.get_or_create_project(message.project_key())
            .insert_metrics(aggregator, outcome_aggregator, message.metrics());
    }

    fn handle_merge_buckets(&mut self, message: MergeBuckets) {
        let aggregator = self.services.aggregator.clone();
        let outcome_aggregator = self.services.outcome_aggregator.clone();
        // Only keep if we have an aggregator, otherwise drop because we know that we were disabled.
        self.get_or_create_project(message.project_key())
            .merge_buckets(aggregator, outcome_aggregator, message.buckets());
    }

    fn handle_flush_buckets(&mut self, message: FlushBuckets) {
        let context = self.services.clone();
        self.get_or_create_project(message.project_key)
            .flush_buckets(context, message.partition_key, message.buckets);
    }

    fn handle_buffer_index(&mut self, message: UpdateBufferIndex) {
        self.index.insert(message.project_key, message.keys);
    }

    fn handle_message(&mut self, message: ProjectCache) {
        match message {
            ProjectCache::RequestUpdate(message) => self.handle_request_update(message),
            ProjectCache::Get(message, sender) => self.handle_get(message, sender),
            ProjectCache::GetCached(message, sender) => {
                sender.send(self.handle_get_cached(message))
            }
            ProjectCache::CheckEnvelope(message, sender) => {
                sender.send(self.handle_check_envelope(message))
            }
            ProjectCache::ValidateEnvelope(message) => self.handle_validate_envelope(message),
            ProjectCache::UpdateRateLimits(message) => self.handle_rate_limits(message),
            ProjectCache::InsertMetrics(message) => self.handle_insert_metrics(message),
            ProjectCache::MergeBuckets(message) => self.handle_merge_buckets(message),
            ProjectCache::FlushBuckets(message) => self.handle_flush_buckets(message),
            ProjectCache::UpdateBufferIndex(message) => self.handle_buffer_index(message),
        }
    }
}

/// Service implementing the [`ProjectCache`] interface.
#[derive(Debug)]
pub struct ProjectCacheService {
    config: Arc<Config>,
    services: Services,
    redis: Option<RedisPool>,
}

impl ProjectCacheService {
    /// Creates a new `ProjectCacheService`.
    pub fn new(config: Arc<Config>, services: Services, redis: Option<RedisPool>) -> Self {
        Self {
            config,
            services,
            redis,
        }
    }
}

impl Service for ProjectCacheService {
    type Interface = ProjectCache;

    fn spawn_handler(self, mut rx: relay_system::Receiver<Self::Interface>) {
        let Self {
            config,
            services,
            redis,
        } = self;
        let buffer_guard = services.buffer.clone();
        let project_cache = services.project_cache.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(config.cache_eviction_interval());
            relay_log::info!("project cache started");

            // Channel for async project state responses back into the project cache.
            let (state_tx, mut state_rx) = mpsc::unbounded_channel();

            // Channel for envelope buffering.
            let (buffer_tx, mut buffer_rx) = mpsc::unbounded_channel();
            let buffer =
                match BufferService::create(buffer_guard, project_cache, config.clone()).await {
                    Ok(buffer) => buffer.start(),
                    Err(err) => {
                        relay_log::error!("failed to start buffer service: {}", LogError(&err));
                        // NOTE: The process will exit with error if the buffer file could not be
                        // opened or the migrations could not be run.
                        std::process::exit(1);
                    }
                };

            // Main broker that serializes public and internal messages, and triggers project state
            // fetches via the project source.
            let mut broker = ProjectCacheBroker {
                config: config.clone(),
                projects: hashbrown::HashMap::new(),
                garbage_disposal: GarbageDisposal::new(),
                source: ProjectSource::start(config, services.upstream_relay.clone(), redis),
                services,
                state_tx,
                buffer_tx,
                index: BTreeMap::new(),
                buffer,
            };

            loop {
                tokio::select! {
                    biased;

                    Some(message) = state_rx.recv() => broker.merge_state(message),
                    Some(managed_envelope) = buffer_rx.recv() => broker.handle_processing(managed_envelope),
                    _ = ticker.tick() => broker.evict_stale_project_caches(),
                    Some(message) = rx.recv() => broker.handle_message(message),
                    else => break,
                }
            }

            relay_log::info!("project cache stopped");
        });
    }
}

#[derive(Clone, Debug)]
pub struct FetchProjectState {
    /// The public key to fetch the project by.
    pub project_key: ProjectKey,

    /// If true, all caches should be skipped and a fresh state should be computed.
    pub no_cache: bool,
}

#[derive(Clone, Debug)]
pub struct FetchOptionalProjectState {
    project_key: ProjectKey,
}

impl FetchOptionalProjectState {
    pub fn project_key(&self) -> ProjectKey {
        self.project_key
    }
}
