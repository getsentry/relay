use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use hashbrown::HashSet;
use relay_base_schema::project::ProjectKey;
use relay_config::{Config, RelayMode};
use relay_metrics::{Aggregator, FlushBuckets, MergeBuckets, MetricMeta};
use relay_quotas::RateLimits;
use relay_redis::RedisPool;
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, Interface, Sender, Service};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::services::global_config::{self, GlobalConfigManager, Subscribe};
use crate::services::outcome::{DiscardReason, TrackOutcome};
use crate::services::processor::{EncodeMetrics, EnvelopeProcessor, ProcessEnvelope};
use crate::services::project::{Project, ProjectSender, ProjectState};
use crate::services::project_local::{LocalProjectSource, LocalProjectSourceService};
#[cfg(feature = "processing")]
use crate::services::project_redis::RedisProjectSource;
use crate::services::project_upstream::{UpstreamProjectSource, UpstreamProjectSourceService};
use crate::services::spooler::{
    self, Buffer, BufferService, DequeueMany, Enqueue, QueueKey, RemoveMany, RestoreIndex,
    UnspooledEnvelope, BATCH_KEY_COUNT,
};
use crate::services::test_store::TestStore;
use crate::services::upstream::UpstreamRelay;

use crate::statsd::{RelayCounters, RelayGauges, RelayHistograms, RelayTimers};
use crate::utils::{
    self, BufferGuard, GarbageDisposal, ManagedEnvelope, RetryBackoff, SleepHandle,
};

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
/// [`EnvelopeProcessor`]: crate::services::processor::EnvelopeProcessor
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

/// Add metric metadata to the aggregator.
#[derive(Debug)]
pub struct AddMetricMeta {
    /// The project key.
    pub project_key: ProjectKey,
    /// The metadata.
    pub meta: MetricMeta,
}

/// Updates the buffer index for [`ProjectKey`] with the [`QueueKey`] keys.
///
/// This message is sent from the project buffer in case of the error while fetching the data from
/// the persistent buffer, ensuring that we still have the index pointing to the keys, which could be found in the
/// persistent storage.
pub struct UpdateSpoolIndex(pub HashSet<QueueKey>);

impl UpdateSpoolIndex {
    pub fn new(keys: HashSet<QueueKey>) -> Self {
        Self(keys)
    }
}

/// Checks the status of underlying buffer spool.
#[derive(Debug)]
pub struct SpoolHealth;

/// The current envelopes index fetched from the underlying buffer spool.
///
/// This index will be received only once shortly after startup and will trigger refresh for the
/// project states for the project keys returned in the message.
#[derive(Debug)]
pub struct RefreshIndexCache(pub HashSet<QueueKey>);

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
/// inspect contents of envelopes for ingestion, as well as [`MergeBuckets`] to aggregate metrics
/// associated with a project.
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
    MergeBuckets(MergeBuckets),
    AddMetricMeta(AddMetricMeta),
    FlushBuckets(FlushBuckets),
    UpdateSpoolIndex(UpdateSpoolIndex),
    SpoolHealth(Sender<bool>),
    RefreshIndexCache(RefreshIndexCache),
}

impl ProjectCache {
    pub fn variant(&self) -> &'static str {
        match self {
            Self::RequestUpdate(_) => "RequestUpdate",
            Self::Get(_, _) => "Get",
            Self::GetCached(_, _) => "GetCached",
            Self::CheckEnvelope(_, _) => "CheckEnvelope",
            Self::ValidateEnvelope(_) => "ValidateEnvelope",
            Self::UpdateRateLimits(_) => "UpdateRateLimits",
            Self::MergeBuckets(_) => "MergeBuckets",
            Self::AddMetricMeta(_) => "AddMetricMeta",
            Self::FlushBuckets(_) => "FlushBuckets",
            Self::UpdateSpoolIndex(_) => "UpdateSpoolIndex",
            Self::SpoolHealth(_) => "SpoolHealth",
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

impl FromMessage<MergeBuckets> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: MergeBuckets, _: ()) -> Self {
        Self::MergeBuckets(message)
    }
}

impl FromMessage<AddMetricMeta> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: AddMetricMeta, _: ()) -> Self {
        Self::AddMetricMeta(message)
    }
}

impl FromMessage<FlushBuckets> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: FlushBuckets, _: ()) -> Self {
        Self::FlushBuckets(message)
    }
}

impl FromMessage<SpoolHealth> for ProjectCache {
    type Response = relay_system::AsyncResponse<bool>;

    fn from_message(_message: SpoolHealth, sender: Sender<bool>) -> Self {
        Self::SpoolHealth(sender)
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
                Ok(state) => state.map(ProjectState::sanitize).map(Arc::new),
                Err(error) => {
                    relay_log::error!(
                        error = &error as &dyn Error,
                        "failed to fetch project from Redis",
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
    pub aggregator: Addr<Aggregator>,
    pub envelope_processor: Addr<EnvelopeProcessor>,
    pub outcome_aggregator: Addr<TrackOutcome>,
    pub project_cache: Addr<ProjectCache>,
    pub test_store: Addr<TestStore>,
    pub upstream_relay: Addr<UpstreamRelay>,
    pub global_config: Addr<GlobalConfigManager>,
}

impl Services {
    /// Creates new [`Services`] context.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        aggregator: Addr<Aggregator>,
        envelope_processor: Addr<EnvelopeProcessor>,
        outcome_aggregator: Addr<TrackOutcome>,
        project_cache: Addr<ProjectCache>,
        test_store: Addr<TestStore>,
        upstream_relay: Addr<UpstreamRelay>,
        global_config: Addr<GlobalConfigManager>,
    ) -> Self {
        Self {
            aggregator,
            envelope_processor,
            outcome_aggregator,
            project_cache,
            test_store,
            upstream_relay,
            global_config,
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
    // Need hashbrown because extract_if is not stable in std yet.
    projects: hashbrown::HashMap<ProjectKey, Project>,
    garbage_disposal: GarbageDisposal<Project>,
    source: ProjectSource,
    state_tx: mpsc::UnboundedSender<UpdateProjectState>,
    buffer_tx: mpsc::UnboundedSender<UnspooledEnvelope>,
    buffer_guard: Arc<BufferGuard>,
    /// Index of the buffered project keys.
    index: HashSet<QueueKey>,
    buffer_unspool_handle: SleepHandle,
    buffer_unspool_backoff: RetryBackoff,
    buffer: Addr<Buffer>,
    global_config: GlobalConfigStatus,
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
    pub fn enqueue(&mut self, key: QueueKey, value: ManagedEnvelope) {
        self.index.insert(key);
        self.buffer.send(Enqueue::new(key, value));
    }

    /// Sends the message to the buffer service to dequeue the envelopes.
    ///
    /// All the found envelopes will be send back through the `buffer_tx` channel and directly
    /// forwarded to `handle_processing`.
    pub fn dequeue(&self, keys: HashSet<QueueKey>) {
        self.buffer
            .send(DequeueMany::new(keys, self.buffer_tx.clone()))
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
            .extract_if(|_, entry| entry.last_updated_at() + delta <= eviction_start);

        // Defer dropping the projects to a dedicated thread:
        let mut count = 0;
        for (project_key, project) in expired {
            let keys = self
                .index
                .extract_if(|key| key.own_key == project_key || key.sampling_key == project_key)
                .collect::<BTreeSet<_>>();

            if !keys.is_empty() {
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
        let aggregator = self.services.aggregator.clone();
        let envelope_processor = self.services.envelope_processor.clone();
        let outcome_aggregator = self.services.outcome_aggregator.clone();

        self.get_or_create_project(project_key).update_state(
            project_cache,
            aggregator,
            state.clone(),
            envelope_processor,
            outcome_aggregator,
            no_cache,
        );

        // Schedule unspool if nothing is running at the moment.
        if !self.buffer_unspool_backoff.started() {
            self.buffer_unspool_backoff.reset();
            self.schedule_unspool();
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
    fn handle_processing(&mut self, key: QueueKey, managed_envelope: ManagedEnvelope) {
        let project_key = managed_envelope.envelope().meta().public_key();

        let Some(project) = self.projects.get_mut(&project_key) else {
            relay_log::error!(
                tags.project_key = %project_key,
                "project could not be found in the cache",
            );

            let mut project = Project::new(project_key, self.config.clone());
            project.prefetch(self.services.project_cache.clone(), false);
            self.projects.insert(project_key, project);
            self.enqueue(key, managed_envelope);
            return;
        };

        let Some(own_project_state) = project.valid_state().filter(|s| !s.invalid()) else {
            relay_log::error!(
                tags.project_key = %project_key,
                "project has no valid cached state",
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
            let reservoir_counters = project.reservoir_counters();

            let sampling_project_state = utils::get_sampling_key(managed_envelope.envelope())
                .and_then(|key| self.projects.get(&key))
                .and_then(|p| p.valid_state())
                .filter(|state| state.organization_id == own_project_state.organization_id);

            let process = ProcessEnvelope {
                envelope: managed_envelope,
                project_state: own_project_state,
                sampling_project_state,
                reservoir_counters,
            };

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

        let key = QueueKey::new(own_key, sampling_key.unwrap_or(own_key));

        // Trigger processing once we have a project state and we either have a sampling project
        // state or we do not need one.
        if project_state.is_some()
            && (sampling_state.is_some() || sampling_key.is_none())
            && !self.buffer_guard.is_over_high_watermark()
            && self.global_config.is_ready()
        {
            return self.handle_processing(key, context);
        }

        self.enqueue(key, context);
    }

    fn handle_rate_limits(&mut self, message: UpdateRateLimits) {
        self.get_or_create_project(message.project_key)
            .merge_rate_limits(message.rate_limits);
    }

    fn handle_merge_buckets(&mut self, message: MergeBuckets) {
        let project_cache = self.services.project_cache.clone();
        let aggregator = self.services.aggregator.clone();
        let outcome_aggregator = self.services.outcome_aggregator.clone();
        let envelope_processor = self.services.envelope_processor.clone();

        let project = self.get_or_create_project(message.project_key());

        if let Some(scoping) = message.scoping() {
            project.set_partial_scoping(scoping);
        }

        project.prefetch(project_cache.clone(), false);
        project.merge_buckets(
            aggregator,
            outcome_aggregator,
            envelope_processor,
            message.buckets(),
            project_cache,
        );
    }

    fn handle_add_metric_meta(&mut self, message: AddMetricMeta) {
        let envelope_processor = self.services.envelope_processor.clone();

        self.get_or_create_project(message.project_key)
            .add_metric_meta(message.meta, envelope_processor);
    }

    fn handle_flush_buckets(&mut self, message: FlushBuckets) {
        let mut output = BTreeMap::new();
        for (project_key, buckets) in message.buckets {
            let outcome_aggregator = self.services.outcome_aggregator.clone();
            let project = self.get_or_create_project(project_key);
            if let Some((scoping, b)) = project.check_buckets(outcome_aggregator, buckets) {
                output.insert(scoping, b);
            }
        }

        self.services
            .envelope_processor
            .send(EncodeMetrics { scopes: output })
    }

    fn handle_buffer_index(&mut self, message: UpdateSpoolIndex) {
        self.index.extend(message.0);
    }

    fn handle_spool_health(&mut self, sender: Sender<bool>) {
        self.buffer.send(spooler::Health(sender))
    }

    fn handle_refresh_index_cache(&mut self, message: RefreshIndexCache) {
        let RefreshIndexCache(index) = message;
        let project_cache = self.services.project_cache.clone();

        for key in index {
            self.index.insert(key);
            self.get_or_create_project(key.own_key)
                .prefetch(project_cache.clone(), false);
            if key.own_key != key.sampling_key {
                self.get_or_create_project(key.sampling_key)
                    .prefetch(project_cache.clone(), false);
            }
        }
    }

    /// Returns backoff timeout for an unspool attempt.
    fn next_unspool_attempt(&mut self) -> Duration {
        self.config.spool_envelopes_unspool_interval() + self.buffer_unspool_backoff.next_backoff()
    }

    fn schedule_unspool(&mut self) {
        if self.buffer_unspool_handle.is_idle() {
            // Set the time for the next attempt.
            let wait = self.next_unspool_attempt();
            self.buffer_unspool_handle.set(wait);
        }
    }

    /// Returns `true` if the project state valid for the [`QueueKey`].
    ///
    /// Which includes the own key and the samplig key for the project.
    /// Note: this function will trigger [`ProjectState`] refresh if it's already expired or not
    /// valid.
    fn is_state_valid(&mut self, key: &QueueKey) -> bool {
        let QueueKey {
            own_key,
            sampling_key,
        } = key;

        let is_own_state_valid = self.projects.get_mut(own_key).map_or(false, |project| {
            // Returns `Some` if the project is cached otherwise None and also triggers refresh
            // in background.
            project
                .get_cached_state(self.services.project_cache.clone(), false)
                // Makes sure that the state also is valid.
                .map_or(false, |state| !state.invalid())
        });

        let is_sampling_state_valid = if own_key != sampling_key {
            self.projects
                .get_mut(sampling_key)
                .map_or(false, |project| {
                    // Returns `Some` if the project is cached otherwise None and also triggers refresh
                    // in background.
                    project
                        .get_cached_state(self.services.project_cache.clone(), false)
                        // Makes sure that the state also is valid.
                        .map_or(false, |state| !state.invalid())
                })
        } else {
            is_own_state_valid
        };

        is_own_state_valid && is_sampling_state_valid
    }

    /// Iterates the buffer index and tries to unspool the envelopes for projects with a valid
    /// state.
    ///
    /// This makes sure we always moving the unspool forward, even if we do not fetch the project
    /// states updates, but still can process data based on the existing cache.
    fn handle_periodic_unspool(&mut self) {
        self.buffer_unspool_handle.reset();

        // If we don't yet have the global config, we will defer dequeuing until we do.
        if let GlobalConfigStatus::Pending = self.global_config {
            self.buffer_unspool_backoff.reset();
            self.schedule_unspool();
            return;
        }
        // If there is nothing spooled, schedule the next check a little bit later.
        // And do *not* attempt to unspool if the assigned permits over low watermark.
        if self.index.is_empty() || !self.buffer_guard.is_below_low_watermark() {
            self.schedule_unspool();
            return;
        }

        let mut index = std::mem::take(&mut self.index);
        let values = index
            .extract_if(|key| self.is_state_valid(key))
            .take(BATCH_KEY_COUNT)
            .collect::<HashSet<_>>();

        if !values.is_empty() {
            self.dequeue(values);
        }

        // Return all the un-used items to the index.
        if !index.is_empty() {
            self.index.extend(index);
        }

        // Schedule unspool once we are done.
        self.buffer_unspool_backoff.reset();
        self.schedule_unspool();
    }

    fn handle_message(&mut self, message: ProjectCache) {
        let ty = message.variant();
        metric!(
            timer(RelayTimers::ProjectCacheMessageDuration),
            message = ty,
            {
                match message {
                    ProjectCache::RequestUpdate(message) => self.handle_request_update(message),
                    ProjectCache::Get(message, sender) => self.handle_get(message, sender),
                    ProjectCache::GetCached(message, sender) => {
                        sender.send(self.handle_get_cached(message))
                    }
                    ProjectCache::CheckEnvelope(message, sender) => {
                        sender.send(self.handle_check_envelope(message))
                    }
                    ProjectCache::ValidateEnvelope(message) => {
                        self.handle_validate_envelope(message)
                    }
                    ProjectCache::UpdateRateLimits(message) => self.handle_rate_limits(message),
                    ProjectCache::MergeBuckets(message) => self.handle_merge_buckets(message),
                    ProjectCache::AddMetricMeta(message) => self.handle_add_metric_meta(message),
                    ProjectCache::FlushBuckets(message) => self.handle_flush_buckets(message),
                    ProjectCache::UpdateSpoolIndex(message) => self.handle_buffer_index(message),
                    ProjectCache::SpoolHealth(sender) => self.handle_spool_health(sender),
                    ProjectCache::RefreshIndexCache(message) => {
                        self.handle_refresh_index_cache(message)
                    }
                }
            }
        )
    }
}

/// Service implementing the [`ProjectCache`] interface.
#[derive(Debug)]
pub struct ProjectCacheService {
    buffer_guard: Arc<BufferGuard>,
    config: Arc<Config>,
    services: Services,
    redis: Option<RedisPool>,
}

impl ProjectCacheService {
    /// Creates a new `ProjectCacheService`.
    pub fn new(
        config: Arc<Config>,
        buffer_guard: Arc<BufferGuard>,
        services: Services,
        redis: Option<RedisPool>,
    ) -> Self {
        Self {
            buffer_guard,
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
            buffer_guard,
            config,
            services,
            redis,
        } = self;
        let project_cache = services.project_cache.clone();
        let outcome_aggregator = services.outcome_aggregator.clone();
        let test_store = services.test_store.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(config.cache_eviction_interval());
            relay_log::info!("project cache started");

            // Channel for async project state responses back into the project cache.
            let (state_tx, mut state_rx) = mpsc::unbounded_channel();

            // Channel for envelope buffering.
            let (buffer_tx, mut buffer_rx) = mpsc::unbounded_channel();
            let buffer_services = spooler::Services {
                outcome_aggregator,
                project_cache,
                test_store,
            };
            let buffer =
                match BufferService::create(buffer_guard.clone(), buffer_services, config.clone())
                    .await
                {
                    Ok(buffer) => buffer.start(),
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

            let Ok(mut subscription) = services.global_config.send(Subscribe).await else {
                // TODO(iker): we accept this sub-optimal error handling. TBD
                // the approach to deal with failures on the subscription
                // mechanism.
                relay_log::error!("failed to subscribe to GlobalConfigService");
                return;
            };

            let global_config = match subscription.borrow().clone() {
                global_config::Status::Ready(_) => {
                    relay_log::info!("global config received");
                    GlobalConfigStatus::Ready
                }
                global_config::Status::Pending => {
                    relay_log::info!("waiting for global config");
                    GlobalConfigStatus::Pending
                }
            };

            // Request the existing index from the spooler.
            buffer.send(RestoreIndex);

            // Main broker that serializes public and internal messages, and triggers project state
            // fetches via the project source.
            let mut broker = ProjectCacheBroker {
                config: config.clone(),
                projects: hashbrown::HashMap::new(),
                garbage_disposal: GarbageDisposal::new(),
                source: ProjectSource::start(
                    config.clone(),
                    services.upstream_relay.clone(),
                    redis,
                ),
                services,
                state_tx,
                buffer_tx,
                buffer_guard,
                index: HashSet::new(),
                buffer_unspool_handle: SleepHandle::idle(),
                buffer_unspool_backoff: RetryBackoff::new(config.http_max_retry_interval()),
                buffer,
                global_config,
            };

            loop {
                tokio::select! {
                    biased;

                    Ok(()) = subscription.changed() => {
                        match subscription.borrow().clone() {
                            global_config::Status::Ready(_) => broker.set_global_config_ready(),
                            // The watch should only be updated if it gets a new value.
                            // This would imply a logical bug.
                            global_config::Status::Pending => relay_log::error!("still waiting for the global config"),
                        }
                    },
                    Some(message) = state_rx.recv() => broker.merge_state(message),
                    // Buffer will not dequeue the envelopes from the spool if there is not enough
                    // permits in `BufferGuard` available. Currently this is 50%.
                    Some(UnspooledEnvelope{managed_envelope, key}) = buffer_rx.recv() => broker.handle_processing(key, managed_envelope),
                    _ = ticker.tick() => broker.evict_stale_project_caches(),
                    () = &mut broker.buffer_unspool_handle => broker.handle_periodic_unspool(),
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use relay_test::mock_service;
    use tokio::select;
    use uuid::Uuid;

    use crate::services::processor::{ProcessingGroup, Ungrouped};
    use crate::testutils::{empty_envelope, empty_envelope_with_dsn};

    use super::*;

    fn mocked_services() -> Services {
        let (aggregator, _) = mock_service("aggregator", (), |&mut (), _| {});
        let (envelope_processor, _) = mock_service("envelope_processor", (), |&mut (), _| {});
        let (outcome_aggregator, _) = mock_service("outcome_aggregator", (), |&mut (), _| {});
        let (project_cache, _) = mock_service("project_cache", (), |&mut (), _| {});
        let (test_store, _) = mock_service("test_store", (), |&mut (), _| {});
        let (upstream_relay, _) = mock_service("upstream_relay", (), |&mut (), _| {});
        let (global_config, _) = mock_service("global_config", (), |&mut (), _| {});

        Services {
            aggregator,
            envelope_processor,
            project_cache,
            outcome_aggregator,
            test_store,
            upstream_relay,
            global_config,
        }
    }

    async fn project_cache_broker_setup(
        services: Services,
        buffer_guard: Arc<BufferGuard>,
        state_tx: mpsc::UnboundedSender<UpdateProjectState>,
        buffer_tx: mpsc::UnboundedSender<UnspooledEnvelope>,
    ) -> (ProjectCacheBroker, Addr<Buffer>) {
        let config: Arc<_> = Config::from_json_value(serde_json::json!({
            "spool": {
                "envelopes": {
                    "path": std::env::temp_dir().join(Uuid::new_v4().to_string()),
                    "max_memory_size": 0, // 0 bytes, to force to spool to disk all the envelopes.
                }
            }
        }))
        .unwrap()
        .into();
        let buffer_services = spooler::Services {
            outcome_aggregator: services.outcome_aggregator.clone(),
            project_cache: services.project_cache.clone(),
            test_store: services.test_store.clone(),
        };
        let buffer = match BufferService::create(
            buffer_guard.clone(),
            buffer_services,
            config.clone(),
        )
        .await
        {
            Ok(buffer) => buffer.start(),
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
                projects: hashbrown::HashMap::new(),
                garbage_disposal: GarbageDisposal::new(),
                source: ProjectSource::start(config, services.upstream_relay.clone(), None),
                services,
                state_tx,
                buffer_tx,
                buffer_guard,
                index: HashSet::new(),
                buffer: buffer.clone(),
                global_config: GlobalConfigStatus::Pending,
                buffer_unspool_handle: SleepHandle::idle(),
                buffer_unspool_backoff: RetryBackoff::new(Duration::from_millis(100)),
            },
            buffer,
        )
    }

    #[tokio::test]
    async fn always_spools() {
        relay_log::init_test!();

        let num_permits = 5;
        let buffer_guard: Arc<_> = BufferGuard::new(num_permits).into();
        let services = mocked_services();
        let (state_tx, _) = mpsc::unbounded_channel();
        let (buffer_tx, mut buffer_rx) = mpsc::unbounded_channel();
        let (mut broker, buffer_svc) =
            project_cache_broker_setup(services.clone(), buffer_guard.clone(), state_tx, buffer_tx)
                .await;

        for _ in 0..8 {
            let envelope = buffer_guard
                .enter(
                    empty_envelope(),
                    services.outcome_aggregator.clone(),
                    services.test_store.clone(),
                    ProcessingGroup::Ungrouped(Ungrouped),
                )
                .unwrap();
            let message = ValidateEnvelope { envelope };

            broker.handle_validate_envelope(message);
            tokio::time::sleep(Duration::from_millis(200)).await;
            // Nothing will be dequeued.
            assert!(buffer_rx.try_recv().is_err())
        }

        // All the messages should have been spooled to disk.
        assert_eq!(buffer_guard.available(), 5);
        assert_eq!(broker.index.len(), 1);

        let project_key = ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap();
        let key = QueueKey {
            own_key: project_key,
            sampling_key: project_key,
        };
        let (tx, mut rx) = mpsc::unbounded_channel();

        // Check if we can also dequeue from the buffer directly.
        buffer_svc.send(spooler::DequeueMany::new([key].into(), tx.clone()));
        tokio::time::sleep(Duration::from_millis(100)).await;

        // We should be able to unspool 5 envelopes since we have 5 permits.
        let mut envelopes = vec![];
        while let Ok(envelope) = rx.try_recv() {
            envelopes.push(envelope)
        }

        // We can unspool only 5 envelopes.
        assert_eq!(envelopes.len(), 5);

        // Drop one, and get one permit back.
        envelopes.pop().unwrap();
        assert_eq!(buffer_guard.available(), 1);

        // Till now we should have enqueued 5 envelopes and dequeued only 1, it means the index is
        // still populated with same keys and values.
        assert_eq!(broker.index.len(), 1);

        // Check if we can also dequeue from the buffer directly.
        buffer_svc.send(spooler::DequeueMany::new([key].into(), tx));
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Cannot dequeue anymore, no more available permits.
        assert!(rx.try_recv().is_err());

        // The rest envelopes will be immediately spooled, since we at 80% buffer gueard usage.
        for _ in 0..10 {
            let envelope = ManagedEnvelope::untracked(
                empty_envelope(),
                services.outcome_aggregator.clone(),
                services.test_store.clone(),
            );
            let message = ValidateEnvelope { envelope };

            broker.handle_validate_envelope(message);
            tokio::time::sleep(Duration::from_millis(100)).await;
            // Nothing will be dequeued.
            assert!(buffer_rx.try_recv().is_err())
        }
    }

    #[tokio::test]
    async fn periodic_unspool() {
        relay_log::init_test!();

        let num_permits = 50;
        let buffer_guard: Arc<_> = BufferGuard::new(num_permits).into();
        let services = mocked_services();
        let (state_tx, _) = mpsc::unbounded_channel();
        let (buffer_tx, mut buffer_rx) = mpsc::unbounded_channel();
        let (mut broker, _buffer_svc) =
            project_cache_broker_setup(services.clone(), buffer_guard.clone(), state_tx, buffer_tx)
                .await;

        broker.global_config = GlobalConfigStatus::Ready;
        let (tx_update, mut rx_update) = mpsc::unbounded_channel();
        let (tx_assert, mut rx_assert) = mpsc::unbounded_channel();

        let dsn1 = "111d836b15bb49d7bbf99e64295d995b";
        let dsn2 = "eeed836b15bb49d7bbf99e64295d995b";

        // Send and spool some envelopes.
        for dsn in [dsn1, dsn2] {
            let envelope = buffer_guard
                .enter(
                    empty_envelope_with_dsn(dsn),
                    services.outcome_aggregator.clone(),
                    services.test_store.clone(),
                    ProcessingGroup::Ungrouped(Ungrouped),
                )
                .unwrap();

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

                    Some(assert) = rx_assert.recv() => {
                        assert_eq!(broker.index.len(), assert);
                    },
                    Some(update) = rx_update.recv() => broker.merge_state(update),
                    () = &mut broker.buffer_unspool_handle => broker.handle_periodic_unspool(),
                }
            }
        });

        // Before updating any project states.
        tx_assert.send(2).unwrap();

        let update_dsn1_project_state = UpdateProjectState {
            project_key: ProjectKey::parse(dsn1).unwrap(),
            state: ProjectState::allowed().into(),
            no_cache: false,
        };

        tx_update.send(update_dsn1_project_state).unwrap();
        assert!(buffer_rx.recv().await.is_some());
        // One of the project should be unspooled.
        tx_assert.send(1).unwrap();

        // Schedule some work...
        tokio::time::sleep(Duration::from_secs(2)).await;

        let update_dsn2_project_state = UpdateProjectState {
            project_key: ProjectKey::parse(dsn2).unwrap(),
            state: ProjectState::allowed().into(),
            no_cache: false,
        };

        tx_update.send(update_dsn2_project_state).unwrap();
        assert!(buffer_rx.recv().await.is_some());
        // The last project should be unspooled.
        tx_assert.send(0).unwrap();
        // Make sure the last assert is tested.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn handle_processing_without_project() {
        let num_permits = 50;
        let buffer_guard: Arc<_> = BufferGuard::new(num_permits).into();
        let services = mocked_services();
        let (state_tx, _) = mpsc::unbounded_channel();
        let (buffer_tx, mut buffer_rx) = mpsc::unbounded_channel();
        let (mut broker, buffer_svc) = project_cache_broker_setup(
            services.clone(),
            buffer_guard.clone(),
            state_tx,
            buffer_tx.clone(),
        )
        .await;

        let dsn = "111d836b15bb49d7bbf99e64295d995b";
        let project_key = ProjectKey::parse(dsn).unwrap();
        let key = QueueKey {
            own_key: project_key,
            sampling_key: project_key,
        };
        let envelope = buffer_guard
            .enter(
                empty_envelope_with_dsn(dsn),
                services.outcome_aggregator.clone(),
                services.test_store.clone(),
                ProcessingGroup::Ungrouped(Ungrouped),
            )
            .unwrap();

        // Index and projects are empty.
        assert!(broker.projects.is_empty());
        assert!(broker.index.is_empty());

        // Since there is no project we should not process anything but create a project and spool
        // the envelope.
        broker.handle_processing(key, envelope);

        // Assert that we have a new project and also added an index.
        assert!(broker.projects.get(&project_key).is_some());
        assert!(broker.index.contains(&key));

        // Check is we actually spooled anything.
        buffer_svc.send(DequeueMany::new([key].into(), buffer_tx.clone()));
        let UnspooledEnvelope {
            key: unspooled_key,
            managed_envelope: _,
        } = buffer_rx.recv().await.unwrap();

        assert_eq!(key, unspooled_key);
    }
}
