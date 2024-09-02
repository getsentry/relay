use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use crate::extractors::RequestMeta;
use crate::services::buffer::{EnvelopeBuffer, EnvelopeBufferError};
use crate::services::processor::{
    EncodeMetrics, EnvelopeProcessor, MetricData, ProcessEnvelope, ProcessingGroup, ProjectMetrics,
};
use crate::services::project::state::UpstreamProjectState;
use crate::Envelope;
use chrono::{DateTime, Utc};
use hashbrown::HashSet;
use relay_base_schema::project::ProjectKey;
use relay_config::{Config, RelayMode};
use relay_metrics::{Bucket, MetricMeta};
use relay_quotas::RateLimits;
use relay_redis::RedisPool;
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, Interface, Sender, Service};
use tokio::sync::mpsc;
#[cfg(feature = "processing")]
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::services::global_config::{self, GlobalConfigManager, Subscribe};
use crate::services::metrics::{Aggregator, FlushBuckets};
use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::services::project::{Project, ProjectFetchState, ProjectSender, ProjectState};
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
use crate::utils::{GarbageDisposal, ManagedEnvelope, MemoryChecker, RetryBackoff, SleepHandle};

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
    pub project_key: ProjectKey,
    /// If true, all caches should be skipped and a fresh state should be computed.
    pub no_cache: bool,
    /// Previously cached fetch state, if available.
    ///
    /// The upstream request will include the revision of the currently cached state,
    /// if the upstream does not have a different revision, this cached
    /// state is re-used and its expiry bumped.
    pub cached_state: ProjectFetchState,
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
///    date, the envelopes is spooled and we continue when the state is fetched.
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

/// Source information where a metric bucket originates from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum BucketSource {
    /// The metric bucket originated from an internal Relay use case.
    ///
    /// The metric bucket originates either from within the same Relay
    /// or was accepted coming from another Relay which is registered as
    /// an internal Relay via Relay's configuration.
    Internal,
    /// The bucket source originated from an untrusted source.
    ///
    /// Managed Relays sending extracted metrics are considered external,
    /// it's a project use case but it comes from an untrusted source.
    External,
}

impl From<&RequestMeta> for BucketSource {
    fn from(value: &RequestMeta) -> Self {
        if value.is_from_internal_relay() {
            Self::Internal
        } else {
            Self::External
        }
    }
}

/// Starts the processing flow for received metrics.
///
/// Enriches the raw data with projcet information and forwards
/// the metrics using [`ProcessProjectMetrics`](crate::services::processor::ProcessProjectMetrics).
#[derive(Debug)]
pub struct ProcessMetrics {
    /// A list of metric items.
    pub data: MetricData,
    /// The target project.
    pub project_key: ProjectKey,
    /// Whether to keep or reset the metric metadata.
    pub source: BucketSource,
    /// The instant at which the request was received.
    pub start_time: Instant,
    /// The value of the Envelope's [`sent_at`](crate::envelope::Envelope::sent_at)
    /// header for clock drift correction.
    pub sent_at: Option<DateTime<Utc>>,
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

/// Handle an envelope that was popped from the envelope buffer.
pub struct DequeuedEnvelope(pub Box<Envelope>);

/// A request to update a project, typically sent by the envelope buffer.
///
/// This message is similar to [`GetProjectState`], except it has no `no_cache` option
/// and it does not send a response, but sends a signal back to the buffer instead.
pub struct UpdateProject(pub ProjectKey);

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
/// inspect contents of envelopes for ingestion, as well as [`ProcessMetrics`] to aggregate metrics
/// associated with a project.
///
/// See the enumerated variants for a full list of available messages for this service.
pub enum ProjectCache {
    RequestUpdate(RequestUpdate),
    Get(GetProjectState, ProjectSender),
    GetCached(GetCachedProjectState, Sender<ProjectState>),
    CheckEnvelope(
        CheckEnvelope,
        Sender<Result<CheckedEnvelope, DiscardReason>>,
    ),
    ValidateEnvelope(ValidateEnvelope),
    UpdateRateLimits(UpdateRateLimits),
    ProcessMetrics(ProcessMetrics),
    AddMetricMeta(AddMetricMeta),
    FlushBuckets(FlushBuckets),
    UpdateSpoolIndex(UpdateSpoolIndex),
    SpoolHealth(Sender<bool>),
    RefreshIndexCache(RefreshIndexCache),
    HandleDequeuedEnvelope(Box<Envelope>, Sender<()>),
    UpdateProject(ProjectKey),
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
            Self::ProcessMetrics(_) => "ProcessMetrics",
            Self::AddMetricMeta(_) => "AddMetricMeta",
            Self::FlushBuckets(_) => "FlushBuckets",
            Self::UpdateSpoolIndex(_) => "UpdateSpoolIndex",
            Self::SpoolHealth(_) => "SpoolHealth",
            Self::RefreshIndexCache(_) => "RefreshIndexCache",
            Self::HandleDequeuedEnvelope(_, _) => "HandleDequeuedEnvelope",
            Self::UpdateProject(_) => "UpdateProject",
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
    type Response = relay_system::BroadcastResponse<ProjectState>;

    fn from_message(message: GetProjectState, sender: ProjectSender) -> Self {
        Self::Get(message, sender)
    }
}

impl FromMessage<GetCachedProjectState> for ProjectCache {
    type Response = relay_system::AsyncResponse<ProjectState>;

    fn from_message(message: GetCachedProjectState, sender: Sender<ProjectState>) -> Self {
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

impl FromMessage<ProcessMetrics> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: ProcessMetrics, _: ()) -> Self {
        Self::ProcessMetrics(message)
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

impl FromMessage<DequeuedEnvelope> for ProjectCache {
    type Response = relay_system::AsyncResponse<()>;

    fn from_message(message: DequeuedEnvelope, sender: Sender<()>) -> Self {
        let DequeuedEnvelope(envelope) = message;
        Self::HandleDequeuedEnvelope(envelope, sender)
    }
}

impl FromMessage<UpdateProject> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: UpdateProject, _: ()) -> Self {
        let UpdateProject(project_key) = message;
        Self::UpdateProject(project_key)
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
    #[cfg(feature = "processing")]
    redis_semaphore: Arc<Semaphore>,
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
        let redis_maxconns = config.redis().map(|configs| {
            let opts = match configs {
                relay_config::RedisPoolConfigs::Unified((_, opts)) => opts,
                relay_config::RedisPoolConfigs::Individual {
                    project_configs: (_, opts),
                    ..
                } => opts,
            };
            opts.max_connections
        });
        #[cfg(feature = "processing")]
        let redis_source = _redis.map(|pool| RedisProjectSource::new(config.clone(), pool));

        Self {
            config,
            local_source,
            upstream_source,
            #[cfg(feature = "processing")]
            redis_source,
            #[cfg(feature = "processing")]
            redis_semaphore: Arc::new(Semaphore::new(
                redis_maxconns.unwrap_or(10).try_into().unwrap(),
            )),
        }
    }

    async fn fetch(
        self,
        project_key: ProjectKey,
        no_cache: bool,
        cached_state: ProjectFetchState,
    ) -> Result<ProjectFetchState, ()> {
        let state_opt = self
            .local_source
            .send(FetchOptionalProjectState { project_key })
            .await
            .map_err(|_| ())?;

        if let Some(state) = state_opt {
            return Ok(ProjectFetchState::new(state));
        }

        match self.config.relay_mode() {
            RelayMode::Proxy => return Ok(ProjectFetchState::allowed()),
            RelayMode::Static => return Ok(ProjectFetchState::disabled()),
            RelayMode::Capture => return Ok(ProjectFetchState::allowed()),
            RelayMode::Managed => (), // Proceed with loading the config from redis or upstream
        }

        let current_revision = cached_state.revision().map(String::from);
        #[cfg(feature = "processing")]
        if let Some(redis_source) = self.redis_source {
            let current_revision = current_revision.clone();

            let redis_permit = self.redis_semaphore.acquire().await.map_err(|_| ())?;
            let state_fetch_result = tokio::task::spawn_blocking(move || {
                redis_source.get_config_if_changed(project_key, current_revision.as_deref())
            })
            .await
            .map_err(|_| ())?;
            drop(redis_permit);

            match state_fetch_result {
                // New state fetched from Redis, possibly pending.
                Ok(UpstreamProjectState::New(state)) => {
                    let state = state.sanitized();
                    if !state.is_pending() {
                        return Ok(ProjectFetchState::new(state));
                    }
                }
                // Redis reported that we're holding an up-to-date version of the state already,
                // refresh the state and return the old cached state again.
                Ok(UpstreamProjectState::NotModified) => {
                    return Ok(ProjectFetchState::refresh(cached_state))
                }
                Err(error) => {
                    relay_log::error!(
                        error = &error as &dyn Error,
                        "failed to fetch project from Redis",
                    );
                }
            };
        };

        let state = self
            .upstream_source
            .send(FetchProjectState {
                project_key,
                current_revision,
                no_cache,
            })
            .await
            .map_err(|_| ())?;

        match state {
            UpstreamProjectState::New(state) => Ok(ProjectFetchState::new(state.sanitized())),
            UpstreamProjectState::NotModified => Ok(ProjectFetchState::refresh(cached_state)),
        }
    }
}

/// Updates the cache with new project state information.
struct UpdateProjectState {
    /// The public key to fetch the project by.
    project_key: ProjectKey,

    /// New project state information.
    state: ProjectFetchState,

    /// If true, all caches should be skipped and a fresh state should be computed.
    no_cache: bool,
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
    pub upstream_relay: Addr<UpstreamRelay>,
    pub global_config: Addr<GlobalConfigManager>,
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
    // Need hashbrown because extract_if is not stable in std yet.
    projects: hashbrown::HashMap<ProjectKey, Project>,
    /// Utility for disposing of expired project data in a background thread.
    garbage_disposal: GarbageDisposal<ProjectGarbage>,
    /// Source for fetching project states from the upstream or from disk.
    source: ProjectSource,
    /// Tx channel used to send the updated project state whenever requested.
    state_tx: mpsc::UnboundedSender<UpdateProjectState>,

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
            if let Some(spool_v1) = self.spool_v1.as_mut() {
                let keys = spool_v1
                    .index
                    .extract_if(|key| key.own_key == project_key || key.sampling_key == project_key)
                    .collect::<BTreeSet<_>>();

                if !keys.is_empty() {
                    spool_v1.buffer.send(RemoveMany::new(project_key, keys))
                }
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
        let envelope_processor = self.services.envelope_processor.clone();

        let old_state = self.get_or_create_project(project_key).update_state(
            &project_cache,
            state,
            &envelope_processor,
            no_cache,
        );
        if let Some(old_state) = old_state {
            self.garbage_disposal.dispose(old_state);
        }

        // Try to schedule unspool if it's not scheduled yet.
        self.schedule_unspool();

        if let Some(envelope_buffer) = self.services.envelope_buffer.as_ref() {
            envelope_buffer.send(EnvelopeBuffer::Ready(project_key))
        };
    }

    fn handle_request_update(&mut self, message: RequestUpdate) {
        let RequestUpdate {
            project_key,
            no_cache,
            cached_state,
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
                .fetch(project_key, no_cache, cached_state)
                .await
                .unwrap_or_else(|()| ProjectFetchState::disabled());

            let message = UpdateProjectState {
                project_key,
                no_cache,
                state,
            };

            sender.send(message).ok();
        });
    }

    fn handle_get(&mut self, message: GetProjectState, sender: ProjectSender) {
        let GetProjectState {
            project_key,
            no_cache,
        } = message;
        let project_cache = self.services.project_cache.clone();
        let project = self.get_or_create_project(project_key);

        project.get_state(project_cache, sender, no_cache);
    }

    fn handle_get_cached(&mut self, message: GetCachedProjectState) -> ProjectState {
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
        let project_key = context.envelope().meta().public_key();
        if let Some(sampling_key) = context.envelope().sampling_key() {
            if sampling_key != project_key {
                let sampling_project = self.get_or_create_project(sampling_key);
                sampling_project.prefetch(project_cache.clone(), false);
            }
        }
        let project = self.get_or_create_project(project_key);

        // Preload the project cache so that it arrives a little earlier in processing. However,
        // do not pass `no_cache`. In case the project is rate limited, we do not want to force
        // a full reload. Fetching must not block the store request.
        project.prefetch(project_cache, false);

        project.check_envelope(context)
    }

    /// Handles the processing of the provided envelope.
    fn handle_processing(&mut self, key: QueueKey, mut managed_envelope: ManagedEnvelope) {
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

        let project_cache = self.services.project_cache.clone();
        let project_info = match project.get_cached_state(project_cache.clone(), false) {
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
            let reservoir_counters = project.reservoir_counters();

            let sampling_project_info = managed_envelope
                .envelope()
                .sampling_key()
                .and_then(|key| self.projects.get_mut(&key))
                .and_then(|p| p.get_cached_state(project_cache, false).enabled())
                .filter(|state| state.organization_id == project_info.organization_id);

            let process = ProcessEnvelope {
                envelope: managed_envelope,
                project_info,
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

        let project_cache = self.services.project_cache.clone();
        let envelope = managed_envelope.envelope();

        // Fetch the project state for our key and make sure it's not invalid.
        let own_key = envelope.meta().public_key();
        let project = self.get_or_create_project(own_key);

        let project_state =
            project.get_cached_state(project_cache.clone(), envelope.meta().no_cache());

        let project_state = match project_state {
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
        let sampling_state = if let Some(sampling_key) = sampling_key {
            let state = self
                .get_or_create_project(sampling_key)
                .get_cached_state(project_cache, envelope.meta().no_cache());
            match state {
                ProjectState::Enabled(state) => Some(state),
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
            && (sampling_state.is_some() || !requires_sampling_state)
            && self.memory_checker.check_memory().has_capacity()
            && self.global_config.is_ready()
        {
            // TODO: Add ready project infos to the processing message.
            relay_log::trace!("Sending envelope to processor");
            return self.handle_processing(key, managed_envelope);
        }

        relay_log::trace!("Enqueueing envelope");
        self.enqueue(key, managed_envelope);
    }

    fn handle_rate_limits(&mut self, message: UpdateRateLimits) {
        self.get_or_create_project(message.project_key)
            .merge_rate_limits(message.rate_limits);
    }

    fn handle_process_metrics(&mut self, message: ProcessMetrics) {
        let project_cache = self.services.project_cache.clone();

        let message = self
            .get_or_create_project(message.project_key)
            .prefetch(project_cache, false)
            .process_metrics(message);

        self.services.envelope_processor.send(message);
    }

    fn handle_add_metric_meta(&mut self, message: AddMetricMeta) {
        let envelope_processor = self.services.envelope_processor.clone();

        self.get_or_create_project(message.project_key)
            .add_metric_meta(message.meta, envelope_processor);
    }

    fn handle_flush_buckets(&mut self, message: FlushBuckets) {
        let aggregator = self.services.aggregator.clone();
        let project_cache = self.services.project_cache.clone();

        let mut no_project = 0;
        let mut scoped_buckets = BTreeMap::new();
        for (project_key, buckets) in message.buckets {
            let project = self.get_or_create_project(project_key);

            let project_info = match project.current_state() {
                ProjectState::Pending => {
                    no_project += 1;
                    // Schedule an update for the project just in case.
                    project.prefetch(project_cache.clone(), false);
                    project.return_buckets(&aggregator, buckets);
                    continue;
                }
                ProjectState::Disabled => {
                    // Project loaded and disabled, discard the buckets.
                    //
                    // Ideally we log outcomes for the metrics here, but currently for metric
                    // outcomes we need a valid scoping, which we cannot construct for disabled
                    // projects.
                    self.garbage_disposal.dispose(buckets);
                    continue;
                }
                ProjectState::Enabled(project_info) => project_info,
            };

            let Some(scoping) = project.scoping() else {
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
                        project_info,
                        rate_limits: project.current_rate_limits().clone(),
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

    fn handle_spool_health(&self, sender: Sender<bool>) {
        match &self.spool_v1 {
            Some(spool_v1) => spool_v1.buffer.send(spooler::Health(sender)),
            None => sender.send(true), // TODO
        }
    }

    fn handle_refresh_index_cache(&mut self, message: RefreshIndexCache) {
        let RefreshIndexCache(index) = message;
        let project_cache = self.services.project_cache.clone();

        for key in index {
            let spool_v1 = self.spool_v1.as_mut().expect("no V1 spool configured");
            spool_v1.index.insert(key);
            self.get_or_create_project(key.own_key)
                .prefetch(project_cache.clone(), false);
            if key.own_key != key.sampling_key {
                self.get_or_create_project(key.sampling_key)
                    .prefetch(project_cache.clone(), false);
            }
        }
    }

    fn handle_dequeued_envelope(
        &mut self,
        envelope: Box<Envelope>,
        envelope_buffer: Addr<EnvelopeBuffer>,
    ) -> Result<(), EnvelopeBufferError> {
        if envelope.meta().start_time().elapsed() > self.config.spool_envelopes_max_age() {
            let mut managed_envelope = ManagedEnvelope::new(
                envelope,
                self.services.outcome_aggregator.clone(),
                self.services.test_store.clone(),
                ProcessingGroup::Ungrouped,
            );
            managed_envelope.reject(Outcome::Invalid(DiscardReason::Timestamp));
            return Ok(());
        }
        let sampling_key = envelope.sampling_key();
        let services = self.services.clone();

        let own_key = envelope.meta().public_key();
        let project = self.get_or_create_project(own_key);
        let project_state = project.get_cached_state(services.project_cache.clone(), false);

        // Check if project config is enabled.
        let project_info = match project_state {
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
                self.get_or_create_project(sampling_key)
                    .get_cached_state(services.project_cache, false),
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

        let project = self.get_or_create_project(own_key);

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

            let reservoir_counters = project.reservoir_counters();
            services.envelope_processor.send(ProcessEnvelope {
                envelope: managed_envelope,
                project_info: project_info.clone(),
                sampling_project_info: sampling_project_info.clone(),
                reservoir_counters,
            });
        }

        Ok(())
    }

    fn handle_update_project(&mut self, project_key: ProjectKey) {
        let project_cache = self.services.project_cache.clone();
        let envelope_buffer = self.services.envelope_buffer.clone();
        let project = self.get_or_create_project(project_key);

        // If the project is already loaded, inform the envelope buffer.
        if !project.current_state().is_pending() {
            if let Some(envelope_buffer) = envelope_buffer {
                envelope_buffer.send(EnvelopeBuffer::Ready(project_key));
            }
        }

        let no_cache = false;
        project.prefetch(project_cache, no_cache);
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
        key.unique_keys().iter().all(|key| {
            self.projects.get_mut(key).is_some_and(|project| {
                // Returns `Some` if the project is cached otherwise None and also triggers refresh
                // in background.
                !project
                    .get_cached_state(self.services.project_cache.clone(), false)
                    .is_pending()
            })
        })
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
                    ProjectCache::ProcessMetrics(message) => self.handle_process_metrics(message),
                    ProjectCache::AddMetricMeta(message) => self.handle_add_metric_meta(message),
                    ProjectCache::FlushBuckets(message) => self.handle_flush_buckets(message),
                    ProjectCache::UpdateSpoolIndex(message) => self.handle_buffer_index(message),
                    ProjectCache::SpoolHealth(sender) => self.handle_spool_health(sender),
                    ProjectCache::RefreshIndexCache(message) => {
                        self.handle_refresh_index_cache(message)
                    }
                    ProjectCache::HandleDequeuedEnvelope(message, sender) => {
                        let envelope_buffer = self
                            .services
                            .envelope_buffer
                            .clone()
                            .expect("Called HandleDequeuedEnvelope without an envelope buffer");

                        if let Err(e) = self.handle_dequeued_envelope(message, envelope_buffer) {
                            relay_log::error!(
                                error = &e as &dyn std::error::Error,
                                "Failed to handle popped envelope"
                            );
                        }
                        // Return response to signal readiness for next envelope:
                        sender.send(())
                    }
                    ProjectCache::UpdateProject(project) => self.handle_update_project(project),
                }
            }
        )
    }
}

/// Service implementing the [`ProjectCache`] interface.
#[derive(Debug)]
pub struct ProjectCacheService {
    config: Arc<Config>,
    memory_checker: MemoryChecker,
    services: Services,
    redis: Option<RedisPool>,
}

impl ProjectCacheService {
    /// Creates a new `ProjectCacheService`.
    pub fn new(
        config: Arc<Config>,
        memory_checker: MemoryChecker,
        services: Services,
        redis: Option<RedisPool>,
    ) -> Self {
        Self {
            config,
            memory_checker,
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
            memory_checker,
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

            // Main broker that serializes public and internal messages, and triggers project state
            // fetches via the project source.
            let mut broker = ProjectCacheBroker {
                config: config.clone(),
                memory_checker,
                projects: hashbrown::HashMap::new(),
                garbage_disposal: GarbageDisposal::new(),
                source: ProjectSource::start(
                    config.clone(),
                    services.upstream_relay.clone(),
                    redis,
                ),
                services,
                state_tx,
                spool_v1_unspool_handle: SleepHandle::idle(),
                spool_v1,
                global_config,
            };

            loop {
                tokio::select! {
                    biased;

                    Ok(()) = subscription.changed() => {
                        metric!(timer(RelayTimers::ProjectCacheTaskDuration), task = "update_global_config", {
                            match subscription.borrow().clone() {
                                global_config::Status::Ready(_) => broker.set_global_config_ready(),
                                // The watch should only be updated if it gets a new value.
                                // This would imply a logical bug.
                                global_config::Status::Pending => relay_log::error!("still waiting for the global config"),
                            }
                        })
                    },
                    Some(message) = state_rx.recv() => {
                        metric!(timer(RelayTimers::ProjectCacheTaskDuration), task = "merge_state", {
                            broker.merge_state(message)
                        })
                    }
                    // Buffer will not dequeue the envelopes from the spool if there is not enough
                    // permits in `BufferGuard` available. Currently this is 50%.
                    Some(UnspooledEnvelope { managed_envelope, key }) = buffer_rx.recv() => {
                        metric!(timer(RelayTimers::ProjectCacheTaskDuration), task = "handle_processing", {
                            broker.handle_processing(key, managed_envelope)
                        })
                    },
                    _ = ticker.tick() => {
                        metric!(timer(RelayTimers::ProjectCacheTaskDuration), task = "evict_project_caches", {
                            broker.evict_stale_project_caches()
                        })
                    }
                    () = &mut broker.spool_v1_unspool_handle => {
                        metric!(timer(RelayTimers::ProjectCacheTaskDuration), task = "periodic_unspool", {
                            broker.handle_periodic_unspool()
                        })
                    }
                    Some(message) = rx.recv() => {
                        metric!(timer(RelayTimers::ProjectCacheTaskDuration), task = "handle_message", {
                            broker.handle_message(message)
                        })
                    }
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

    /// Currently cached revision if available.
    ///
    /// The upstream is allowed to omit full project configs
    /// for requests for which the requester already has the most
    /// recent revision.
    ///
    /// Settings this to `None` will essentially always re-fetch
    /// the project config.
    pub current_revision: Option<String>,

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

/// Sum type for all objects which need to be discareded through the [`GarbageDisposal`].
#[derive(Debug)]
#[allow(dead_code)] // Fields are never read, only used for discarding/dropping data.
enum ProjectGarbage {
    Project(Project),
    ProjectFetchState(ProjectFetchState),
    Metrics(Vec<Bucket>),
}

impl From<Project> for ProjectGarbage {
    fn from(value: Project) -> Self {
        Self::Project(value)
    }
}

impl From<ProjectFetchState> for ProjectGarbage {
    fn from(value: ProjectFetchState) -> Self {
        Self::ProjectFetchState(value)
    }
}

impl From<Vec<Bucket>> for ProjectGarbage {
    fn from(value: Vec<Bucket>) -> Self {
        Self::Metrics(value)
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
        let (upstream_relay, _) = mock_service("upstream_relay", (), |&mut (), _| {});
        let (global_config, _) = mock_service("global_config", (), |&mut (), _| {});

        Services {
            envelope_buffer: None,
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
        state_tx: mpsc::UnboundedSender<UpdateProjectState>,
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
                memory_checker,
                projects: hashbrown::HashMap::new(),
                garbage_disposal: GarbageDisposal::new(),
                source: ProjectSource::start(config, services.upstream_relay.clone(), None),
                services,
                state_tx,
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
        let (state_tx, _) = mpsc::unbounded_channel();
        let (buffer_tx, mut buffer_rx) = mpsc::unbounded_channel();
        let (mut broker, _buffer_svc) =
            project_cache_broker_setup(services.clone(), state_tx, buffer_tx).await;

        broker.global_config = GlobalConfigStatus::Ready;
        let (tx_update, mut rx_update) = mpsc::unbounded_channel();
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

                    Some(assert) = rx_assert.recv() => {
                        assert_eq!(broker.spool_v1.as_ref().unwrap().index.len(), assert);
                    },
                    Some(update) = rx_update.recv() => broker.merge_state(update),
                    () = &mut broker.spool_v1_unspool_handle => broker.handle_periodic_unspool(),
                }
            }
        });

        // Before updating any project states.
        tx_assert.send(2).unwrap();

        let update_dsn1_project_state = UpdateProjectState {
            project_key: ProjectKey::parse(dsn1).unwrap(),
            state: ProjectFetchState::allowed(),
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
            state: ProjectFetchState::allowed(),
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
        let services = mocked_services();
        let (state_tx, _) = mpsc::unbounded_channel();
        let (buffer_tx, mut buffer_rx) = mpsc::unbounded_channel();
        let (mut broker, buffer_svc) =
            project_cache_broker_setup(services.clone(), state_tx, buffer_tx.clone()).await;

        let dsn = "111d836b15bb49d7bbf99e64295d995b";
        let project_key = ProjectKey::parse(dsn).unwrap();
        let key = QueueKey {
            own_key: project_key,
            sampling_key: project_key,
        };
        let envelope = ManagedEnvelope::new(
            empty_envelope_with_dsn(dsn),
            services.outcome_aggregator.clone(),
            services.test_store.clone(),
            ProcessingGroup::Ungrouped,
        );

        // Index and projects are empty.
        assert!(broker.projects.is_empty());
        assert!(broker.spool_v1.as_mut().unwrap().index.is_empty());

        // Since there is no project we should not process anything but create a project and spool
        // the envelope.
        broker.handle_processing(key, envelope);

        // Assert that we have a new project and also added an index.
        assert!(broker.projects.get(&project_key).is_some());
        assert!(broker.spool_v1.as_mut().unwrap().index.contains(&key));

        // Check is we actually spooled anything.
        buffer_svc.send(DequeueMany::new([key].into(), buffer_tx.clone()));
        let UnspooledEnvelope {
            managed_envelope, ..
        } = buffer_rx.recv().await.unwrap();

        assert_eq!(key, QueueKey::from_envelope(managed_envelope.envelope()));
    }
}
