use std::sync::Arc;

use actix::{Actor, Message};
use actix_web::ResponseError;
use tokio::sync::mpsc;
use tokio::time::Instant;

use relay_common::ProjectKey;
use relay_config::{Config, RelayMode};
use relay_metrics::{self, FlushBuckets, InsertMetrics, MergeBuckets};
use relay_quotas::RateLimits;
use relay_redis::RedisPool;
use relay_statsd::metric;
use relay_system::{compat, Addr, FromMessage, Interface, Sender, Service};

use crate::actors::outcome::DiscardReason;
use crate::actors::processor::ProcessEnvelope;
use crate::actors::project::{Project, ProjectSender, ProjectState};
use crate::actors::project_local::LocalProjectSource;
use crate::actors::project_upstream::UpstreamProjectSource;
use crate::envelope::Envelope;
use crate::service::REGISTRY;
use crate::statsd::{RelayCounters, RelayGauges, RelayHistograms, RelayTimers};
use crate::utils::{self, EnvelopeContext, GarbageDisposal};

#[cfg(feature = "processing")]
use crate::actors::project_redis::RedisProjectSource;

#[derive(Clone, Debug, thiserror::Error)]
pub enum ProjectError {
    #[error("could not schedule project fetching")]
    ScheduleFailed,
}

impl ResponseError for ProjectError {}

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
#[derive(Clone)]
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
    pub envelope: Option<(Box<Envelope>, EnvelopeContext)>,
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
    envelope: Box<Envelope>,
    context: EnvelopeContext,
}

impl CheckEnvelope {
    /// Uses a cached project state and checks the envelope.
    pub fn new(envelope: Box<Envelope>, context: EnvelopeContext) -> Self {
        Self { envelope, context }
    }
}

/// Validates the envelope against project configuration and rate limits.
///
/// This ensures internally that the project state is up to date and then runs the same checks as
/// [`CheckEnvelope`]. Once the envelope has been validated, remaining items are forwarded to the
/// next stage:
///
///  - If the envelope needs dynamic sampling, this sends [`AddSamplingState`] to the
///    [`ProjectCache`] to add the required project state.
///  - Otherwise, the envelope is directly submitted to the [`EnvelopeProcessor`].
///
/// [`EnvelopeProcessor`]: crate::actors::processor::EnvelopeProcessor
pub struct ValidateEnvelope {
    envelope: Box<Envelope>,
    context: EnvelopeContext,
}

impl ValidateEnvelope {
    pub fn new(envelope: Box<Envelope>, context: EnvelopeContext) -> Self {
        Self { envelope, context }
    }
}

/// Adds the project state for dynamic sampling and sends the envelope to processing.
///
/// If the project state is up to date, the envelope will be immediately submitted for processing.
/// Otherwise, this queues the envelope and flushes it when the project has been updated.
///
/// This message will trigger an update of the project state internally if the state is stale or
/// outdated.
pub struct AddSamplingState {
    project_key: ProjectKey,
    message: ProcessEnvelope,
}

impl AddSamplingState {
    pub fn new(project_key: ProjectKey, message: ProcessEnvelope) -> Self {
        Self {
            project_key,
            message,
        }
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
    AddSamplingState(AddSamplingState),
    UpdateRateLimits(UpdateRateLimits),
    InsertMetrics(InsertMetrics),
    MergeBuckets(MergeBuckets),
    FlushBuckets(FlushBuckets),
}

impl ProjectCache {
    pub fn from_registry() -> Addr<Self> {
        REGISTRY.get().unwrap().project_cache.clone()
    }
}

impl Interface for ProjectCache {}

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

impl FromMessage<AddSamplingState> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: AddSamplingState, _: ()) -> Self {
        Self::AddSamplingState(message)
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
    local_source: actix::Addr<LocalProjectSource>,
    upstream_source: actix::Addr<UpstreamProjectSource>,
    #[cfg(feature = "processing")]
    redis_source: Option<RedisProjectSource>,
}

impl ProjectSource {
    pub fn new(config: Arc<Config>, _redis: Option<RedisPool>) -> Self {
        let local_source = LocalProjectSource::new(config.clone()).start();
        let upstream_source = UpstreamProjectSource::new(config.clone()).start();

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
        let state_opt = compat::send(self.local_source, FetchOptionalProjectState { project_key })
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

        compat::send(
            self.upstream_source,
            FetchProjectState {
                project_key,
                no_cache,
            },
        )
        .await
        .map_err(|_| ())?
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

/// Service implementing the [`ProjectCache`] interface.
pub struct ProjectCacheService {
    config: Arc<Config>,
    // need hashbrown because drain_filter is not stable in std yet
    projects: hashbrown::HashMap<ProjectKey, Project>,
    garbage_disposal: GarbageDisposal<Project>,
    source: ProjectSource,
    state_tx: mpsc::UnboundedSender<UpdateProjectState>,
    state_rx: mpsc::UnboundedReceiver<UpdateProjectState>,
}

impl ProjectCacheService {
    pub fn new(config: Arc<Config>, redis: Option<RedisPool>) -> Self {
        let (state_tx, state_rx) = mpsc::unbounded_channel();
        Self {
            config: config.clone(),
            projects: hashbrown::HashMap::new(),
            garbage_disposal: GarbageDisposal::new(),
            source: ProjectSource::new(config, redis),
            state_tx,
            state_rx,
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
        for (_, project) in expired {
            self.garbage_disposal.dispose(project);
            count += 1;
        }
        metric!(counter(RelayCounters::EvictingStaleProjectCaches) += count);

        // Log garbage queue size:
        let queue_size = self.garbage_disposal.queue_size() as f64;
        relay_statsd::metric!(gauge(RelayGauges::ProjectCacheGarbageQueueSize) = queue_size);

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

    fn merge_state(&mut self, message: UpdateProjectState) {
        let UpdateProjectState {
            project_key,
            state,
            no_cache,
        } = message;

        self.get_or_create_project(project_key)
            .update_state(state, no_cache);
    }

    fn handle_request_update(&mut self, message: RequestUpdate) {
        let RequestUpdate {
            project_key,
            no_cache,
        } = message;

        // Bump the update time of the project in our hashmap to evade eviction.
        self.get_or_create_project(project_key)
            .refresh_updated_timestamp();

        let source = self.source.clone();
        let sender = self.state_tx.clone();

        tokio::spawn(async move {
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
        self.get_or_create_project(message.project_key)
            .get_state(sender, message.no_cache);
    }

    fn handle_get_cached(&mut self, message: GetCachedProjectState) -> Option<Arc<ProjectState>> {
        self.get_or_create_project(message.project_key)
            .get_cached_state(false)
    }

    fn handle_check_envelope(
        &mut self,
        message: CheckEnvelope,
    ) -> Result<CheckedEnvelope, DiscardReason> {
        let project = self.get_or_create_project(message.envelope.meta().public_key());

        // Preload the project cache so that it arrives a little earlier in processing. However,
        // do not pass `no_cache`. In case the project is rate limited, we do not want to force
        // a full reload. Fetching must not block the store request.
        project.prefetch(false);

        project.check_envelope(message.envelope, message.context)
    }

    fn handle_validate_envelope(&mut self, message: ValidateEnvelope) {
        // Preload the project cache for dynamic sampling in parallel to the main one.
        if let Some(sampling_key) = utils::get_sampling_key(&message.envelope) {
            self.get_or_create_project(sampling_key)
                .prefetch(message.envelope.meta().no_cache());
        }

        self.get_or_create_project(message.envelope.meta().public_key())
            .enqueue_validation(message.envelope, message.context);
    }

    fn handle_add_sampling_state(&mut self, message: AddSamplingState) {
        self.get_or_create_project(message.project_key)
            .enqueue_sampling(message.message);
    }

    fn handle_rate_limits(&mut self, message: UpdateRateLimits) {
        self.get_or_create_project(message.project_key)
            .merge_rate_limits(message.rate_limits);
    }

    fn handle_insert_metrics(&mut self, message: InsertMetrics) {
        // Only keep if we have an aggregator, otherwise drop because we know that we were disabled.
        self.get_or_create_project(message.project_key())
            .insert_metrics(message.metrics());
    }

    fn handle_merge_buckets(&mut self, message: MergeBuckets) {
        // Only keep if we have an aggregator, otherwise drop because we know that we were disabled.
        self.get_or_create_project(message.project_key())
            .merge_buckets(message.buckets());
    }

    fn handle_flush_buckets(&mut self, message: FlushBuckets) {
        self.get_or_create_project(message.project_key)
            .flush_buckets(message.partition_key, message.buckets);
    }

    async fn handle_message(&mut self, message: ProjectCache) {
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
            ProjectCache::AddSamplingState(message) => self.handle_add_sampling_state(message),
            ProjectCache::UpdateRateLimits(message) => self.handle_rate_limits(message),
            ProjectCache::InsertMetrics(message) => self.handle_insert_metrics(message),
            ProjectCache::MergeBuckets(message) => self.handle_merge_buckets(message),
            ProjectCache::FlushBuckets(message) => self.handle_flush_buckets(message),
        }
    }
}

impl Service for ProjectCacheService {
    type Interface = ProjectCache;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.config.cache_eviction_interval());
            relay_log::info!("project cache started");

            loop {
                tokio::select! {
                    biased;

                    Some(message) = self.state_rx.recv() => self.merge_state(message),
                    _ = ticker.tick() => self.evict_stale_project_caches(),
                    Some(message) = rx.recv() => self.handle_message(message).await,
                    else => break,
                }
            }

            relay_log::info!("project cache stopped");
        });
    }
}

#[derive(Clone)]
pub struct FetchProjectState {
    /// The public key to fetch the project by.
    pub project_key: ProjectKey,

    /// If true, all caches should be skipped and a fresh state should be computed.
    pub no_cache: bool,
}

// TODO: Remove once `UpstreamProjectSource` was moved to tokio
impl Message for FetchProjectState {
    type Result = Result<Arc<ProjectState>, ()>;
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

// TODO: Remove once `RedisProjectSource` and `LocalProjectSource` were moved to tokio
impl Message for FetchOptionalProjectState {
    type Result = Option<Arc<ProjectState>>;
}
