use std::sync::Arc;
use std::time::Instant;

use actix::prelude::{Actor, Message, SyncArbiter};
use actix_web::ResponseError;
use failure::Fail;
use futures::compat::Future01CompatExt;

use relay_common::ProjectKey;
use relay_config::{Config, RelayMode};
use relay_metrics::{self, FlushBuckets, InsertMetrics, MergeBuckets};
use relay_quotas::RateLimits;
use relay_redis::RedisPool;
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, Interface, Sender, Service};
use tokio::sync::mpsc;

use crate::actors::outcome::DiscardReason;
use crate::actors::processor::ProcessEnvelope;
use crate::actors::project::{Project, ProjectState};
use crate::actors::project_local::LocalProjectSource;
use crate::actors::project_upstream::UpstreamProjectSource;
use crate::envelope::Envelope;
use crate::service::REGISTRY;
use crate::statsd::{RelayCounters, RelayGauges, RelayHistograms, RelayTimers};
use crate::utils::{self, EnvelopeContext, GarbageDisposal};

#[cfg(feature = "processing")]
use {crate::actors::project_redis::RedisProjectSource, relay_common::clone};

#[derive(Fail, Debug)]
pub enum ProjectError {
    #[fail(display = "failed to fetch project state from upstream")]
    FetchFailed,

    #[fail(display = "could not schedule project fetching")]
    ScheduleFailed,
}

impl ResponseError for ProjectError {}

/// Fetches a project state from one of the available sources.
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
pub struct UpdateProjectState {
    /// The public key to fetch the project by.
    project_key: ProjectKey,

    /// If true, all caches should be skipped and a fresh state should be computed.
    no_cache: bool,
}

impl UpdateProjectState {
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
    pub envelope: Option<(Envelope, EnvelopeContext)>,
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
    envelope: Envelope,
    context: EnvelopeContext,
}

impl CheckEnvelope {
    /// Uses a cached project state and checks the envelope.
    pub fn new(envelope: Envelope, context: EnvelopeContext) -> Self {
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
    envelope: Envelope,
    context: EnvelopeContext,
}

impl ValidateEnvelope {
    pub fn new(envelope: Envelope, context: EnvelopeContext) -> Self {
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

pub enum ProjectCache {
    // TODO(ja): Rename to RequestUpdate or FetchProjectState
    Update(UpdateProjectState),
    Get(
        GetProjectState,
        Sender<Result<Arc<ProjectState>, ProjectError>>,
    ),
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

impl FromMessage<UpdateProjectState> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: UpdateProjectState, _: ()) -> Self {
        Self::Update(message)
    }
}

impl FromMessage<GetProjectState> for ProjectCache {
    type Response = relay_system::AsyncResponse<Result<Arc<ProjectState>, ProjectError>>;

    fn from_message(
        message: GetProjectState,
        sender: Sender<Result<Arc<ProjectState>, ProjectError>>,
    ) -> Self {
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

#[derive(Clone, Debug)]
struct ProjectSource {
    config: Arc<Config>,
    local_source: actix::Addr<LocalProjectSource>,
    upstream_source: actix::Addr<UpstreamProjectSource>,
    #[cfg(feature = "processing")]
    redis_source: Option<actix::Addr<RedisProjectSource>>,
}

impl ProjectSource {
    pub fn new(config: Arc<Config>, _redis: Option<RedisPool>) -> Self {
        let local_source = LocalProjectSource::new(config.clone()).start();
        let upstream_source = UpstreamProjectSource::new(config.clone()).start();

        #[cfg(feature = "processing")]
        let redis_source = _redis.map(|pool| {
            SyncArbiter::start(
                config.cpu_concurrency(),
                clone!(config, || RedisProjectSource::new(
                    config.clone(),
                    pool.clone()
                )),
            )
        });

        Self {
            config,
            local_source,
            upstream_source,
            #[cfg(feature = "processing")]
            redis_source,
        }
    }

    async fn fetch(
        &self,
        project_key: ProjectKey,
        no_cache: bool,
    ) -> Result<ProjectStateResponse, ()> {
        // TODO(ja): Remove ProjectStateResponse
        let state_opt = self
            .local_source
            .send(FetchOptionalProjectState { project_key })
            .compat()
            .await
            .map_err(|_| ())?;

        if let Some(state) = state_opt {
            return Ok(ProjectStateResponse::new(state));
        }

        match self.config.relay_mode() {
            RelayMode::Proxy => {
                return Ok(ProjectStateResponse::new(Arc::new(ProjectState::allowed())));
            }
            RelayMode::Static => {
                return Ok(ProjectStateResponse::new(Arc::new(ProjectState::missing())));
            }
            RelayMode::Capture => {
                return Ok(ProjectStateResponse::new(Arc::new(ProjectState::allowed())));
            }
            RelayMode::Managed => {
                // Proceed with loading the config from redis or upstream
            }
        }

        #[cfg(not(feature = "processing"))]
        let state_opt = None;

        #[cfg(feature = "processing")]
        let state_opt = if let Some(ref redis_source) = self.redis_source {
            redis_source
                .send(FetchOptionalProjectState { project_key })
                .compat()
                .await
                .map_err(|_| ())?
        } else {
            None
        };

        if let Some(state) = state_opt {
            return Ok(ProjectStateResponse::new(state));
        }

        Ok(self
            .upstream_source
            .send(FetchProjectState {
                project_key,
                no_cache,
            })
            .compat()
            .await
            .map_err(|_| ())??)
    }
}

/// TODO(ja): Doc
struct UpdateProjectState2 {
    /// The public key to fetch the project by.
    project_key: ProjectKey,

    /// TODO(ja): Doc
    state: Arc<ProjectState>,

    /// If true, all caches should be skipped and a fresh state should be computed.
    no_cache: bool,
}

pub struct ProjectCacheService {
    config: Arc<Config>,
    projects: hashbrown::HashMap<ProjectKey, Project>, // need hashbrown because drain_filter is not stable in std yet
    garbage_disposal: GarbageDisposal<Project>,
    source: ProjectSource,
    inner: (
        mpsc::UnboundedSender<UpdateProjectState2>,
        mpsc::UnboundedReceiver<UpdateProjectState2>,
    ),
}

impl ProjectCacheService {
    pub fn new(config: Arc<Config>, redis: Option<RedisPool>) -> Self {
        Self {
            config,
            projects: hashbrown::HashMap::new(),
            garbage_disposal: GarbageDisposal::new(),
            source: ProjectSource::new(config, redis),
            inner: mpsc::unbounded_channel(),
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

    fn merge_state(&mut self, message: UpdateProjectState2) {
        let UpdateProjectState2 {
            project_key,
            state,
            no_cache,
        } = message;

        self.get_or_create_project(project_key)
            .update_state(state, no_cache);
    }

    async fn handle_update(&mut self, message: UpdateProjectState) {
        let UpdateProjectState {
            project_key,
            no_cache,
        } = message;

        // Bump the update time of the project in our hashmap to evade eviction.
        self.get_or_create_project(project_key)
            .refresh_updated_timestamp();

        let source = self.source.clone();
        let sender = self.inner.0.clone();

        tokio::spawn(async move {
            let state = match source.fetch(project_key, no_cache).await {
                Ok(response) => response.state,
                Err(()) => Arc::new(ProjectState::err()),
            };

            let message = UpdateProjectState2 {
                project_key,
                state,
                no_cache,
            };

            sender.send(message).ok();
        });
    }

    async fn handle_get(
        &mut self,
        message: GetProjectState,
    ) -> Result<Arc<ProjectState>, ProjectError> {
        self.get_or_create_project(message.project_key)
            .get_or_fetch_state(message.no_cache)
            .await // TODO(ja): This is evil
    }

    fn handle_get_cached(&mut self, message: GetCachedProjectState) -> Option<Arc<ProjectState>> {
        let project = self.get_or_create_project(message.project_key);
        project.get_or_fetch_state(false);
        project.valid_state()
    }

    fn handle_check_envelope(
        &mut self,
        message: CheckEnvelope,
    ) -> Result<CheckedEnvelope, DiscardReason> {
        let project = self.get_or_create_project(message.envelope.meta().public_key());

        // Preload the project cache so that it arrives a little earlier in processing. However,
        // do not pass `no_cache`. In case the project is rate limited, we do not want to force
        // a full reload. Fetching must not block the store request.
        project.get_or_fetch_state(false);

        project.check_envelope(message.envelope, message.context)
    }

    fn handle_validate_envelope(&mut self, message: ValidateEnvelope) {
        // Preload the project cache for dynamic sampling in parallel to the main one.
        if let Some(sampling_key) = utils::get_sampling_key(&message.envelope) {
            self.get_or_create_project(sampling_key)
                .get_or_fetch_state(message.envelope.meta().no_cache());
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
        // TODO
    }

    async fn handle_message(&mut self, message: ProjectCache) {
        match message {
            ProjectCache::Update(message) => self.handle_update(message).await,
            // TODO(ja): This await is evil.
            ProjectCache::Get(message, sender) => sender.send(self.handle_get(message).await),
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
            relay_log::info!("project cache started");

            loop {
                tokio::select! {
                    biased;

                    Some(message) = self.inner.1.recv() => self.merge_state(message),
                    // context.run_interval(self.config.cache_eviction_interval(), |slf, _| {
                    //     slf.evict_stale_project_caches()
                    // });
                    Some(message) = rx.recv() => self.handle_message(message).await,
                    else => break,
                }
            }

            relay_log::info!("project cache stopped");
        });
    }
}

#[derive(Debug)]
pub struct ProjectStateResponse {
    pub state: Arc<ProjectState>,
}

impl ProjectStateResponse {
    pub fn new(state: Arc<ProjectState>) -> Self {
        ProjectStateResponse { state }
    }
}

#[derive(Clone)]
pub struct FetchProjectState {
    /// The public key to fetch the project by.
    pub project_key: ProjectKey,

    /// If true, all caches should be skipped and a fresh state should be computed.
    pub no_cache: bool,
}

impl Message for FetchProjectState {
    type Result = Result<ProjectStateResponse, ()>;
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

impl Message for FetchOptionalProjectState {
    type Result = Option<Arc<ProjectState>>;
}
