use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;
use actix_web::ResponseError;
use failure::Fail;
use futures01::{future, Future};

use relay_common::ProjectKey;
use relay_config::{Config, RelayMode};
use relay_metrics::{self, AggregateMetricsError, Bucket, FlushBuckets, Metric};
use relay_quotas::{RateLimits, Scoping};
use relay_redis::RedisPool;
use relay_statsd::metric;

use crate::actors::envelopes::{EnvelopeManager, SendMetrics};
use crate::actors::outcome::DiscardReason;
use crate::actors::project::{Project, ProjectState};
use crate::actors::project_local::LocalProjectSource;
use crate::actors::project_upstream::UpstreamProjectSource;
use crate::envelope::Envelope;
use crate::statsd::{RelayCounters, RelayHistograms, RelayTimers};
use crate::utils::{ActorResponse, EnvelopeContext, Response};

use super::project::ExpiryState;

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

pub struct ProjectCache {
    config: Arc<Config>,
    projects: HashMap<ProjectKey, Project>,
    local_source: Addr<LocalProjectSource>,
    upstream_source: Addr<UpstreamProjectSource>,
    #[cfg(feature = "processing")]
    redis_source: Option<Addr<RedisProjectSource>>,
}

impl ProjectCache {
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

        ProjectCache {
            config,
            projects: HashMap::new(),
            local_source,
            upstream_source,
            #[cfg(feature = "processing")]
            redis_source,
        }
    }

    /// Evict projects that are over its expiry date.
    ///
    /// Ideally, we would use `check_expiry` to determine expiry here.
    /// However, for eviction, we want to add an additional delay, such that we do not delete
    /// a project that has expired recently and for which a fetch is already underway in
    /// [`super::project_upstream`].
    fn evict_stale_project_caches(&mut self) {
        metric!(counter(RelayCounters::EvictingStaleProjectCaches) += 1);
        let eviction_start = Instant::now();
        let delta = 2 * self.config.project_cache_expiry() + self.config.project_grace_period();

        self.projects
            .retain(|_, entry| entry.last_updated_at() + delta > eviction_start);

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
}

impl Actor for ProjectCache {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        // Set the mailbox size to the size of the envelope buffer. This is a rough estimate but
        // should ensure that we're not dropping messages if the main arbiter running this actor
        // gets hammered a bit.
        let mailbox_size = self.config.envelope_buffer_size() as usize;
        context.set_mailbox_capacity(mailbox_size);

        context.run_interval(self.config.cache_eviction_interval(), |slf, _| {
            slf.evict_stale_project_caches()
        });

        relay_log::info!("project cache started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        relay_log::info!("project cache stopped");
    }
}

impl Supervised for ProjectCache {}

impl SystemService for ProjectCache {}

impl Default for ProjectCache {
    fn default() -> Self {
        unimplemented!("register with the SystemRegistry instead")
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

impl Message for UpdateProjectState {
    type Result = ();
}

impl Handler<UpdateProjectState> for ProjectCache {
    type Result = ();

    fn handle(&mut self, message: UpdateProjectState, context: &mut Self::Context) -> Self::Result {
        let UpdateProjectState {
            project_key,
            no_cache,
        } = message;

        let project = self.get_or_create_project(project_key);

        // Bump the update time of the project in our hashmap to evade eviction.
        project.refresh_updated_timestamp();

        let relay_mode = self.config.relay_mode();

        let upstream_source = self.upstream_source.clone();
        #[cfg(feature = "processing")]
        let redis_source = self.redis_source.clone();

        self.local_source
            .send(FetchOptionalProjectState { project_key })
            .map_err(|_| ())
            .and_then(move |response| {
                if let Some(state) = response {
                    return Box::new(future::ok(ProjectStateResponse::new(state)))
                        as ResponseFuture<_, _>;
                }

                match relay_mode {
                    RelayMode::Proxy => {
                        return Box::new(future::ok(ProjectStateResponse::new(Arc::new(
                            ProjectState::allowed(),
                        ))));
                    }
                    RelayMode::Static => {
                        return Box::new(future::ok(ProjectStateResponse::new(Arc::new(
                            ProjectState::missing(),
                        ))));
                    }
                    RelayMode::Capture => {
                        return Box::new(future::ok(ProjectStateResponse::new(Arc::new(
                            ProjectState::allowed(),
                        ))));
                    }
                    RelayMode::Managed => {
                        // Proceed with loading the config from redis or upstream
                    }
                }

                #[cfg(not(feature = "processing"))]
                let fetch_redis = future::ok(None);

                #[cfg(feature = "processing")]
                let fetch_redis: ResponseFuture<_, _> = if let Some(ref redis_source) = redis_source
                {
                    Box::new(
                        redis_source
                            .send(FetchOptionalProjectState { project_key })
                            .map_err(|_| ()),
                    )
                } else {
                    Box::new(future::ok(None))
                };

                let fetch_redis = fetch_redis.and_then(move |response| {
                    if let Some(state) = response {
                        return Box::new(future::ok(ProjectStateResponse::new(state)))
                            as ResponseFuture<_, _>;
                    }

                    let fetch_upstream = upstream_source
                        .send(FetchProjectState {
                            project_key,
                            no_cache,
                        })
                        .map_err(|_| ())
                        .and_then(move |result| result.map_err(|_| ()));

                    Box::new(fetch_upstream)
                });

                Box::new(fetch_redis)
            })
            .into_actor(self)
            .then(move |state_result, slf, _context| {
                let project = slf.get_or_create_project(project_key);
                project.update_state(state_result.ok(), no_cache);
                fut::ok(())
            })
            .spawn(context);
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

impl Message for GetProjectState {
    type Result = Result<Arc<ProjectState>, ProjectError>;
}

impl Handler<GetProjectState> for ProjectCache {
    type Result = Response<Arc<ProjectState>, ProjectError>;

    fn handle(&mut self, message: GetProjectState, _context: &mut Context<Self>) -> Self::Result {
        let project = self.get_or_create_project(message.project_key);
        project.get_or_fetch_state(message.no_cache)
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

impl Message for GetCachedProjectState {
    type Result = Option<Arc<ProjectState>>;
}

impl Handler<GetCachedProjectState> for ProjectCache {
    type Result = Option<Arc<ProjectState>>;

    fn handle(
        &mut self,
        message: GetCachedProjectState,
        _context: &mut Context<Self>,
    ) -> Self::Result {
        let project = self.get_or_create_project(message.project_key);
        project.get_or_fetch_state(false);
        project.valid_state()
    }
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
    project_key: ProjectKey,
    envelope: Envelope,
    context: EnvelopeContext,
    fetch: bool,
}

impl CheckEnvelope {
    /// Fetches the project state and checks the envelope.
    pub fn fetched(project_key: ProjectKey, envelope: Envelope, context: EnvelopeContext) -> Self {
        Self {
            project_key,
            envelope,
            context,
            fetch: true,
        }
    }

    /// Uses a cached project state and checks the envelope.
    pub fn cached(project_key: ProjectKey, envelope: Envelope, context: EnvelopeContext) -> Self {
        Self {
            project_key,
            envelope,
            context,
            fetch: false,
        }
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

/// Scoping information along with a checked envelope.
#[derive(Debug)]
pub struct CheckEnvelopeResponse {
    pub result: Result<CheckedEnvelope, DiscardReason>,
    pub scoping: Scoping,
}

impl Message for CheckEnvelope {
    type Result = Result<CheckEnvelopeResponse, ProjectError>;
}

impl Handler<CheckEnvelope> for ProjectCache {
    type Result = ActorResponse<Self, CheckEnvelopeResponse, ProjectError>;

    fn handle(&mut self, message: CheckEnvelope, context: &mut Self::Context) -> Self::Result {
        let project = self.get_or_create_project(message.project_key);
        if message.fetch {
            // Project state fetching is allowed, so ensure the state is fetched and up-to-date.
            // This will return synchronously if the state is still cached.
            project
                .get_or_fetch_state(message.envelope.meta().no_cache())
                .into_actor()
                .map(self, context, move |_, slf, _context| {
                    // TODO RaduW can we do better that this ????
                    // (need to retrieve project again to get around borrowing problems)
                    let project = slf.get_or_create_project(message.project_key);
                    project.check_envelope(message.envelope, message.context)
                })
        } else {
            // Preload the project cache so that it arrives a little earlier in processing. However,
            // do not pass `no_cache`. In case the project is rate limited, we do not want to force
            // a full reload.
            project.get_or_fetch_state(false);

            // message.fetch == false: Fetching must not block the store request. The
            // EnvelopeManager will later fetch the project state.
            ActorResponse::ok(project.check_envelope(message.envelope, message.context))
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

impl Message for UpdateRateLimits {
    type Result = ();
}

impl Handler<UpdateRateLimits> for ProjectCache {
    type Result = ();

    fn handle(&mut self, message: UpdateRateLimits, _context: &mut Self::Context) -> Self::Result {
        let UpdateRateLimits {
            project_key,
            rate_limits,
        } = message;
        let project = self.get_or_create_project(project_key);
        project.merge_rate_limits(rate_limits);
    }
}
/// A message containing a list of [`Metric`]s to be inserted into the aggregator.
#[derive(Debug)]
pub struct InsertMetrics {
    /// The project key
    project_key: ProjectKey,
    metrics: Vec<Metric>,
}

impl InsertMetrics {
    /// Creates a new message containing a list of [`Metric`]s.
    pub fn new<I>(project_key: ProjectKey, metrics: I) -> Self
    where
        I: IntoIterator<Item = Metric>,
    {
        Self {
            project_key,
            metrics: metrics.into_iter().collect(),
        }
    }
}

impl Message for InsertMetrics {
    type Result = Result<(), AggregateMetricsError>;
}

impl Handler<InsertMetrics> for ProjectCache {
    type Result = Result<(), AggregateMetricsError>;

    fn handle(&mut self, message: InsertMetrics, _context: &mut Self::Context) -> Self::Result {
        // Only keep if we have an aggregator, otherwise drop because we know that we were disabled.
        let project = self.get_or_create_project(message.project_key);
        project.insert_metrics(message.metrics);
        Ok(())
    }
}

#[derive(Debug)]
pub struct MergeBuckets {
    project_key: ProjectKey,
    buckets: Vec<Bucket>,
}

impl MergeBuckets {
    /// Creates a new message containing a list of [`Bucket`]s.
    pub fn new(project_key: ProjectKey, buckets: Vec<Bucket>) -> Self {
        Self {
            project_key,
            buckets,
        }
    }
}

impl Message for MergeBuckets {
    type Result = Result<(), AggregateMetricsError>;
}

impl Handler<MergeBuckets> for ProjectCache {
    type Result = Result<(), AggregateMetricsError>;

    fn handle(&mut self, message: MergeBuckets, _context: &mut Self::Context) -> Self::Result {
        // Only keep if we have an aggregator, otherwise drop because we know that we were disabled.
        let project = self.get_or_create_project(message.project_key);
        project.merge_buckets(message.buckets);
        Ok(())
    }
}

impl Handler<FlushBuckets> for ProjectCache {
    type Result = ResponseFuture<(), Vec<Bucket>>;

    fn handle(&mut self, message: FlushBuckets, _context: &mut Self::Context) -> Self::Result {
        let config = self.config.clone();
        let project_key = message.project_key();
        let project = self.get_or_create_project(project_key);
        let expiry_state = project.expiry_state();

        // Schedule an update to the project state if it is outdated, regardless of whether the
        // metrics can be forwarded or not. We never wait for this update.
        if !matches!(expiry_state, ExpiryState::Updated(_)) {
            project.get_or_fetch_state(false);
        }

        let project_state = match expiry_state {
            ExpiryState::Updated(state) => state,
            ExpiryState::Stale(state) => state,
            ExpiryState::Expired => {
                // If the state is outdated, we need to wait for an updated state. Put them back into the
                // aggregator and wait for the next flush cycle.
                return Box::new(future::err(message.into_buckets()));
            }
        };

        let scoping = match project.scoping() {
            Some(scoping) => scoping,
            _ => return Box::new(future::err(message.into_buckets())),
        };

        // Only send if the project state is valid, otherwise drop this bucket.
        if project_state.check_disabled(config.as_ref()).is_err() {
            return Box::new(future::ok(()));
        }

        let future = EnvelopeManager::from_registry()
            .send(SendMetrics {
                buckets: message.into_buckets(),
                scoping,
                project_key,
            })
            .then(move |send_result| match send_result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(buckets)) => Err(buckets),
                Err(_) => {
                    relay_log::error!("dropped metric buckets: envelope manager mailbox full");
                    Ok(())
                }
            });

        Box::new(future)
    }
}
