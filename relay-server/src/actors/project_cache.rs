use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;
use actix_web::ResponseError;
use failure::Fail;
use futures::{future, Future};
use serde::{Deserialize, Serialize};

use relay_common::{metric, ProjectKey};
use relay_config::{Config, RelayMode};
use relay_metrics::{AggregateMetricsError, Bucket, FlushBuckets, InsertMetrics, MergeBuckets};
use relay_redis::RedisPool;

use crate::actors::envelopes::EnvelopeManager;
use crate::actors::outcome::OutcomeProducer;
use crate::actors::project::{
    CheckEnvelope, CheckEnvelopeResponse, GetCachedProjectState, GetProjectState, Project,
    ProjectState, UpdateRateLimits,
};
use crate::actors::project_local::LocalProjectSource;
use crate::actors::project_upstream::UpstreamProjectSource;
use crate::actors::upstream::UpstreamRelay;
use crate::metrics::{RelayCounters, RelayHistograms, RelayTimers};
use crate::utils::Response;

use itertools::Update;
#[cfg(feature = "processing")]
use {crate::actors::project_redis::RedisProjectSource, relay_common::clone};

#[derive(Fail, Debug)]
pub enum ProjectError {
    #[fail(display = "failed to fetch project state from upstream")]
    FetchFailed,

    #[fail(display = "could not schedule project fetching")]
    ScheduleFailed(#[cause] MailboxError),
}

impl ResponseError for ProjectError {}

struct ProjectEntry {
    last_updated_at: Instant,
    project: Project,
}

pub struct ProjectCache {
    config: Arc<Config>,
    projects: HashMap<ProjectKey, ProjectEntry>,

    event_manager: Addr<EnvelopeManager>,
    outcome_producer: Addr<OutcomeProducer>,
    local_source: Addr<LocalProjectSource>,
    upstream_source: Addr<UpstreamProjectSource>,
    #[cfg(feature = "processing")]
    redis_source: Option<Addr<RedisProjectSource>>,
}

impl ProjectCache {
    pub fn new(
        config: Arc<Config>,
        event_manager: Addr<EnvelopeManager>,
        outcome_producer: Addr<OutcomeProducer>,
        upstream_relay: Addr<UpstreamRelay>,
        _redis: Option<RedisPool>,
    ) -> Self {
        let local_source = LocalProjectSource::new(config.clone()).start();
        let upstream_source = UpstreamProjectSource::new(config.clone(), upstream_relay).start();

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
            event_manager,
            outcome_producer,
            local_source,
            upstream_source,
            #[cfg(feature = "processing")]
            redis_source,
        }
    }

    fn evict_stale_project_caches(&mut self) {
        metric!(counter(RelayCounters::EvictingStaleProjectCaches) += 1);
        let eviction_start = Instant::now();
        let delta = 2 * self.config.project_cache_expiry() + self.config.project_grace_period();

        self.projects
            .retain(|_, entry| entry.last_updated_at + delta > eviction_start);

        metric!(timer(RelayTimers::ProjectStateEvictionDuration) = eviction_start.elapsed());
    }

    fn get_project(&mut self, public_key: ProjectKey, ctx: &mut Context<Self>) -> &mut Project {
        metric!(histogram(RelayHistograms::ProjectStateCacheSize) = self.projects.len() as u64);

        let cfg = self.config.clone();
        let evt_mgr = self.event_manager.clone();
        let outcome_prod = self.outcome_producer.clone();

        &mut self
            .projects
            .entry(public_key)
            .and_modify(|_| {
                metric!(counter(RelayCounters::ProjectCacheHit) += 1);
            })
            .or_insert_with(move || {
                metric!(counter(RelayCounters::ProjectCacheMiss) += 1);
                let project = Project::new(public_key, cfg, evt_mgr, outcome_prod);
                ProjectEntry {
                    last_updated_at: Instant::now(),
                    project,
                }
            })
            .project
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

#[derive(Clone)]
pub struct FetchProjectState {
    /// The public key to fetch the project by.
    pub public_key: ProjectKey,

    /// If true, all caches should be skipped and a fresh state should be computed.
    pub no_cache: bool,
}

impl Message for FetchProjectState {
    type Result = Result<ProjectStateResponse, ()>;
}

//TODO remove at some point
#[derive(Debug)]
pub struct ProjectStateResponse {
    pub state: Arc<ProjectState>,
}

impl ProjectStateResponse {
    pub fn new(state: Arc<ProjectState>) -> Self {
        ProjectStateResponse { state }
    }
}

#[derive(Clone, Debug)]
pub struct FetchOptionalProjectState {
    pub public_key: ProjectKey,
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
    pub public_key: ProjectKey,

    /// If true, all caches should be skipped and a fresh state should be computed.
    pub no_cache: bool,
}

impl Message for UpdateProjectState {
    type Result = ();
}

impl Handler<UpdateProjectState> for ProjectCache {
    type Result = ();

    fn handle(&mut self, message: UpdateProjectState, context: &mut Self::Context) -> Self::Result {
        let UpdateProjectState {
            public_key,
            no_cache,
        } = message;

        if let Some(mut entry) = self.projects.get_mut(&public_key) {
            // Bump the update time of the project in our hashmap to evade eviction. Eviction is a
            // sequential scan over self.projects, so this needs to be as fast as possible and
            // probably should not involve sending a message to each Addr<Project> (this is the
            // reason ProjectEntry exists as a wrapper).
            //
            // This is somewhat racy as we do not know for sure whether the project actor sending
            // us this message is the same one that we have in self.projects. Practically this
            // should not matter because the race implies that the project we have in self.projects
            // has been created very recently anyway.
            entry.last_updated_at = Instant::now();
        }

        let relay_mode = self.config.relay_mode();

        let upstream_source = self.upstream_source.clone();
        #[cfg(feature = "processing")]
        let redis_source = self.redis_source.clone();

        self.local_source
            .send(FetchOptionalProjectState { public_key })
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
                            .send(FetchOptionalProjectState { public_key })
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
                            public_key,
                            no_cache,
                        })
                        .map_err(|_| ())
                        .and_then(move |result| result.map_err(|_| ()));

                    Box::new(fetch_upstream)
                });

                Box::new(fetch_redis)
            })
            .into_actor(self)
            .then(move |state_result, slf, context| {
                let proj = slf.get_project(public_key, context);
                proj.update_state(state_result, no_cache, context);
                fut::ok::<_, (), _>(())
            })
            .spawn(context);
    }
}

impl Handler<GetProjectState> for ProjectCache {
    type Result = Response<Arc<ProjectState>, ProjectError>;

    fn handle(&mut self, message: GetProjectState, context: &mut Context<Self>) -> Self::Result {
        let proj = self.get_project(message.public_key, context);
        proj.get_or_fetch_state(message.no_cache, context)
    }
}

impl Handler<GetCachedProjectState> for ProjectCache {
    type Result = Option<Arc<ProjectState>>;

    fn handle(
        &mut self,
        message: GetCachedProjectState,
        context: &mut Context<Self>,
    ) -> Self::Result {
        self.get_project(message.public_key, context).state_clone()
    }
}

impl Handler<CheckEnvelope> for ProjectCache {
    type Result = ActorResponse<Self, CheckEnvelopeResponse, ProjectError>;

    fn handle(&mut self, message: CheckEnvelope, context: &mut Self::Context) -> Self::Result {
        unimplemented!();
    }
}
impl Handler<UpdateRateLimits> for ProjectCache {
    type Result = ();

    fn handle(&mut self, message: UpdateRateLimits, _context: &mut Self::Context) -> Self::Result {
        unimplemented!();
    }
}

impl Handler<InsertMetrics> for ProjectCache {
    type Result = Result<(), AggregateMetricsError>;

    fn handle(&mut self, message: InsertMetrics, context: &mut Self::Context) -> Self::Result {
        unimplemented!();
    }
}

impl Handler<MergeBuckets> for ProjectCache {
    type Result = Result<(), AggregateMetricsError>;

    fn handle(&mut self, message: MergeBuckets, context: &mut Self::Context) -> Self::Result {
        unimplemented!();
    }
}

impl Handler<FlushBuckets> for ProjectCache {
    type Result = ResponseFuture<(), Vec<Bucket>>;

    fn handle(&mut self, message: FlushBuckets, context: &mut Self::Context) -> Self::Result {
        unimplemented!();
    }
}
