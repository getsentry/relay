use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;
use actix_web::ResponseError;
use failure::Fail;
use futures::{future, Future};
use serde::{Deserialize, Serialize};

use relay_common::{metric, ProjectKey};
use relay_config::{Config, RelayMode};
use relay_redis::RedisPool;

use crate::actors::project::{Project, ProjectState};
use crate::actors::project_local::LocalProjectSource;
use crate::actors::project_upstream::UpstreamProjectSource;
use crate::actors::upstream::UpstreamRelay;
use crate::metrics::{RelayCounters, RelayHistograms, RelayTimers};
use crate::utils::Response;

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
    project: Addr<Project>,
}

pub struct ProjectCache {
    config: Arc<Config>,
    projects: HashMap<ProjectKey, ProjectEntry>,

    local_source: Addr<LocalProjectSource>,
    upstream_source: Addr<UpstreamProjectSource>,
    #[cfg(feature = "processing")]
    redis_source: Option<Addr<RedisProjectSource>>,
}

impl ProjectCache {
    pub fn new(
        config: Arc<Config>,
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
}

impl Actor for ProjectCache {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        // Set the mailbox size to the size of the event buffer. This is a rough estimate but
        // should ensure that we're not dropping messages if the main arbiter running this actor
        // gets hammered a bit.
        let mailbox_size = self.config.event_buffer_size() as usize;
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

/// Resolves the project with the given identifier.
///
/// The returned `Project` is an actor that synchronizes state access internally. When it is fetched
/// for the first time, its state is unpopulated. Only when `GetProjectState` is sent to the project
/// for the first time, it starts to resolve the state from one of the sources.
///
/// If the optional `public_key` is set, then the public keys of the project are checked for a
/// redirect. If a redirect is detected, then the target project is resolved and returned instead.
///
/// **Note** that due to redirects, the returned project may have a different identifier.
#[derive(Debug, Deserialize, Serialize)]
pub struct GetProject {
    pub public_key: ProjectKey,
}

impl Message for GetProject {
    type Result = Addr<Project>;
}

impl Handler<GetProject> for ProjectCache {
    type Result = Addr<Project>;

    fn handle(&mut self, message: GetProject, context: &mut Context<Self>) -> Self::Result {
        let GetProject { public_key } = message;
        let config = self.config.clone();
        metric!(histogram(RelayHistograms::ProjectStateCacheSize) = self.projects.len() as u64);

        match self.projects.entry(public_key) {
            Entry::Occupied(entry) => {
                metric!(counter(RelayCounters::ProjectCacheHit) += 1);
                entry.get().project.clone()
            }
            Entry::Vacant(entry) => {
                metric!(counter(RelayCounters::ProjectCacheMiss) += 1);
                let project = Project::new(public_key, config, context.address()).start();
                entry.insert(ProjectEntry {
                    last_updated_at: Instant::now(),
                    project: project.clone(),
                });
                project
            }
        }
    }
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
pub struct FetchProjectState {
    pub public_key: ProjectKey,
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

impl Message for FetchProjectState {
    type Result = Result<ProjectStateResponse, ()>;
}

#[derive(Clone, Debug)]
pub struct FetchOptionalProjectState {
    pub public_key: ProjectKey,
}

impl Message for FetchOptionalProjectState {
    type Result = Option<Arc<ProjectState>>;
}

impl Handler<FetchProjectState> for ProjectCache {
    type Result = Response<ProjectStateResponse, ()>;

    fn handle(&mut self, message: FetchProjectState, _context: &mut Self::Context) -> Self::Result {
        let FetchProjectState { public_key } = message;
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

        let fetch_local = self
            .local_source
            .send(FetchOptionalProjectState { public_key })
            .map_err(|_| ());

        let future = fetch_local.and_then(move |response| {
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
            let fetch_redis: ResponseFuture<_, _> = if let Some(ref redis_source) = redis_source {
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
                    .send(message)
                    .map_err(|_| ())
                    .and_then(move |result| result.map_err(|_| ()));

                Box::new(fetch_upstream)
            });

            Box::new(fetch_redis)
        });

        Response::future(future)
    }
}
