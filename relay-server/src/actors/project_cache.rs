use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;
use actix_web::ResponseError;
use failure::Fail;
use futures::{future, Future};
use serde::{Deserialize, Serialize};

use relay_common::{metric, ProjectId};
use relay_config::{Config, RelayMode};

use crate::actors::project::{Project, ProjectState};
use crate::actors::project_local::LocalProjectSource;
use crate::actors::project_upstream::UpstreamProjectSource;
use crate::actors::upstream::UpstreamRelay;
use crate::metrics::{RelayCounters, RelayHistograms, RelayTimers};
use crate::utils::{RedisPool, Response};

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
    projects: HashMap<ProjectId, ProjectEntry>,

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
        let eviction_threshold = match eviction_start
            .checked_sub(2 * self.config.project_cache_expiry())
            .and_then(|x| x.checked_sub(self.config.project_grace_period()))
        {
            Some(x) => x,
            None => {
                // There was an underflow when subtracting from `eviction_start`, which means
                // we produced the smallest possible instant. There is no way we have projects
                // that need to be evicted.
                log::warn!("Overflow/underflow while computing eviction_threshold. No projects will be evicted");
                return;
            }
        };

        self.projects
            .retain(|_, entry| entry.last_updated_at > eviction_threshold);

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

        log::info!("project cache started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        log::info!("project cache stopped");
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetProject {
    pub id: ProjectId,
}

impl Message for GetProject {
    type Result = Addr<Project>;
}

impl Handler<GetProject> for ProjectCache {
    type Result = Addr<Project>;

    fn handle(&mut self, message: GetProject, context: &mut Context<Self>) -> Self::Result {
        let config = self.config.clone();
        metric!(histogram(RelayHistograms::ProjectStateCacheSize) = self.projects.len() as u64);
        match self.projects.entry(message.id) {
            Entry::Occupied(entry) => {
                metric!(counter(RelayCounters::ProjectCacheHit) += 1);
                entry.get().project.clone()
            }
            Entry::Vacant(entry) => {
                metric!(counter(RelayCounters::ProjectCacheMiss) += 1);
                let project = Project::new(message.id, config, context.address()).start();
                entry.insert(ProjectEntry {
                    last_updated_at: Instant::now(),
                    project: project.clone(),
                });
                project
            }
        }
    }
}

#[derive(Clone, Copy)]
pub struct FetchProjectState {
    pub id: ProjectId,
}

pub struct ProjectStateResponse {
    pub state: Arc<ProjectState>,
    pub is_local: bool,
}

impl ProjectStateResponse {
    pub fn managed(state: Arc<ProjectState>) -> Self {
        ProjectStateResponse {
            state,
            is_local: false,
        }
    }

    pub fn local(state: Arc<ProjectState>) -> Self {
        ProjectStateResponse {
            state,
            is_local: true,
        }
    }
}

impl Message for FetchProjectState {
    type Result = Result<ProjectStateResponse, ()>;
}

pub struct FetchOptionalProjectState {
    pub id: ProjectId,
}

impl Message for FetchOptionalProjectState {
    type Result = Option<Arc<ProjectState>>;
}

impl Handler<FetchProjectState> for ProjectCache {
    type Result = Response<ProjectStateResponse, ()>;

    fn handle(&mut self, message: FetchProjectState, _context: &mut Self::Context) -> Self::Result {
        if let Some(mut entry) = self.projects.get_mut(&message.id) {
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
            .send(FetchOptionalProjectState { id: message.id })
            .map_err(|_| ());

        let future = fetch_local.and_then(move |response| {
            if let Some(state) = response {
                return Box::new(future::ok(ProjectStateResponse::local(state)))
                    as ResponseFuture<_, _>;
            }

            match relay_mode {
                RelayMode::Proxy => {
                    return Box::new(future::ok(ProjectStateResponse::local(Arc::new(
                        ProjectState::allowed(),
                    ))));
                }
                RelayMode::Static => {
                    return Box::new(future::ok(ProjectStateResponse::local(Arc::new(
                        ProjectState::missing(),
                    ))));
                }
                RelayMode::Capture => {
                    return Box::new(future::ok(ProjectStateResponse::local(Arc::new(
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
                        .send(FetchOptionalProjectState { id: message.id })
                        .map_err(|_| ()),
                )
            } else {
                Box::new(future::ok(None))
            };

            let fetch_redis = fetch_redis.and_then(move |response| {
                if let Some(state) = response {
                    return Box::new(future::ok(ProjectStateResponse::local(state)))
                        as ResponseFuture<_, _>;
                }

                let fetch_upstream = upstream_source
                    .send(message.clone())
                    .map_err(|_| ())
                    .and_then(move |result| result.map_err(|_| ()));

                Box::new(fetch_upstream)
            });

            Box::new(fetch_redis)
        });

        Response::r#async(future)
    }
}
