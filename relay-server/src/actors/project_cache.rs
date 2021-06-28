use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;
use actix_web::ResponseError;
use failure::Fail;
use futures::{future, Future};

use relay_common::{metric, ProjectKey};
use relay_config::{Config, RelayMode};
use relay_metrics::{AggregateMetricsError, Bucket, FlushBuckets, InsertMetrics, MergeBuckets};
use relay_redis::RedisPool;

use crate::actors::envelopes::{EnvelopeManager, SendMetrics};
use crate::actors::outcome::OutcomeProducer;
use crate::actors::project::{
    CheckEnvelope, CheckEnvelopeResponse, GetCachedProjectState, GetProjectState, Outdated,
    Project, ProjectState, UpdateRateLimits,
};
use crate::actors::project_local::LocalProjectSource;
use crate::actors::project_upstream::UpstreamProjectSource;
use crate::actors::upstream::UpstreamRelay;
use crate::metrics::{RelayCounters, RelayHistograms, RelayTimers};
use crate::utils::{ActorResponse, Response};

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

    fn get_project(&mut self, project_key: ProjectKey) -> &mut Project {
        metric!(histogram(RelayHistograms::ProjectStateCacheSize) = self.projects.len() as u64);

        let cfg = self.config.clone();
        let outcome_prod = self.outcome_producer.clone();

        &mut self
            .projects
            .entry(project_key)
            .and_modify(|_| {
                metric!(counter(RelayCounters::ProjectCacheHit) += 1);
            })
            .or_insert_with(move || {
                metric!(counter(RelayCounters::ProjectCacheMiss) += 1);
                let project = Project::new(project_key, cfg, outcome_prod);
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
    pub project_key: ProjectKey,

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
    pub project_key: ProjectKey,
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
    pub project_key: ProjectKey,

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
            project_key,
            no_cache,
        } = message;

        if let Some(mut entry) = self.projects.get_mut(&project_key) {
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
            .then(move |state_result, slf, context| {
                let proj = slf.get_project(project_key);
                proj.update_state(state_result.ok(), no_cache, context);
                fut::ok::<_, (), _>(())
            })
            .spawn(context);
    }
}

impl Handler<GetProjectState> for ProjectCache {
    type Result = Response<Arc<ProjectState>, ProjectError>;

    fn handle(&mut self, message: GetProjectState, context: &mut Context<Self>) -> Self::Result {
        let proj = self.get_project(message.project_key);
        proj.get_or_fetch_state(message.no_cache, context)
    }
}

impl Handler<GetCachedProjectState> for ProjectCache {
    type Result = Option<Arc<ProjectState>>;

    fn handle(&mut self, message: GetCachedProjectState, _ctx: &mut Context<Self>) -> Self::Result {
        self.get_project(message.project_key).state_clone()
    }
}

impl Handler<CheckEnvelope> for ProjectCache {
    type Result = ActorResponse<Self, CheckEnvelopeResponse, ProjectError>;

    fn handle(&mut self, message: CheckEnvelope, context: &mut Self::Context) -> Self::Result {
        let project = self.get_project(message.project_key);
        if message.fetch {
            // Project state fetching is allowed, so ensure the state is fetched and up-to-date.
            // This will return synchronously if the state is still cached.
            project
                .get_or_fetch_state(message.envelope.meta().no_cache(), context)
                .into_actor()
                .map(self, context, move |_, slf, _ctx| {
                    // TODO RaduW can we do better that this ????
                    // (need to retrieve project again to get around borwoing problems)
                    let project = slf.get_project(message.project_key);
                    project.check_envelope_scoped(message)
                })
        } else {
            // Preload the project cache so that it arrives a little earlier in processing. However,
            // do not pass `no_cache`. In case the project is rate limited, we do not want to force
            // a full reload.
            project.get_or_fetch_state(false, context);

            // message.fetch == false: Fetching must not block the store request. The
            // EnvelopeManager will later fetch the project state.
            ActorResponse::ok(project.check_envelope_scoped(message))
        }
    }
}
impl Handler<UpdateRateLimits> for ProjectCache {
    type Result = ();

    fn handle(&mut self, message: UpdateRateLimits, _ctx: &mut Self::Context) -> Self::Result {
        let UpdateRateLimits {
            project_key,
            rate_limits,
        } = message;
        let project = self.get_project(project_key);
        project.merge_rate_limits(rate_limits);
    }
}

impl Handler<InsertMetrics> for ProjectCache {
    type Result = Result<(), AggregateMetricsError>;

    fn handle(&mut self, message: InsertMetrics, context: &mut Self::Context) -> Self::Result {
        // Only keep if we have an aggregator, otherwise drop because we know that we were disabled.
        let project = self.get_project(message.project_key);
        if let Some(aggregator) = project.get_or_create_aggregator(context) {
            aggregator.do_send(message);
        }

        Ok(())
    }
}

impl Handler<MergeBuckets> for ProjectCache {
    type Result = Result<(), AggregateMetricsError>;

    fn handle(&mut self, message: MergeBuckets, context: &mut Self::Context) -> Self::Result {
        // Only keep if we have an aggregator, otherwise drop because we know that we were disabled.
        let project = self.get_project(message.project_key);
        if let Some(aggregator) = project.get_or_create_aggregator(context) {
            aggregator.do_send(message);
        }

        Ok(())
    }
}

impl Handler<FlushBuckets> for ProjectCache {
    type Result = ResponseFuture<(), Vec<Bucket>>;

    fn handle(&mut self, message: FlushBuckets, context: &mut Self::Context) -> Self::Result {
        let config = self.config.clone();
        let project_key = message.project_key();
        let project = self.get_project(project_key);
        let outdated = match project.state() {
            Some(state) => state.outdated(config.as_ref()),
            None => Outdated::HardOutdated,
        };

        // Schedule an update to the project state if it is outdated, regardless of whether the
        // metrics can be forwarded or not. We never wait for this update.
        if outdated != Outdated::Updated {
            project.get_or_fetch_state(false, context);
        }

        // If the state is outdated, we need to wait for an updated state. Put them back into the
        // aggregator and wait for the next flush cycle.
        if outdated == Outdated::HardOutdated {
            return Box::new(future::err(message.into_buckets()));
        }

        let (state, scoping) = match (project.state(), project.scoping()) {
            (Some(state), Some(scoping)) => (state, scoping),
            _ => return Box::new(future::err(message.into_buckets())),
        };

        // Only send if the project state is valid, otherwise drop this bucket.
        if state.check_disabled(config.as_ref()).is_err() {
            return Box::new(future::ok(()));
        }

        let future = self
            .event_manager
            .send(SendMetrics {
                buckets: message.into_buckets(),
                scoping,
                project_key,
                project_cache: context.address(),
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
