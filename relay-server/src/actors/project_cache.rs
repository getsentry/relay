use std::borrow::Cow;
use std::collections::{hash_map::Entry, BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix::fut;
use actix::prelude::*;
use actix_web::{http::Method, ResponseError};
use failure::Fail;
use futures::{future, future::Shared, sync::oneshot, Future};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use relay_common::{metric, LogError, ProjectId, RetryBackoff};
use relay_config::{Config, RelayMode};

use crate::actors::project::{Project, ProjectState};
use crate::actors::project_local_cache::ProjectLocalCache;
use crate::actors::upstream::{SendQuery, UpstreamQuery, UpstreamRelay};
use crate::metrics::{RelayCounters, RelayHistograms, RelayTimers};
use crate::utils::{self, ErrorBoundary};

#[derive(Fail, Debug)]
pub enum ProjectError {
    #[fail(display = "failed to fetch project state from upstream")]
    FetchFailed,

    #[fail(display = "could not schedule project fetching")]
    ScheduleFailed(#[cause] MailboxError),
}

impl ResponseError for ProjectError {}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetProjectStates {
    pub projects: Vec<ProjectId>,
    #[cfg(feature = "processing")]
    #[serde(default)]
    pub full_config: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetProjectStatesResponse {
    pub configs: HashMap<ProjectId, ErrorBoundary<Option<ProjectState>>>,
}

impl UpstreamQuery for GetProjectStates {
    type Response = GetProjectStatesResponse;

    fn method(&self) -> Method {
        Method::POST
    }

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed("/api/0/relays/projectconfigs/")
    }
}

#[derive(Clone, Copy, Debug)]
struct ProjectUpdate {
    project_id: ProjectId,
    instant: Instant,
}

impl ProjectUpdate {
    pub fn new(project_id: ProjectId) -> Self {
        ProjectUpdate {
            project_id,
            instant: Instant::now(),
        }
    }
}

#[derive(Debug)]
struct ProjectStateChannel {
    sender: oneshot::Sender<Arc<ProjectState>>,
    receiver: Shared<oneshot::Receiver<Arc<ProjectState>>>,
    deadline: Instant,
}

impl ProjectStateChannel {
    pub fn new(timeout: Duration) -> Self {
        let (sender, receiver) = oneshot::channel();

        Self {
            sender,
            receiver: receiver.shared(),
            deadline: Instant::now() + timeout,
        }
    }

    pub fn send(self, state: ProjectState) {
        self.sender.send(Arc::new(state)).ok();
    }

    pub fn receiver(&self) -> Shared<oneshot::Receiver<Arc<ProjectState>>> {
        self.receiver.clone()
    }

    pub fn expired(&self) -> bool {
        Instant::now() > self.deadline
    }
}

pub struct ProjectCache {
    backoff: RetryBackoff,
    config: Arc<Config>,
    upstream: Addr<UpstreamRelay>,
    projects: HashMap<ProjectId, Addr<Project>>,
    local_cache: Addr<ProjectLocalCache>,
    state_channels: HashMap<ProjectId, ProjectStateChannel>,
    updates: VecDeque<ProjectUpdate>,
}

impl ProjectCache {
    pub fn new(
        config: Arc<Config>,
        upstream: Addr<UpstreamRelay>,
        local_cache: Addr<ProjectLocalCache>,
    ) -> Self {
        ProjectCache {
            backoff: RetryBackoff::new(config.http_max_retry_interval()),
            config,
            upstream,
            projects: HashMap::new(),
            local_cache,
            state_channels: HashMap::new(),
            updates: VecDeque::new(),
        }
    }

    /// Returns the backoff timeout for a batched upstream query.
    ///
    /// If previous queries succeeded, this will be the general batch interval. Additionally, an
    /// exponentially increasing backoff is used for retrying the upstream request.
    fn next_backoff(&mut self) -> Duration {
        self.config.query_batch_interval() + self.backoff.next_backoff()
    }

    /// Schedules a batched upstream query with exponential backoff.
    fn schedule_fetch(&mut self, context: &mut Context<Self>) {
        utils::run_later(self.next_backoff(), Self::fetch_states).spawn(context)
    }

    /// Executes an upstream request to fetch project configs.
    ///
    /// This assumes that currently no request is running. If the upstream request fails or new
    /// channels are pushed in the meanwhile, this will reschedule automatically.
    fn fetch_states(&mut self, context: &mut Context<Self>) {
        if self.state_channels.is_empty() {
            log::error!("project state update scheduled without projects");
            return;
        }

        let eviction_start = Instant::now();
        let batch_size = self.config.query_batch_size();
        let num_batches = self.config.max_concurrent_queries();

        // Pop n items from state_channels. Intuitively we would use
        // `self.state_channels.drain().take(n)`, but that clears the entire hashmap regardless how
        // much of the iterator is consumed.
        //
        // Instead we have to collect the keys we want into a separate vector and pop them
        // one-by-one.
        let projects: Vec<_> = self
            .state_channels
            .keys()
            .copied()
            .take(batch_size * num_batches)
            .collect();

        let channels: BTreeMap<_, _> = projects
            .iter()
            .filter_map(|id| Some((*id, self.state_channels.remove(id)?)))
            .filter(|(_id, channel)| !channel.expired())
            .collect();

        // Remove outdated projects that are not being refreshed from the cache. If the project is
        // being updated now, also remove its update entry from the queue, since we will be
        // inserting a new timestamp at the end (see `extend`).
        let eviction_threshold = eviction_start - 2 * self.config.project_cache_expiry();
        while let Some(update) = self.updates.get(0) {
            if update.instant > eviction_threshold {
                break;
            }

            if !channels.contains_key(&update.project_id) {
                self.projects.remove(&update.project_id);
            }

            self.updates.pop_front();
        }

        // The remaining projects are not outdated anymore. Still, clean them from the queue to
        // reinsert them at the end, as they are now receiving an updated timestamp. Then,
        // batch-insert all new projects with the new timestamp.
        self.updates
            .retain(|update| !channels.contains_key(&update.project_id));
        self.updates
            .extend(projects.iter().copied().map(ProjectUpdate::new));

        metric!(timer(RelayTimers::ProjectStateEvictionDuration) = eviction_start.elapsed());
        metric!(histogram(RelayHistograms::ProjectStatePending) = self.state_channels.len() as u64);

        log::debug!(
            "updating project states for {}/{} projects (attempt {})",
            channels.len(),
            channels.len() + self.state_channels.len(),
            self.backoff.attempt(),
        );

        let request_start = Instant::now();

        let requests: Vec<_> = channels
            .into_iter()
            .chunks(batch_size)
            .into_iter()
            .map(|channels_batch| {
                let channels_batch: BTreeMap<_, _> = channels_batch.collect();
                log::debug!("sending request of size {}", channels_batch.len());
                metric!(
                    histogram(RelayHistograms::ProjectStateRequestBatchSize) =
                        channels_batch.len() as u64
                );

                let request = GetProjectStates {
                    projects: channels_batch.keys().copied().collect(),
                    #[cfg(feature = "processing")]
                    full_config: self.config.processing_enabled(),
                };

                // count number of http requests for project states
                metric!(counter(RelayCounters::ProjectStateRequest) += 1);

                self.upstream
                    .send(SendQuery(request))
                    .map_err(ProjectError::ScheduleFailed)
                    .map(move |response| (channels_batch, response))
            })
            .collect();

        // Wait on results of all fanouts. We fail everything if a single one fails with a
        // MailboxError, but errors of a single fanout don't propagate like that.
        future::join_all(requests)
            .into_actor(self)
            .and_then(move |responses, slf, ctx| {
                metric!(timer(RelayTimers::ProjectStateRequestDuration) = request_start.elapsed());

                for (channels_batch, response) in responses {
                    match response {
                        Ok(mut response) => {
                            // If a single request succeeded we reset the backoff. We decided to
                            // only backoff if we see that the project config endpoint is
                            // completely down and did not answer a single request successfully.
                            //
                            // Otherwise we might refuse to fetch any project configs because of a
                            // single, reproducible 500 we observed for a particular project.
                            slf.backoff.reset();

                            // count number of project states returned (via http requests)
                            metric!(
                                histogram(RelayHistograms::ProjectStateReceived) =
                                    response.configs.len() as u64
                            );
                            for (id, channel) in channels_batch {
                                let state = response
                                    .configs
                                    .remove(&id)
                                    .unwrap_or(ErrorBoundary::Ok(None))
                                    .unwrap_or_else(|error| {
                                        let e = LogError(error);
                                        log::error!("error fetching project state {}: {}", id, e);
                                        Some(ProjectState::err())
                                    })
                                    .unwrap_or_else(ProjectState::missing);

                                channel.send(state);
                            }
                        }
                        Err(error) => {
                            log::error!("error fetching project states: {}", LogError(&error));

                            // Put the channels back into the queue, in addition to channels that
                            // have been pushed in the meanwhile. We will retry again shortly.
                            slf.state_channels.extend(channels_batch);

                            metric!(
                                histogram(RelayHistograms::ProjectStatePending) =
                                    slf.state_channels.len() as u64
                            );
                        }
                    }
                }

                if !slf.state_channels.is_empty() {
                    slf.schedule_fetch(ctx);
                }

                fut::ok(())
            })
            .drop_err()
            .spawn(context);
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
                entry.get().clone()
            }
            Entry::Vacant(entry) => {
                metric!(counter(RelayCounters::ProjectCacheMiss) += 1);
                let project = Project::new(message.id, config, context.address()).start();
                entry.insert(project.clone());
                project
            }
        }
    }
}

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

    pub fn local(state: ProjectState) -> Self {
        ProjectStateResponse {
            state: Arc::new(state),
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
    type Result = Result<OptionalProjectStateResponse, ()>;
}

pub struct OptionalProjectStateResponse {
    pub state: Option<Arc<ProjectState>>,
}

impl Handler<FetchProjectState> for ProjectCache {
    type Result = ResponseActFuture<Self, ProjectStateResponse, ()>;

    fn handle(&mut self, message: FetchProjectState, _context: &mut Self::Context) -> Self::Result {
        let fetch_local_cache = self
            .local_cache
            .send(FetchOptionalProjectState { id: message.id })
            .map_err(|_| ())
            .into_actor(self);

        let future = fetch_local_cache.and_then(move |response, slf, ctx| {
            if let Ok(response) = response {
                // response should be infallible for local cache
                if let Some(state) = response.state {
                    return Box::new(fut::ok(ProjectStateResponse {
                        state,
                        is_local: true,
                    })) as ResponseActFuture<_, _, _>;
                }
            }

            match slf.config.relay_mode() {
                RelayMode::Proxy => {
                    return Box::new(fut::ok(
                        ProjectStateResponse::local(ProjectState::allowed()),
                    ));
                }
                RelayMode::Static => {
                    return Box::new(fut::ok(
                        ProjectStateResponse::local(ProjectState::missing()),
                    ));
                }
                RelayMode::Capture => {
                    return Box::new(fut::ok(
                        ProjectStateResponse::local(ProjectState::allowed()),
                    ));
                }
                RelayMode::Managed => {
                    // Proceed with loading the config from upstream
                }
            }

            if !slf.backoff.started() {
                slf.backoff.reset();
                slf.schedule_fetch(ctx);
            }

            // There's an edge case where a project is represented by two Project actors. This can
            // happen if our project eviction logic removes an actor from `project_cache.projects`
            // while it is still being held onto. This in turn happens because we have no efficient way
            // of determining the refcount of an `Addr<Project>`.
            //
            // Instead of fixing the race condition, let's just make sure we don't fetch project caches
            // twice. If the cleanup/eviction logic were to be fixed to take the addr's refcount into
            // account, there should never be an instance where `state_channels` already contains a
            // channel for our current `message.id`.
            let query_timeout = slf.config.query_timeout();

            let channel = slf
                .state_channels
                .entry(message.id)
                .or_insert_with(|| ProjectStateChannel::new(query_timeout));

            Box::new(
                channel
                    .receiver()
                    .map(|x| ProjectStateResponse::managed((*x).clone()))
                    .map_err(|_| ())
                    .into_actor(slf),
            )
        });

        Box::new(future)
    }
}
