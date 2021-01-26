use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix::fut;
use actix::prelude::*;
use actix_web::http::Method;
use futures::{future, future::Shared, sync::oneshot, Future};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use relay_common::{metric, ProjectKey, RetryBackoff};
use relay_config::Config;
use relay_log::LogError;

use crate::actors::project::ProjectState;
use crate::actors::project_cache::{FetchProjectState, ProjectError, ProjectStateResponse};
use crate::actors::upstream::{RequestPriority, SendQuery, UpstreamQuery, UpstreamRelay};
use crate::metrics::{RelayCounters, RelayHistograms, RelayTimers};
use crate::utils::{self, ErrorBoundary};

#[macro_use]
mod _macro {
    /// The current version of the project states endpoint.
    ///
    /// Only this version is supported by Relay. All other versions are forwarded to the Upstream.
    /// The endpoint version is added as `version` query parameter to every outgoing request.
    #[macro_export]
    macro_rules! project_states_version {
        () => {
            2
        };
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetProjectStates {
    pub public_keys: Vec<ProjectKey>,
    #[serde(default)]
    pub full_config: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetProjectStatesResponse {
    #[serde(default)]
    pub configs: HashMap<ProjectKey, ErrorBoundary<Option<ProjectState>>>,
}

impl UpstreamQuery for GetProjectStates {
    type Response = GetProjectStatesResponse;

    fn method(&self) -> Method {
        Method::POST
    }

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed(concat!(
            "/api/0/relays/projectconfigs/?version=",
            project_states_version!()
        ))
    }

    fn priority() -> RequestPriority {
        RequestPriority::High
    }

    fn retry() -> bool {
        false
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

pub struct UpstreamProjectSource {
    backoff: RetryBackoff,
    config: Arc<Config>,
    upstream: Addr<UpstreamRelay>,
    state_channels: HashMap<ProjectKey, ProjectStateChannel>,
}

impl UpstreamProjectSource {
    pub fn new(config: Arc<Config>, upstream: Addr<UpstreamRelay>) -> Self {
        UpstreamProjectSource {
            backoff: RetryBackoff::new(config.http_max_retry_interval()),
            config,
            upstream,
            state_channels: HashMap::new(),
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
            relay_log::error!("project state update scheduled without projects");
            return;
        }

        let batch_size = self.config.query_batch_size();
        let num_batches = self.config.max_concurrent_queries();
        let total_count = batch_size * num_batches;

        // Pop `total_count` items from state_channels. Intuitively, we would use
        // `self.state_channels.drain().take(n)`, but that clears the entire hashmap regardless how
        // much of the iterator is consumed.
        //
        // Instead, we have to collect the keys we want into a separate vector and pop them
        // one-by-one.
        let projects: Vec<_> = self
            .state_channels
            .keys()
            .copied()
            .take(total_count)
            .collect();

        let channels: BTreeMap<_, _> = projects
            .iter()
            .filter_map(|id| Some((*id, self.state_channels.remove(id)?)))
            .filter(|(_id, channel)| !channel.expired())
            .collect();

        metric!(histogram(RelayHistograms::ProjectStatePending) = self.state_channels.len() as u64);

        relay_log::debug!(
            "updating project states for {}/{} projects (attempt {})",
            channels.len(),
            channels.len() + self.state_channels.len(),
            self.backoff.attempt(),
        );

        let request_start = Instant::now();

        // Distribute the projects evenly across HTTP requests to avoid large chunks too early. Do
        // this by dividing and rounding up to the next integer. We achieve this by adding the
        // remainder of the division which rounds up `total_count` to the next multiple of
        // num_batches. Worst case, we're left with one project per request, but that's fine.
        let actual_batch_size = (total_count + (total_count % num_batches)) / num_batches;

        let requests: Vec<_> = channels
            .into_iter()
            .chunks(actual_batch_size)
            .into_iter()
            .map(|channels_batch| {
                let channels_batch: BTreeMap<_, _> = channels_batch.collect();
                relay_log::debug!("sending request of size {}", channels_batch.len());
                metric!(
                    histogram(RelayHistograms::ProjectStateRequestBatchSize) =
                        channels_batch.len() as u64
                );

                let query = GetProjectStates {
                    public_keys: channels_batch.keys().copied().collect(),
                    full_config: self.config.processing_enabled(),
                };

                // count number of http requests for project states
                metric!(counter(RelayCounters::ProjectStateRequest) += 1);

                self.upstream
                    .send(SendQuery(query))
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
                            for (key, channel) in channels_batch {
                                let state = response
                                    .configs
                                    .remove(&key)
                                    .unwrap_or(ErrorBoundary::Ok(None))
                                    .unwrap_or_else(|error| {
                                        let e = LogError(error);
                                        relay_log::error!(
                                            "error fetching project state {}: {}",
                                            key,
                                            e
                                        );
                                        Some(ProjectState::err())
                                    })
                                    .unwrap_or_else(ProjectState::missing);

                                channel.send(state.sanitize());
                            }
                        }
                        Err(error) => {
                            relay_log::error!(
                                "error fetching project states: {}",
                                LogError(&error)
                            );

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
                    // we still have some project configs waiting for state
                    // try again next time
                    slf.schedule_fetch(ctx);
                } else {
                    // No open channels left, if this is because we fetched everything we
                    // have already reset the backoff. If however, this is because we had
                    // failures but the channels have been cleaned up because the requests
                    // expired we need to reset the backoff so that the next request is not
                    // simply ignored (by handle) and does a schedule_fetch().
                    // Explanation 2: We use the backoff member for two purposes:
                    //  -1 to schedule repeated fetch requests (at less and less frequent intervals)
                    //  -2 as a flag to know if a fetch is already scheduled.
                    // Resetting it in here signals that we don't have a backoff scheduled (either
                    // because everything went fine or because all the requests have expired).
                    // Next time a user wants a project it should schedule fetch requests.
                    slf.backoff.reset();
                }

                fut::ok(())
            })
            .drop_err()
            .spawn(context);
    }
}

impl Actor for UpstreamProjectSource {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        // Set the mailbox size to the size of the event buffer. This is a rough estimate but
        // should ensure that we're not dropping messages if the main arbiter running this actor
        // gets hammered a bit.
        let mailbox_size = self.config.event_buffer_size() as usize;
        context.set_mailbox_capacity(mailbox_size);

        relay_log::info!("project upstream cache started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        relay_log::info!("project upstream cache stopped");
    }
}

impl Handler<FetchProjectState> for UpstreamProjectSource {
    type Result = ResponseFuture<ProjectStateResponse, ()>;

    fn handle(&mut self, message: FetchProjectState, context: &mut Self::Context) -> Self::Result {
        if !self.backoff.started() {
            self.backoff.reset();
            self.schedule_fetch(context);
        }

        let query_timeout = self.config.query_timeout();

        // There's an edge case where a project is represented by two Project actors. This can
        // happen if our project eviction logic removes an actor from `project_cache.projects`
        // while it is still being held onto. This in turn happens because we have no efficient way
        // of determining the refcount of an `Addr<Project>`.
        //
        // Instead of fixing the race condition, let's just make sure we don't fetch project caches
        // twice. If the cleanup/eviction logic were to be fixed to take the addr's refcount into
        // account, there should never be an instance where `state_channels` already contains a
        // channel for our current `message.id`.
        let channel = self
            .state_channels
            .entry(message.public_key)
            .or_insert_with(|| ProjectStateChannel::new(query_timeout));

        Box::new(
            channel
                .receiver()
                .map_err(|_| ())
                .map(|x| ProjectStateResponse::new((*x).clone())),
        )
    }
}
