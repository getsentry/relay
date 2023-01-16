use std::borrow::Cow;
use std::collections::{hash_map::Entry, BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

// TODO(actix): These two import will be removed when the `Upstream` actor is migrated to new tokio
// runtime.
use actix::SystemService;
use actix_web::http::Method;
use futures::{future, FutureExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::Instant;

use relay_common::{ProjectKey, RetryBackoff};
use relay_config::Config;
use relay_log::LogError;
use relay_statsd::metric;
use relay_system::{
    compat, BroadcastChannel, BroadcastResponse, BroadcastSender, FromMessage, Interface, Service,
};

use crate::actors::project::ProjectState;
use crate::actors::project_cache::{FetchProjectState, ProjectError};
use crate::actors::upstream::{
    RequestPriority, SendQuery, UpstreamQuery, UpstreamRelay, UpstreamRequestError,
};
use crate::statsd::{RelayCounters, RelayHistograms, RelayTimers};
use crate::utils::ErrorBoundary;

/// This is the return type from [`UpstreamProjectSourceService::fetch_states`] function. Returns
/// list of the results:
/// * when the response successful, we get back the tuple of [`ProjectStateChannel`] tree and the
///   results from the requests to upstream
/// * otherwise we get the tuple of [`ProjectStateChannel`]
///   tree which we can re-used to re-schedule the fetch or the channel error.
type ProjectStatesResponses = Vec<
    Result<
        (
            BTreeMap<ProjectKey, ProjectStateChannel>,
            Result<GetProjectStatesResponse, UpstreamRequestError>,
        ),
        (BTreeMap<ProjectKey, ProjectStateChannel>, ProjectError),
    >,
>;

/// A query to retrieve a batch of project states from upstream.
///
/// This query does not implement `Deserialize`. To parse the query, use a wrapper that skips
/// invalid project keys instead of failing the entire batch.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetProjectStates {
    public_keys: Vec<ProjectKey>,
    full_config: bool,
    no_cache: bool,
}

/// The response of the projects states requests.
///
/// A [`ProjectKey`] is either pending or has a result, it can not appear in both and doing
/// so is undefined.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetProjectStatesResponse {
    #[serde(default)]
    pub configs: HashMap<ProjectKey, ErrorBoundary<Option<ProjectState>>>,
    #[serde(default)]
    pub pending: Vec<ProjectKey>,
}

impl UpstreamQuery for GetProjectStates {
    type Response = GetProjectStatesResponse;

    fn method(&self) -> Method {
        Method::POST
    }

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed("/api/0/relays/projectconfigs/?version=3")
    }

    fn priority() -> RequestPriority {
        RequestPriority::High
    }

    fn retry() -> bool {
        false
    }
}

/// The wrapper struct for the incoming external requests which also keeps addition information.
#[derive(Debug)]
struct ProjectStateChannel {
    channel: BroadcastChannel<Arc<ProjectState>>,
    deadline: Instant,
    no_cache: bool,
    attempts: u64,
}

impl ProjectStateChannel {
    pub fn new(
        sender: BroadcastSender<Arc<ProjectState>>,
        timeout: Duration,
        no_cache: bool,
    ) -> Self {
        let now = Instant::now();
        Self {
            no_cache,
            channel: sender.into_channel(),
            deadline: now + timeout,
            attempts: 0,
        }
    }

    pub fn no_cache(&mut self) {
        self.no_cache = true;
    }

    pub fn attach(&mut self, sender: BroadcastSender<Arc<ProjectState>>) {
        self.channel.attach(sender)
    }

    pub fn send(self, state: ProjectState) {
        self.channel.send(Arc::new(state))
    }

    pub fn expired(&self) -> bool {
        Instant::now() > self.deadline
    }
}

/// This is the [`UpstreamProjectSourceService`] interface.
#[derive(Debug)]
pub struct UpstreamProjectSource(FetchProjectState, BroadcastSender<Arc<ProjectState>>);

impl Interface for UpstreamProjectSource {}

impl FromMessage<FetchProjectState> for UpstreamProjectSource {
    type Response = BroadcastResponse<Arc<ProjectState>>;

    fn from_message(
        message: FetchProjectState,
        sender: BroadcastSender<Arc<ProjectState>>,
    ) -> Self {
        Self(message, sender)
    }
}

/// Service responsible for fetching the [`ProjectState`] from the upstream.
/// Internally it maintains the buffer queue of the incoming requests, which got scheduled to fetch the
/// `ProjectState` and takes care of the backoff in case there is a problem with the requests.
#[derive(Debug)]
pub struct UpstreamProjectSourceService {
    backoff: RetryBackoff,
    config: Arc<Config>,
    state_channels: HashMap<ProjectKey, ProjectStateChannel>,
    inner_tx: mpsc::UnboundedSender<(
        HashMap<ProjectKey, ProjectStateChannel>,
        ProjectStatesResponses,
    )>,
    inner_rx: mpsc::UnboundedReceiver<(
        HashMap<ProjectKey, ProjectStateChannel>,
        ProjectStatesResponses,
    )>,
}

impl UpstreamProjectSourceService {
    /// Creates a new [`UpstreamProjectSourceService`] instance.
    pub fn new(config: Arc<Config>) -> Self {
        let (inner_tx, inner_rx) = mpsc::unbounded_channel();

        Self {
            backoff: RetryBackoff::new(config.http_max_retry_interval()),
            state_channels: HashMap::new(),
            config,
            inner_tx,
            inner_rx,
        }
    }

    /// Returns the backoff timeout for a batched upstream query.
    ///
    /// If previous queries succeeded, this will be the general batch interval. Additionally, an
    /// exponentially increasing backoff is used for retrying the upstream request.
    fn next_backoff(&mut self) -> Duration {
        self.config.query_batch_interval() + self.backoff.next_backoff()
    }

    /// Executes an upstream request to fetch project configs.
    ///
    /// This assumes that currently no request is running. If the upstream request fails or new
    /// channels are pushed in the meanwhile, this will reschedule automatically.
    async fn fetch_states(
        config: Arc<Config>,
        mut state_channels: HashMap<ProjectKey, ProjectStateChannel>,
        inner_tx: mpsc::UnboundedSender<(
            HashMap<ProjectKey, ProjectStateChannel>,
            ProjectStatesResponses,
        )>,
        attempt: usize,
    ) {
        let batch_size = config.query_batch_size();
        let num_batches = config.max_concurrent_queries();

        // Pop N items from state_channels. Intuitively, we would use
        // `state_channels.drain().take(n)`, but that clears the entire hashmap regardless how
        // much of the iterator is consumed.
        //
        // Instead, we have to collect the keys we want into a separate vector and pop them
        // one-by-one.
        let projects: Vec<_> = (state_channels.keys().copied())
            .take(batch_size * num_batches)
            .collect();

        let fresh_channels = (projects.iter())
            .filter_map(|id| Some((*id, state_channels.remove(id)?)))
            .filter(|(id, channel)| {
                if channel.expired() {
                    metric!(
                        histogram(RelayHistograms::ProjectStateAttempts) = channel.attempts,
                        result = "timeout",
                    );
                    metric!(
                        counter(RelayCounters::ProjectUpstreamCompleted) += 1,
                        result = "timeout",
                    );
                    relay_log::error!("error fetching project state {}: deadline exceeded", id);
                }
                !channel.expired()
            });

        // Separate regular channels from those with the `nocache` flag. The latter go in separate
        // requests, since the upstream will block the response.
        let (nocache_channels, cache_channels): (Vec<_>, Vec<_>) =
            fresh_channels.partition(|(_id, channel)| channel.no_cache);

        let total_count = cache_channels.len() + nocache_channels.len();

        metric!(histogram(RelayHistograms::ProjectStatePending) = state_channels.len() as u64);

        relay_log::debug!(
            "updating project states for {}/{} projects (attempt {})",
            total_count,
            total_count + state_channels.len(),
            attempt,
        );

        let request_start = Instant::now();

        let cache_batches = cache_channels.into_iter().chunks(batch_size);
        let nocache_batches = nocache_channels.into_iter().chunks(batch_size);

        let mut requests = vec![];
        for channels_batch in cache_batches.into_iter().chain(nocache_batches.into_iter()) {
            let mut channels_batch: BTreeMap<_, _> = channels_batch.collect();
            for channel in channels_batch.values_mut() {
                channel.attempts += 1;
            }
            relay_log::debug!("sending request of size {}", channels_batch.len());
            metric!(
                histogram(RelayHistograms::ProjectStateRequestBatchSize) =
                    channels_batch.len() as u64
            );

            let query = GetProjectStates {
                public_keys: channels_batch.keys().copied().collect(),
                full_config: config.processing_enabled(),
                no_cache: channels_batch.values().any(|c| c.no_cache),
            };

            // count number of http requests for project states
            metric!(counter(RelayCounters::ProjectStateRequest) += 1);

            let future_request = compat::send(UpstreamRelay::from_registry(), SendQuery(query))
                .map(|response| match response {
                    Ok(response) => Ok((channels_batch, response)),
                    // Also propagate the channels in error case, since we will want to try to
                    // fetch the projects again.
                    _ => Err((channels_batch, ProjectError::ScheduleFailed)),
                });

            requests.push(future_request);
        }

        // Wait on results of all fanouts. We fail everything if a single one fails with a
        // MailboxError, but errors of a single fanout don't propagate like that.
        let responses = future::join_all(requests).await;
        metric!(timer(RelayTimers::ProjectStateRequestDuration) = request_start.elapsed());

        // Send back all resolved responses and also unused channels.
        if let Err(err) = inner_tx.send((state_channels, responses)) {
            relay_log::error!("Unable to forward the requests to further processing: {err}");
        }
    }

    /// Handles the responses from the upstream.
    fn handle_responses(&mut self, responses: ProjectStatesResponses) {
        for response in responses {
            match response {
                Ok((channels_batch, Ok(mut response))) => {
                    // If a single request succeeded we reset the backoff. We decided to
                    // only backoff if we see that the project config endpoint is
                    // completely down and did not answer a single request successfully.
                    //
                    // Otherwise we might refuse to fetch any project configs because of a
                    // single, reproducible 500 we observed for a particular project.
                    self.backoff.reset();

                    // Count number of project states returned (via http requests).
                    metric!(
                        histogram(RelayHistograms::ProjectStateReceived) =
                            response.configs.len() as u64
                    );
                    for (key, channel) in channels_batch {
                        if response.pending.contains(&key) {
                            self.state_channels.insert(key, channel);
                            continue;
                        }
                        let state = response
                            .configs
                            .remove(&key)
                            .unwrap_or(ErrorBoundary::Ok(None))
                            .unwrap_or_else(|error| {
                                relay_log::error!(
                                    "error fetching project state {}: {}",
                                    key,
                                    LogError(error)
                                );
                                Some(ProjectState::err())
                            })
                            .unwrap_or_else(ProjectState::missing);
                        let result = if state.invalid() { "invalid" } else { "ok" };
                        metric!(
                            histogram(RelayHistograms::ProjectStateAttempts) = channel.attempts,
                            result = result,
                        );
                        metric!(
                            counter(RelayCounters::ProjectUpstreamCompleted) += 1,
                            result = result,
                        );
                        channel.send(state.sanitize());
                    }
                }
                Ok((channels_batch, Err(err))) => {
                    relay_log::error!("error fetching project states: {}", LogError(&err));
                    metric!(
                        histogram(RelayHistograms::ProjectStatePending) =
                            self.state_channels.len() as u64
                    );
                    // Put the channels back into the queue, we will retry again shortly.
                    self.state_channels.extend(channels_batch);
                }
                Err((channels_batch, err)) => {
                    relay_log::error!("Failed to schedule projects fetch: {}", LogError(&err));
                    self.state_channels.extend(channels_batch);
                }
            }
        }

        if !self.state_channels.is_empty() {
            self.schedule_fetch()
        } else {
            // No open channels left, if this is because we fetched everything we
            // have already reset the backoff. If however, this is because we had
            // failures but the channels have been cleaned up because the requests
            // expired we need to reset the backoff so that the next request is not
            // simply ignored (by handle) and does a schedule_fetch().
            // Explanation 2: We use the backoff member for two purposes:
            //  - 1 to schedule repeated fetch requests (at less and less frequent intervals)
            //  - 2 as a flag to know if a fetch is already scheduled.
            // Resetting it in here signals that we don't have a backoff scheduled (either
            // because everything went fine or because all the requests have expired).
            // Next time a user wants a project it should schedule fetch requests.
            self.backoff.reset();
        }
    }

    /// Spawns the [`tokio::task`] to fetch the project configs from upstream.
    fn schedule_fetch(&mut self) {
        if self.state_channels.is_empty() {
            relay_log::error!("project state schedule fetch request without projects");
            return;
        }

        let channels: HashMap<ProjectKey, ProjectStateChannel> =
            self.state_channels.drain().collect();
        let config = self.config.clone();
        let attempt = self.backoff.attempt();
        let inner_tx = self.inner_tx.clone();
        let wait = self.next_backoff();

        tokio::spawn(async move {
            tokio::time::sleep(wait).await;
            Self::fetch_states(config, channels, inner_tx, attempt).await
        });
    }

    /// Handles the incoming external messages.
    fn handle_message(&mut self, message: UpstreamProjectSource) {
        let UpstreamProjectSource(
            FetchProjectState {
                project_key,
                no_cache,
            },
            sender,
        ) = message;

        let query_timeout = self.config.query_timeout();

        // If there is already channel for the requested project key, we attach to it,
        // otherwise create a new one.
        match self.state_channels.entry(project_key) {
            Entry::Vacant(entry) => {
                entry.insert(ProjectStateChannel::new(sender, query_timeout, no_cache));
            }
            Entry::Occupied(mut entry) => {
                let channel = entry.get_mut();
                channel.attach(sender);
                // Ensure upstream skips caches if one of the recipients requests an uncached response. This
                // operation is additive across requests.
                if no_cache {
                    channel.no_cache();
                }
            }
        }

        // Schedule the fetch if there is nothing running at this moment.
        if !self.backoff.started() && !self.state_channels.is_empty() {
            self.backoff.reset();
            self.schedule_fetch();
        }
    }
}

impl Service for UpstreamProjectSourceService {
    type Interface = UpstreamProjectSource;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            relay_log::info!("project upstream cache started");
            loop {
                tokio::select! {
                    biased;

                    Some((state_channels, responses)) = self.inner_rx.recv() => {
                        self.state_channels.extend(state_channels);
                        self.handle_responses(responses);
                    }
                    Some(message) = rx.recv() => self.handle_message(message),

                    else => break,
                }
            }
            relay_log::info!("project upstream cache stopped");
        });
    }
}
