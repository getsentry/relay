use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::Duration;

use futures::future;
use itertools::Itertools;
use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use relay_dynamic_config::ErrorBoundary;
use relay_statsd::metric;
use relay_system::{
    Addr, BroadcastChannel, BroadcastResponse, BroadcastSender, FromMessage, Interface, Service,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::services::projects::project::Revision;
use crate::services::projects::project::{ParsedProjectState, ProjectState};
use crate::services::projects::source::{FetchProjectState, SourceProjectState};
use crate::services::upstream::{
    Method, RequestPriority, SendQuery, UpstreamQuery, UpstreamRelay, UpstreamRequestError,
};
use crate::statsd::{RelayCounters, RelayHistograms, RelayTimers};
use crate::utils::{RetryBackoff, SleepHandle};

/// A query to retrieve a batch of project states from upstream.
///
/// This query does not implement `Deserialize`. To parse the query, use a wrapper that skips
/// invalid project keys instead of failing the entire batch.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetProjectStates {
    /// List of requested project keys.
    public_keys: Vec<ProjectKey>,
    /// List of revisions for each project key.
    ///
    /// The revisions are mapped by index to the project key,
    /// this is a separate field to keep the API compatible.
    revisions: Vec<Revision>,
    /// If `true` the upstream should return a full configuration.
    ///
    /// Upstreams will ignore this for non-internal Relays.
    full_config: bool,
    /// If `true` the upstream should not serve from cache.
    no_cache: bool,
}

/// The response of the projects states requests.
///
/// A [`ProjectKey`] is either pending or has a result, it can not appear in both and doing
/// so is undefined.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetProjectStatesResponse {
    /// Map of [`ProjectKey`] to [`ParsedProjectState`] that was fetched from the upstream.
    #[serde(default)]
    configs: HashMap<ProjectKey, ErrorBoundary<Option<ParsedProjectState>>>,
    /// The [`ProjectKey`]'s that couldn't be immediately retrieved from the upstream.
    #[serde(default)]
    pending: HashSet<ProjectKey>,
    /// The [`ProjectKey`]'s that the upstream has no updates for.
    ///
    /// List is only populated when the request contains revision information
    /// for all requested configurations.
    #[serde(default)]
    unchanged: HashSet<ProjectKey>,
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

    fn route(&self) -> &'static str {
        "project_configs"
    }
}

/// The wrapper struct for the incoming external requests which also keeps addition information.
#[derive(Debug)]
struct ProjectStateChannel {
    // Main broadcast channel.
    channel: BroadcastChannel<SourceProjectState>,
    // Additional broadcast channels tracked from merge operations.
    merged: Vec<BroadcastChannel<SourceProjectState>>,
    revision: Revision,
    deadline: Instant,
    no_cache: bool,
    attempts: u64,
    /// How often the request failed.
    errors: usize,
    /// How often a "pending" response was received for this project state.
    pending: usize,
}

impl ProjectStateChannel {
    pub fn new(
        sender: BroadcastSender<SourceProjectState>,
        revision: Revision,
        timeout: Duration,
        no_cache: bool,
    ) -> Self {
        let now = Instant::now();
        Self {
            no_cache,
            channel: sender.into_channel(),
            merged: Vec::new(),
            revision,
            deadline: now + timeout,
            attempts: 0,
            errors: 0,
            pending: 0,
        }
    }

    pub fn no_cache(&mut self) {
        self.no_cache = true;
    }

    /// Attaches a new sender to the same channel.
    ///
    /// Also makes sure the new sender's revision matches the already requested revision.
    /// If the new revision is different from the contained revision this clears the revision.
    /// To not have multiple fetches per revision per batch, we need to find a common denominator
    /// for requests with different revisions, which is always to fetch the full project config.
    pub fn attach(&mut self, sender: BroadcastSender<SourceProjectState>, revision: Revision) {
        self.channel.attach(sender);
        if self.revision != revision {
            self.revision = Revision::default();
        }
    }

    pub fn send(self, state: SourceProjectState) {
        for channel in self.merged {
            channel.send(state.clone());
        }
        self.channel.send(state)
    }

    pub fn expired(&self) -> bool {
        Instant::now() > self.deadline
    }

    pub fn merge(&mut self, channel: ProjectStateChannel) {
        let ProjectStateChannel {
            channel,
            merged,
            revision,
            deadline,
            no_cache,
            attempts,
            errors,
            pending,
        } = channel;

        self.merged.push(channel);
        self.merged.extend(merged);
        if self.revision != revision {
            self.revision = Revision::default();
        }
        self.deadline = self.deadline.max(deadline);
        self.no_cache |= no_cache;
        self.attempts += attempts;
        self.errors += errors;
        self.pending += pending;
    }
}

/// The map of project keys with their project state channels.
type ProjectStateChannels = HashMap<ProjectKey, ProjectStateChannel>;

/// This is the [`UpstreamProjectSourceService`] interface.
///
/// The service is responsible for fetching the [`ParsedProjectState`] from the upstream.
/// Internally it maintains the buffer queue of the incoming requests, which got scheduled to fetch the
/// state and takes care of the backoff in case there is a problem with the requests.
#[derive(Debug)]
pub struct UpstreamProjectSource(FetchProjectState, BroadcastSender<SourceProjectState>);

impl Interface for UpstreamProjectSource {}

impl FromMessage<FetchProjectState> for UpstreamProjectSource {
    type Response = BroadcastResponse<SourceProjectState>;

    fn from_message(
        message: FetchProjectState,
        sender: BroadcastSender<SourceProjectState>,
    ) -> Self {
        Self(message, sender)
    }
}

/// The batch of the channels which used to fetch the project states.
struct ChannelsBatch {
    nocache_channels: Vec<(ProjectKey, ProjectStateChannel)>,
    cache_channels: Vec<(ProjectKey, ProjectStateChannel)>,
}

/// Collected Upstream responses, with associated project state channels.
struct UpstreamResponse {
    channels_batch: ProjectStateChannels,
    response: Result<GetProjectStatesResponse, UpstreamRequestError>,
}

/// The service which handles the fetching of the [`ParsedProjectState`] from upstream.
#[derive(Debug)]
pub struct UpstreamProjectSourceService {
    backoff: RetryBackoff,
    config: Arc<Config>,
    upstream_relay: Addr<UpstreamRelay>,
    state_channels: ProjectStateChannels,
    inner_tx: mpsc::UnboundedSender<Vec<Option<UpstreamResponse>>>,
    inner_rx: mpsc::UnboundedReceiver<Vec<Option<UpstreamResponse>>>,
    fetch_handle: SleepHandle,
    /// Instant when the last fetch failed, `None` if there aren't any failures.
    ///
    /// Relay updates this value to the instant when the first fetch fails, and
    /// resets it to `None` on successful responses. Relay does nothing during
    /// long times without requests.
    last_failed_fetch: Option<Instant>,
    /// Duration of continued fetch fails before emitting an error.
    ///
    /// Relay emits an error if all requests for at least this interval fail.
    failure_interval: Duration,
}

impl UpstreamProjectSourceService {
    /// Creates a new [`UpstreamProjectSourceService`] instance.
    pub fn new(config: Arc<Config>, upstream_relay: Addr<UpstreamRelay>) -> Self {
        let (inner_tx, inner_rx) = mpsc::unbounded_channel();

        Self {
            backoff: RetryBackoff::new(config.http_max_retry_interval()),
            state_channels: HashMap::new(),
            fetch_handle: SleepHandle::idle(),
            upstream_relay,
            inner_tx,
            inner_rx,
            last_failed_fetch: None,
            failure_interval: config.http_project_failure_interval(),
            config,
        }
    }

    /// Returns the backoff timeout for a batched upstream query.
    ///
    /// If previous queries succeeded, this will be the general batch interval. Additionally, an
    /// exponentially increasing backoff is used for retrying the upstream request.
    fn next_backoff(&mut self) -> Duration {
        self.config.query_batch_interval() + self.backoff.next_backoff()
    }

    /// Prepares the batches of the cache and nocache channels which could be used to request the
    /// project states.
    fn prepare_batches(&mut self) -> ChannelsBatch {
        let batch_size = self.config.query_batch_size();
        let num_batches = self.config.max_concurrent_queries();

        // Pop N items from state_channels. Intuitively, we would use
        // `state_channels.drain().take(n)`, but that clears the entire hashmap regardless how
        // much of the iterator is consumed.
        //
        // Instead, we have to collect the keys we want into a separate vector and pop them
        // one-by-one.
        let projects: Vec<_> = (self.state_channels.keys().copied())
            .take(batch_size * num_batches)
            .collect();

        let fresh_channels = (projects.iter())
            .filter_map(|id| Some((*id, self.state_channels.remove(id)?)))
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
                    relay_log::error!(
                        errors = channel.errors,
                        pending = channel.pending,
                        tags.did_error = channel.errors > 0,
                        tags.was_pending = channel.pending > 0,
                        tags.project_key = id.to_string(),
                        "error fetching project state {id}: deadline exceeded",
                    );
                }
                !channel.expired()
            });

        // Separate regular channels from those with the `nocache` flag. The latter go in separate
        // requests, since the upstream will block the response.
        let (nocache_channels, cache_channels): (Vec<_>, Vec<_>) =
            fresh_channels.partition(|(_id, channel)| channel.no_cache);

        let total_count = cache_channels.len() + nocache_channels.len();

        metric!(histogram(RelayHistograms::ProjectStatePending) = self.state_channels.len() as u64);

        relay_log::debug!(
            "updating project states for {}/{} projects (attempt {})",
            total_count,
            total_count + self.state_channels.len(),
            self.backoff.attempt(),
        );

        ChannelsBatch {
            nocache_channels,
            cache_channels,
        }
    }

    /// Merges a [`ProjectStateChannel`] into the existing list of tracked channels.
    ///
    /// A channel is removed when querying the upstream for the project,
    /// when the upstream returns pending for this project it needs to be returned to
    /// the list of channels. If there is already another request for the same project
    /// outstanding those two requests must be merged.
    fn merge_channel(&mut self, key: ProjectKey, channel: ProjectStateChannel) {
        match self.state_channels.entry(key) {
            Entry::Vacant(e) => {
                e.insert(channel);
            }
            Entry::Occupied(mut e) => {
                e.get_mut().merge(channel);
            }
        }
    }

    /// Executes an upstream request to fetch project configs.
    ///
    /// This assumes that currently no request is running. If the upstream request fails or new
    /// channels are pushed in the meanwhile, this will reschedule automatically.
    async fn fetch_states(
        config: Arc<Config>,
        upstream_relay: Addr<UpstreamRelay>,
        channels: ChannelsBatch,
    ) -> Vec<Option<UpstreamResponse>> {
        let request_start = Instant::now();
        let batch_size = config.query_batch_size();
        let cache_batches = channels.cache_channels.into_iter().chunks(batch_size);
        let nocache_batches = channels.nocache_channels.into_iter().chunks(batch_size);

        let mut requests = vec![];
        // The `nocache_batches.into_iter()` still must be called here, since compiler produces the
        // error: `that nocache_batches is not an iterator`.
        // Since `IntoChunks` is not an iterator itself but only implements `IntoIterator` trait.
        #[allow(clippy::useless_conversion)]
        for channels_batch in cache_batches.into_iter().chain(nocache_batches.into_iter()) {
            let mut channels_batch: ProjectStateChannels = channels_batch.collect();
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
                revisions: channels_batch
                    .values()
                    .map(|c| c.revision.clone())
                    .collect(),
                full_config: config.processing_enabled() || config.request_full_project_config(),
                no_cache: channels_batch.values().any(|c| c.no_cache),
            };

            // count number of http requests for project states
            metric!(counter(RelayCounters::ProjectStateRequest) += 1);

            let upstream_relay = upstream_relay.clone();
            requests.push(async move {
                match upstream_relay.send(SendQuery(query)).await {
                    Ok(response) => Some(UpstreamResponse {
                        channels_batch,
                        response,
                    }),
                    // If sending of the request to upstream fails:
                    // - drop the current batch of the channels
                    // - report the error, since this is the case we should not have in proper
                    //   workflow
                    // - return `None` to signal that we do not have any response from the Upstream
                    //   and we should ignore this.
                    Err(_err) => {
                        relay_log::error!("failed to send the request to upstream: channel full");
                        None
                    }
                }
            });
        }

        // Wait on results of all fanouts, and return the resolved responses.
        let responses = future::join_all(requests).await;
        metric!(timer(RelayTimers::ProjectStateRequestDuration) = request_start.elapsed());
        responses
    }

    /// Schedules the next trigger for fetching the project states.
    ///
    /// The next trigger will be scheduled only if the current handle is idle.
    fn schedule_fetch(&mut self) {
        if self.fetch_handle.is_idle() {
            let wait = self.next_backoff();
            self.fetch_handle.set(wait);
        }
    }

    /// Handles the responses from the upstream.
    fn handle_responses(&mut self, responses: Vec<Option<UpstreamResponse>>) {
        // Iterate only over the returned responses.
        for response in responses.into_iter().flatten() {
            let UpstreamResponse {
                channels_batch,
                response,
            } = response;

            match response {
                Ok(mut response) => {
                    // If a single request succeeded we reset the backoff. We decided to
                    // only backoff if we see that the project config endpoint is
                    // completely down and did not answer a single request successfully.
                    //
                    // Otherwise we might refuse to fetch any project configs because of a
                    // single, reproducible 500 we observed for a particular project.
                    self.backoff.reset();
                    self.last_failed_fetch = None;

                    // Count number of project states returned (via http requests).
                    metric!(
                        histogram(RelayHistograms::ProjectStateReceived) =
                            response.configs.len() as u64
                    );
                    for (key, mut channel) in channels_batch {
                        if response.pending.contains(&key) {
                            channel.pending += 1;
                            self.merge_channel(key, channel);
                            continue;
                        }

                        let mut result = "ok";
                        let state = if response.unchanged.contains(&key) {
                            result = "ok_unchanged";
                            SourceProjectState::NotModified
                        } else {
                            let state = response
                                .configs
                                .remove(&key)
                                .unwrap_or(ErrorBoundary::Ok(None));

                            let state = match state {
                                ErrorBoundary::Err(error) => {
                                    result = "invalid";
                                    let error = &error as &dyn std::error::Error;
                                    relay_log::error!(error, "error fetching project state {key}");
                                    ProjectState::Pending
                                }
                                ErrorBoundary::Ok(None) => ProjectState::Disabled,
                                ErrorBoundary::Ok(Some(state)) => state.into(),
                            };

                            SourceProjectState::New(state)
                        };

                        metric!(
                            histogram(RelayHistograms::ProjectStateAttempts) = channel.attempts,
                            result = result,
                        );
                        metric!(
                            counter(RelayCounters::ProjectUpstreamCompleted) += 1,
                            result = result,
                        );

                        channel.send(state);
                    }
                }
                Err(err) => {
                    self.track_failed_response();

                    let attempts = channels_batch
                        .values()
                        .map(|b| b.attempts)
                        .max()
                        .unwrap_or(0);
                    // Only log an error if the request failed more than once.
                    // We are not interested in single failures. Our retry mechanism is able to
                    // handle those.
                    if attempts >= 2 {
                        relay_log::error!(
                            error = &err as &dyn std::error::Error,
                            attempts = attempts,
                            "error fetching project states",
                        );
                    }

                    metric!(
                        histogram(RelayHistograms::ProjectStatePending) =
                            self.state_channels.len() as u64
                    );
                    // Put the channels back into the queue, we will retry again shortly.
                    self.state_channels.extend(channels_batch.into_iter().map(
                        |(key, mut channel)| {
                            channel.errors += 1;
                            (key, channel)
                        },
                    ))
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

    /// Tracks the last failed fetch, and emits an error if it exceeds the failure interval.
    fn track_failed_response(&mut self) {
        match self.last_failed_fetch {
            None => self.last_failed_fetch = Some(Instant::now()),
            Some(last_failed) => {
                let failure_duration = last_failed.elapsed();
                if failure_duration >= self.failure_interval {
                    relay_log::error!(
                        failure_duration = format!("{} seconds", failure_duration.as_secs()),
                        backoff_attempts = self.backoff.attempt(),
                        "can't fetch project states"
                    );
                }
            }
        }
        metric!(counter(RelayCounters::ProjectUpstreamFailed) += 1);
    }

    /// Creates the async task to fetch the project states.
    fn do_fetch(&mut self) {
        self.fetch_handle.reset();

        if self.state_channels.is_empty() {
            relay_log::error!("project state schedule fetch request without projects");
            return;
        }

        let config = self.config.clone();
        let inner_tx = self.inner_tx.clone();
        let channels = self.prepare_batches();
        let upstream_relay = self.upstream_relay.clone();

        relay_system::spawn!(async move {
            let responses = Self::fetch_states(config, upstream_relay, channels).await;
            // Send back all resolved responses and also unused channels.
            // These responses will be handled by `handle_responses` function.
            if inner_tx.send(responses).is_err() {
                relay_log::error!("unable to forward the requests to further processing");
            }
        });
    }

    /// Handles the incoming external messages.
    fn handle_message(&mut self, message: UpstreamProjectSource) {
        let UpstreamProjectSource(
            FetchProjectState {
                project_key,
                current_revision,
                no_cache,
            },
            sender,
        ) = message;

        let query_timeout = self.config.query_timeout();

        // If there is already channel for the requested project key, we attach to it,
        // otherwise create a new one.
        match self.state_channels.entry(project_key) {
            Entry::Vacant(entry) => {
                entry.insert(ProjectStateChannel::new(
                    sender,
                    current_revision,
                    query_timeout,
                    no_cache,
                ));
            }
            Entry::Occupied(mut entry) => {
                let channel = entry.get_mut();
                channel.attach(sender, current_revision);
                // Ensure upstream skips caches if one of the recipients requests an uncached response. This
                // operation is additive across requests.
                if no_cache {
                    channel.no_cache();
                }
            }
        }

        // Schedule the fetch if there is nothing running at this moment.
        if !self.backoff.started() {
            self.backoff.reset();
            self.schedule_fetch();
        }
    }
}

impl Service for UpstreamProjectSourceService {
    type Interface = UpstreamProjectSource;

    async fn run(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        relay_log::info!("project upstream cache started");
        loop {
            tokio::select! {
                biased;

                () = &mut self.fetch_handle => self.do_fetch(),
                Some(responses) = self.inner_rx.recv() => self.handle_responses(responses),
                Some(message) = rx.recv() => self.handle_message(message),

                else => break,
            }
        }
        relay_log::info!("project upstream cache stopped");
    }
}

#[cfg(test)]
mod tests {
    use crate::http::Response;
    use futures::future::poll_immediate;

    use super::*;

    fn to_response(body: &impl serde::Serialize) -> Response {
        let body = serde_json::to_vec(body).unwrap();
        let response = http::response::Response::builder()
            .status(http::StatusCode::OK)
            .header(http::header::CONTENT_LENGTH, body.len())
            .body(body)
            .unwrap();

        Response(response.into())
    }

    #[tokio::test]
    async fn test_schedule_merge_channels() {
        let (upstream_addr, mut upstream_rx) = Addr::custom();
        let config = Arc::new(Config::from_json_value(serde_json::json!({})).unwrap());
        let project_key = ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap();

        macro_rules! next_send_request {
            () => {{
                let UpstreamRelay::SendRequest(mut req) = upstream_rx.recv().await.unwrap() else {
                    panic!()
                };
                req.configure(&config);
                req
            }};
        }

        let service =
            UpstreamProjectSourceService::new(Arc::clone(&config), upstream_addr).start_detached();

        let mut response1 = service.send(FetchProjectState {
            project_key,
            current_revision: "123".into(),
            no_cache: false,
        });

        // Wait for the upstream request to make sure we're in the pending state.
        let request1 = next_send_request!();

        // Add another request for the same project, which should be combined into a single
        // request, after responding to the first inflight request.
        let mut response2 = service.send(FetchProjectState {
            project_key,
            current_revision: Revision::default(),
            no_cache: false,
        });

        // Return pending to the service.
        // Now the two requests should be combined.
        request1
            .respond(Ok(to_response(&serde_json::json!({
                "pending": [project_key],
            }))))
            .await;

        // Make sure there is no response yet.
        assert!(poll_immediate(&mut response1).await.is_none());
        assert!(poll_immediate(&mut response2).await.is_none());

        // Send a response to the second request which should successfully resolve both responses.
        next_send_request!()
            .respond(Ok(to_response(&serde_json::json!({
                "unchanged": [project_key],
            }))))
            .await;

        let (response1, response2) = futures::future::join(response1, response2).await;
        assert!(matches!(response1, Ok(SourceProjectState::NotModified)));
        assert!(matches!(response2, Ok(SourceProjectState::NotModified)));

        // No more messages to upstream expected.
        assert!(upstream_rx.try_recv().is_err());
    }
}
