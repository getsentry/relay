use std::borrow::Cow;
use std::cmp;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::mem;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use actix::fut;
use actix::prelude::*;
use actix_web::{http::Method, ResponseError};
use chrono::{DateTime, Utc};
use futures::{future::Shared, sync::oneshot, Future};
use serde_json;
use url::Url;

use semaphore_common::{
    processor_compat::PiiConfig, Config, ProjectId, PublicKey, RetryBackoff, Uuid,
};

use actors::controller::{Controller, Shutdown, Subscribe, TimeoutError};
use actors::upstream::{SendQuery, UpstreamQuery, UpstreamRelay};
use extractors::EventMeta;
use utils::{self, LogError, One, Response, SyncActorFuture, SyncHandle};

#[derive(Fail, Debug)]
pub enum ProjectError {
    #[fail(display = "failed to fetch project state from upstream")]
    FetchFailed,

    #[fail(display = "could not schedule project fetching")]
    ScheduleFailed(#[cause] MailboxError),

    #[fail(display = "shutdown timer expired")]
    Shutdown,
}

impl ResponseError for ProjectError {}

pub struct Project {
    id: ProjectId,
    config: Arc<Config>,
    manager: Addr<ProjectCache>,
    state: Option<Arc<ProjectState>>,
    state_channel: Option<Shared<oneshot::Receiver<Arc<ProjectState>>>>,
    retry_after: Instant,
    is_local: bool,
}

impl Project {
    pub fn new(id: ProjectId, config: Arc<Config>, manager: Addr<ProjectCache>) -> Self {
        Project {
            id,
            config,
            manager,
            state: None,
            state_channel: None,
            retry_after: Instant::now(),
            is_local: false,
        }
    }

    pub fn state(&self) -> Option<&ProjectState> {
        self.state.as_ref().map(AsRef::as_ref)
    }

    fn get_or_fetch_state(
        &mut self,
        context: &mut Context<Self>,
    ) -> Response<Arc<ProjectState>, ProjectError> {
        if let Some(ref state) = self.state {
            // In case the state is fetched from a local file, don't use own caching logic. Rely on
            // `ProjectCache#local_states` for caching.
            if !self.is_local && !state.outdated(&self.config) {
                return Response::ok(state.clone());
            }
        }

        debug!("project {} state requested", self.id);

        let channel = match self.state_channel {
            Some(ref channel) => channel.clone(),
            None => {
                let channel = self.fetch_state(context);
                self.state_channel = Some(channel.clone());
                channel
            }
        };

        let future = channel
            .map(|shared| (*shared).clone())
            .map_err(|_| ProjectError::FetchFailed);

        Response::async(future)
    }

    fn fetch_state(
        &mut self,
        context: &mut Context<Self>,
    ) -> Shared<oneshot::Receiver<Arc<ProjectState>>> {
        let (sender, receiver) = oneshot::channel();
        let id = self.id;

        self.manager
            .send(FetchProjectState { id })
            .into_actor(self)
            .and_then(move |state_result, slf, _ctx| {
                slf.state_channel = None;
                slf.is_local = state_result
                    .as_ref()
                    .map(|resp| resp.is_local)
                    .unwrap_or(false);
                slf.state = state_result.map(|resp| resp.state).ok();

                if let Some(ref state) = slf.state {
                    debug!("project {} state updated", id);
                    sender.send(state.clone()).ok();
                }

                fut::ok(())
            }).drop_err()
            .spawn(context);

        receiver.shared()
    }
}

impl Actor for Project {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        debug!("project {} initialized without state", self.id);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        debug!("project {} removed from cache", self.id);
    }
}

pub struct GetProjectId;

impl Message for GetProjectId {
    type Result = One<ProjectId>;
}

impl Handler<GetProjectId> for Project {
    type Result = One<ProjectId>;

    fn handle(&mut self, _message: GetProjectId, _context: &mut Context<Self>) -> Self::Result {
        One(self.id)
    }
}

/// A helper enum indicating the public key state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PublicKeyStatus {
    /// The state of the public key is not known.
    ///
    /// This can indicate that the key is not yet known or that the
    /// key just does not exist.  We can not tell these two cases
    /// apart as there is always a lag since the last update from the
    /// upstream server.  As such the project state uses a heuristic
    /// to decide if it should treat a key as not existing or just
    /// not yet known.
    Unknown,
    /// This key is known but was disabled.
    Disabled,
    /// This key is known and is enabled.
    Enabled,
}

/// These are config values that the user can modify in the UI.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProjectConfig {
    /// URLs that are permitted for cross original JavaScript requests.
    pub allowed_domains: Vec<String>,
    /// List of relay public keys that are permitted to access this project.
    pub trusted_relays: Vec<PublicKey>,
    /// Configuration for PII stripping.
    pub pii_config: Option<PiiConfig>,
}

/// The project state is a cached server state of a project.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectState {
    /// The timestamp of when the state was received.
    #[serde(default = "Utc::now")]
    pub last_fetch: DateTime<Utc>,
    /// The timestamp of when the state was last changed.
    ///
    /// This might be `None` in some rare cases like where states
    /// are faked locally.
    #[serde(default)]
    pub last_change: Option<DateTime<Utc>>,
    /// Indicates that the project is disabled.
    #[serde(default)]
    pub disabled: bool,
    /// A container of known public keys in the project.
    #[serde(default)]
    pub public_keys: HashMap<String, bool>,
    /// The project's slug if available.
    #[serde(default)]
    pub slug: Option<String>,
    /// The project's current config
    #[serde(default)]
    pub config: ProjectConfig,
    /// The project state's revision id.
    #[serde(default)]
    pub rev: Option<Uuid>,
}

impl ProjectState {
    /// Project state for a missing project.
    pub fn missing() -> Self {
        ProjectState {
            last_fetch: Utc::now(),
            last_change: None,
            disabled: true,
            public_keys: HashMap::new(),
            slug: None,
            config: Default::default(),
            rev: None,
        }
    }

    /// Returns the current status of a key.
    pub fn get_public_key_status(&self, public_key: &str) -> PublicKeyStatus {
        match self.public_keys.get(public_key) {
            Some(&true) => PublicKeyStatus::Enabled,
            Some(&false) => PublicKeyStatus::Disabled,
            None => PublicKeyStatus::Unknown,
        }
    }

    /// Returns `true` if the entire project should be considered
    /// disabled (blackholed, deleted etc.).
    pub fn disabled(&self) -> bool {
        self.disabled
    }

    /// Returns whether this state is outdated and needs to be refetched.
    pub fn outdated(&self, config: &Config) -> bool {
        SystemTime::from(self.last_fetch)
            .elapsed()
            .map(|e| match self.slug {
                Some(_) => e > config.project_cache_expiry(),
                None => e > config.cache_miss_expiry(),
            }).unwrap_or(false)
    }

    /// Returns the project config.
    pub fn config(&self) -> &ProjectConfig {
        &self.config
    }

    /// Checks if this origin is allowed for this project.
    fn is_valid_origin(&self, origin: Option<&Url>) -> bool {
        // Generally accept any event without an origin.
        let origin = match origin {
            Some(origin) => origin,
            None => return true,
        };

        // If the list of allowed domains is empty, we accept any origin. Otherwise, we have to
        // match with the whitelist.
        let allowed = &self.config().allowed_domains;
        !allowed.is_empty() && allowed
            .iter()
            .any(|x| x.as_str() == "*" || Some(x.as_str()) == origin.host_str())
    }

    /// Determines whether the given event should be accepted or dropped.
    pub fn get_event_action(&self, meta: &EventMeta, config: &Config) -> EventAction {
        // Try to verify the request origin with the project config.
        if !self.is_valid_origin(meta.origin()) {
            return EventAction::Discard;
        }

        if self.outdated(config) {
            // if the state is out of date, we proceed as if it was still up to date. The
            // upstream relay (or sentry) will still filter events.

            // we assume it is unlikely to re-activate a disabled public key.
            // thus we handle events pretending the config is still valid,
            // except queueing events for unknown DSNs as they might have become
            // available in the meanwhile.
            match self.get_public_key_status(meta.auth().public_key()) {
                PublicKeyStatus::Enabled => EventAction::Accept,
                PublicKeyStatus::Disabled => EventAction::Discard,
                PublicKeyStatus::Unknown => EventAction::Accept,
            }
        } else {
            // only drop events if we know for sure the project is disabled.
            if self.disabled() {
                return EventAction::Discard;
            }

            // since the config has been fetched recently, we assume unknown
            // public keys do not exist and drop events eagerly.
            match self.get_public_key_status(meta.auth().public_key()) {
                PublicKeyStatus::Enabled => EventAction::Accept,
                PublicKeyStatus::Disabled => EventAction::Discard,
                PublicKeyStatus::Unknown => EventAction::Discard,
            }
        }
    }
}

pub struct GetProjectState;

impl Message for GetProjectState {
    type Result = Result<Arc<ProjectState>, ProjectError>;
}

impl Handler<GetProjectState> for Project {
    type Result = Response<Arc<ProjectState>, ProjectError>;

    fn handle(&mut self, _message: GetProjectState, context: &mut Context<Self>) -> Self::Result {
        self.get_or_fetch_state(context)
    }
}

pub struct GetEventAction {
    meta: Arc<EventMeta>,
    fetch: bool,
}

impl GetEventAction {
    pub fn fetched(meta: Arc<EventMeta>) -> Self {
        GetEventAction { meta, fetch: true }
    }

    pub fn cached(meta: Arc<EventMeta>) -> Self {
        GetEventAction { meta, fetch: false }
    }
}

/// Indicates what should happen to events based on their meta data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventAction {
    /// The event should be discarded.
    Discard,
    /// The event should be discarded and the client should back off for some time.
    RetryAfter(u64),
    /// The event should be processed and sent to upstream.
    Accept,
}

impl Message for GetEventAction {
    type Result = Result<EventAction, ProjectError>;
}

impl Handler<GetEventAction> for Project {
    type Result = Response<EventAction, ProjectError>;

    fn handle(&mut self, message: GetEventAction, context: &mut Self::Context) -> Self::Result {
        // Check for an eventual rate limit. Note that we have to ensure that the computation of the
        // backoff duration must yield a positive number or it would otherwise panic.
        let now = Instant::now();
        if now < self.retry_after {
            // Compensate for the missing subsec part by adding 1s
            let secs = (self.retry_after - now).as_secs() + 1;
            return Response::ok(EventAction::RetryAfter(secs));
        }

        if message.fetch {
            // Project state fetching is allowed, so ensure the state is fetched and up-to-date.
            // This will return synchronously if the state is still cached.
            let config = self.config.clone();
            self.get_or_fetch_state(context)
                .map(move |state| state.get_event_action(&message.meta, &config))
        } else {
            // Fetching is not permitted (as part of the store request). In case the state is not
            // cached, assume that the event can be accepted. The EventManager will later fetch the
            // project state and reevaluate the event action.
            Response::ok(self.state().map_or(EventAction::Accept, |state| {
                state.get_event_action(&message.meta, &self.config)
            }))
        }
    }
}

#[derive(Debug)]
pub struct RetryAfter {
    pub secs: u64,
}

impl Message for RetryAfter {
    type Result = ();
}

impl Handler<RetryAfter> for Project {
    type Result = ();

    fn handle(&mut self, message: RetryAfter, _context: &mut Self::Context) -> Self::Result {
        let retry_after = Instant::now() + Duration::from_secs(message.secs);
        self.retry_after = cmp::max(self.retry_after, retry_after);
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetProjectStates {
    pub projects: Vec<ProjectId>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetProjectStatesResponse {
    pub configs: HashMap<ProjectId, Option<ProjectState>>,
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

pub struct ProjectCache {
    backoff: RetryBackoff,
    config: Arc<Config>,
    upstream: Addr<UpstreamRelay>,
    projects: HashMap<ProjectId, Addr<Project>>,
    local_states: HashMap<ProjectId, Arc<ProjectState>>,
    state_channels: HashMap<ProjectId, oneshot::Sender<ProjectState>>,
    shutdown: SyncHandle,
}

impl ProjectCache {
    pub fn new(config: Arc<Config>, upstream: Addr<UpstreamRelay>) -> Self {
        ProjectCache {
            backoff: RetryBackoff::from_config(&config),
            config,
            upstream,
            projects: HashMap::new(),
            local_states: HashMap::new(),
            state_channels: HashMap::new(),
            shutdown: SyncHandle::new(),
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
        utils::run_later(self.next_backoff(), Self::fetch_states)
            .sync(&self.shutdown, ())
            .spawn(context)
    }

    /// Executes an upstream request to fetch project configs.
    ///
    /// This assumes that currently no request is running. If the upstream request fails or new
    /// channels are pushed in the meanwhile, this will reschedule automatically.
    fn fetch_states(&mut self, context: &mut Context<Self>) {
        let channels = mem::replace(&mut self.state_channels, HashMap::new());
        debug!(
            "updating project states for {} projects (attempt {})",
            channels.len(),
            self.backoff.attempt(),
        );

        let request = GetProjectStates {
            projects: channels.keys().cloned().collect(),
        };

        self.upstream
            .send(SendQuery(request))
            .map_err(ProjectError::ScheduleFailed)
            .into_actor(self)
            .and_then(|response, slf, ctx| {
                match response {
                    Ok(mut response) => {
                        slf.backoff.reset();

                        for (id, channel) in channels {
                            let state = response
                                .configs
                                .remove(&id)
                                .unwrap_or(None)
                                .unwrap_or_else(ProjectState::missing);

                            channel.send(state).ok();
                        }
                    }
                    Err(error) => {
                        error!("error fetching project states: {}", LogError(&error));

                        if !slf.shutdown.requested() {
                            // Put the channels back into the queue, in addition to channels that have
                            // been pushed in the meanwhile. We will retry again shortly.
                            slf.state_channels.extend(channels);
                        }
                    }
                }

                if !slf.state_channels.is_empty() {
                    slf.schedule_fetch(ctx);
                }

                fut::ok(())
            }).sync(&self.shutdown, ProjectError::Shutdown)
            .drop_err()
            .spawn(context);
    }
}

fn load_local_states(projects_path: &Path) -> io::Result<HashMap<ProjectId, Arc<ProjectState>>> {
    let mut states = HashMap::new();

    let directory = match fs::read_dir(projects_path) {
        Ok(directory) => directory,
        Err(error) => {
            return match error.kind() {
                io::ErrorKind::NotFound => Ok(states),
                _ => Err(error),
            };
        }
    };

    // only printed when directory even exists.
    debug!("Loading local states from directory {:?}", projects_path);

    for entry in directory {
        let entry = entry?;
        let path = entry.path();

        if !entry.metadata()?.is_file() {
            warn!("skipping {:?}, not a file", path);
            continue;
        }

        if path.extension().map(|x| x != "json").unwrap_or(true) {
            warn!("skipping {:?}, file extension must be .json", path);
            continue;
        }

        let id = match path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.parse().ok())
        {
            Some(id) => id,
            None => {
                warn!("skipping {:?}, filename is not a valid project id", path);
                continue;
            }
        };

        let state = serde_json::from_reader(io::BufReader::new(fs::File::open(path)?))?;
        states.insert(id, Arc::new(state));
    }

    Ok(states)
}

fn poll_local_states(
    manager: Addr<ProjectCache>,
    config: Arc<Config>,
) -> impl Future<Item = (), Error = ()> {
    let (sender, receiver) = oneshot::channel();

    let _ = thread::spawn(move || {
        let path = config.project_configs_path();
        let mut sender = Some(sender);

        loop {
            match load_local_states(&path) {
                Ok(states) => {
                    manager.do_send(UpdateLocalStates { states });
                    sender.take().map(|sender| sender.send(()).ok());
                }
                Err(error) => error!(
                    "failed to load static project configs: {}",
                    LogError(&error)
                ),
            }

            thread::sleep(config.local_cache_interval());
        }
    });

    receiver.map_err(|_| ())
}

impl Actor for ProjectCache {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        info!("project cache started");
        Controller::from_registry().do_send(Subscribe(context.address().recipient()));

        // Start the background thread that reads the local states from disk.
        // `poll_local_states` returns a future that resolves as soon as the first read is done.
        poll_local_states(context.address(), self.config.clone())
            .into_actor(self)
            // Block entire actor on first local state read, such that we don't e.g. drop events on
            // startup
            .wait(context);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("project cache stopped");
    }
}

struct UpdateLocalStates {
    states: HashMap<ProjectId, Arc<ProjectState>>,
}

impl Message for UpdateLocalStates {
    type Result = ();
}

impl Handler<UpdateLocalStates> for ProjectCache {
    type Result = ();

    fn handle(&mut self, message: UpdateLocalStates, _context: &mut Context<Self>) -> Self::Result {
        self.local_states = message.states;
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
        self.projects
            .entry(message.id)
            .or_insert_with(|| Project::new(message.id, config, context.address()).start())
            .clone()
    }
}

struct FetchProjectState {
    id: ProjectId,
}

struct ProjectStateResponse {
    state: Arc<ProjectState>,
    is_local: bool,
}

impl Message for FetchProjectState {
    type Result = Result<ProjectStateResponse, ()>;
}

impl Handler<FetchProjectState> for ProjectCache {
    type Result = Response<ProjectStateResponse, ()>;

    fn handle(&mut self, message: FetchProjectState, context: &mut Self::Context) -> Self::Result {
        if !self.backoff.started() {
            self.backoff.reset();
            self.schedule_fetch(context);
        }

        if let Some(state) = self.local_states.get(&message.id) {
            return Response::ok(ProjectStateResponse {
                state: state.clone(),
                is_local: true,
            });
        }

        let (sender, receiver) = oneshot::channel();
        if self.state_channels.insert(message.id, sender).is_some() {
            error!("project {} state fetched multiple times", message.id);
        }

        Response::async(
            receiver
                .map(|state| ProjectStateResponse {
                    state: Arc::new(state),
                    is_local: false,
                }).map_err(|_| ()),
        )
    }
}

impl Handler<Shutdown> for ProjectCache {
    type Result = ResponseFuture<(), TimeoutError>;

    fn handle(&mut self, message: Shutdown, _context: &mut Self::Context) -> Self::Result {
        match message.timeout {
            Some(timeout) => self.shutdown.timeout(timeout),
            None => self.shutdown.now(),
        }
    }
}
