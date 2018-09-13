use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::{ErrorKind, Read};
use std::mem;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use actix::fut;
use actix::prelude::*;
use actix_web::{http::Method, ResponseError};
use chrono::{DateTime, Utc};
use futures::{
    future::{Either, Shared},
    sync::oneshot,
    Future, IntoFuture,
};
use futures_cpupool::CpuPool;
use serde_json;
use url::Url;
use uuid::Uuid;

use semaphore_common::{processor::PiiConfig, Config, ProjectId, PublicKey, RetryBackoff};

use actors::controller::{Controller, Shutdown, Subscribe, TimeoutError};
use actors::upstream::{SendQuery, UpstreamQuery, UpstreamRelay};
use extractors::EventMeta;
use utils::{self, LogError, One, Response, SyncActorFuture, SyncHandle};

#[derive(Fail, Debug)]
pub enum ProjectError {
    #[fail(display = "project state request canceled")]
    Canceled,

    #[fail(display = "failed to fetch project state")]
    FetchFailed,

    #[fail(display = "failed to read project state from disk")]
    ReadFailed(#[cause] io::Error),

    #[fail(display = "failed to read project json from disk")]
    ReadDeserializeFailed(#[cause] serde_json::Error),

    #[fail(display = "could not schedule project fetching")]
    ScheduleFailed(#[cause] MailboxError),

    #[fail(display = "shutdown timer expired")]
    Shutdown,
}

impl ResponseError for ProjectError {}

#[derive(Clone, Copy, Debug)]
enum StateSource {
    Upstream,
    Disk,
}

pub struct Project {
    id: ProjectId,
    config: Arc<Config>,
    manager: Addr<ProjectCache>,
    cpu_pool: CpuPool,
    state: Option<(Arc<ProjectState>, StateSource)>,
    state_channel: Option<Shared<oneshot::Receiver<Option<Arc<ProjectState>>>>>,
}

impl Project {
    pub fn new(
        id: ProjectId,
        config: Arc<Config>,
        cpu_pool: CpuPool,
        manager: Addr<ProjectCache>,
    ) -> Self {
        Project {
            id,
            config,
            manager,
            cpu_pool,
            state: None,
            state_channel: None,
        }
    }

    pub fn state(&self) -> Option<&ProjectState> {
        self.state.as_ref().map(|(ref state, _)| state.as_ref())
    }

    fn get_or_fetch_state(
        &mut self,
        context: &mut Context<Self>,
    ) -> Response<Arc<ProjectState>, ProjectError> {
        trace!("project {} state requested", self.id);

        if let Some((ref state, _)) = self.state {
            if !state.outdated(&self.config) {
                trace!("Fresh cache for project {}", self.id);
                return Response::ok(state.clone());
            }
            trace!("Outdated cache for project {}, fetching", self.id);
        } else {
            trace!("No cache for project {}, fetching", self.id);
        }

        let channel = match self.state_channel {
            Some(ref channel) => channel.clone(),
            None => {
                let channel = self.fetch_state(context);
                self.state_channel = Some(channel.clone());
                channel
            }
        };

        Response::async(channel.map_err(|_| ProjectError::Canceled).and_then(
            |option| match *option {
                Some(ref state) => Ok(state.clone()),
                None => Err(ProjectError::FetchFailed),
            },
        ))
    }

    fn read_state_from_disk(
        &mut self,
    ) -> impl Future<Item = Option<Arc<ProjectState>>, Error = ProjectError> {
        let state_file_path = self.config.project_config_path(self.id);
        let slf_state = self.state.clone();

        self.cpu_pool.spawn_fn(move || {
            let mut file = match fs::File::open(state_file_path.clone()) {
                Ok(x) => x,
                Err(error) => {
                    if error.kind() == ErrorKind::NotFound {
                        debug!("No static project state found at {:?}", state_file_path);
                        return Ok(None);
                    } else {
                        Err(ProjectError::ReadFailed(error))?
                    }
                }
            };

            let metadata = file.metadata().map_err(ProjectError::ReadFailed)?;
            let mtime =
                DateTime::<Utc>::from(metadata.modified().map_err(ProjectError::ReadFailed)?);

            if let Some((ref state, StateSource::Disk)) = slf_state {
                if let Some(last_change) = state.last_change {
                    if mtime <= last_change {
                        return Ok(Some(state.clone()));
                    }
                }
            }

            let mut buf = vec![];
            file.read_to_end(&mut buf)
                .map_err(ProjectError::ReadFailed)?;

            let mut state: ProjectState =
                serde_json::from_slice(&buf).map_err(ProjectError::ReadDeserializeFailed)?;
            if state.last_change.is_some() {
                warn!("The lastChange parameter is ignored for static project configs.");
            }
            state.last_change = Some(mtime);
            Ok(Some(Arc::new(state)))
        })
    }

    fn fetch_state(
        &mut self,
        context: &mut Context<Self>,
    ) -> Shared<oneshot::Receiver<Option<Arc<ProjectState>>>> {
        let (sender, receiver) = oneshot::channel();
        let id = self.id;

        self.read_state_from_disk()
            .into_actor(self)
            .and_then(move |state, slf, _ctx| {
                if let Some(state) = state {
                    Either::A(Ok(Some((state, StateSource::Disk))).into_future())
                        .into_actor(slf)
                } else {
                    Either::B(if slf.config.credentials().is_none() {
                        error!(
                                "No credentials configured. Configure project {} in your projects folder at {:?}",
                                id,
                                slf.config.project_config_path(id)
                            );
                        Either::A(
                            Ok(Some((
                                Arc::new(ProjectState::missing()),
                                StateSource::Upstream,
                            ))).into_future(),
                        )
                    } else {
                        Either::B(
                            slf.manager
                                .send(FetchProjectState { id })
                                .map_err(|_| ProjectError::FetchFailed)
                                .map(|state| {
                                    Some((Arc::new(state.ok()?), StateSource::Upstream))
                                }),
                        )
                    }).into_actor(slf)
                }
            })
            .map_err(|error, slf, _ctx| {
                error!(
                    "Error while fetching project state for {}: {:?}",
                    slf.id, error
                );
            })
            .and_then(move |state_result, slf, _ctx| {
                slf.state_channel = None;
                slf.state = state_result;

                if slf.state.is_some() {
                    debug!("project {} state updated", id);
                }

                sender.send(slf.state.as_ref().map(|x| x.0.clone())).ok();
                fut::ok(())
            })
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
    pub public_keys: HashMap<String, bool>,
    /// The project's slug if available.
    pub slug: Option<String>,
    /// The project's current config
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
            .map(|e| {
                println!("{:?}", self.last_change);
                match self.last_change {
                    // detect whether it's a "missing" project state
                    // TODO(markus): Make own enum variant out of this?
                    Some(_) => e > config.project_cache_expiry(),
                    None => e > config.cache_miss_expiry(),
                }
            })
            .unwrap_or(false)
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
        !allowed.is_empty()
            && allowed
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
    /// The event should be processed and sent to upstream.
    Accept,
}

impl Message for GetEventAction {
    type Result = Result<EventAction, ProjectError>;
}

impl Handler<GetEventAction> for Project {
    type Result = Response<EventAction, ProjectError>;

    fn handle(&mut self, message: GetEventAction, context: &mut Self::Context) -> Self::Result {
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
    cpu_pool: CpuPool,
    upstream: Addr<UpstreamRelay>,
    projects: HashMap<ProjectId, Addr<Project>>,
    state_channels: HashMap<ProjectId, oneshot::Sender<ProjectState>>,
    shutdown: SyncHandle,
}

impl ProjectCache {
    pub fn new(config: Arc<Config>, cpu_pool: CpuPool, upstream: Addr<UpstreamRelay>) -> Self {
        ProjectCache {
            backoff: RetryBackoff::from_config(&config),
            config,
            cpu_pool,
            upstream,
            projects: HashMap::new(),
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
            })
            .sync(&self.shutdown, ProjectError::Shutdown)
            .drop_err()
            .spawn(context);
    }
}

impl Actor for ProjectCache {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        info!("project cache started");
        Controller::from_registry().do_send(Subscribe(context.address().recipient()));
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("project cache stopped");
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
        let cpu_pool = self.cpu_pool.clone();
        self.projects
            .entry(message.id)
            .or_insert_with(|| {
                Project::new(message.id, config, cpu_pool, context.address()).start()
            })
            .clone()
    }
}

pub struct FetchProjectState {
    id: ProjectId,
}

impl Message for FetchProjectState {
    type Result = Result<ProjectState, ()>;
}

impl Handler<FetchProjectState> for ProjectCache {
    type Result = Response<ProjectState, ()>;

    fn handle(&mut self, message: FetchProjectState, context: &mut Self::Context) -> Self::Result {
        if !self.backoff.started() {
            self.backoff.reset();
            self.schedule_fetch(context);
        }

        let (sender, receiver) = oneshot::channel();
        if self.state_channels.insert(message.id, sender).is_some() {
            error!("project {} state fetched multiple times", message.id);
        }

        Response::async(receiver.map_err(|_| ()))
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
