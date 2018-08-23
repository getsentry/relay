use std::borrow::Cow;
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::time::Duration;

use actix::prelude::*;
use actix_web::{http::Method, ResponseError};
use futures::{future, future::Shared, sync::oneshot, Future};
use url::Url;

use semaphore_aorta::{ProjectStateSnapshot, PublicKeyStatus};
use semaphore_common::{processor::PiiConfig, ProjectId};

use actors::events::EventMetaData;
use actors::upstream::{SendQuery, UpstreamQuery, UpstreamRelay, UpstreamRequestError};
use constants::BATCH_TIMEOUT;
use utils::Response;

#[derive(Fail, Debug)]
pub enum ProjectError {
    #[fail(display = "project state request canceled")]
    Canceled,
    #[fail(display = "failed to fetch project state")]
    FetchFailed,
    #[fail(display = "failed to fetch projects from upstream")]
    UpstreamFailed(#[fail(cause)] UpstreamRequestError),
}

impl ResponseError for ProjectError {}

fn is_valid_origin(state: &ProjectStateSnapshot, origin: Option<&Url>) -> bool {
    // Generally accept any event without an origin.
    let origin = match origin {
        Some(origin) => origin,
        None => return true,
    };

    // If the list of allowed domains is empty, we accept any origin. Otherwise, we have to
    // match with the whitelist.
    let allowed = &state.config().allowed_domains;
    !allowed.is_empty()
        && allowed
            .iter()
            .any(|x| x.as_str() == "*" || Some(x.as_str()) == origin.host_str())
}

fn get_event_action(state: &ProjectStateSnapshot, meta: &EventMetaData) -> EventAction {
    // Try to verify the request origin with the project config.
    if !is_valid_origin(state, meta.origin()) {
        return EventAction::Discard;
    }

    // TODO: Use real config here.
    if state.outdated(&Default::default()) {
        // if the snapshot is out of date, we proceed as if it was still up to date. The
        // upstream relay (or sentry) will still filter events.

        // we assume it is unlikely to re-activate a disabled public key.
        // thus we handle events pretending the config is still valid,
        // except queueing events for unknown DSNs as they might have become
        // available in the meanwhile.
        match state.get_public_key_status(&meta.auth().public_key()) {
            PublicKeyStatus::Enabled => EventAction::Accept,
            PublicKeyStatus::Disabled => EventAction::Discard,
            PublicKeyStatus::Unknown => EventAction::Accept,
        }
    } else {
        // only drop events if we know for sure the project is disabled.
        if state.disabled() {
            return EventAction::Discard;
        }

        // since the config has been fetched recently, we assume unknown
        // public keys do not exist and drop events eagerly.
        match state.get_public_key_status(&meta.auth().public_key()) {
            PublicKeyStatus::Enabled => EventAction::Accept,
            PublicKeyStatus::Disabled => EventAction::Discard,
            PublicKeyStatus::Unknown => EventAction::Discard,
        }
    }
}

pub struct Project {
    id: ProjectId,
    manager: Addr<ProjectManager>,
    state: Option<Arc<ProjectStateSnapshot>>,
    state_channel: Option<Shared<oneshot::Receiver<Option<Arc<ProjectStateSnapshot>>>>>,
}

impl Project {
    pub fn new(id: ProjectId, manager: Addr<ProjectManager>) -> Self {
        Project {
            id,
            manager,
            state: None,
            state_channel: None,
        }
    }

    pub fn state(&self) -> Option<&ProjectStateSnapshot> {
        self.state.as_ref().map(AsRef::as_ref)
    }

    fn get_or_fetch_state(
        &mut self,
        context: &mut Context<Self>,
    ) -> Response<Arc<ProjectStateSnapshot>, ProjectError> {
        if let Some(ref state) = self.state {
            return Response::ok(state.clone());
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

        Response::async(channel.map_err(|_| ProjectError::Canceled).and_then(
            |option| match *option {
                Some(ref state) => Ok(state.clone()),
                None => Err(ProjectError::FetchFailed),
            },
        ))
    }

    fn fetch_state(
        &mut self,
        context: &mut Context<Self>,
    ) -> Shared<oneshot::Receiver<Option<Arc<ProjectStateSnapshot>>>> {
        let (sender, receiver) = oneshot::channel();
        let id = self.id;

        self.manager
            .send(FetchProjectState { id })
            .into_actor(self)
            .and_then(move |state_result, actor, _context| {
                actor.state_channel = None;
                actor.state = state_result.map(Arc::new).ok();

                if actor.state.is_some() {
                    debug!("project {} state updated", id);
                }

                sender.send(actor.state.clone()).ok();
                future::ok(()).into_actor(actor)
            })
            .drop_err()
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
    type Result = Option<ProjectId>;
}

impl Handler<GetProjectId> for Project {
    type Result = Option<ProjectId>;

    fn handle(&mut self, _message: GetProjectId, _context: &mut Context<Self>) -> Self::Result {
        Some(self.id)
    }
}

pub struct GetProjectState;

impl Message for GetProjectState {
    type Result = Result<Arc<ProjectStateSnapshot>, ProjectError>;
}

impl Handler<GetProjectState> for Project {
    type Result = Response<Arc<ProjectStateSnapshot>, ProjectError>;

    fn handle(&mut self, _message: GetProjectState, context: &mut Context<Self>) -> Self::Result {
        self.get_or_fetch_state(context)
    }
}

pub struct GetEventAction {
    meta: Arc<EventMetaData>,
    fetch: bool,
}

impl GetEventAction {
    pub fn fetched(meta: Arc<EventMetaData>) -> Self {
        GetEventAction { meta, fetch: true }
    }

    pub fn cached(meta: Arc<EventMetaData>) -> Self {
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
            self.get_or_fetch_state(context)
                .map(move |state| get_event_action(&state, &message.meta))
        } else {
            // Fetching is not permitted (as part of the store request). In case the state is not
            // cached, assume that the event can be accepted. The EventManager will later fetch the
            // project state and reevaluate the event action.
            Response::ok(self.state().map_or(EventAction::Accept, |state| {
                get_event_action(&state, &message.meta)
            }))
        }
    }
}

pub struct GetPiiConfig;

impl Message for GetPiiConfig {
    type Result = Result<Option<PiiConfig>, ProjectError>;
}

impl Handler<GetPiiConfig> for Project {
    type Result = Response<Option<PiiConfig>, ProjectError>;

    fn handle(&mut self, _message: GetPiiConfig, context: &mut Self::Context) -> Self::Result {
        // TODO: Implement actual fetching
        self.get_or_fetch_state(context).map(|_state| None)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetProjectStates {
    pub projects: Vec<ProjectId>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetProjectStatesResponse {
    pub configs: HashMap<ProjectId, Option<ProjectStateSnapshot>>,
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

pub struct ProjectManager {
    upstream: Addr<UpstreamRelay>,
    projects: HashMap<ProjectId, Addr<Project>>,
    state_channels: HashMap<ProjectId, oneshot::Sender<ProjectStateSnapshot>>,
}

impl ProjectManager {
    pub fn new(upstream: Addr<UpstreamRelay>) -> Self {
        ProjectManager {
            upstream,
            projects: HashMap::new(),
            state_channels: HashMap::new(),
        }
    }

    pub fn schedule_fetch(&mut self, context: &mut Context<Self>) {
        if self.state_channels.is_empty() {
            context.run_later(Duration::from_secs(BATCH_TIMEOUT), Self::fetch_states);
        }
    }

    pub fn fetch_states(&mut self, context: &mut Context<Self>) {
        let channels = mem::replace(&mut self.state_channels, HashMap::new());
        debug!("updating project states for {} projects", channels.len());

        let request = GetProjectStates {
            projects: channels.keys().cloned().collect(),
        };

        self.upstream
            .send(SendQuery(request))
            .map_err(|_| ProjectError::UpstreamFailed)
            .and_then(|response| {
                match response {
                    Ok(mut response) => {
                        for (id, channel) in channels {
                            let state = response
                                .configs
                                .remove(&id)
                                .unwrap_or(None)
                                .unwrap_or_else(ProjectStateSnapshot::missing);

                            channel.send(state).ok();
                        }
                    }
                    Err(error) => {
                        error!("error fetching project states: {}", error);

                        // NOTE: We're dropping `channels` here, which closes the receiver on the
                        // other end. Project actors will interpret this as fetch failure.
                    }
                }

                Ok(())
            })
            .into_actor(self)
            .drop_err()
            .spawn(context);
    }
}

impl Actor for ProjectManager {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!("project manager started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("project manager stopped");
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetProject {
    pub id: ProjectId,
}

impl Message for GetProject {
    type Result = Addr<Project>;
}

impl Handler<GetProject> for ProjectManager {
    type Result = Addr<Project>;

    fn handle(&mut self, message: GetProject, context: &mut Context<Self>) -> Self::Result {
        self.projects
            .entry(message.id)
            .or_insert_with(|| Project::new(message.id, context.address()).start())
            .clone()
    }
}

pub struct FetchProjectState {
    id: ProjectId,
}

impl Message for FetchProjectState {
    type Result = Result<ProjectStateSnapshot, ()>;
}

impl Handler<FetchProjectState> for ProjectManager {
    type Result = Response<ProjectStateSnapshot, ()>;

    fn handle(&mut self, message: FetchProjectState, ctx: &mut Self::Context) -> Self::Result {
        self.schedule_fetch(ctx);

        let (sender, receiver) = oneshot::channel();
        if self.state_channels.insert(message.id, sender).is_some() {
            error!("project {} state fetched multiple times", message.id);
        }

        Response::async(receiver.map_err(|_| ()))
    }
}
