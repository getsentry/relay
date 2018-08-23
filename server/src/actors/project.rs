use std::borrow::Cow;
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::time::Duration;

use actix::fut::wrap_future;
use actix::{
    Actor, ActorFuture, Addr, AsyncContext, Context, ContextFutureSpawner, Handler, Message,
    WrapFuture,
};
use actix_web::http::Method;
use actix_web::ResponseError;
use bytes::Bytes;
use futures::future::{self, Future, Shared};
use futures::sync::oneshot;
use url::Url;

use semaphore_aorta::{ProjectStateSnapshot, PublicKeyEventAction, PublicKeyStatus};
use semaphore_common::ProjectId;

use actors::events::EventMetaData;
use actors::upstream::{
    SendQuery, SendRequest, UpstreamQuery, UpstreamRelay, UpstreamRequestError,
};
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

fn get_event_action(state: &ProjectStateSnapshot, meta: &EventMetaData) -> PublicKeyEventAction {
    // Try to verify the request origin with the project config.
    if !is_valid_origin(state, meta.origin()) {
        return PublicKeyEventAction::Discard;
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
            PublicKeyStatus::Enabled => PublicKeyEventAction::Send,
            PublicKeyStatus::Disabled => PublicKeyEventAction::Discard,
            PublicKeyStatus::Unknown => PublicKeyEventAction::Send,
        }
    } else {
        // only drop events if we know for sure the project is disabled.
        if state.disabled() {
            return PublicKeyEventAction::Discard;
        }

        // since the config has been fetched recently, we assume unknown
        // public keys do not exist and drop events eagerly.
        match state.get_public_key_status(&meta.auth().public_key()) {
            PublicKeyStatus::Enabled => PublicKeyEventAction::Send,
            PublicKeyStatus::Disabled => PublicKeyEventAction::Discard,
            PublicKeyStatus::Unknown => PublicKeyEventAction::Discard,
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

        let request = self.manager.send(FetchProjectState { id: self.id });
        let future = wrap_future::<_, Self>(request)
            .and_then(move |state_result, actor, _context| {
                actor.state_channel = None;
                actor.state = state_result.map(Arc::new).ok();

                sender.send(actor.state.clone()).ok();
                wrap_future(future::ok(()))
            })
            .drop_err();

        context.spawn(future);
        receiver.shared()
    }
}

impl Actor for Project {
    type Context = Context<Self>;
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

pub struct GetEventAction {
    pub meta: Arc<EventMetaData>,
}

impl Message for GetEventAction {
    type Result = Result<PublicKeyEventAction, ProjectError>;
}

impl Handler<GetEventAction> for Project {
    type Result = Response<PublicKeyEventAction, ProjectError>;

    fn handle(&mut self, message: GetEventAction, context: &mut Self::Context) -> Self::Result {
        self.get_or_fetch_state(context)
            .map(move |state| get_event_action(&state, &message.meta))
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

        let request = GetProjectStates {
            projects: channels.keys().cloned().collect(),
        };

        let future = self
            .upstream
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
            });

        context.spawn(wrap_future(future).drop_err());
    }
}

impl Actor for ProjectManager {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!("Project manager started");
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        println!("Project manager stopped");
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
            error!("invariant violation: project state fetched twice for same project");
        }

        Response::async(receiver.map_err(|_| ()))
    }
}

/// NOTE: This message is implemented on the project manager to ensure it completes even if the
/// corresponding `Project` actor is stopped and its context dropped. Otherwise, we would drop
/// events in a race condition between store and cleanup.
pub struct QueueEvent {
    pub data: Bytes,
    pub meta: Arc<EventMetaData>,
    pub project_id: ProjectId,
}

impl Message for QueueEvent {
    type Result = ();
}

impl Handler<QueueEvent> for ProjectManager {
    type Result = ();

    fn handle(&mut self, message: QueueEvent, context: &mut Self::Context) -> Self::Result {
        let request = SendRequest::post(format!("/api/{}/store/", message.project_id)).build(
            move |builder| {
                if let Some(origin) = message.meta.origin() {
                    builder.header("Origin", origin.to_string());
                }

                builder
                    .header("X-Sentry-Auth", message.meta.auth().to_string())
                    .body(message.data)
            },
        );

        self.upstream
            .send(request)
            .map(|_| ())
            .into_actor(self)
            .drop_err()
            .spawn(context);
    }
}
