use std::borrow::Cow;
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::time::Duration;

use actix::fut::wrap_future;
use actix::{Actor, ActorFuture, Addr, AsyncContext, Context, Handler, Message, Response};

use actix_web::http::Method;
use actix_web::ResponseError;

use futures::future::{self, Future, Shared};
use futures::sync::oneshot;

// TODO(ja): Move this here and rename to ProjectState
use semaphore_aorta::ProjectStateSnapshot;
use semaphore_common::ProjectId;

use actors::upstream::{SendRequest, SendRequestError, UpstreamRelay, UpstreamRequest};

#[derive(Fail, Debug)]
pub enum ProjectError {
    #[fail(display = "project state request canceled")]
    Canceled,
    #[fail(display = "failed to fetch project state")]
    FetchFailed,
    #[fail(display = "failed to fetch projects from upstream")]
    UpstreamFailed(#[fail(cause)] SendRequestError),
}

impl ResponseError for ProjectError {}

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

    fn get_or_fetch_state(
        &mut self,
        context: &mut Context<Self>,
    ) -> Response<Arc<ProjectStateSnapshot>, ProjectError> {
        if let Some(ref state) = self.state {
            return Response::reply(Ok(state.clone()));
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

impl UpstreamRequest for GetProjectStates {
    type Response = GetProjectStatesResponse;

    fn get_upstream_request_target(&self) -> (Method, Cow<str>) {
        (Method::POST, Cow::Borrowed("/api/0/relays/projectconfigs/"))
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
            context.run_later(Duration::from_secs(1), Self::fetch_states);
        }
    }

    pub fn fetch_states(&mut self, context: &mut Context<Self>) {
        let channels = mem::replace(&mut self.state_channels, HashMap::new());

        let request = GetProjectStates {
            projects: channels.keys().cloned().collect(),
        };

        let future = self
            .upstream
            .send(SendRequest(request))
            .map_err(|_| ProjectError::UpstreamFailed)
            .and_then(|response| {
                match response {
                    Ok(mut response) => {
                        for (id, channel) in channels {
                            let state = response
                                .configs
                                .remove(&id)
                                .unwrap_or(None)
                                .unwrap_or_else(|| ProjectStateSnapshot::missing());

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
