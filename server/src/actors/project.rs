use std::collections::HashMap;
use std::sync::Arc;

use actix::{Actor, Addr, AsyncContext, Context, Handler, MailboxError, Message, Response};
use futures::Future;
use parking_lot::RwLock;

// TODO(ja): Move this here and rename to ProjectState
use semaphore_aorta::ProjectStateSnapshot;
use semaphore_common::ProjectId;

use actors::upstream::UpstreamRelay;

#[derive(Fail, Debug)]
pub enum ProjectError {
    #[fail(display = "internal error: {0}", _0)]
    Mailbox(#[fail(cause)] MailboxError),
    #[fail(display = "failed to fetch project state")]
    FetchFailed,
}

pub struct Project {
    id: ProjectId,
    manager: Addr<ProjectManager>,
    state: Arc<RwLock<Option<Arc<ProjectStateSnapshot>>>>,
}

impl Project {
    pub fn new(id: ProjectId, manager: Addr<ProjectManager>) -> Self {
        Project {
            id,
            manager,
            state: Arc::new(RwLock::new(None)),
        }
    }

    pub fn state(&self) -> Option<Arc<ProjectStateSnapshot>> {
        (*self.state.read()).clone()
    }

    pub fn get_state(&self) -> Response<Arc<ProjectStateSnapshot>, ProjectError> {
        let state = self.state.clone();
        match self.state() {
            Some(ref state) => Response::reply(Ok(state.clone())),
            None => Response::async(
                self.manager
                    .send(FetchProjectState { id: self.id })
                    .map_err(ProjectError::Mailbox)
                    .and_then(move |response| {
                        let response = Arc::new(response?);
                        *state.write() = Some(response.clone());
                        Ok(response)
                    }),
            ),
        }
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

    fn handle(&mut self, _message: GetProjectState, _ctx: &mut Context<Self>) -> Self::Result {
        self.get_state()
    }
}

pub struct ProjectManager {
    projects: HashMap<ProjectId, Addr<Project>>,
    upstream: Addr<UpstreamRelay>,
}

impl ProjectManager {
    pub fn new(upstream: Addr<UpstreamRelay>) -> Self {
        ProjectManager {
            projects: HashMap::new(),
            upstream,
        }
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
    id: ProjectId,
}

impl Message for GetProject {
    type Result = Addr<Project>;
}

impl Handler<GetProject> for ProjectManager {
    type Result = Addr<Project>;

    fn handle(&mut self, message: GetProject, ctx: &mut Context<Self>) -> Self::Result {
        self.projects
            .entry(message.id)
            .or_insert_with(|| Project::new(message.id, ctx.address()).start())
            .clone()
    }
}

pub struct FetchProjectState {
    id: ProjectId,
}

impl Message for FetchProjectState {
    type Result = Result<ProjectStateSnapshot, ProjectError>;
}

impl Handler<FetchProjectState> for ProjectManager {
    type Result = Response<ProjectStateSnapshot, ProjectError>;

    fn handle(&mut self, message: FetchProjectState, _ctx: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}
