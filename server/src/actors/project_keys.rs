use std::borrow::Cow;
use std::collections::hash_map::{Entry, HashMap};
use std::sync::Arc;
use std::time::Instant;

use actix::fut;
use actix::prelude::*;
use actix_web::http::Method;
use failure::Fail;
use futures::{future::Shared, sync::oneshot, Future};
use serde::{Deserialize, Serialize};

use semaphore_common::{metric, Config, LogError, ProjectId};

use crate::actors::upstream::{SendQuery, UpstreamQuery, UpstreamRelay};

type ProjectKey = String;

#[derive(Clone, Copy, Debug, Fail)]
#[fail(display = "failed to fetch project id for project key")]
pub struct ProjectKeyError;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GetProjectIds {
    pub public_keys: Vec<ProjectKey>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetProjectIdsResponse {
    pub project_ids: HashMap<ProjectKey, Option<ProjectId>>,
}

impl UpstreamQuery for GetProjectIds {
    type Response = GetProjectIdsResponse;

    fn method(&self) -> Method {
        Method::POST
    }

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed("/api/0/relays/projectids/")
    }
}

#[derive(Debug)]
struct ProjectIdChannel {
    sender: oneshot::Sender<Option<ProjectId>>,
    receiver: Shared<oneshot::Receiver<Option<ProjectId>>>,
}

impl ProjectIdChannel {
    pub fn new() -> Self {
        let (sender, receiver) = oneshot::channel();

        Self {
            sender,
            receiver: receiver.shared(),
        }
    }

    pub fn send(self, project_id: Option<ProjectId>) {
        self.sender.send(project_id).ok();
    }

    pub fn receiver(&self) -> Shared<oneshot::Receiver<Option<ProjectId>>> {
        self.receiver.clone()
    }
}

/// Reverse lookup for project keys.
///
/// This is used for the legacy store endpoint (`/api/store/`) to resolve the project id from a
/// public key.
pub struct ProjectKeyLookup {
    config: Arc<Config>,
    upstream: Addr<UpstreamRelay>,
    project_ids: HashMap<ProjectKey, Option<ProjectId>>,
    id_channels: HashMap<ProjectKey, ProjectIdChannel>,
}

impl ProjectKeyLookup {
    pub fn new(config: Arc<Config>, upstream: Addr<UpstreamRelay>) -> Self {
        Self {
            config,
            upstream,
            project_ids: HashMap::new(),
            id_channels: HashMap::new(),
        }
    }

    fn fetch_project_id(&mut self, public_key: ProjectKey, context: &mut Context<Self>) {
        log::debug!("fetching project id for public key {}", public_key);
        let public_keys = vec![public_key];

        let request = GetProjectIds {
            public_keys: public_keys.clone(),
        };

        metric!(counter("project_id.request") += 1);
        let request_start = Instant::now();

        self.upstream
            .send(SendQuery(request))
            .into_actor(self)
            .then(move |result, slf, _context| {
                metric!(timer("project_id.request.duration") = request_start.elapsed());

                let mut project_ids = match result {
                    Ok(Ok(response)) => response.project_ids,
                    Ok(Err(upstream_err)) => {
                        log::error!("error fetching project ids: {}", LogError(&upstream_err));
                        HashMap::new()
                    }
                    Err(mailbox_err) => {
                        log::error!("error fetching project ids: {}", LogError(&mailbox_err));
                        HashMap::new()
                    }
                };

                for key in public_keys {
                    let project_id = project_ids.remove(&key).and_then(|opt| opt);
                    if let Some(channel) = slf.id_channels.remove(&key) {
                        channel.send(project_id);
                    }
                    slf.project_ids.insert(key, project_id);
                }

                fut::ok(())
            })
            .spawn(context);
    }
}

impl Actor for ProjectKeyLookup {
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

pub struct GetProjectId(pub ProjectKey);

impl Message for GetProjectId {
    type Result = Result<Option<ProjectId>, ProjectKeyError>;
}

impl Handler<GetProjectId> for ProjectKeyLookup {
    type Result = Response<Option<ProjectId>, ProjectKeyError>;

    fn handle(&mut self, message: GetProjectId, context: &mut Context<Self>) -> Self::Result {
        let key = message.0;

        if let Some(project_id) = self.project_ids.get(&key) {
            return Response::reply(Ok(*project_id));
        }

        let channel = match self.id_channels.entry(key.clone()) {
            Entry::Occupied(entry) => entry.get().receiver(),
            Entry::Vacant(entry) => {
                let channel = ProjectIdChannel::new();
                let receiver = channel.receiver();
                entry.insert(channel);

                // Fetch each project id individually. The result is cached indefinitely and those
                // requests only happen infrequently. Since the store endpoint waits on this result,
                // we need to execute as fast as possible.
                self.fetch_project_id(key, context);

                receiver
            }
        };

        let response = channel.map(|shared| *shared).map_err(|_| ProjectKeyError);
        Response::r#async(response)
    }
}
