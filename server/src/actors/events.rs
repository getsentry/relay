use std::net::IpAddr;
use std::sync::Arc;

use actix::{Actor, Addr, Handler, Message, SyncContext};
use bytes::Bytes;
use url::Url;
use uuid::Uuid;

use semaphore_common::Auth;

use actors::project::Project;
use actors::upstream::UpstreamRelay;

#[derive(Debug, Fail)]
#[fail(display = "could not process event")]
pub struct ProcessingError;

#[derive(Debug, Clone)]
pub struct EventMetaData {
    /// Authentication information (DSN and client)..
    pub auth: Auth,

    /// Value of the origin header in the incoming request, if present.
    pub origin: Option<Url>,

    /// IP address of the submitting remote.
    pub remote_addr: Option<IpAddr>,
}

impl EventMetaData {
    pub fn auth(&self) -> &Auth {
        &self.auth
    }

    pub fn origin(&self) -> Option<&Url> {
        self.origin.as_ref()
    }

    pub fn remote_addr(&self) -> Option<IpAddr> {
        self.remote_addr
    }
}

pub struct EventProcessor {
    upstream: Addr<UpstreamRelay>,
}

impl EventProcessor {
    pub fn new(upstream: Addr<UpstreamRelay>) -> Self {
        EventProcessor { upstream }
    }
}

impl Actor for EventProcessor {
    type Context = SyncContext<Self>;
}

pub struct ProcessEvent {
    pub data: Bytes,
    pub meta: Arc<EventMetaData>,
    pub project: Addr<Project>,
}

pub struct ProcessEventResponse {
    pub event_id: Uuid,
    pub data: Bytes,
}

impl Message for ProcessEvent {
    type Result = Result<ProcessEventResponse, ProcessingError>;
}

impl Handler<ProcessEvent> for EventProcessor {
    type Result = Result<ProcessEventResponse, ProcessingError>;

    fn handle(&mut self, message: ProcessEvent, context: &mut Self::Context) -> Self::Result {
        unimplemented!()
    }
}
