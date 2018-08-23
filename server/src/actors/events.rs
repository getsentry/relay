use std::net::IpAddr;
use std::sync::Arc;

use actix::prelude::*;
use bytes::Bytes;
use futures::prelude::*;
use url::Url;
use uuid::Uuid;

use semaphore_common::v8::{self, Annotated, Event};
use semaphore_common::Auth;

use actors::project::{GetPiiConfig, Project, ProjectError};

#[derive(Debug, Fail)]
pub enum ProcessingError {
    #[fail(display = "invalid JSON data")]
    InvalidJson(#[cause] v8::Error),

    #[fail(display = "could not schedule project fetching")]
    ScheduleFailed(#[cause] MailboxError),

    #[fail(display = "failed to resolve PII config for project")]
    ProjectFailed(#[cause] ProjectError),

    #[fail(display = "could not serialize event payload")]
    SerializeFailed(#[cause] v8::Error),
}

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

pub struct EventProcessor;

impl EventProcessor {
    pub fn new() -> Self {
        EventProcessor
    }
}

impl Actor for EventProcessor {
    type Context = SyncContext<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!("Event processing worker started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("Event processing worker stopped");
    }
}

pub struct ProcessEvent {
    pub data: Bytes,
    pub meta: Arc<EventMetaData>,
    pub project: Addr<Project>,
}

pub struct ProcessEventResponse {
    pub event_id: Option<Uuid>,
    pub data: Bytes,
}

impl Message for ProcessEvent {
    type Result = Result<ProcessEventResponse, ProcessingError>;
}

impl Handler<ProcessEvent> for EventProcessor {
    type Result = Result<ProcessEventResponse, ProcessingError>;

    fn handle(&mut self, message: ProcessEvent, _context: &mut Self::Context) -> Self::Result {
        let mut event = Annotated::<Event>::from_json_bytes(&message.data)
            .map_err(ProcessingError::InvalidJson)?;

        if let Some(event) = event.value_mut() {
            match event.id.value() {
                Some(Some(_)) => (),
                _ => event.id.set_value(Some(Some(Uuid::new_v4()))),
            }
        }

        let pii_config = message
            .project
            .send(GetPiiConfig)
            .wait()
            .map_err(ProcessingError::ScheduleFailed)?
            .map_err(ProcessingError::ProjectFailed)?;

        let processed_event = match pii_config {
            Some(pii_config) => pii_config.processor().process_root_value(event),
            None => event,
        };

        let event_id = processed_event
            .value()
            .and_then(|event| event.id.value())
            .and_then(|id| *id);

        let data = processed_event
            .to_json()
            .map_err(ProcessingError::SerializeFailed)?
            .into();

        Ok(ProcessEventResponse { event_id, data })
    }
}
