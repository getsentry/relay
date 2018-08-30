use std::sync::Arc;

use actix::prelude::*;
use bytes::Bytes;
use futures::prelude::*;
use num_cpus;
use serde_json;
use uuid::Uuid;

use semaphore_common::v8::{self, Annotated, Event};
use semaphore_common::Config;

use actors::project::{
    EventAction, GetEventAction, GetProjectId, GetProjectState, Project, ProjectError, ProjectState,
};
use actors::upstream::{SendRequest, UpstreamRelay, UpstreamRequestError};
use extractors::EventMetaData;

macro_rules! clone {
    (@param _) => ( _ );
    (@param $x:ident) => ( $x );
    ($($n:ident),+ , || $body:expr) => (
        {
            $( let $n = $n.clone(); )+
            move || $body
        }
    );
    ($($n:ident),+ , |$($p:tt),+| $body:expr) => (
        {
            $( let $n = $n.clone(); )+
            move |$(clone!(@param $p),)+| $body
        }
    );
}

#[derive(Debug, Fail)]
pub enum ProcessingError {
    #[fail(display = "invalid JSON data")]
    InvalidJson(#[cause] v8::Error),

    #[fail(display = "could not schedule project fetching")]
    ScheduleFailed(#[cause] MailboxError),

    #[fail(display = "failed to resolve PII config for project")]
    PiiFailed(#[cause] ProjectError),

    #[fail(display = "failed to resolve project information")]
    ProjectFailed,

    #[fail(display = "event submission rejected")]
    EventRejected,

    #[fail(display = "could not serialize event payload")]
    SerializeFailed(#[cause] v8::Error),

    #[fail(display = "could not send event to upstream")]
    SendFailed(#[cause] UpstreamRequestError),

    #[fail(display = "event exceeded its configured lifetime")]
    Timeout,
}

struct EventProcessor;

impl EventProcessor {
    pub fn new() -> Self {
        EventProcessor
    }
}

impl Actor for EventProcessor {
    type Context = SyncContext<Self>;
}

struct ProcessEvent {
    pub data: Arc<Bytes>,
    pub meta: Arc<EventMetaData>,
    pub event_id: Uuid,
    pub project_state: Arc<ProjectState>,
}

struct ProcessEventResponse {
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
            event.id.set_value(Some(Some(message.event_id)))
        }

        let processed_event = match message.project_state.config.pii_config {
            Some(ref pii_config) => pii_config.processor().process_root_value(event),
            None => event,
        };

        let data = processed_event
            .to_json()
            .map_err(ProcessingError::SerializeFailed)?
            .into();

        Ok(ProcessEventResponse { data })
    }
}

pub struct EventManager {
    config: Arc<Config>,
    upstream: Addr<UpstreamRelay>,
    processor: Addr<EventProcessor>,
}

impl EventManager {
    pub fn new(config: Arc<Config>, upstream: Addr<UpstreamRelay>) -> Self {
        // TODO: Make the number configurable via config file
        let thread_count = num_cpus::get();

        info!("starting {} event processing workers", thread_count);
        let processor = SyncArbiter::start(thread_count, EventProcessor::new);

        EventManager {
            config,
            upstream,
            processor,
        }
    }
}

impl Actor for EventManager {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!("event manager started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("event manager stopped");
    }
}

#[derive(Deserialize)]
struct EventIdHelper {
    #[serde(default, rename = "event_id")]
    id: Option<Uuid>,
}

pub struct QueueEvent {
    pub data: Arc<Bytes>,
    pub meta: Arc<EventMetaData>,
    pub project: Addr<Project>,
}

impl Message for QueueEvent {
    type Result = Result<Uuid, ProcessingError>;
}

impl Handler<QueueEvent> for EventManager {
    type Result = Result<Uuid, ProcessingError>;

    fn handle(&mut self, message: QueueEvent, context: &mut Self::Context) -> Self::Result {
        // Ensure that the event has a UUID. It will be returned from this message and from the
        // incoming store request. To uncouple it from the workload on the processing workers, this
        // requires to synchronously parse a minimal part of the JSON payload. If the JSON payload
        // is invalid, processing can be skipped altogether.
        let event_id = serde_json::from_slice::<EventIdHelper>(&message.data)
            .map(|event| event.id)
            .map_err(ProcessingError::InvalidJson)?
            .unwrap_or_else(Uuid::new_v4);

        // Actual event handling is performed asynchronously in a separate future. The lifetime of
        // that future will be tied to the EventManager's context. This allows to keep the Project
        // actor alive even if it is cleaned up in the ProjectManager.
        context.notify(HandleEvent {
            data: message.data,
            meta: message.meta,
            project: message.project,
            event_id,
        });

        Ok(event_id)
    }
}

struct HandleEvent {
    pub data: Arc<Bytes>,
    pub meta: Arc<EventMetaData>,
    pub project: Addr<Project>,
    pub event_id: Uuid,
}

impl Message for HandleEvent {
    type Result = Result<(), ()>;
}

impl Handler<HandleEvent> for EventManager {
    type Result = ResponseActFuture<Self, (), ()>;

    fn handle(&mut self, message: HandleEvent, _context: &mut Self::Context) -> Self::Result {
        let upstream = self.upstream.clone();
        let processor = self.processor.clone();

        let HandleEvent {
            data,
            meta,
            project,
            event_id,
        } = message;

        let future = project
            .send(GetEventAction::fetched(meta.clone()))
            .map_err(ProcessingError::ScheduleFailed)
            .and_then(|action| match action.map_err(ProcessingError::PiiFailed)? {
                EventAction::Accept => Ok(()),
                EventAction::Discard => Err(ProcessingError::EventRejected),
            })
            .and_then(clone!(project, |_| project
                .send(GetProjectState)
                .map_err(ProcessingError::ScheduleFailed)
                .and_then(|result| result.map_err(ProcessingError::PiiFailed))))
            .and_then(clone!(meta, event_id, |project_state| processor
                .send(ProcessEvent {
                    data,
                    meta,
                    event_id,
                    project_state,
                })
                .map_err(ProcessingError::ScheduleFailed)
                .flatten()))
            .join(
                project
                    .send(GetProjectId)
                    .map_err(ProcessingError::ScheduleFailed)
                    .and_then(|option| option.ok_or(ProcessingError::ProjectFailed)),
            )
            .and_then(move |(processed, project_id)| {
                let request = SendRequest::post(format!("/api/{}/store/", project_id)).build(
                    move |builder| {
                        if let Some(origin) = meta.origin() {
                            builder.header("Origin", origin.to_string());
                        }

                        builder
                            .header("X-Sentry-Auth", meta.auth().to_string())
                            .header("X-Forwarded-For", meta.forwarded_for())
                            .body(processed.data)
                    },
                );

                upstream
                    .send(request)
                    .map_err(ProcessingError::ScheduleFailed)
                    .and_then(|result| result.map_err(ProcessingError::SendFailed))
            })
            .into_actor(self)
            .timeout(self.config.event_buffer_expiry(), ProcessingError::Timeout)
            .map_err(move |error, _, _| {
                error!("error processing event {}: {}", event_id, error);
                metric!(counter("event.rejected") += 1);
            });

        Box::new(future)
    }
}
