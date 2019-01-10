use std::sync::Arc;
use std::time::Instant;

use ::actix::fut::result;
use ::actix::prelude::*;
use bytes::{Bytes, BytesMut};
use failure::Fail;
use futures::prelude::*;
use python_json_read_adapter;
use sentry::integrations::failure::event_from_fail;
use serde::Deserialize;
use serde_json;

use semaphore_common::{metric, Config, LogError, ProjectId, Uuid};
use semaphore_general::pii::PiiProcessor;
use semaphore_general::processor::{process_value, ProcessingState};
use semaphore_general::protocol::{Event, EventId};
use semaphore_general::types::Annotated;

use crate::actors::controller::{Controller, Shutdown, Subscribe, TimeoutError};
use crate::actors::project::{
    EventAction, GetEventAction, GetProjectId, GetProjectState, Project, ProjectError,
    ProjectState, RetryAfter,
};
use crate::actors::upstream::{SendRequest, UpstreamRelay, UpstreamRequestError};
use crate::extractors::EventMeta;
use crate::utils::{SyncActorFuture, SyncHandle};

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
pub enum EventError {
    #[fail(display = "invalid JSON data")]
    InvalidJson(#[cause] serde_json::Error),

    #[fail(display = "Too many events (max_concurrent_events reached)")]
    TooManyEvents,
}

#[derive(Debug, Fail)]
enum ProcessingError {
    #[fail(display = "invalid event data")]
    InvalidJson(#[cause] serde_json::Error),

    #[fail(display = "could not schedule project fetch")]
    ScheduleFailed(#[cause] MailboxError),

    #[fail(display = "failed to determine event action")]
    NoAction(#[cause] ProjectError),

    #[fail(display = "failed to resolve PII config for project")]
    PiiFailed(#[cause] ProjectError),

    #[fail(display = "event submission rejected")]
    EventRejected,

    #[fail(display = "could not serialize event payload")]
    SerializeFailed(#[cause] serde_json::Error),

    #[fail(display = "could not send event to upstream")]
    SendFailed(#[cause] UpstreamRequestError),

    #[fail(display = "sending failed due to rate limit ({}s)", _0)]
    RateLimited(u64),

    #[fail(display = "event exceeded its configured lifetime")]
    Timeout,

    #[fail(display = "shutdown timer expired")]
    Shutdown,
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
    pub data: Bytes,
    pub meta: Arc<EventMeta>,
    pub event_id: EventId,
    pub project_id: ProjectId,
    pub project_state: Arc<ProjectState>,
    pub log_failed_payloads: bool,
    pub start_time: Instant,
}

impl ProcessEvent {
    fn add_to_sentry_event(&self, event: &mut sentry::protocol::Event<'_>) {
        // Inject the body payload for debugging purposes and identify the exception
        event.message = Some(format!("body: {}", String::from_utf8_lossy(&self.data)));
        if let Some(exception) = event.exception.last_mut() {
            exception.ty = "BadEventPayload".into();
        }

        // Identify the project as user to make payload errors indexable by customer
        event.user = Some(sentry::User {
            id: Some(self.project_id.to_string()),
            ..Default::default()
        });

        // Inject all available meta as extra
        event.extra.insert(
            "sentry_auth".to_string(),
            self.meta.auth().to_string().into(),
        );
        event.extra.insert(
            "forwarded_for".to_string(),
            self.meta.forwarded_for().into(),
        );
        if let Some(origin) = self.meta.origin() {
            event
                .extra
                .insert("origin".to_string(), origin.to_string().into());
        }
        if let Some(remote_addr) = self.meta.remote_addr() {
            event
                .extra
                .insert("remote_addr".to_string(), remote_addr.to_string().into());
        }
    }

    fn process(&self) -> Result<ProcessEventResponse, ProcessingError> {
        log::trace!("processing event {}", self.event_id);
        let mut event = Annotated::<Event>::from_json_bytes(&self.data).map_err(|error| {
            if self.log_failed_payloads {
                let mut event = event_from_fail(&error);
                self.add_to_sentry_event(&mut event);
                sentry::capture_event(event);
            }

            ProcessingError::InvalidJson(error)
        })?;

        if let Some(event) = event.value_mut() {
            event.id = Annotated::new(self.event_id);
        }

        if let Some(ref pii_config) = self.project_state.config.pii_config {
            let mut processor = PiiProcessor::new(&pii_config);
            process_value(&mut event, &mut processor, ProcessingState::root());
        };

        let data = event
            .to_json()
            .map_err(ProcessingError::SerializeFailed)?
            .into();

        Ok(ProcessEventResponse { data })
    }
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
        metric!(timer("event.wait_time") = message.start_time.elapsed());
        metric!(timer("event.processing_time"), { message.process() })
    }
}

pub struct EventManager {
    config: Arc<Config>,
    upstream: Addr<UpstreamRelay>,
    processor: Addr<EventProcessor>,
    current_active_events: u32,
    shutdown: SyncHandle,
}

impl EventManager {
    pub fn new(config: Arc<Config>, upstream: Addr<UpstreamRelay>) -> Self {
        // TODO: Make the number configurable via config file
        let thread_count = num_cpus::get();

        log::info!("starting {} event processing workers", thread_count);
        let processor = SyncArbiter::start(thread_count, EventProcessor::new);

        EventManager {
            config,
            upstream,
            processor,
            current_active_events: 0,
            shutdown: SyncHandle::new(),
        }
    }
}

impl Actor for EventManager {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        log::info!("event manager started");
        Controller::from_registry().do_send(Subscribe(context.address().recipient()));
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        log::info!("event manager stopped");
    }
}

#[derive(Deserialize)]
struct EventIdHelper {
    #[serde(default, rename = "event_id")]
    id: Option<EventId>,
}

pub struct QueueEvent {
    pub data: Bytes,
    pub meta: Arc<EventMeta>,
    pub project: Addr<Project>,
}

impl Message for QueueEvent {
    type Result = Result<EventId, EventError>;
}

impl Handler<QueueEvent> for EventManager {
    type Result = Result<EventId, EventError>;

    fn handle(&mut self, message: QueueEvent, context: &mut Self::Context) -> Self::Result {
        let mut data: BytesMut = message.data.into();

        // python clients are well known to send crappy JSON in the Sentry world.  The reason
        // for this is that they send NaN and Infinity as invalid JSON tokens.  The code sentry
        // server could deal with this but we cannot.  To work around this issue we run a basic
        // character substitution on the input stream but only if we detect a Python agent.
        //
        // this is done here so that the rest of the code can assume valid JSON.
        if message.meta.needs_legacy_python_json_support() {
            python_json_read_adapter::translate_slice(&mut data[..]);
        }

        // Ensure that the event has a UUID. It will be returned from this message and from the
        // incoming store request. To uncouple it from the workload on the processing workers, this
        // requires to synchronously parse a minimal part of the JSON payload. If the JSON payload
        // is invalid, processing can be skipped altogether.
        let event_id = serde_json::from_slice::<EventIdHelper>(&data)
            .map(|event| event.id)
            .map_err(EventError::InvalidJson)?
            .unwrap_or_else(|| EventId(Uuid::new_v4()));

        if self.config.event_buffer_size() <= self.current_active_events {
            return Err(EventError::TooManyEvents);
        }

        self.current_active_events += 1;

        // Actual event handling is performed asynchronously in a separate future. The lifetime of
        // that future will be tied to the EventManager's context. This allows to keep the Project
        // actor alive even if it is cleaned up in the ProjectManager.
        context.notify(HandleEvent {
            data: data.into(),
            meta: message.meta,
            project: message.project,
            event_id,
        });

        log::trace!("queued event {}", event_id);
        Ok(event_id)
    }
}

struct HandleEvent {
    pub data: Bytes,
    pub meta: Arc<EventMeta>,
    pub project: Addr<Project>,
    pub event_id: EventId,
}

impl Message for HandleEvent {
    type Result = Result<(), ()>;
}

impl Handler<HandleEvent> for EventManager {
    type Result = ResponseActFuture<Self, (), ()>;

    fn handle(&mut self, message: HandleEvent, _context: &mut Self::Context) -> Self::Result {
        // We measure three timers while handling events, once they have been initially accepted:
        //
        // 1. `event.wait_time`: The time we take to get all dependencies for events before
        //    they actually start processing. This includes scheduling overheads, project config
        //    fetching, batched requests and congestions in the sync processor arbiter. This does
        //    not include delays in the incoming request (body upload) and skips all events that are
        //    fast-rejected.
        //
        // 2. `event.processing_time`: The time the sync processor takes to parse the event payload,
        //    apply normalizations, strip PII and finally re-serialize it into a byte stream. This
        //    is recorded directly in the EventProcessor.
        //
        // 3. `event.total_time`: The full time an event takes from being initially accepted up to
        //    being sent to the upstream (including delays in the upstream). This can be regarded
        //    the total time an event spent in this relay, corrected by incoming network delays.
        let start_time = Instant::now();

        let upstream = self.upstream.clone();
        let processor = self.processor.clone();
        let log_failed_payloads = self.config.log_failed_payloads();

        let HandleEvent {
            data,
            meta,
            project,
            event_id,
        } = message;

        let future = project
            .send(GetProjectId)
            .map(|one| one.into_inner())
            .map_err(ProcessingError::ScheduleFailed)
            .and_then(move |project_id| {
                project
                    .send(GetEventAction::fetched(meta.clone()))
                    .map_err(ProcessingError::ScheduleFailed)
                    .and_then(|action| match action.map_err(ProcessingError::NoAction)? {
                        EventAction::Accept => Ok(()),
                        EventAction::RetryAfter(s) => Err(ProcessingError::RateLimited(s)),
                        EventAction::Discard => Err(ProcessingError::EventRejected),
                    })
                    .and_then(clone!(project, |_| project
                        .send(GetProjectState)
                        .map_err(ProcessingError::ScheduleFailed)
                        .and_then(|result| result.map_err(ProcessingError::PiiFailed))))
                    .and_then(clone!(meta, |project_state| processor
                        .send(ProcessEvent {
                            data,
                            meta,
                            event_id,
                            project_id,
                            project_state,
                            log_failed_payloads,
                            start_time,
                        })
                        .map_err(ProcessingError::ScheduleFailed)
                        .flatten()))
                    .and_then(move |processed| {
                        log::trace!("sending event {}", event_id);
                        let request = SendRequest::post(format!("/api/{}/store/", project_id))
                            .build(move |builder| {
                                if let Some(origin) = meta.origin() {
                                    builder.header("Origin", origin.to_string());
                                }

                                builder
                                    .header("X-Sentry-Auth", meta.auth().to_string())
                                    .header("X-Forwarded-For", meta.forwarded_for())
                                    .header("Content-Type", "application/json")
                                    .body(processed.data)
                            });

                        upstream
                            .send(request)
                            .map_err(ProcessingError::ScheduleFailed)
                            .and_then(move |result| {
                                result.map_err(move |error| match error {
                                    UpstreamRequestError::RateLimited(secs) => {
                                        project.do_send(RetryAfter { secs });
                                        ProcessingError::RateLimited(secs)
                                    }
                                    other => ProcessingError::SendFailed(other),
                                })
                            })
                            .inspect(move |_| {
                                metric!(timer("event.total_time") = start_time.elapsed())
                            })
                    })
            })
            .into_actor(self)
            .timeout(self.config.event_buffer_expiry(), ProcessingError::Timeout)
            .sync(&self.shutdown, ProcessingError::Shutdown)
            .map(|_, _, _| metric!(counter("event.accepted") += 1))
            .map_err(move |error, _, _| {
                log::warn!("error processing event {}: {}", event_id, LogError(&error));
                metric!(counter("event.rejected") += 1);
            })
            .then(|x, slf, _| {
                slf.current_active_events -= 1;
                result(x)
            });

        Box::new(future)
    }
}

impl Handler<Shutdown> for EventManager {
    type Result = ResponseFuture<(), TimeoutError>;

    fn handle(&mut self, message: Shutdown, _context: &mut Self::Context) -> Self::Result {
        match message.timeout {
            Some(timeout) => self.shutdown.timeout(timeout),
            None => self.shutdown.now(),
        }
    }
}
