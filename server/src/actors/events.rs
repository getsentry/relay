use std::sync::{Arc, Mutex};
use std::time::Instant;

use actix::fut::result;
use actix::prelude::*;
use bytes::{Bytes, BytesMut};
use failure::Fail;
use futures::prelude::*;
use json_forensics;
use sentry::integrations::failure::event_from_fail;
use serde::Deserialize;

use semaphore_common::{clone, metric, Config, LogError, ProjectId, Uuid};
use semaphore_general::filter::FilterStatKey;
use semaphore_general::pii::PiiProcessor;
use semaphore_general::processor::{process_value, ProcessingState};
use semaphore_general::protocol::{Event, EventId};
use semaphore_general::reason::{OutcomeInvalidReason, OutcomeReason};
use semaphore_general::types::Annotated;
use serde_json;

use crate::actors::controller::{Controller, Shutdown, Subscribe, TimeoutError};
use crate::actors::outcome::{KafkaOutcomeMessage, Outcome, OutcomeProducer};
use crate::actors::project::{
    EventAction, GetEventAction, GetProjectId, GetProjectState, Project, ProjectError,
    ProjectState, RetryAfter,
};
use crate::actors::upstream::{SendRequest, UpstreamRelay, UpstreamRequestError};
use crate::extractors::EventMeta;
use crate::service::ServerError;
use crate::utils::{One, SyncActorFuture, SyncHandle};

#[cfg(feature = "processing")]
use {
    crate::actors::store::{StoreError, StoreEvent, StoreForwarder},
    crate::service::ServerErrorKind,
    failure::ResultExt,
    semaphore_general::filter::should_filter,
    semaphore_general::store::{GeoIpLookup, StoreConfig, StoreProcessor},
};

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
    EventRejected(OutcomeReason),

    #[fail(display = "event filtered with reason: {:?}", _0)]
    EventFiltered(FilterStatKey),

    #[fail(display = "could not serialize event payload")]
    SerializeFailed(#[cause] serde_json::Error),

    #[fail(display = "could not send event to upstream")]
    SendFailed(#[cause] UpstreamRequestError),

    #[cfg(feature = "processing")]
    #[fail(display = "could not store event")]
    StoreFailed(#[cause] StoreError),

    #[fail(display = "sending failed due to rate limit ({}s)", _0)]
    RateLimited(u64, String),

    #[fail(display = "event exceeded its configured lifetime")]
    Timeout,

    #[fail(display = "shutdown timer expired")]
    Shutdown,

    #[fail(display = "upstream rate limited")]
    UpstreamRateLimited,
}

#[cfg(feature = "processing")]
struct EventProcessor {
    config: Arc<Config>,
    geoip_lookup: Option<Arc<GeoIpLookup>>,
}

#[cfg(not(feature = "processing"))]
struct EventProcessor;

impl EventProcessor {
    #[cfg(feature = "processing")]
    pub fn new(config: Arc<Config>, geoip_lookup: Option<Arc<GeoIpLookup>>) -> Self {
        Self {
            config,
            geoip_lookup,
        }
    }

    #[cfg(not(feature = "processing"))]
    pub fn new() -> Self {
        Self
    }

    fn process(&self, message: &ProcessEvent) -> Result<ProcessEventResponse, ProcessingError> {
        log::trace!("processing event {}", message.event_id);
        let mut event = Annotated::<Event>::from_json_bytes(&message.data).map_err(|error| {
            if message.log_failed_payloads {
                let mut event = event_from_fail(&error);
                message.add_to_sentry_event(&mut event);
                sentry::capture_event(event);
            }

            ProcessingError::InvalidJson(error)
        })?;

        if let Some(event) = event.value_mut() {
            event.id = Annotated::new(message.event_id);
        }

        for pii_config in message.project_state.config.pii_configs() {
            let mut processor = PiiProcessor::new(pii_config);
            process_value(&mut event, &mut processor, ProcessingState::root());
        }

        #[cfg(feature = "processing")]
        {
            if self.config.processing_enabled() {
                let geoip_lookup = self.geoip_lookup.as_ref().map(Arc::as_ref);
                let auth = message.meta.auth();

                let store_config = StoreConfig {
                    project_id: Some(message.project_id),
                    client_ip: message.meta.client_addr().map(From::from),
                    client: auth.client_agent().map(str::to_owned),
                    key_id: Some(auth.public_key().to_owned()),
                    protocol_version: Some(auth.version().to_string()),
                    grouping_config: message.project_state.config.grouping_config.clone(),
                    valid_platforms: Default::default(), // TODO(ja): Pending removal
                    max_secs_in_future: Some(self.config.max_secs_in_future()),
                    max_secs_in_past: Some(self.config.max_secs_in_past()),
                    enable_trimming: Some(true),
                    is_renormalize: Some(false),
                    remove_other: Some(true),
                    normalize_user_agent: Some(true),
                };

                let mut store_processor = StoreProcessor::new(store_config, geoip_lookup);
                process_value(&mut event, &mut store_processor, ProcessingState::root());

                if let Some(event) = event.value() {
                    let client_ip = message.meta.client_addr();
                    let filter_settings = &message.project_state.config.filter_settings;
                    if let Err(reason) = should_filter(event, client_ip, filter_settings) {
                        // If the event should be filtered, no more processing is needed
                        return Ok(ProcessEventResponse::Filtered { reason });
                    }
                }
            }
        }

        let data = event
            .to_json()
            .map_err(ProcessingError::SerializeFailed)?
            .into();

        Ok(ProcessEventResponse::Valid { data })
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
        if let Some(client_addr) = self.meta.client_addr() {
            event
                .extra
                .insert("client_addr".to_string(), client_addr.to_string().into());
        }
    }
}

#[cfg_attr(not(feature = "processing"), allow(dead_code))]
enum ProcessEventResponse {
    Valid { data: Bytes },
    Filtered { reason: FilterStatKey },
}

impl Message for ProcessEvent {
    type Result = Result<ProcessEventResponse, ProcessingError>;
}

impl Handler<ProcessEvent> for EventProcessor {
    type Result = Result<ProcessEventResponse, ProcessingError>;

    fn handle(&mut self, message: ProcessEvent, _context: &mut Self::Context) -> Self::Result {
        metric!(timer("event.wait_time") = message.start_time.elapsed());
        metric!(timer("event.processing_time"), { self.process(&message) })
    }
}

pub struct EventManager {
    config: Arc<Config>,
    upstream: Addr<UpstreamRelay>,
    processor: Addr<EventProcessor>,
    current_active_events: u32,
    shutdown: SyncHandle,
    outcome_producer: Addr<OutcomeProducer>,

    #[cfg(feature = "processing")]
    store_forwarder: Option<Addr<StoreForwarder>>,
}

impl EventManager {
    #[cfg(feature = "processing")]
    pub fn create(
        config: Arc<Config>,
        upstream: Addr<UpstreamRelay>,
        outcome_producer: Addr<OutcomeProducer>,
    ) -> Result<Self, ServerError> {
        let geoip_lookup = match config.geoip_path() {
            Some(p) => Some(Arc::new(
                GeoIpLookup::open(p).context(ServerErrorKind::GeoIpError)?,
            )),
            None => None,
        };

        // TODO: Make the number configurable via config file
        let thread_count = num_cpus::get();
        log::info!("starting {} event processing workers", thread_count);

        let processor = SyncArbiter::start(
            thread_count,
            clone!(config, || {
                EventProcessor::new(config.clone(), geoip_lookup.clone())
            }),
        );

        let store_forwarder = if config.processing_enabled() {
            Some(StoreForwarder::create(config.clone())?.start())
        } else {
            None
        };

        Ok(EventManager {
            config,
            upstream,
            processor,
            current_active_events: 0,
            shutdown: SyncHandle::new(),
            store_forwarder,
            outcome_producer,
        })
    }

    #[cfg(not(feature = "processing"))]
    pub fn create(
        config: Arc<Config>,
        upstream: Addr<UpstreamRelay>,
        outcome_producer: Addr<OutcomeProducer>,
    ) -> Result<Self, ServerError> {
        // TODO: Make the number configurable via config file
        let thread_count = num_cpus::get();

        log::info!("starting {} event processing workers", thread_count);
        let processor = SyncArbiter::start(thread_count, EventProcessor::new);

        Ok(EventManager {
            config,
            upstream,
            processor,
            current_active_events: 0,
            shutdown: SyncHandle::new(),
            outcome_producer,
        })
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
    pub start_time: Instant,
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
            json_forensics::translate_slice(&mut data[..]);
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
            start_time: message.start_time,
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
    pub start_time: Instant,
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

        let upstream = self.upstream.clone();
        let processor = self.processor.clone();
        let log_failed_payloads = self.config.log_failed_payloads();
        let outcome_producer = self.outcome_producer.clone();

        #[cfg(feature = "processing")]
        let store_forwarder = self.store_forwarder.clone();

        let HandleEvent {
            data,
            meta,
            project,
            event_id,
            start_time,
        } = message;

        let project_id_for_err = Arc::new(Mutex::new(None::<u64>));
        let org_id_for_err = Arc::new(Mutex::new(None::<u64>));

        let future = project
            .send(GetProjectId)
            .map(One::into_inner)
            .map_err(ProcessingError::ScheduleFailed)
            .and_then(clone! { project_id_for_err, org_id_for_err, |project_id| {
                *project_id_for_err.lock().unwrap() = Some(project_id);
                project
                    .send(GetEventAction::fetched(meta.clone()))
                    .map_err(ProcessingError::ScheduleFailed)
                    .and_then(|action| match action.map_err(ProcessingError::NoAction)? {
                        EventAction::Accept => Ok(()),
                        EventAction::RetryAfter(secs, reason) => {
                            Err(ProcessingError::RateLimited(secs, reason))
                        }
                        EventAction::Discard(reason) => Err(ProcessingError::EventRejected(reason)),
                    })
                    .and_then(clone!(project, |_| project
                        .send(GetProjectState)
                        .map_err(ProcessingError::ScheduleFailed)
                        .and_then(|result| result.map_err(ProcessingError::PiiFailed))))
                    .and_then(clone! (meta,  |project_state| {
                        *org_id_for_err.lock().unwrap() = project_state.organization_id;
                       processor
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
                        .flatten()
                        }))
                    .and_then(|response| match response {
                        ProcessEventResponse::Valid { data } => Ok(data),
                        ProcessEventResponse::Filtered { reason } => {
                            Err(ProcessingError::EventFiltered(reason))
                        }
                    })
                    .and_then(move |processed| {
                        #[cfg(feature = "processing")]
                        {
                            if let Some(store_forwarder) = store_forwarder {
                                log::trace!("sending event to kafka {}", event_id);
                                let future = store_forwarder
                                    .send(StoreEvent {
                                        payload: processed,
                                        event_id,
                                        start_time,
                                        project_id,
                                    })
                                    .map_err(ProcessingError::ScheduleFailed)
                                    .and_then(move |result| {
                                        result.map_err(ProcessingError::StoreFailed)
                                    });

                                return Box::new(future) as ResponseFuture<_, _>;
                            }
                        }

                        log::trace!("sending event to sentry endpoint {}", event_id);
                        let request = SendRequest::post(format!("/api/{}/store/", project_id))
                            .build(move |builder| {
                                if let Some(origin) = meta.origin() {
                                    builder.header("Origin", origin.to_string());
                                }

                                builder
                                    .header("X-Sentry-Auth", meta.auth().to_string())
                                    .header("X-Forwarded-For", meta.forwarded_for())
                                    .header("Content-Type", "application/json")
                                    .body(processed)
                            });

                        let future = upstream
                            .send(request)
                            .map_err(ProcessingError::ScheduleFailed)
                            .and_then(move |result| {
                                result.map_err(move |error| match error {
                                    UpstreamRequestError::RateLimited(secs) => {
                                        project.do_send(RetryAfter { secs });
                                        ProcessingError::UpstreamRateLimited
                                    }
                                    other => ProcessingError::SendFailed(other),
                                })
                            });

                        Box::new(future) as ResponseFuture<_, _>
                    })
            }})
            .inspect(move |_| metric!(timer("event.total_time") = start_time.elapsed()))
            .into_actor(self)
            .timeout(self.config.event_buffer_expiry(), ProcessingError::Timeout)
            .sync(&self.shutdown, ProcessingError::Shutdown)
            .map(|_, _, _| metric!(counter("event.accepted") += 1))
            .map_err(move |error, _, _| {
                log::warn!("error processing event {}: {}", event_id, LogError(&error));
                metric!(counter("event.rejected") += 1);
                let outcome_params: Option<(Outcome, OutcomeReason)> = match &error {
                    ProcessingError::InvalidJson(_)
                    | ProcessingError::SerializeFailed(_)
                    | ProcessingError::ScheduleFailed(_)
                    | ProcessingError::PiiFailed(_)
                    | ProcessingError::Timeout
                    | ProcessingError::Shutdown => {
                        Some((Outcome::Invalid, OutcomeInvalidReason::Internal.into()))
                    }

                    #[cfg(feature = "processing")]
                    ProcessingError::StoreFailed(_store_error) => {
                        Some((Outcome::Invalid, OutcomeInvalidReason::Internal.into()))
                    }
                    ProcessingError::EventRejected(outcome_reason) => {
                        Some((Outcome::Invalid, outcome_reason.clone()))
                    }
                    ProcessingError::EventFiltered(filter_stat_key) => {
                        Some((Outcome::Filtered, (*filter_stat_key).into()))
                    }

                    ProcessingError::NoAction(_)
                    // if we have upstream than we don't emit outcomes (the upstream should deal with this)
                    | ProcessingError::SendFailed(_)
                    | ProcessingError::UpstreamRateLimited => None,

                    ProcessingError::RateLimited(_timeout, reason) => Some((
                        Outcome::RateLimited,
                        OutcomeReason::RateLimited(reason.clone()),
                    )),
                };
                if let Some((outcome, reason)) = outcome_params {
                    outcome_producer.do_send(KafkaOutcomeMessage {
                        timestamp: Instant::now(),
                        project_id: *(project_id_for_err.lock().unwrap()),
                        org_id: *(org_id_for_err.lock().unwrap()),
                        key_id: None,
                        outcome,
                        event_id: Some(event_id),
                        reason,
                    })
                }
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
