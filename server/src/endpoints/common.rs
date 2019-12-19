//! Common facilities for ingesting events through store-like endpoints.

use std::rc::Rc;
use std::sync::Arc;

use actix::prelude::*;
use actix_web::http::StatusCode;
use actix_web::{HttpRequest, HttpResponse, ResponseError};
use failure::Fail;
use futures::prelude::*;
use parking_lot::Mutex;
use sentry::Hub;
use sentry_actix::ActixWebHubExt;
use serde::Deserialize;

use semaphore_common::{clone, metric, tryf, LogError};
use semaphore_general::protocol::EventId;

use crate::actors::events::{QueueEvent, QueueEventError};
use crate::actors::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::actors::project::{EventAction, GetEventAction, GetProject, ProjectError, RateLimit};
use crate::body::StorePayloadError;
use crate::constants::ITEM_NAME_EVENT;
use crate::envelope::{Envelope, EnvelopeError, ItemType, Items};
use crate::extractors::{EventMeta, StartTime};
use crate::service::ServiceState;
use crate::utils::{ApiErrorResponse, FormDataIter, MultipartError};

#[derive(Fail, Debug)]
pub enum BadStoreRequest {
    #[fail(display = "unsupported protocol version ({})", _0)]
    UnsupportedProtocolVersion(u16),

    #[fail(display = "could not schedule event processing")]
    ScheduleFailed(#[cause] MailboxError),

    #[fail(display = "failed to fetch project information")]
    ProjectFailed(#[cause] ProjectError),

    #[fail(display = "empty event data")]
    EmptyBody,

    #[fail(display = "invalid JSON data")]
    InvalidJson(#[cause] serde_json::Error),

    #[fail(display = "invalid messagepack data")]
    InvalidMsgpack(#[cause] rmp_serde::decode::Error),

    #[fail(display = "invalid event envelope")]
    InvalidEnvelope(#[cause] EnvelopeError),

    #[fail(display = "invalid multipart data")]
    InvalidMultipart(#[cause] MultipartError),

    #[fail(display = "invalid minidump")]
    InvalidMinidump,

    #[fail(display = "missing minidump")]
    MissingMinidump,

    #[fail(display = "failed to queue event")]
    QueueFailed(#[cause] QueueEventError),

    #[fail(display = "failed to read request body")]
    PayloadError(#[cause] StorePayloadError),

    #[fail(display = "event rejected due to rate limit: {:?}", _0)]
    RateLimited(RateLimit),

    #[fail(display = "event submission rejected with_reason: {:?}", _0)]
    EventRejected(DiscardReason),

    /// User specified an unparseable event ID as part of an attachment endpoint request.
    #[fail(display = "invalid event id")]
    InvalidEventId,
}

impl BadStoreRequest {
    fn to_outcome(&self) -> Outcome {
        match self {
            BadStoreRequest::UnsupportedProtocolVersion(_) => {
                Outcome::Invalid(DiscardReason::AuthVersion)
            }

            BadStoreRequest::EmptyBody => Outcome::Invalid(DiscardReason::NoData),
            BadStoreRequest::InvalidJson(_) => Outcome::Invalid(DiscardReason::InvalidJson),
            BadStoreRequest::InvalidMsgpack(_) => Outcome::Invalid(DiscardReason::InvalidMsgpack),
            BadStoreRequest::InvalidMultipart(_) => {
                Outcome::Invalid(DiscardReason::InvalidMultipart)
            }
            BadStoreRequest::InvalidMinidump => Outcome::Invalid(DiscardReason::InvalidMinidump),
            BadStoreRequest::MissingMinidump => {
                Outcome::Invalid(DiscardReason::MissingMinidumpUpload)
            }
            BadStoreRequest::InvalidEnvelope(_) => Outcome::Invalid(DiscardReason::InvalidEnvelope),

            BadStoreRequest::QueueFailed(event_error) => match event_error {
                QueueEventError::TooManyEvents => Outcome::Invalid(DiscardReason::Internal),
            },

            BadStoreRequest::ProjectFailed(project_error) => match project_error {
                ProjectError::FetchFailed => Outcome::Invalid(DiscardReason::ProjectState),
                _ => Outcome::Invalid(DiscardReason::Internal),
            },

            BadStoreRequest::ScheduleFailed(_) => Outcome::Invalid(DiscardReason::Internal),

            BadStoreRequest::EventRejected(reason) => Outcome::Invalid(*reason),

            BadStoreRequest::PayloadError(payload_error) => {
                Outcome::Invalid(payload_error.discard_reason())
            }

            BadStoreRequest::RateLimited(retry_after) => Outcome::RateLimited(retry_after.clone()),

            // should actually never create an outcome
            BadStoreRequest::InvalidEventId => Outcome::Invalid(DiscardReason::Internal),
        }
    }
}

impl ResponseError for BadStoreRequest {
    fn error_response(&self) -> HttpResponse {
        let body = ApiErrorResponse::from_fail(self);

        match self {
            BadStoreRequest::RateLimited(RateLimit(_, retry_after)) => {
                // For rate limits, we return a special status code and indicate the client to hold
                // off until the rate limit period has expired. Currently, we only support the
                // delay-seconds variant of the Rate-Limit header.
                HttpResponse::build(StatusCode::TOO_MANY_REQUESTS)
                    .header("Retry-After", retry_after.remaining_seconds().to_string())
                    .json(&body)
            }
            BadStoreRequest::ProjectFailed(project_error) => match project_error {
                ProjectError::FetchFailed => {
                    // This particular project is somehow broken. We could treat this as 503 but it's
                    // more likely that the error is local to this project.
                    HttpResponse::InternalServerError().json(&body)
                }
                ProjectError::ScheduleFailed(_) => HttpResponse::ServiceUnavailable().json(&body),
            },

            BadStoreRequest::ScheduleFailed(_) | BadStoreRequest::QueueFailed(_) => {
                // These errors indicate that something's wrong with our actor system, most likely
                // mailbox congestion or a faulty shutdown. Indicate an unavailable service to the
                // client. It might retry event submission at a later time.
                HttpResponse::ServiceUnavailable().json(&body)
            }
            BadStoreRequest::EventRejected(_) => {
                // The event has been discarded, which is generally indicated with a 403 error.
                // Originally, Sentry also used this status code for event filters, but these are
                // now executed asynchronously in `EventProcessor`.
                HttpResponse::Forbidden().json(&body)
            }
            _ => {
                // In all other cases, we indicate a generic bad request to the client and render
                // the cause. This was likely the client's fault.
                HttpResponse::BadRequest().json(&body)
            }
        }
    }
}

#[derive(Deserialize)]
struct EventIdHelper {
    #[serde(default, rename = "event_id")]
    id: Option<EventId>,
}

/// Extracts the event id from a JSON payload.
///
/// If the payload contains no event id, `Ok(None)` is returned. This function also validates that
/// the provided is valid and returns an `Err` on parse errors. If the event id itself is malformed,
/// an `Err` is returned.
pub fn event_id_from_json(data: &[u8]) -> Result<Option<EventId>, BadStoreRequest> {
    serde_json::from_slice(data)
        .map(|helper: EventIdHelper| helper.id)
        .map_err(BadStoreRequest::InvalidJson)
}

/// Extracts the event id from a MessagePack payload.
///
/// If the payload contains no event id, `Ok(None)` is returned. This function also validates that
/// the provided is valid and returns an `Err` on parse errors. If the event id itself is malformed,
/// an `Err` is returned.
pub fn event_id_from_msgpack(data: &[u8]) -> Result<Option<EventId>, BadStoreRequest> {
    rmp_serde::from_slice(data)
        .map(|helper: EventIdHelper| helper.id)
        .map_err(BadStoreRequest::InvalidMsgpack)
}

/// Extracts the event id from `sentry` JSON payload or the `sentry[event_id]` formdata key.
///
/// If the event id itself is malformed, an `Err` is returned. If there is a `sentry` key containing
/// malformed JSON, an error is returned.
pub fn event_id_from_formdata(data: &[u8]) -> Result<Option<EventId>, BadStoreRequest> {
    for entry in FormDataIter::new(data) {
        if entry.key() == "sentry" {
            return event_id_from_json(entry.value().as_bytes());
        } else if entry.key() == "sentry[event_id]" {
            return entry
                .value()
                .parse()
                .map(Some)
                .map_err(|_| BadStoreRequest::InvalidEventId);
        }
    }

    Ok(None)
}

/// Extracts the event id from multiple items.
///
/// Submitting multiple event payloads is undefined behavior. This function will check for an event
/// id in the following precedence:
///
///  1. The `Event` item.
///  2. The `__sentry-event` event attachment.
///  3. The `sentry` JSON payload.
///  4. The `sentry[event_id]` formdata key.
pub fn event_id_from_items(items: &Items) -> Result<Option<EventId>, BadStoreRequest> {
    if let Some(item) = items.iter().find(|item| item.ty() == ItemType::Event) {
        if let Some(event_id) = event_id_from_json(&item.payload())? {
            return Ok(Some(event_id));
        }
    }

    if let Some(item) = items
        .iter()
        .find(|item| item.name() == Some(ITEM_NAME_EVENT))
    {
        if let Some(event_id) = event_id_from_msgpack(&item.payload())? {
            return Ok(Some(event_id));
        }
    }

    if let Some(item) = items.iter().find(|item| item.ty() == ItemType::FormData) {
        // Swallow all other errors here since it is quite common to receive invalid secondary
        // payloads. `EventProcessor` also retains events in such cases.
        if let Ok(Some(event_id)) = event_id_from_formdata(&item.payload()) {
            return Ok(Some(event_id));
        }
    }

    Ok(None)
}

/// Handles Sentry events.
///
/// Sentry events may come either directly from a http request ( the store endpoint
/// calls this method directly) or are generated inside Semaphore from requests to
/// other endpoints (e.g. the security endpoint)
///
/// If store_event receives a non empty store_body it will use it as the body of the
/// event otherwise it will try to create a store_body from the request.
///
pub fn handle_store_like_request<F, R, I>(
    meta: EventMeta,
    is_event: bool,
    start_time: StartTime,
    request: HttpRequest<ServiceState>,
    extract_envelope: F,
    create_response: R,
) -> ResponseFuture<HttpResponse, BadStoreRequest>
where
    F: FnOnce(&HttpRequest<ServiceState>, EventMeta) -> I + 'static,
    I: IntoFuture<Item = Envelope, Error = BadStoreRequest> + 'static,
    R: FnOnce(EventId) -> HttpResponse + 'static,
{
    let start_time = start_time.into_inner();

    // For now, we only handle <= v8 and drop everything else
    let version = meta.version();
    if version > semaphore_common::PROTOCOL_VERSION {
        // TODO: Delegate to forward_upstream here
        tryf!(Err(BadStoreRequest::UnsupportedProtocolVersion(version)));
    }

    let project_id = meta.project_id();
    let hub = Hub::from_request(&request);
    hub.configure_scope(|scope| {
        scope.set_user(Some(sentry::User {
            id: Some(project_id.to_string()),
            ..Default::default()
        }));
    });

    metric!(counter(&format!("event.protocol.v{}", version)) += 1);

    let event_manager = request.state().event_manager();
    let project_manager = request.state().project_cache();
    let outcome_producer = request.state().outcome_producer();
    let remote_addr = meta.client_addr();

    let cloned_meta = Arc::new(meta.clone());
    let event_id = Rc::new(Mutex::new(None));

    let future = project_manager
        .send(GetProject { id: project_id })
        .map_err(BadStoreRequest::ScheduleFailed)
        .and_then(clone!(event_id, |project| {
            extract_envelope(&request, meta)
                .into_future()
                .and_then(clone!(project, |envelope| {
                    *event_id.lock() = Some(envelope.event_id());

                    project
                        .send(GetEventAction::cached(cloned_meta))
                        .map_err(BadStoreRequest::ScheduleFailed)
                        .and_then(move |action| {
                            match action.map_err(BadStoreRequest::ProjectFailed)? {
                                EventAction::Accept => Ok(envelope),
                                EventAction::RetryAfter(retry_after) => {
                                    Err(BadStoreRequest::RateLimited(retry_after))
                                }
                                EventAction::Discard(reason) => {
                                    Err(BadStoreRequest::EventRejected(reason))
                                }
                            }
                        })
                }))
                .and_then(move |envelope| {
                    event_manager
                        .send(QueueEvent {
                            envelope,
                            project,
                            start_time,
                        })
                        .map_err(BadStoreRequest::ScheduleFailed)
                        .and_then(|result| result.map_err(BadStoreRequest::QueueFailed))
                        .map(create_response)
                })
        }))
        .or_else(move |error: BadStoreRequest| {
            metric!(counter("event.rejected") += 1);

            if is_event {
                outcome_producer.do_send(TrackOutcome {
                    timestamp: start_time,
                    project_id,
                    org_id: None,
                    key_id: None,
                    outcome: error.to_outcome(),
                    event_id: *event_id.lock(),
                    remote_addr,
                });
            }

            let response = error.error_response();
            if response.status().is_server_error() {
                log::error!("error handling request: {}", LogError(&error));
            }

            Ok(response)
        });

    Box::new(future)
}
