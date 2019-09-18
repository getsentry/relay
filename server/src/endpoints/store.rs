//! Handles event store requests.

use std::sync::Arc;

use actix::prelude::*;
use actix_web::http::{Method, StatusCode};
use actix_web::middleware::cors::Cors;
use actix_web::{HttpRequest, HttpResponse, ResponseError};
use failure::Fail;
use futures::prelude::*;
use sentry::Hub;
use sentry_actix::ActixWebHubExt;
use serde::Serialize;

use semaphore_common::{clone, metric, tryf, ProjectId, ProjectIdParseError};
use semaphore_general::protocol::EventId;

use crate::actors::events::{EventError, QueueEvent};
use crate::actors::project::{EventAction, GetEventAction, GetProject, ProjectError};
use crate::body::{StoreBody, StorePayloadError};
use crate::extractors::{EventMeta, StartTime};
use crate::service::{ServiceApp, ServiceState};
use crate::utils::ApiErrorResponse;

use crate::actors::outcome::{DiscardReason, Outcome, TrackOutcome};

#[derive(Fail, Debug)]
pub enum BadStoreRequest {
    #[fail(display = "invalid project path parameter")]
    BadProject(#[cause] ProjectIdParseError),

    #[fail(display = "unsupported protocol version ({})", _0)]
    UnsupportedProtocolVersion(u16),

    #[fail(display = "could not schedule event processing")]
    ScheduleFailed(#[cause] MailboxError),

    #[fail(display = "failed to fetch project information")]
    ProjectFailed(#[cause] ProjectError),

    #[fail(display = "failed to process event")]
    ProcessingFailed(#[cause] EventError),

    #[fail(display = "failed to read request body")]
    PayloadError(#[cause] StorePayloadError),

    #[fail(display = "event rejected due to rate limit ({}s)", _0)]
    RateLimited(u64),

    #[fail(display = "event submission rejected")]
    EventRejected,
}

impl ResponseError for BadStoreRequest {
    fn error_response(&self) -> HttpResponse {
        let body = ApiErrorResponse::from_fail(self);

        match self {
            BadStoreRequest::RateLimited(secs) => {
                // For rate limits, we return a special status code and indicate the client to hold
                // off until the rate limit period has expired. Currently, we only support the
                // delay-seconds variant of the Rate-Limit header.
                HttpResponse::build(StatusCode::TOO_MANY_REQUESTS)
                    .header("Retry-After", secs.to_string())
                    .json(&body)
            }
            BadStoreRequest::ScheduleFailed(_) | BadStoreRequest::ProjectFailed(_) => {
                // These errors indicate that something's wrong with our actor system, most likely
                // mailbox congestion or a faulty shutdown. Indicate an unavailable service to the
                // client. It might retry event submission at a later time.
                HttpResponse::ServiceUnavailable().json(&body)
            }
            _ => {
                // In all other cases, we indicate a generic bad request to the client and render
                // the cause. This was likely the client's fault.
                HttpResponse::BadRequest().json(&body)
            }
        }
    }
}

#[derive(Serialize)]
struct StoreResponse {
    id: EventId,
}

fn store_event(
    meta: EventMeta,
    start_time: StartTime,
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    let start_time = start_time.into_inner();

    // For now, we only handle <= v8 and drop everything else
    if meta.auth().version() > 8 {
        // TODO: Delegate to forward_upstream here
        tryf!(Err(BadStoreRequest::UnsupportedProtocolVersion(
            meta.auth().version()
        )));
    }

    // Make sure we have a project ID. Does not check if the project exists yet
    let project_id = tryf!(request
        .match_info()
        .get("project")
        .unwrap_or_default()
        .parse::<ProjectId>()
        .map_err(BadStoreRequest::BadProject));

    let hub = Hub::from_request(&request);
    hub.configure_scope(|scope| {
        scope.set_user(Some(sentry::User {
            id: Some(project_id.to_string()),
            ..Default::default()
        }));
    });

    metric!(counter(&format!("event.protocol.v{}", meta.auth().version())) += 1);

    let meta = Arc::new(meta);
    let config = request.state().config();
    let event_manager = request.state().event_manager();
    let project_manager = request.state().project_cache();
    let outcome_producer = request.state().outcome_producer();

    let future = project_manager
        .send(GetProject { id: project_id })
        .map_err(BadStoreRequest::ScheduleFailed)
        .and_then(move |project| {
            project
                .send(GetEventAction::cached(meta.clone()))
                .map_err(BadStoreRequest::ScheduleFailed)
                .and_then( clone! { outcome_producer, |action| match action.map_err(BadStoreRequest::ProjectFailed)? {
                        EventAction::Accept => Ok(()),
                        EventAction::RetryAfter(secs, reason) => {
                            outcome_producer.do_send(TrackOutcome {
                                timestamp: start_time,
                                project_id: Some(project_id),
                                org_id: None,
                                key_id: None,
                                outcome: Outcome::RateLimited(reason),
                                event_id: None,
                            });
                            Err(BadStoreRequest::RateLimited(secs))
                        }
                        EventAction::Discard(reason) => {
                            outcome_producer.do_send(TrackOutcome {
                                timestamp: start_time,
                                project_id: Some(project_id),
                                org_id: None,
                                key_id: None,
                                outcome: Outcome::Invalid(reason),
                                event_id: None,
                            });
                            Err(BadStoreRequest::EventRejected)
                        }
                    }}
                )
                .and_then(clone!{ outcome_producer, |()| {
                    StoreBody::new(&request)
                        .limit(config.max_event_payload_size())
                        .map_err(move |e| {
                            outcome_producer.do_send(TrackOutcome {
                                timestamp: start_time,
                                project_id: Some(project_id),
                                org_id: None,
                                key_id: None,
                                outcome: Outcome::Invalid(DiscardReason::from(&e)),
                                event_id: None,
                            });
                            BadStoreRequest::PayloadError(e)
                        })
                }})
                .and_then(clone!{ outcome_producer,  |data| {
                    event_manager
                        .send(QueueEvent {
                            data,
                            meta,
                            project,
                            start_time,
                        })
                        .map_err(BadStoreRequest::ScheduleFailed)
                        .and_then(|result| result.map_err(BadStoreRequest::ProcessingFailed))
                        .map_err(move |e| {
                            outcome_producer.do_send(TrackOutcome {
                                timestamp: start_time,
                                project_id: Some(project_id),
                                org_id: None,
                                key_id: None,
                                outcome: (&e).into(),
                                event_id: None,
                            });
                            e
                        })
                        .map(|id| HttpResponse::Accepted().json(StoreResponse { id }))
                }})
        })
        .map_err(move |error| {
            metric!(counter("event.rejected") += 1);
            error
        });

    Box::new(future)
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    // XXX: does not handle the legacy /api/store/ endpoint
    Cors::for_app(app)
        .allowed_methods(vec!["POST"])
        .allowed_headers(vec![
            "x-sentry-auth",
            "x-requested-with",
            "x-forwarded-for",
            "origin",
            "referer",
            "accept",
            "content-type",
            "authentication",
        ])
        .expose_headers(vec!["X-Sentry-Error", "Retry-After"])
        .max_age(3600)
        .resource(r"/api/{project:\d+}/store/", |r| {
            r.method(Method::POST).with(store_event);
        })
        .register()
}
