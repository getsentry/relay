//! Handles event store requests.

use std::sync::Arc;

use actix::prelude::*;
use actix_web::http::{Method, StatusCode};
use actix_web::middleware::cors::Cors;
use actix_web::{HttpRequest, HttpResponse, Json, ResponseError};
use futures::prelude::*;
use sentry::{self, Hub};
use sentry_actix::ActixWebHubExt;
use uuid::Uuid;

use semaphore_common::{ProjectId, ProjectIdParseError};

use actors::events::{EventError, QueueEvent};
use actors::project::{EventAction, GetEventAction, GetProject, ProjectError};
use body::{StoreBody, StorePayloadError};
use extractors::EventMeta;
use service::{ServiceApp, ServiceState};
use utils::ApiErrorResponse;

#[derive(Fail, Debug)]
enum BadStoreRequest {
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

    #[fail(display = "event rejected due to rate limiting")]
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
    id: Uuid,
}

fn store_event(
    meta: EventMeta,
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<Json<StoreResponse>, BadStoreRequest> {
    // For now, we only handle <= v8 and drop everything else
    if meta.auth().version() > 8 {
        // TODO: Delegate to forward_upstream here
        tryf!(Err(BadStoreRequest::UnsupportedProtocolVersion(
            meta.auth().version()
        )));
    }

    // Make sure we have a project ID. Does not check if the project exists yet
    let project_id = tryf!(
        request
            .match_info()
            .get("project")
            .unwrap_or_default()
            .parse::<ProjectId>()
            .map_err(BadStoreRequest::BadProject)
    );

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

    let future = project_manager
        .send(GetProject { id: project_id })
        .map_err(BadStoreRequest::ScheduleFailed)
        .and_then(move |project| {
            project
                .send(GetEventAction::cached(meta.clone()))
                .map_err(BadStoreRequest::ScheduleFailed)
                .and_then(
                    |action| match action.map_err(BadStoreRequest::ProjectFailed)? {
                        EventAction::Accept => Ok(()),
                        EventAction::RetryAfter(secs) => Err(BadStoreRequest::RateLimited(secs)),
                        EventAction::Discard => Err(BadStoreRequest::EventRejected),
                    },
                ).and_then(move |_| {
                    StoreBody::new(&request)
                        .limit(config.max_event_payload_size())
                        .map_err(BadStoreRequest::PayloadError)
                }).and_then(move |data| {
                    event_manager
                        .send(QueueEvent {
                            data,
                            meta,
                            project,
                        }).map_err(BadStoreRequest::ScheduleFailed)
                        .and_then(|result| result.map_err(BadStoreRequest::ProcessingFailed))
                        .map(|id| Json(StoreResponse { id }))
                })
        }).map_err(move |error| {
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
        ]).max_age(3600)
        .resource(r"/api/{project:\d+}/store/", |r| {
            r.method(Method::POST).with(store_event);
        }).register()
}
