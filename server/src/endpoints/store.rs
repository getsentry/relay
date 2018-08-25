//! Handles event store requests.

use std::sync::Arc;

use actix::prelude::*;
use actix_web::http::{header, Method};
use actix_web::middleware::cors::Cors;
use actix_web::{HttpRequest, HttpResponse, Json, ResponseError};
use futures::prelude::*;
use sentry::{self, Hub};
use sentry_actix::ActixWebHubExt;
use url::Url;
use uuid::Uuid;

use semaphore_common::{Auth, AuthParseError, ProjectId, ProjectIdParseError};

use actors::events::{EventMetaData, ProcessingError, QueueEvent};
use actors::project::{EventAction, GetEventAction, GetProject};
use extractors::{StoreBody, StorePayloadError};
use service::{ServiceApp, ServiceState};
use utils::ApiErrorResponse;

#[derive(Fail, Debug)]
enum BadStoreRequest {
    #[fail(display = "invalid project path parameter")]
    BadProject(#[cause] ProjectIdParseError),

    #[fail(display = "bad x-sentry-auth header")]
    BadAuth(#[cause] AuthParseError),

    #[fail(display = "unsupported protocol version ({})", _0)]
    UnsupportedProtocolVersion(u16),

    #[fail(display = "could not schedule event processing")]
    ScheduleFailed(#[cause] MailboxError),

    #[fail(display = "failed to fetch project information")]
    ProjectFailed,

    #[fail(display = "failed to process event")]
    ProcessingFailed(#[cause] ProcessingError),

    #[fail(display = "failed to read request body")]
    PayloadError(#[cause] StorePayloadError),

    #[fail(display = "event submission rejected")]
    EventRejected,
}

impl ResponseError for BadStoreRequest {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::BadRequest().json(&ApiErrorResponse::from_fail(self))
    }
}

fn auth_from_request<S>(req: &HttpRequest<S>) -> Result<Auth, BadStoreRequest> {
    let auth = req
        .headers()
        .get("x-sentry-auth")
        .and_then(|x| x.to_str().ok());

    if let Some(auth) = auth {
        return auth.parse::<Auth>().map_err(BadStoreRequest::BadAuth);
    }

    Auth::from_querystring(req.query_string().as_bytes()).map_err(BadStoreRequest::BadAuth)
}

fn parse_header_url<T>(req: &HttpRequest<T>, header: header::HeaderName) -> Option<Url> {
    req.headers()
        .get(header)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<Url>().ok())
        .and_then(|u| match u.scheme() {
            "http" | "https" => Some(u),
            _ => None,
        })
}

fn meta_from_request<S>(request: &HttpRequest<S>, auth: Auth) -> EventMetaData {
    EventMetaData {
        auth,
        origin: parse_header_url(request, header::ORIGIN)
            .or_else(|| parse_header_url(request, header::REFERER)),
        remote_addr: request.peer_addr().map(|peer| peer.ip()),
    }
}

#[derive(Serialize)]
struct StoreResponse {
    id: Uuid,
}

fn store_event(
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<Json<StoreResponse>, BadStoreRequest> {
    // Check auth and fail as early as possible
    let auth = tryf!(auth_from_request(&request));

    // For now, we only handle <= v8 and drop everything else
    if auth.version() > 8 {
        // TODO: Delegate to forward_upstream here
        tryf!(Err(BadStoreRequest::UnsupportedProtocolVersion(
            auth.version()
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

    metric!(counter(&format!("event.protocol.v{}", auth.version())) += 1);

    let meta = Arc::new(meta_from_request(&request, auth));
    let config = request.state().config();
    let event_manager = request.state().event_manager();
    let project_manager = request.state().project_cache();

    let future = {and_then! {
        let project = await project_manager
            .send(GetProject { id: project_id })
            .map_err(BadStoreRequest::ScheduleFailed);

        let action = await project
            .send(GetEventAction::cached(meta.clone()))
            .map_err(BadStoreRequest::ScheduleFailed);

        let _ = await (match action.map_err(|_| BadStoreRequest::ProjectFailed)? {
            EventAction::Accept => Ok(()),
            EventAction::Discard => Err(BadStoreRequest::EventRejected),
        });

        let data = await StoreBody::new(&request)
            .limit(config.max_event_payload_size())
            .map_err(BadStoreRequest::PayloadError);

        let result = await event_manager
            .send(QueueEvent {
                data,
                meta,
                project,
            })
            .map_err(BadStoreRequest::ScheduleFailed);

        result
            .map_err(BadStoreRequest::ProcessingFailed)
            .map(|id| StoreResponse { id })
    }};

    Box::new(future
        .map(|response| {
            metric!(counter("event.accepted") += 1);
            Json(response)
        })
        .map_err(|error| {
            metric!(counter("event.rejected") += 1);
            error
        }))
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    Cors::for_app(app)
        .allowed_methods(vec!["POST"])
        .allowed_headers(vec![
            "x-sentry-auth",
            "x-requested-with",
            "origin",
            "referer",
            "accept",
            "content-type",
            "authentication",
        ])
        .max_age(3600)
        .resource(r"/api/{project:\d+}/store/", |r| {
            r.method(Method::POST).with(store_event);
        })
        .register()
}
