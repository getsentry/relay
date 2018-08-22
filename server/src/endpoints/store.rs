//! Handles event store requests.
use actix::{MailboxError, ResponseFuture};
use actix_web::http::{header, Method};
use actix_web::middleware::cors::Cors;
use actix_web::{HttpRequest, HttpResponse, Json, ResponseError};
use futures::Future;
use sentry::{self, Hub};
use sentry_actix::ActixWebHubExt;
use url::Url;

use semaphore_aorta::{ApiErrorResponse, PublicKeyEventAction};
use semaphore_common::{Auth, AuthParseError, ProjectId, ProjectIdParseError};

use actors::events::{StoreEvent, StoreEventResponse};
use actors::project::{EventMetaData, GetEventAction, GetProject};
use body::{StoreBody, StorePayloadError};
use service::{ServiceApp, ServiceState};

#[derive(Fail, Debug)]
enum BadStoreRequest {
    #[fail(display = "invalid project path parameter")]
    BadProject(#[cause] ProjectIdParseError),
    #[fail(display = "bad x-sentry-auth header")]
    BadAuth(#[cause] AuthParseError),
    #[fail(display = "unsupported protocol version ({})", _0)]
    UnsupportedProtocolVersion(u16),
    #[fail(display = "internal error: failed to process request")]
    InternalTimeout(#[cause] MailboxError),
    #[fail(display = "failed to fetch project information")]
    ProjectFailed,
    #[fail(display = "failed to process event")]
    ProcessingFailed,
    #[fail(display = "failed to read request body")]
    PayloadError(#[cause] StorePayloadError),
    #[fail(display = "event submission rejected")]
    StoreRejected,
}

impl ResponseError for BadStoreRequest {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::BadRequest().json(&ApiErrorResponse::from_fail(self))
    }
}

fn auth_from_request<S>(req: &HttpRequest<S>) -> Result<Auth, BadStoreRequest> {
    // try auth from header
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

fn meta_from_request<S>(request: &HttpRequest<S>, auth: &Auth) -> EventMetaData {
    EventMetaData {
        public_key: String::new(),
        origin: parse_header_url(request, header::ORIGIN)
            .or_else(|| parse_header_url(request, header::REFERER)),
        remote_addr: request.peer_addr().map(|peer| peer.ip()),
        sentry_client: auth.client_agent().map(|agent| agent.to_string()),
    }
}

fn store_event(
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<Json<StoreEventResponse>, BadStoreRequest> {
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

    let meta = meta_from_request(&request, &auth);

    let future = request
        .state()
        .project_manager()
        .send(GetProject { id: project_id })
        .map_err(BadStoreRequest::InternalTimeout)
        .and_then(|project| {
            project
                .send(GetEventAction(meta.clone()))
                .map_err(BadStoreRequest::InternalTimeout)
                .and_then(
                    |action| match action.map_err(|_| BadStoreRequest::ProjectFailed)? {
                        PublicKeyEventAction::Send => Ok(()),
                        PublicKeyEventAction::Queue => Ok(()), // TODO: Kill ::Queue
                        PublicKeyEventAction::Discard => Err(BadStoreRequest::StoreRejected),
                    },
                )
                .and_then(|_| {
                    StoreBody::new(&request)
                        .limit(request.state().aorta_config().max_event_payload_size)
                        .map_err(BadStoreRequest::PayloadError)
                })
                .and_then(|data| {
                    request
                        .state()
                        .event_processor()
                        .send(StoreEvent {
                            meta,
                            data,
                            project,
                        })
                        .map_err(BadStoreRequest::InternalTimeout)
                })
                .and_then(|result| result.map_err(|_| BadStoreRequest::ProcessingFailed))
        })
        .map(|response| {
            metric!(counter("event.accepted") += 1);
            Json(response)
        })
        .map_err(|error| {
            metric!(counter("event.rejected") += 1);
            error
        });

    Box::new(future)
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    Cors::for_app(app)
        .allowed_methods(vec!["POST"])
        .allowed_headers(vec![
            "x-sentry-auth",
            "x-requested-with",
            "origin",
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
