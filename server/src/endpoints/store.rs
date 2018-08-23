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

use semaphore_aorta::{ApiErrorResponse, PublicKeyEventAction};
use semaphore_common::{Auth, AuthParseError, ProjectId, ProjectIdParseError};

use actors::events::{EventMetaData, ProcessEvent, ProcessEventResponse};
use actors::project::{GetEventAction, GetProject, QueueEvent};
use extractors::{StoreBody, StorePayloadError};
use service::{ServiceApp, ServiceState};

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

#[derive(Fail, Debug)]
enum BadStoreRequest {
    #[fail(display = "invalid project path parameter")]
    BadProject(#[cause] ProjectIdParseError),
    #[fail(display = "bad x-sentry-auth header")]
    BadAuth(#[cause] AuthParseError),
    #[fail(display = "unsupported protocol version ({})", _0)]
    UnsupportedProtocolVersion(u16),
    #[fail(display = "timeout while processing request")]
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
    let config = request.state().aorta_config();
    let event_processor = request.state().event_processor();
    let project_manager = request.state().project_manager();

    let future = project_manager
        .send(GetProject { id: project_id })
        .map_err(BadStoreRequest::InternalTimeout)
        .and_then(move |project| {
            project
                .send(GetEventAction { meta: meta.clone() })
                .map_err(BadStoreRequest::InternalTimeout)
                .and_then(
                    |action| match action.map_err(|_| BadStoreRequest::ProjectFailed)? {
                        PublicKeyEventAction::Send => Ok(()),
                        PublicKeyEventAction::Queue => Ok(()), // TODO: Kill ::Queue
                        PublicKeyEventAction::Discard => Err(BadStoreRequest::StoreRejected),
                    },
                )
                .and_then(move |_| {
                    StoreBody::new(&request)
                        .limit(config.max_event_payload_size)
                        .map_err(BadStoreRequest::PayloadError)
                })
                .and_then(clone!(project, meta, |data| event_processor
                    .send(ProcessEvent {
                        data,
                        meta,
                        project
                    })
                    .map_err(BadStoreRequest::InternalTimeout)))
                .and_then(|result| result.map_err(|_| BadStoreRequest::ProcessingFailed))
                .and_then(move |ProcessEventResponse { event_id, data }| {
                    project_manager
                        .send(QueueEvent {
                            data: data,
                            meta,
                            project_id,
                        })
                        .map_err(BadStoreRequest::InternalTimeout)
                        .map(move |_| StoreResponse { id: event_id })
                })
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
