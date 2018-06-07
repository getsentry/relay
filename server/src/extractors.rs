use std::sync::Arc;

use actix_web::dev::JsonBody;
use actix_web::error::{Error, JsonPayloadError, PayloadError, ResponseError};
use actix_web::{http::header, FromRequest, HttpMessage, HttpRequest, HttpResponse, State};
use failure::Fail;
use futures::{future, Future};
use sentry_types::{Auth, AuthParseError};
use url::Url;

use semaphore_aorta::{ApiErrorResponse, EventMeta, EventVariant, ForeignEvent, ForeignPayload,
                      ProjectState, StoreChangeset};
use semaphore_common::{ProjectId, ProjectIdParseError};

use body::{EncodedJsonBody, EncodedJsonPayloadError};
use service::ServiceState;

use sentry;

#[derive(Fail, Debug)]
pub enum BadProjectRequest {
    #[fail(display = "invalid project path parameter")]
    BadProject(#[cause] ProjectIdParseError),
    #[fail(display = "bad x-sentry-auth header")]
    BadAuth(#[cause] AuthParseError),
    #[fail(display = "bad JSON payload")]
    BadJson(#[cause] JsonPayloadError),
    #[fail(display = "bad JSON payload")]
    BadEncodedJson(#[cause] EncodedJsonPayloadError),
    #[fail(display = "bad payload")]
    BadPayload(#[cause] PayloadError),
    #[fail(display = "unsupported protocol version ({})", _0)]
    UnsupportedProtocolVersion(u16),
}

impl ResponseError for BadProjectRequest {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::BadRequest().json(&ApiErrorResponse::from_fail(self))
    }
}

/// An extractor for the entire service state.
pub type CurrentServiceState = State<ServiceState>;

/// A common extractor for handling project requests.
///
/// This extractor not only validates the authentication and project id
/// from the URL but also exposes that information in request extensions
/// so that other systems can access it.
///
/// In particular it wraps another extractor that can give convenient
/// access to the request's payload.
#[derive(Debug)]
pub struct ProjectRequest<T: FromRequest<ServiceState> + 'static> {
    http_req: HttpRequest<ServiceState>,
    payload: Option<T>,
}

impl<T: FromRequest<ServiceState> + 'static> ProjectRequest<T> {
    /// Returns the project identifier for this request.
    pub fn project_id(&self) -> ProjectId {
        *self.http_req.extensions().get().unwrap()
    }

    /// Returns the auth info
    pub fn auth(&self) -> &Auth {
        self.http_req.extensions().get().unwrap()
    }

    /// Returns the current service state.
    pub fn service_state(&self) -> &ServiceState {
        self.http_req.state()
    }

    /// Gets or creates the project state.
    pub fn get_or_create_project_state(&self) -> Arc<ProjectState> {
        self.service_state()
            .trove_state()
            .get_or_create_project_state(self.project_id())
    }

    /// Extracts the embedded payload.
    ///
    /// This can only be called once.  Panics if there is no payload.
    pub fn take_payload(&mut self) -> T {
        self.payload.take().unwrap()
    }
}

fn get_auth_from_request<S>(req: &HttpRequest<S>) -> Result<Auth, BadProjectRequest> {
    // try auth from header
    match req.headers()
        .get("x-sentry-auth")
        .and_then(|x| x.to_str().ok())
    {
        Some(auth) => match auth.parse::<Auth>() {
            Ok(val) => return Ok(val),
            Err(err) => return Err(BadProjectRequest::BadAuth(err)),
        },
        None => {}
    }

    // fall back to auth from url
    Auth::from_querystring(req.query_string().as_bytes()).map_err(BadProjectRequest::BadAuth)
}

impl<T: FromRequest<ServiceState> + 'static> FromRequest<ServiceState> for ProjectRequest<T> {
    type Config = <T as FromRequest<ServiceState>>::Config;
    type Result = Box<Future<Item = Self, Error = Error>>;

    fn from_request(req: &HttpRequest<ServiceState>, cfg: &Self::Config) -> Self::Result {
        let mut req = req.clone();
        let auth = match get_auth_from_request(&req) {
            Ok(auth) => auth,
            Err(err) => return Box::new(future::err(err.into())),
        };

        req.extensions_mut().insert(auth.clone());

        match req.match_info()
            .get("project")
            .unwrap_or_default()
            .parse::<ProjectId>()
            .map_err(BadProjectRequest::BadProject)
        {
            Ok(project_id) => req.extensions_mut().insert(project_id),
            Err(err) => return Box::new(future::err(err.into())),
        };

        Box::new(
            T::from_request(&req, cfg)
                .into()
                .map(move |payload| ProjectRequest {
                    http_req: req,
                    payload: Some(payload),
                }),
        )
    }
}

fn parse_header_origin<T>(req: &HttpRequest<T>, header: header::HeaderName) -> Option<Url> {
    req.headers()
        .get(header)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<Url>().ok())
        .and_then(|u| match u.scheme() {
            "http" | "https" => Some(u),
            _ => None,
        })
}

fn event_meta_from_req(req: &HttpRequest<ServiceState>) -> EventMeta {
    let auth: &Auth = req.extensions().get().unwrap();
    EventMeta {
        remote_addr: req.peer_addr().map(|sock_addr| sock_addr.ip()),
        sentry_client: auth.client_agent().map(|x| x.to_string()),
        origin: parse_header_origin(req, header::ORIGIN)
            .or_else(|| parse_header_origin(req, header::REFERER)),
    }
}

/// Extracts an event variant from the payload.
pub struct IncomingEvent {
    event: EventVariant,
    meta: EventMeta,
    public_key: String,
}

impl IncomingEvent {
    fn new(event: EventVariant, meta: EventMeta, public_key: String) -> IncomingEvent {
        IncomingEvent {
            event,
            meta,
            public_key,
        }
    }
}

impl From<IncomingEvent> for StoreChangeset {
    fn from(mut inc: IncomingEvent) -> StoreChangeset {
        inc.event.ensure_id();
        StoreChangeset {
            event: inc.event,
            meta: inc.meta,
            public_key: inc.public_key,
        }
    }
}

fn log_failed_payload(err: &EncodedJsonPayloadError) {
    let causes: Vec<_> = err.causes().skip(1).collect();
    for cause in causes.iter().rev() {
        warn!("payload processing issue: {}", cause);
    }
    warn!("failed processing event: {}", err);
    debug!(
        "bad event payload: {}",
        err.utf8_body().unwrap_or("<broken>")
    );

    sentry::with_client_and_scope(|client, scope| {
        let mut event = sentry::integrations::failure::event_from_fail(err);
        let last = event.exceptions.len() - 1;
        event.exceptions[last].ty = "BadEventPayload".into();
        if let Some(body) = err.utf8_body() {
            event.message = Some(format!("payload: {}", body));
        };
        client.capture_event(event, Some(scope));
    });
}

impl FromRequest<ServiceState> for IncomingEvent {
    type Config = ();
    type Result = Box<Future<Item = Self, Error = Error>>;

    #[inline]
    fn from_request(req: &HttpRequest<ServiceState>, _cfg: &Self::Config) -> Self::Result {
        let auth: &Auth = req.extensions().get().unwrap();
        let max_payload = req.state().aorta_config().max_event_payload_size;
        let log_failed_payloads = req.state().config().log_failed_payloads();

        // anything up to 7 is considered sentry v7
        if auth.version() <= 7 {
            let meta = event_meta_from_req(req);
            let public_key = auth.public_key().to_string();
            Box::new(
                EncodedJsonBody::new(req.clone())
                    .limit(max_payload)
                    .map_err(move |e| {
                        if log_failed_payloads {
                            log_failed_payload(&e);
                        }
                        BadProjectRequest::BadEncodedJson(e).into()
                    })
                    .map(move |e| IncomingEvent::new(EventVariant::SentryV7(e), meta, public_key)),
            )

        // for now don't handle unsupported versions.  later fallback to raw
        } else {
            Box::new(future::err(
                BadProjectRequest::UnsupportedProtocolVersion(auth.version()).into(),
            ))
        }
    }
}

/// Extracts a foreign event from the payload.
pub struct IncomingForeignEvent(pub IncomingEvent);

impl From<IncomingForeignEvent> for StoreChangeset {
    fn from(inc: IncomingForeignEvent) -> StoreChangeset {
        inc.0.into()
    }
}

impl FromRequest<ServiceState> for IncomingForeignEvent {
    type Config = ();
    type Result = Box<Future<Item = Self, Error = Error>>;

    #[inline]
    fn from_request(req: &HttpRequest<ServiceState>, _cfg: &Self::Config) -> Self::Result {
        let auth: &Auth = req.extensions().get().unwrap();
        let max_payload = req.state().aorta_config().max_event_payload_size;

        let meta = event_meta_from_req(req);
        let store_type = req.match_info().get("store_type").unwrap().to_string();
        let public_key = auth.public_key().to_string();
        let headers = req.headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").into()))
            .collect();

        let is_json = match req.mime_type() {
            Ok(Some(mime)) => mime.type_() == "application" && mime.subtype() == "json",
            _ => false,
        };

        if is_json {
            Box::new(
                JsonBody::new(req.clone())
                    .limit(max_payload)
                    .map_err(|e| BadProjectRequest::BadJson(e).into())
                    .map(move |payload| {
                        IncomingForeignEvent(IncomingEvent::new(
                            EventVariant::Foreign(ForeignEvent {
                                store_type: store_type,
                                headers: headers,
                                payload: ForeignPayload::Json(payload),
                            }),
                            meta,
                            public_key,
                        ))
                    }),
            )
        } else {
            Box::new(
                req.clone()
                    .body()
                    .limit(max_payload)
                    .map_err(|e| BadProjectRequest::BadPayload(e).into())
                    .map(move |payload| {
                        IncomingForeignEvent(IncomingEvent::new(
                            EventVariant::Foreign(ForeignEvent {
                                store_type: store_type,
                                headers: headers,
                                payload: ForeignPayload::Raw(payload.to_vec()),
                            }),
                            meta,
                            public_key,
                        ))
                    }),
            )
        }
    }
}
