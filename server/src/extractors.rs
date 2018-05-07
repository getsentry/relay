use std::sync::Arc;

use actix_web::dev::JsonBody;
use actix_web::error::{Error, JsonPayloadError, PayloadError, ResponseError};
use actix_web::{FromRequest, HttpMessage, HttpRequest, HttpResponse, State, http::header};
use futures::{future, Future};
use sentry_types::{Auth, AuthParseError};
use url::Url;

use smith_aorta::{ApiErrorResponse, EventMeta, EventVariant, ForeignEvent, ForeignPayload,
                  ProjectState, StoreChangeset};
use smith_common::{ProjectId, ProjectIdParseError};
use smith_trove::TroveState;

use body::{EncodedJsonBody, EncodedJsonPayloadError};

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

/// An extractor for the trove state.
pub type CurrentTroveState = State<Arc<TroveState>>;

/// A common extractor for handling project requests.
///
/// This extractor not only validates the authentication and project id
/// from the URL but also exposes that information in request extensions
/// so that other systems can access it.
///
/// In particular it wraps another extractor that can give convenient
/// access to the request's payload.
#[derive(Debug)]
pub struct ProjectRequest<T: FromRequest<Arc<TroveState>> + 'static> {
    http_req: HttpRequest<Arc<TroveState>>,
    payload: Option<<<T as FromRequest<Arc<TroveState>>>::Result as Future>::Item>,
}

impl<T: FromRequest<Arc<TroveState>> + 'static> ProjectRequest<T> {
    /// Returns the project identifier for this request.
    pub fn project_id(&self) -> ProjectId {
        *self.http_req.extensions_ro().get().unwrap()
    }

    /// Returns the current trove state.
    pub fn trove_state(&self) -> Arc<TroveState> {
        self.http_req.state().clone()
    }

    /// Gets or creates the project state.
    pub fn get_or_create_project_state(&self) -> Arc<ProjectState> {
        self.trove_state()
            .get_or_create_project_state(self.project_id())
    }

    /// Extracts the embedded payload.
    ///
    /// This can only be called once.  Panics if there is no payload.
    pub fn take_payload(
        &mut self,
    ) -> <<T as FromRequest<Arc<TroveState>>>::Result as Future>::Item {
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
    Auth::from_pairs(
        req.query()
            .iter()
            .map(|&(ref a, ref b)| -> (&str, &str) { (&a, &b) }),
    ).map_err(BadProjectRequest::BadAuth)
}

impl<T: FromRequest<Arc<TroveState>> + 'static> FromRequest<Arc<TroveState>> for ProjectRequest<T> {
    type Config = <T as FromRequest<Arc<TroveState>>>::Config;
    type Result = Box<Future<Item = Self, Error = Error>>;

    fn from_request(req: &HttpRequest<Arc<TroveState>>, cfg: &Self::Config) -> Self::Result {
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
            <<<T as FromRequest<Arc<TroveState>>>::Result as Future>::Item>::from_request(
                &req,
                cfg,
            ).map(move |payload| ProjectRequest {
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

fn event_meta_from_req(req: &HttpRequest<Arc<TroveState>>) -> EventMeta {
    let auth: &Auth = req.extensions_ro().get().unwrap();
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

impl FromRequest<Arc<TroveState>> for IncomingEvent {
    type Config = ();
    type Result = Box<Future<Item = Self, Error = Error>>;

    #[inline]
    fn from_request(req: &HttpRequest<Arc<TroveState>>, _cfg: &Self::Config) -> Self::Result {
        let auth: &Auth = req.extensions_ro().get().unwrap();

        // anything up to 7 is considered sentry v7
        if auth.version() <= 7 {
            let meta = event_meta_from_req(req);
            let public_key = auth.public_key().to_string();
            Box::new(
                EncodedJsonBody::new(req.clone())
                    .limit(524_288)
                    .map_err(|e| BadProjectRequest::BadEncodedJson(e).into())
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

impl FromRequest<Arc<TroveState>> for IncomingForeignEvent {
    type Config = ();
    type Result = Box<Future<Item = Self, Error = Error>>;

    #[inline]
    fn from_request(req: &HttpRequest<Arc<TroveState>>, _cfg: &Self::Config) -> Self::Result {
        let auth: &Auth = req.extensions_ro().get().unwrap();

        let meta = event_meta_from_req(req);
        let store_type = req.match_info().get("store_type").unwrap().to_string();
        let public_key = auth.public_key().to_string();
        let headers = req.headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").into()))
            .collect();

        let is_json = match req.mime_type() {
            Ok(Some(ref mime)) => mime.type_() == "application" && mime.subtype() == "json",
            _ => false,
        };

        if is_json {
            Box::new(
                JsonBody::new(req.clone())
                    .limit(524_288)
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
                    .limit(524_288)
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
