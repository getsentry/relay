use std::sync::Arc;

use actix_web::dev::JsonBody;
use actix_web::error::{Error, JsonPayloadError, ResponseError};
use actix_web::{FromRequest, HttpMessage, HttpRequest, HttpResponse, State};
use futures::{future, Future};
use http::StatusCode;
use sentry_types::{Auth, AuthParseError};

use smith_aorta::{ApiErrorResponse, EventMeta, EventVariant, ProjectState};
use smith_common::{ProjectId, ProjectIdParseError};
use smith_trove::TroveState;

#[derive(Fail, Debug)]
pub enum BadProjectRequest {
    #[fail(display = "invalid project path parameter")]
    BadProject(#[cause] ProjectIdParseError),
    #[fail(display = "bad x-sentry-auth header")]
    BadAuth(#[cause] AuthParseError),
    #[fail(display = "bad JSON payload")]
    BadJson(#[cause] JsonPayloadError),
    #[fail(display = "unsupported protocol version ({})", _0)]
    UnsupportedProtocolVersion(u16),
}

impl ResponseError for BadProjectRequest {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(StatusCode::BAD_REQUEST).json(&ApiErrorResponse::from_fail(self))
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
    /// Returns the sentry protocol auth for this request.
    pub fn auth(&self) -> &Auth {
        self.http_req.extensions_ro().get().unwrap()
    }

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

/// Extracts an event variant from the payload.
pub struct Event {
    event: EventVariant,
    meta: EventMeta,
}

impl Event {
    fn new(mut event: EventVariant, meta: EventMeta) -> Event {
        event.ensure_id();
        Event { event, meta }
    }

    /// Converts the event wrapper into the contained event meta and variant.
    pub fn into_inner(self) -> (EventVariant, EventMeta) {
        (self.event, self.meta)
    }
}

impl FromRequest<Arc<TroveState>> for Event {
    type Config = ();
    type Result = Box<Future<Item = Self, Error = Error>>;

    #[inline]
    fn from_request(req: &HttpRequest<Arc<TroveState>>, _cfg: &Self::Config) -> Self::Result {
        let auth: &Auth = req.extensions_ro().get().unwrap();

        let meta = EventMeta {
            remote_addr: req.peer_addr().map(|sock_addr| sock_addr.ip()),
            user_agent: auth.client_agent().map(|x| x.to_string()),
        };

        // anything up to 7 is considered sentry v7
        if auth.version() <= 7 {
            Box::new(
                JsonBody::new(req.clone())
                    .limit(524_288)
                    .map_err(|e| BadProjectRequest::BadJson(e).into())
                    .map(|e| Event::new(EventVariant::SentryV7(e), meta)),
            )

        // for now don't handle unsupported versions.  later fallback to raw
        } else {
            Box::new(future::err(
                BadProjectRequest::UnsupportedProtocolVersion(auth.version()).into(),
            ))
        }
    }
}
