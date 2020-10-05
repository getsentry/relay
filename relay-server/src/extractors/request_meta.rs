use std::net::IpAddr;
use std::time::Instant;

use actix::ResponseFuture;
use actix_web::dev::AsyncResult;
use actix_web::http::header;
use actix_web::{FromRequest, HttpMessage, HttpRequest, HttpResponse, ResponseError};
use failure::Fail;
use futures::{future, Future};
use serde::{Deserialize, Serialize};
use url::Url;

use relay_common::{
    tryf, Auth, Dsn, ParseAuthError, ParseDsnError, ParseProjectIdError, ParseProjectKeyError,
    ProjectId, ProjectKey,
};
use relay_quotas::Scoping;

use crate::actors::project_keys::GetProjectId;
use crate::extractors::ForwardedFor;
use crate::middlewares::StartTime;
use crate::service::ServiceState;
use crate::utils::ApiErrorResponse;

#[derive(Debug, Fail)]
pub enum BadEventMeta {
    #[fail(display = "missing authorization information")]
    MissingAuth,

    #[fail(display = "multiple authorization payloads detected")]
    MultipleAuth,

    #[fail(display = "bad project path parameter")]
    BadProject(#[cause] ParseProjectIdError),

    #[fail(display = "bad x-sentry-auth header")]
    BadAuth(#[fail(cause)] ParseAuthError),

    #[fail(display = "bad sentry DSN")]
    BadDsn(#[fail(cause)] ParseDsnError),

    #[fail(display = "bad sentry DSN public key")]
    BadPublicKey(ParseProjectKeyError),

    #[fail(display = "bad project key: project does not exist")]
    BadProjectKey,

    #[fail(display = "could not schedule event processing")]
    ScheduleFailed,
}

impl ResponseError for BadEventMeta {
    fn error_response(&self) -> HttpResponse {
        let mut builder = match *self {
            Self::MissingAuth | Self::MultipleAuth | Self::BadProjectKey | Self::BadAuth(_) => {
                HttpResponse::Unauthorized()
            }
            Self::BadProject(_) | Self::BadDsn(_) | Self::BadPublicKey(_) => {
                HttpResponse::BadRequest()
            }
            Self::ScheduleFailed => HttpResponse::ServiceUnavailable(),
        };

        builder.json(&ApiErrorResponse::from_fail(self))
    }
}

/// Wrapper around a Sentry DSN with parsed public key.
#[derive(Debug, Clone)]
pub struct FullDsn {
    dsn: Dsn,
    public_key: ProjectKey,
}

impl FullDsn {
    /// Ensures a valid public key in the DSN.
    fn from_dsn(dsn: Dsn) -> Result<Self, ParseProjectKeyError> {
        let public_key = ProjectKey::parse(dsn.public_key())?;
        Ok(Self { dsn, public_key })
    }
}

impl<'de> Deserialize<'de> for FullDsn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let dsn = Dsn::deserialize(deserializer)?;
        Self::from_dsn(dsn).map_err(serde::de::Error::custom)
    }
}

impl Serialize for FullDsn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.dsn.serialize(serializer)
    }
}

const fn default_version() -> u16 {
    relay_common::PROTOCOL_VERSION
}

/// Request information for sentry ingest data, such as events, envelopes or metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestMeta<D = FullDsn> {
    /// The DSN describing the target of this envelope.
    dsn: D,

    /// The client SDK that sent this event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client: Option<String>,

    /// The protocol version that the client speaks.
    #[serde(default = "default_version")]
    version: u16,

    /// Value of the origin header in the incoming request, if present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    origin: Option<Url>,

    /// IP address of the submitting remote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    remote_addr: Option<IpAddr>,

    /// The full chain of request forward addresses, including the `remote_addr`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    forwarded_for: String,

    /// The user agent that sent this event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    user_agent: Option<String>,

    /// The time at which the request started.
    //
    // NOTE: This is internal-only and not exposed to Envelope headers.
    #[serde(skip, default = "Instant::now")]
    start_time: Instant,
}

impl<D> RequestMeta<D> {
    /// Returns the client that sent this event (Sentry SDK identifier).
    pub fn client(&self) -> Option<&str> {
        self.client.as_deref()
    }

    /// Returns the protocol version of the event payload.
    pub fn version(&self) -> u16 {
        self.version
    }

    /// Returns a reference to the origin URL
    pub fn origin(&self) -> Option<&Url> {
        self.origin.as_ref()
    }

    /// The IP address of the Relay or client that ingested the event.
    #[allow(unused)]
    pub fn remote_addr(&self) -> Option<IpAddr> {
        self.remote_addr
    }

    /// The IP address of the client that this event originates from.
    ///
    /// This differs from `remote_addr` if the event was sent through a Relay or any other proxy
    /// before.
    pub fn client_addr(&self) -> Option<IpAddr> {
        let client = self.forwarded_for().split(',').next()?;
        client.trim().parse().ok()
    }

    /// Returns the value of the forwarded for header
    pub fn forwarded_for(&self) -> &str {
        &self.forwarded_for
    }

    /// The user agent that sent this event.
    ///
    /// This is the value of the `User-Agent` header. In contrast, `auth.client_agent()` identifies
    /// the SDK that sent the event.
    pub fn user_agent(&self) -> Option<&str> {
        self.user_agent.as_deref()
    }

    /// The time at which the request started.
    pub fn start_time(&self) -> Instant {
        self.start_time
    }
}

impl RequestMeta {
    #[cfg(test)]
    pub fn new(dsn: Dsn) -> Self {
        RequestMeta {
            dsn: FullDsn::from_dsn(dsn).expect("invalid DSN key"),
            client: Some("sentry/client".to_string()),
            version: 7,
            origin: Some("http://origin/".parse().unwrap()),
            remote_addr: Some("192.168.0.1".parse().unwrap()),
            forwarded_for: String::new(),
            user_agent: Some("sentry/agent".to_string()),
            start_time: Instant::now(),
        }
    }

    /// Returns a reference to the DSN.
    ///
    /// The DSN declares the project and auth information and upstream address. When RequestMeta is
    /// constructed from a web request, the DSN is set to point to the upstream host.
    pub fn dsn(&self) -> &Dsn {
        &self.dsn.dsn
    }

    /// Returns the project identifier that the DSN points to.
    pub fn project_id(&self) -> ProjectId {
        self.dsn().project_id()
    }

    /// Returns the public key part of the DSN for authentication.
    pub fn public_key(&self) -> ProjectKey {
        self.dsn.public_key
    }

    /// Formats the Sentry authentication header.
    ///
    /// This header must be included in store requests.
    pub fn auth_header(&self) -> String {
        let mut auth = format!(
            "Sentry sentry_key={}, sentry_version={}",
            self.public_key(),
            self.version
        );

        if let Some(ref client) = self.client {
            use std::fmt::Write;
            write!(auth, ", sentry_client={}", client).ok();
        }

        auth
    }

    /// Returns scoping information from the request.
    ///
    /// The scoping returned from this function is not complete since it lacks info from the Project
    /// state. To fetch full scoping information, invoke the `GetScoping` message on `Project`.
    pub fn get_partial_scoping(&self) -> Scoping {
        Scoping {
            organization_id: 0,
            project_id: self.project_id(),
            public_key: self.public_key(),
            key_id: None,
        }
    }
}

pub type PartialMeta = RequestMeta<Option<Dsn>>;

impl PartialMeta {
    /// Returns a reference to the DSN.
    ///
    /// The DSN declares the project and auth information and upstream address. When RequestMeta is
    /// constructed from a web request, the DSN is set to point to the upstream host.
    pub fn dsn(&self) -> Option<&Dsn> {
        self.dsn.as_ref()
    }

    /// Completes missing information with complete `RequestMeta`.
    ///
    /// All fields that set in this instance will remain.
    pub fn copy_to(self, mut complete: RequestMeta) -> RequestMeta {
        // DSN needs to be validated by the caller and will not be copied over.

        if self.client.is_some() {
            complete.client = self.client;
        }
        if self.version != default_version() {
            complete.version = self.version;
        }
        if self.origin.is_some() {
            complete.origin = self.origin;
        }
        if self.remote_addr.is_some() {
            complete.remote_addr = self.remote_addr;
        }
        if !self.forwarded_for.is_empty() {
            complete.forwarded_for = self.forwarded_for;
        }
        if self.user_agent.is_some() {
            complete.user_agent = self.user_agent;
        }

        complete
    }
}

fn get_auth_header<'a, S>(req: &'a HttpRequest<S>, header_name: &str) -> Option<&'a str> {
    req.headers()
        .get(header_name)
        .and_then(|x| x.to_str().ok())
        .filter(|h| h.len() >= 7 && h[..7].eq_ignore_ascii_case("sentry "))
}

fn auth_from_request<S>(req: &HttpRequest<S>) -> Result<Auth, BadEventMeta> {
    let mut auth = None;

    // try to extract authentication info from http header "x-sentry-auth"
    if let Some(header) = get_auth_header(req, "x-sentry-auth") {
        auth = Some(header.parse::<Auth>().map_err(BadEventMeta::BadAuth)?);
    }

    // try to extract authentication info from http header "authorization"
    if let Some(header) = get_auth_header(req, "authorization") {
        if auth.is_some() {
            return Err(BadEventMeta::MultipleAuth);
        }

        auth = Some(header.parse::<Auth>().map_err(BadEventMeta::BadAuth)?);
    }

    // try to extract authentication info from URL query_param .../?sentry_...=<key>...
    let query = req.query_string();
    if query.contains("sentry_") {
        if auth.is_some() {
            return Err(BadEventMeta::MultipleAuth);
        }

        auth = Some(Auth::from_querystring(query.as_bytes()).map_err(BadEventMeta::BadAuth)?);
    }

    // try to extract authentication info from URL path segment .../{sentry_key}/...
    if let Some(sentry_key) = req.match_info().get("sentry_key") {
        if auth.is_some() {
            return Err(BadEventMeta::MultipleAuth);
        }

        auth = Some(
            Auth::from_pairs(std::iter::once(("sentry_key", sentry_key)))
                .map_err(|_| BadEventMeta::MissingAuth)?,
        );
    }

    auth.ok_or(BadEventMeta::MissingAuth)
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

fn extract_event_meta(
    request: &HttpRequest<ServiceState>,
) -> ResponseFuture<RequestMeta, BadEventMeta> {
    let start_time = StartTime::from_request(request, &()).into_inner();
    let auth = tryf!(auth_from_request(request));

    let version = auth.version();
    let client = auth.client_agent().map(str::to_owned);
    let origin = parse_header_url(request, header::ORIGIN)
        .or_else(|| parse_header_url(request, header::REFERER));
    let remote_addr = request.peer_addr().map(|peer| peer.ip());
    let forwarded_for = ForwardedFor::from(request).into_inner();
    let user_agent = request
        .headers()
        .get(header::USER_AGENT)
        .and_then(|h| h.to_str().ok())
        .map(str::to_owned);

    let state = request.state();
    let config = state.config();

    let project_future = match request.match_info().get("project") {
        Some(s) => {
            // The project_id was declared in the URL. Use it directly.
            let id_result = s.parse::<ProjectId>().map_err(BadEventMeta::BadProject);
            Box::new(future::result(id_result)) as ResponseFuture<_, _>
        }
        None => {
            // The legacy endpoint (/api/store) was hit without a project id. Fetch the project
            // id from the key lookup. Since this is the uncommon case, block the request until the
            // project id is here.
            let future = state
                .key_lookup()
                .send(GetProjectId(auth.public_key().to_owned()))
                .map_err(|_| BadEventMeta::ScheduleFailed)
                .and_then(|result| result.map_err(|_| BadEventMeta::ScheduleFailed))
                .and_then(|opt| opt.ok_or(BadEventMeta::BadProjectKey));

            Box::new(future) as ResponseFuture<_, _>
        }
    };

    Box::new(project_future.and_then(move |project_id| {
        let upstream = config.upstream_descriptor();

        let dsn_string = format!(
            "{}://{}:@{}/{}",
            upstream.scheme(),
            auth.public_key(),
            upstream.host(),
            project_id,
        );

        let dsn = dsn_string.parse().map_err(BadEventMeta::BadDsn)?;
        let dsn = FullDsn::from_dsn(dsn).map_err(BadEventMeta::BadPublicKey)?;

        Ok(RequestMeta {
            dsn,
            version,
            client,
            origin,
            remote_addr,
            forwarded_for,
            user_agent,
            start_time,
        })
    }))
}

impl FromRequest<ServiceState> for RequestMeta {
    type Config = ();
    type Result = AsyncResult<Self, actix_web::Error>;

    fn from_request(request: &HttpRequest<ServiceState>, _cfg: &Self::Config) -> Self::Result {
        AsyncResult::from(Ok(extract_event_meta(request)))
    }
}
