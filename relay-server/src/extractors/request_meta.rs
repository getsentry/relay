use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;
use std::time::Instant;

use actix_web::http::header;
use actix_web::{FromRequest, HttpMessage, HttpRequest, HttpResponse, ResponseError};
use failure::Fail;
use serde::{Deserialize, Serialize};
use url::Url;

use relay_common::{
    Auth, Dsn, ParseAuthError, ParseDsnError, ParseProjectIdError, ParseProjectKeyError, ProjectId,
    ProjectKey, Scheme,
};
use relay_quotas::Scoping;

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

    #[fail(display = "bad sentry DSN public key")]
    BadPublicKey(ParseProjectKeyError),
}

impl ResponseError for BadEventMeta {
    fn error_response(&self) -> HttpResponse {
        let mut builder = match *self {
            Self::MissingAuth | Self::MultipleAuth | Self::BadAuth(_) => {
                HttpResponse::Unauthorized()
            }
            Self::BadProject(_) | Self::BadPublicKey(_) => HttpResponse::BadRequest(),
        };

        builder.json(&ApiErrorResponse::from_fail(self))
    }
}

/// Wrapper around a Sentry DSN with parsed public key.
///
/// The Sentry DSN type carries a plain public key string. However, Relay handles copy `ProjectKey`
/// types internally. Converting from `String` to `ProjectKey` is fallible and should be caught when
/// deserializing the request.
///
/// This type caches the parsed project key in addition to the DSN. Other than that, it
/// transparently serializes to and deserializes from a DSN string.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PartialDsn {
    pub scheme: Scheme,
    pub public_key: ProjectKey,
    pub host: String,
    pub port: u16,
    pub path: String,
    pub project_id: Option<ProjectId>,
}

impl PartialDsn {
    /// Ensures a valid public key and project ID in the DSN.
    fn from_dsn(dsn: Dsn) -> Result<Self, ParseDsnError> {
        if dsn.project_id().value() == 0 {
            return Err(ParseDsnError::InvalidProjectId(
                ParseProjectIdError::InvalidValue,
            ));
        }

        let public_key = dsn
            .public_key()
            .parse()
            .map_err(|_| ParseDsnError::NoUsername)?;

        Ok(Self {
            scheme: dsn.scheme(),
            public_key,
            host: dsn.host().to_owned(),
            port: dsn.port(),
            path: dsn.path().to_owned(),
            project_id: Some(dsn.project_id()),
        })
    }

    /// Returns the project identifier that the DSN points to.
    pub fn project_id(&self) -> Option<ProjectId> {
        self.project_id
    }

    /// Returns the public key part of the DSN for authentication.
    pub fn public_key(&self) -> ProjectKey {
        self.public_key
    }
}

impl fmt::Display for PartialDsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}:@{}", self.scheme, self.public_key, self.host)?;
        if self.port != self.scheme.default_port() {
            write!(f, ":{}", self.port)?;
        }
        let project_id = self.project_id.unwrap_or_else(|| ProjectId::new(0));
        write!(f, "/{}{}", self.path.trim_start_matches('/'), project_id)
    }
}

impl FromStr for PartialDsn {
    type Err = ParseDsnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_dsn(Dsn::from_str(s)?)
    }
}

impl<'de> Deserialize<'de> for PartialDsn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let dsn = Dsn::deserialize(deserializer)?;
        Self::from_dsn(dsn).map_err(serde::de::Error::custom)
    }
}

impl Serialize for PartialDsn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

const fn default_version() -> u16 {
    relay_common::PROTOCOL_VERSION
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn make_false() -> bool {
    false
}

/// Request information for sentry ingest data, such as events, envelopes or metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestMeta<D = PartialDsn> {
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

    /// A flag that indicates that project options caching should be bypassed.
    #[serde(default = "make_false", skip_serializing_if = "is_false")]
    no_cache: bool,

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

    /// Indicates that caches should be bypassed.
    pub fn no_cache(&self) -> bool {
        self.no_cache
    }

    /// The time at which the request started.
    pub fn start_time(&self) -> Instant {
        self.start_time
    }
}

impl RequestMeta {
    /// Creates meta for an outbound request of this Relay.
    pub fn outbound(dsn: PartialDsn) -> Self {
        Self {
            dsn,
            client: Some(crate::constants::CLIENT.to_owned()),
            version: default_version(),
            origin: None,
            remote_addr: None,
            forwarded_for: "".to_string(),
            user_agent: Some(crate::constants::SERVER.to_owned()),
            no_cache: false,
            start_time: Instant::now(),
        }
    }

    #[cfg(test)]
    // TODO: Remove Dsn here?
    pub fn new(dsn: relay_common::Dsn) -> Self {
        RequestMeta {
            dsn: PartialDsn::from_dsn(dsn).expect("invalid DSN"),
            client: Some("sentry/client".to_string()),
            version: 7,
            origin: Some("http://origin/".parse().unwrap()),
            remote_addr: Some("192.168.0.1".parse().unwrap()),
            forwarded_for: String::new(),
            user_agent: Some("sentry/agent".to_string()),
            no_cache: false,
            start_time: Instant::now(),
        }
    }

    /// Returns a reference to the DSN.
    ///
    /// The DSN declares the project and auth information and upstream address. When RequestMeta is
    /// constructed from a web request, the DSN is set to point to the upstream host.
    pub fn dsn(&self) -> &PartialDsn {
        &self.dsn
    }

    /// Returns the project identifier that the DSN points to.
    ///
    /// Returns `None` if the envelope was sent to the legacy `/api/store/` endpoint. In this case,
    /// the DSN will be filled in during normalization. In all other cases, this will return
    /// `Some(ProjectId)`.
    pub fn project_id(&self) -> Option<ProjectId> {
        self.dsn.project_id
    }

    /// Updates the DSN to the given project ID.
    pub fn set_project_id(&mut self, project_id: ProjectId) {
        self.dsn.project_id = Some(project_id);
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
            project_id: self.project_id().unwrap_or_else(|| ProjectId::new(0)),
            public_key: self.public_key(),
            key_id: None,
        }
    }
}

pub type PartialMeta = RequestMeta<Option<PartialDsn>>;

impl PartialMeta {
    /// Returns a reference to the DSN.
    ///
    /// The DSN declares the project and auth information and upstream address. When RequestMeta is
    /// constructed from a web request, the DSN is set to point to the upstream host.
    pub fn dsn(&self) -> Option<&PartialDsn> {
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
        if self.no_cache {
            complete.no_cache = true;
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

impl FromRequest<ServiceState> for RequestMeta {
    type Config = ();
    type Result = Result<Self, BadEventMeta>;

    fn from_request(request: &HttpRequest<ServiceState>, _cfg: &Self::Config) -> Self::Result {
        let start_time = StartTime::from_request(request, &()).into_inner();
        let auth = auth_from_request(request)?;

        let project_id = match request.match_info().get("project") {
            // The project_id was declared in the URL. Use it directly.
            Some(s) => Some(s.parse().map_err(BadEventMeta::BadProject)?),

            // The legacy endpoint (/api/store) was hit without a project id. Fetch the project
            // id from the key lookup. Since this is the uncommon case, block the request until the
            // project id is here.
            None => None,
        };

        let config = request.state().config();
        let upstream = config.upstream_descriptor();

        let (public_key, key_flags) =
            ProjectKey::parse_with_flags(auth.public_key()).map_err(BadEventMeta::BadPublicKey)?;

        let dsn = PartialDsn {
            scheme: upstream.scheme(),
            public_key,
            host: upstream.host().to_owned(),
            port: upstream.port(),
            path: String::new(),
            project_id,
        };

        Ok(RequestMeta {
            dsn,
            version: auth.version(),
            client: auth.client_agent().map(str::to_owned),
            origin: parse_header_url(request, header::ORIGIN)
                .or_else(|| parse_header_url(request, header::REFERER)),
            remote_addr: request.peer_addr().map(|peer| peer.ip()),
            forwarded_for: ForwardedFor::from(request).into_inner(),
            user_agent: request
                .headers()
                .get(header::USER_AGENT)
                .and_then(|h| h.to_str().ok())
                .map(str::to_owned),
            no_cache: key_flags.contains(&"no-cache"),
            start_time,
        })
    }
}
