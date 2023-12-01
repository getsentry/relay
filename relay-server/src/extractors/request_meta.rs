use std::convert::Infallible;
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::time::Instant;

use axum::extract::rejection::PathRejection;
use axum::extract::{ConnectInfo, FromRequestParts, Path};
use axum::http::header::{self, AsHeaderName};
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::RequestPartsExt;
use data_encoding::BASE64;
use relay_base_schema::project::{ParseProjectKeyError, ProjectId, ProjectKey};
use relay_common::{Auth, Dsn, ParseAuthError, ParseDsnError, Scheme};
use relay_config::UpstreamDescriptor;
use relay_event_normalization::{ClientHints, RawUserAgentInfo};
use relay_quotas::Scoping;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::extractors::{ForwardedFor, StartTime};
use crate::service::ServiceState;
use crate::statsd::RelayCounters;
use crate::utils::ApiErrorResponse;

#[derive(Debug, thiserror::Error)]
pub enum BadEventMeta {
    #[error("missing authorization information")]
    MissingAuth,

    #[error("multiple authorization payloads detected")]
    MultipleAuth,

    #[error("unsupported protocol version ({0})")]
    UnsupportedProtocolVersion(u16),

    #[error("bad envelope authentication header")]
    BadEnvelopeAuth(#[source] serde_json::Error),

    #[error("bad project path parameter")]
    BadProject(#[from] PathRejection),

    #[error("bad x-sentry-auth header")]
    BadAuth(#[from] ParseAuthError),

    #[error("bad sentry DSN public key")]
    BadPublicKey(#[from] ParseProjectKeyError),
}

impl From<Infallible> for BadEventMeta {
    fn from(infallible: Infallible) -> Self {
        match infallible {}
    }
}

impl IntoResponse for BadEventMeta {
    fn into_response(self) -> Response {
        let code = match self {
            Self::MissingAuth
            | Self::MultipleAuth
            | Self::BadAuth(_)
            | Self::BadEnvelopeAuth(_) => StatusCode::UNAUTHORIZED,
            Self::UnsupportedProtocolVersion(_) | Self::BadProject(_) | Self::BadPublicKey(_) => {
                StatusCode::BAD_REQUEST
            }
        };

        (code, ApiErrorResponse::from_error(&self)).into_response()
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
        let project_id = dsn
            .project_id()
            .value()
            .parse()
            .map_err(|_| ParseDsnError::NoProjectId)?;

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
            project_id: Some(project_id),
        })
    }

    /// Creates a new [`PartialDsn`] for a relay outbound request.
    pub fn outbound(scoping: &Scoping, upstream: &UpstreamDescriptor<'_>) -> Self {
        Self {
            scheme: upstream.scheme(),
            public_key: scoping.project_key,
            host: upstream.host().to_owned(),
            port: upstream.port(),
            path: "".to_owned(),
            project_id: Some(scoping.project_id),
        }
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
        serializer.collect_str(self)
    }
}

const fn default_version() -> u16 {
    relay_event_schema::protocol::PROTOCOL_VERSION
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn make_false() -> bool {
    false
}

/// Request information for sentry ingest data, such as events, envelopes or metrics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

    #[serde(default, skip_serializing_if = "ClientHints::is_empty")]
    client_hints: ClientHints<String>,

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
    ///
    /// The client is formatted as `"sdk/version"`, for example `"raven-node/2.6.3"`.
    pub fn client(&self) -> Option<&str> {
        self.client.as_deref()
    }

    /// Returns the name of the client that sent the event without version.
    ///
    /// If the client is not sent in standard format, this method returns `None`.
    pub fn client_name(&self) -> Option<&str> {
        let client = self.client()?;
        let (name, _version) = client.split_once('/')?;
        Some(name)
    }

    /// Returns the protocol version of the event payload.
    #[allow(dead_code)] // used in tests and processing mode
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

    pub fn client_hints(&self) -> &ClientHints<String> {
        &self.client_hints
    }

    /// Indicates that caches should be bypassed.
    pub fn no_cache(&self) -> bool {
        self.no_cache
    }

    /// The time at which the request started.
    pub fn start_time(&self) -> Instant {
        self.start_time
    }

    /// Sets the start time for this [`RequestMeta`] on the current envelope.
    pub fn set_start_time(&mut self, start_time: Instant) {
        self.start_time = start_time
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
            client_hints: ClientHints::default(),
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
            write!(auth, ", sentry_client={client}").ok();
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
            project_key: self.public_key(),
            key_id: None,
        }
    }
}

/// Request information without required authentication parts.
///
/// This is identical to [`RequestMeta`] with the exception that the DSN, used to authenticate, is
/// optional.
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

        complete.client_hints.copy_from(self.client_hints);

        if self.no_cache {
            complete.no_cache = true;
        }

        complete
    }
}

#[axum::async_trait]
impl<S> FromRequestParts<S> for PartialMeta
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let mut ua = RawUserAgentInfo::default();
        for (key, value) in &parts.headers {
            ua.set_ua_field_from_header(key.as_str(), value.to_str().ok().map(str::to_string));
        }

        Ok(RequestMeta {
            dsn: None,
            version: default_version(),
            client: None,
            origin: parse_header_url(parts, header::ORIGIN)
                .or_else(|| parse_header_url(parts, header::REFERER)),
            remote_addr: ConnectInfo::<SocketAddr>::from_request_parts(parts, state)
                .await
                .map(|ConnectInfo(peer)| peer.ip())
                .ok(),
            forwarded_for: ForwardedFor::from_request_parts(parts, state)
                .await?
                .into_inner(),
            user_agent: ua.user_agent,
            no_cache: false,
            start_time: StartTime::from_request_parts(parts, state)
                .await?
                .into_inner(),
            client_hints: ua.client_hints,
        })
    }
}

fn get_auth_header(req: &Parts, header_name: impl AsHeaderName) -> Option<&str> {
    req.headers
        .get(header_name)
        .and_then(|x| x.to_str().ok())
        .filter(|h| h.len() >= 7 && h[..7].eq_ignore_ascii_case("sentry "))
}

fn auth_from_parts(req: &Parts, path_key: Option<String>) -> Result<Auth, BadEventMeta> {
    let mut auth = None;

    // try to extract authentication info from http header "x-sentry-auth"
    if let Some(header) = get_auth_header(req, "x-sentry-auth") {
        auth = Some(header.parse::<Auth>()?);
    }

    // try to extract authentication info from http header "authorization"
    if let Some(header) = get_auth_header(req, header::AUTHORIZATION) {
        if auth.is_some() {
            return Err(BadEventMeta::MultipleAuth);
        }

        auth = Some(header.parse::<Auth>()?);
    }

    // try to get authentication info from basic auth
    if let Some(basic_auth) = req
        .headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|x| {
            if x.len() >= 6 && x[..6].eq_ignore_ascii_case("basic ") {
                x.get(6..)
            } else {
                None
            }
        })
        .and_then(|value| {
            let decoded = String::from_utf8(BASE64.decode(value.as_bytes()).ok()?).ok()?;
            let (public_key, _) = decoded.split_once(':')?;
            Auth::from_pairs([("sentry_key", public_key)]).ok()
        })
    {
        if auth.is_some() {
            return Err(BadEventMeta::MultipleAuth);
        }
        auth = Some(basic_auth);
    }

    // try to extract authentication info from URL query_param .../?sentry_...=<key>...
    let query = req.uri.query().unwrap_or_default();
    if query.contains("sentry_") {
        if auth.is_some() {
            return Err(BadEventMeta::MultipleAuth);
        }

        auth = Some(Auth::from_querystring(query.as_bytes())?);
    }

    // try to extract authentication info from URL path segment .../{sentry_key}/...
    if let Some(sentry_key) = path_key {
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

fn parse_header_url(req: &Parts, header: impl AsHeaderName) -> Option<Url> {
    req.headers
        .get(header)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<Url>().ok())
        .and_then(|u| match u.scheme() {
            "http" | "https" => Some(u),
            _ => None,
        })
}

/// Path parameters containing authentication information for store endpoints.
///
/// These parameters implement part of the authentication mechanism. For more information, see
/// [`RequestMeta`].
#[derive(Debug, serde::Deserialize)]
struct StorePath {
    /// The numeric identifier of the Sentry project.
    ///
    /// This parameter is part of the store endpoint paths, which are generally located under
    /// `/api/:project_id/*`. By default, all store endpoints have the project ID in the path. To
    /// resolve the project and associated information, Relay actually uses the DSN's
    /// [`ProjectKey`]. During ingestion, the stated project ID from the URI path is validated
    /// against information resolved from the upstream.
    ///
    /// The legacy endpoint (`/api/store/`) does not have the project ID. In this case, Relay skips
    /// ID validation during ingestion.
    project_id: Option<ProjectId>,

    /// The DSN's public key, also referred to as project key.
    ///
    /// Some endpoints require this key in the path. On all other endpoints, the key is either sent
    /// as header or query parameter.
    sentry_key: Option<String>,
}

#[axum::async_trait]
impl FromRequestParts<ServiceState> for RequestMeta {
    type Rejection = BadEventMeta;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let Path(store_path): Path<StorePath> =
            parts.extract().await.map_err(BadEventMeta::BadProject)?;

        let auth = auth_from_parts(parts, store_path.sentry_key)?;
        let partial_meta = parts.extract::<PartialMeta>().await?;
        let (public_key, key_flags) = ProjectKey::parse_with_flags(auth.public_key())?;

        let config = state.config();
        let upstream = config.upstream_descriptor();

        let dsn = PartialDsn {
            scheme: upstream.scheme(),
            public_key,
            host: upstream.host().to_owned(),
            port: upstream.port(),
            path: String::new(),
            project_id: store_path.project_id,
        };

        // For now, we only handle <= v8 and drop everything else
        let version = auth.version();
        if version > relay_event_schema::protocol::PROTOCOL_VERSION {
            return Err(BadEventMeta::UnsupportedProtocolVersion(version));
        }

        relay_statsd::metric!(
            counter(RelayCounters::EventProtocol) += 1,
            version = &version.to_string()
        );

        Ok(RequestMeta {
            dsn,
            version,
            client: auth.client_agent().map(str::to_owned),
            origin: partial_meta.origin,
            remote_addr: partial_meta.remote_addr,
            forwarded_for: partial_meta.forwarded_for,
            user_agent: partial_meta.user_agent,
            no_cache: key_flags.contains(&"no-cache"),
            start_time: partial_meta.start_time,
            client_hints: partial_meta.client_hints,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl RequestMeta {
        // TODO: Remove Dsn here?
        pub fn new(dsn: relay_common::Dsn) -> Self {
            Self {
                dsn: PartialDsn::from_dsn(dsn).expect("invalid DSN"),
                client: Some("sentry/client".to_string()),
                version: 7,
                origin: Some("http://origin/".parse().unwrap()),
                remote_addr: Some("192.168.0.1".parse().unwrap()),
                forwarded_for: String::new(),
                user_agent: Some("sentry/agent".to_string()),
                no_cache: false,
                start_time: Instant::now(),
                client_hints: ClientHints::default(),
            }
        }
    }

    #[test]
    fn test_request_meta_roundtrip() {
        let json = r#"{
            "dsn": "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42",
            "client": "sentry-client",
            "version": 7,
            "origin": "http://origin/",
            "remote_addr": "192.168.0.1",
            "forwarded_for": "8.8.8.8",
            "user_agent": "0x8000",
            "no_cache": false,
            "client_hints":  {
            "sec_ch_ua_platform": "macOS",
            "sec_ch_ua_platform_version": "13.1.0",
            "sec_ch_ua": "\"Not_A Brand\";v=\"99\", \"Google Chrome\";v=\"109\", \"Chromium\";v=\"109\""
            }
        }"#;

        let mut deserialized: RequestMeta = serde_json::from_str(json).unwrap();

        let reqmeta = RequestMeta {
            dsn: PartialDsn::from_str("https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42")
                .unwrap(),
            client: Some("sentry-client".to_owned()),
            version: 7,
            origin: Some(Url::parse("http://origin/").unwrap()),
            remote_addr: Some(IpAddr::from_str("192.168.0.1").unwrap()),
            forwarded_for: "8.8.8.8".to_string(),
            user_agent: Some("0x8000".to_string()),
            no_cache: false,
            start_time: Instant::now(),
            client_hints: ClientHints {
                sec_ch_ua_platform: Some("macOS".to_owned()),
                sec_ch_ua_platform_version: Some("13.1.0".to_owned()),
                sec_ch_ua: Some(
                    "\"Not_A Brand\";v=\"99\", \"Google Chrome\";v=\"109\", \"Chromium\";v=\"109\""
                        .to_owned(),
                ),
                sec_ch_ua_model: None,
            },
        };
        deserialized.start_time = reqmeta.start_time;
        assert_eq!(deserialized, reqmeta);
    }
}
