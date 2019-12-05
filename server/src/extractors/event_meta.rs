use std::net::IpAddr;

use actix::ResponseFuture;
use actix_web::dev::AsyncResult;
use actix_web::http::header;
use actix_web::{FromRequest, HttpMessage, HttpRequest, HttpResponse, ResponseError};
use failure::Fail;
use futures::{future, Future};
use serde::{Deserialize, Serialize};
use url::Url;

use semaphore_common::{
    tryf, Auth, AuthParseError, Dsn, DsnParseError, ProjectId, ProjectIdParseError,
};

use crate::actors::project_keys::GetProjectId;
use crate::extractors::ForwardedFor;
use crate::service::ServiceState;
use crate::utils::ApiErrorResponse;

#[derive(Debug, Fail)]
pub enum BadEventMeta {
    #[fail(display = "bad project path parameter")]
    BadProject(#[cause] ProjectIdParseError),

    #[fail(display = "bad x-sentry-auth header")]
    BadAuth(#[fail(cause)] AuthParseError),

    #[fail(display = "bad sentry DSN")]
    BadDsn(#[fail(cause)] DsnParseError),

    #[fail(display = "bad project key: project does not exist")]
    BadProjectKey,

    #[fail(display = "could not schedule event processing")]
    ScheduleFailed,
}

impl ResponseError for BadEventMeta {
    fn error_response(&self) -> HttpResponse {
        let mut builder = match *self {
            Self::ScheduleFailed => HttpResponse::ServiceUnavailable(),
            Self::BadProjectKey => HttpResponse::Unauthorized(),
            _ => HttpResponse::BadRequest(),
        };

        builder.json(&ApiErrorResponse::from_fail(self))
    }
}

fn default_version() -> u16 {
    semaphore_common::PROTOCOL_VERSION
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMeta {
    /// The DSN describing the target of this envelope.
    dsn: Dsn,

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
}

impl EventMeta {
    #[cfg(test)]
    pub fn new(dsn: Dsn) -> Self {
        EventMeta {
            dsn,
            client: Some("sentry/client".to_string()),
            version: 7,
            origin: Some("http://origin/".parse().unwrap()),
            remote_addr: Some("192.168.0.1".parse().unwrap()),
            forwarded_for: String::new(),
            user_agent: Some("sentry/agent".to_string()),
        }
    }

    /// Overwrites this event meta instance with information from another.
    ///
    /// All fields that are not set in the other instance will remain.
    pub fn merge(&mut self, other: EventMeta) {
        self.dsn = other.dsn;
        if let Some(client) = other.client {
            self.client = Some(client);
        }
        self.version = other.version;
        if let Some(origin) = other.origin {
            self.origin = Some(origin);
        }
        if let Some(remote_addr) = other.remote_addr {
            self.remote_addr = Some(remote_addr);
        }
        self.forwarded_for = other.forwarded_for;
        if let Some(user_agent) = other.user_agent {
            self.user_agent = Some(user_agent);
        }
    }

    /// Returns a reference to the DSN.
    ///
    /// The DSN declares the project and auth information and upstream address. When EventMeta is
    /// constructed from a web request, the DSN is set to point to the upstream host.
    pub fn dsn(&self) -> &Dsn {
        &self.dsn
    }

    /// Returns the project identifier that the DSN points to.
    pub fn project_id(&self) -> ProjectId {
        // TODO(ja): sentry-types does not expose the DSN at the moment.
        unsafe { std::mem::transmute(self.dsn().project_id()) }
    }

    /// Returns the public key part of the DSN for authentication.
    pub fn public_key(&self) -> &str {
        &self.dsn.public_key()
    }

    /// Returns the client that sent this event (Sentry SDK identifier).
    pub fn client(&self) -> Option<&str> {
        self.client.as_ref().map(String::as_str)
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
        self.user_agent.as_ref().map(String::as_str)
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
}

fn auth_from_request<S>(req: &HttpRequest<S>) -> Result<Auth, BadEventMeta> {
    let auth = req
        .headers()
        .get("x-sentry-auth")
        .and_then(|x| x.to_str().ok());

    if let Some(auth) = auth {
        return auth.parse::<Auth>().map_err(BadEventMeta::BadAuth);
    }

    Auth::from_querystring(req.query_string().as_bytes()).map_err(BadEventMeta::BadAuth)
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
) -> ResponseFuture<EventMeta, BadEventMeta> {
    // println!("XXXXXXXXXXXXXXXXX    extracting meta now");
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

        Ok(EventMeta {
            dsn: dsn_string.parse().map_err(BadEventMeta::BadDsn)?,
            version,
            client,
            origin,
            remote_addr,
            forwarded_for,
            user_agent,
        })
    }))
}

impl FromRequest<ServiceState> for EventMeta {
    type Config = ();
    type Result = AsyncResult<Self, actix_web::Error>;

    fn from_request(request: &HttpRequest<ServiceState>, _cfg: &Self::Config) -> Self::Result {
        // println!("XXXXXXXXXXXXXXXX       eventmeta fromrequest");
        AsyncResult::from(Ok(extract_event_meta(request)))
    }
}
