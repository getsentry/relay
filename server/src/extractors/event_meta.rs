use std::net::IpAddr;

use actix_web::http::header;
use actix_web::{FromRequest, HttpMessage, HttpRequest, HttpResponse, ResponseError};
use failure::Fail;
use serde::{Deserialize, Serialize};
use url::Url;

use semaphore_common::{Auth, AuthParseError, Dsn, DsnParseError, ProjectId};

use crate::extractors::ForwardedFor;
use crate::utils::ApiErrorResponse;

#[derive(Debug, Fail)]
pub enum BadEventMeta {
    #[fail(display = "bad x-sentry-auth header")]
    BadAuth(#[fail(cause)] AuthParseError),

    #[fail(display = "bad sentry DSN")]
    BadDsn(#[fail(cause)] DsnParseError),
}

impl ResponseError for BadEventMeta {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::BadRequest().json(&ApiErrorResponse::from_fail(self))
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
            client: None,
            version: default_version(),
            origin: None,
            remote_addr: None,
            forwarded_for: String::new(),
            user_agent: None,
        }
    }

    /// Completes this event meta instance with information from another.
    ///
    /// All fields that are set in this instance will remain. Only missing fields will be updated if
    /// they are defined in the other EvenMeta instance.
    pub fn default_to(&mut self, other: EventMeta) {
        if self.origin.is_none() {
            self.origin = other.origin;
        }
        if self.remote_addr.is_none() {
            self.remote_addr = other.remote_addr;
        }
        if self.forwarded_for.is_empty() {
            self.forwarded_for = other.forwarded_for;
        }
        if self.user_agent.is_none() {
            self.user_agent = other.user_agent;
        }
    }

    /// Returns a reference to the auth info
    pub fn dsn(&self) -> &Dsn {
        &self.dsn
    }

    /// TODO(ja): Describe
    pub fn project_id(&self) -> ProjectId {
        // TODO(ja): Fix this in sentry-types
        unsafe { std::mem::transmute(self.dsn.project_id()) }
    }

    /// TODO(ja): Describe
    pub fn public_key(&self) -> &str {
        &self.dsn.public_key()
    }

    /// TODO(ja): Describe
    pub fn client(&self) -> Option<&str> {
        self.client.as_ref().map(String::as_str)
    }

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

    /// TODO(ja): Describe
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

impl<S> FromRequest<S> for EventMeta {
    type Config = ();
    type Result = Result<Self, BadEventMeta>;

    fn from_request(request: &HttpRequest<S>, _cfg: &Self::Config) -> Self::Result {
        let auth = auth_from_request(request)?;
        let dsn = format!("").parse().map_err(BadEventMeta::BadDsn)?;

        Ok(EventMeta {
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
        })
    }
}
