use std::net::IpAddr;

use actix_web::http::header;
use actix_web::{FromRequest, HttpMessage, HttpRequest, HttpResponse, ResponseError};
use failure::Fail;
use url::Url;

use semaphore_common::{Auth, AuthParseError};

use crate::extractors::ForwardedFor;
use crate::utils::ApiErrorResponse;

#[derive(Debug, Fail)]
pub enum BadEventMeta {
    #[fail(display = "bad x-sentry-auth header")]
    BadAuth(#[fail(cause)] AuthParseError),
}

impl ResponseError for BadEventMeta {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::BadRequest().json(&ApiErrorResponse::from_fail(self))
    }
}

#[derive(Debug, Clone)]
pub struct EventMeta {
    /// Authentication information (DSN and client)..
    auth: Auth,

    /// Value of the origin header in the incoming request, if present.
    origin: Option<Url>,

    /// IP address of the submitting remote.
    remote_addr: Option<IpAddr>,

    /// The full chain of request forward addresses, including the `remote_addr`.
    forwarded_for: String,

    /// The user agent that sent this event.
    user_agent: Option<String>,
}

impl EventMeta {
    /// Returns a reference to the auth info
    pub fn auth(&self) -> &Auth {
        &self.auth
    }

    /// Returns a reference to the origin URL
    pub fn origin(&self) -> Option<&Url> {
        self.origin.as_ref()
    }

    /// The IP address of the Relay or client that ingested the event.
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
        Ok(EventMeta {
            auth: auth_from_request(request)?,
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
