use actix_web::actix::*;
use actix_web::http::StatusCode;
use actix_web::{Error, FromRequest, HttpMessage, HttpRequest, HttpResponse, ResponseError};
use failure::Fail;
use futures01::prelude::*;
use serde::de::DeserializeOwned;

use relay_auth::{RelayId, UnpackError};
use relay_common::tryf;
use relay_config::RelayInfo;
use relay_log::Hub;

use crate::actors::relays::{GetRelay, RelayCache};
use crate::body::RequestBody;
use crate::middlewares::ActixWebHubExt;
use crate::service::ServiceState;
use crate::utils::ApiErrorResponse;

/// Maximum size of a JSON request body.
const MAX_JSON_SIZE: usize = 262_144;

#[derive(Debug)]
pub struct SignedJson<T> {
    pub inner: T,
    pub relay: RelayInfo,
}

#[derive(Fail, Debug)]
enum SignatureError {
    #[fail(display = "invalid relay signature")]
    BadSignature(#[cause] UnpackError),
    #[fail(display = "missing header: {}", _0)]
    MissingHeader(&'static str),
    #[fail(display = "malformed header: {}", _0)]
    MalformedHeader(&'static str),
    #[fail(display = "Unknown relay id")]
    UnknownRelay,
    #[fail(display = "invalid JSON data")]
    InvalidJson(#[cause] serde_json::Error),
}

impl ResponseError for SignatureError {
    fn error_response(&self) -> HttpResponse {
        let status = match self {
            SignatureError::InvalidJson(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::UNAUTHORIZED,
        };

        HttpResponse::build(status).json(&ApiErrorResponse::from_fail(self))
    }
}

impl From<UnpackError> for SignatureError {
    fn from(error: UnpackError) -> Self {
        match error {
            UnpackError::BadPayload(json_error) => Self::InvalidJson(json_error),
            other => Self::BadSignature(other),
        }
    }
}

impl<T: DeserializeOwned + 'static> FromRequest<ServiceState> for SignedJson<T> {
    type Config = ();
    type Result = ResponseFuture<Self, Error>;

    fn from_request(req: &HttpRequest<ServiceState>, _cfg: &Self::Config) -> Self::Result {
        macro_rules! extract_header {
            ($name:expr) => {
                tryf!(req
                    .headers()
                    .get($name)
                    .ok_or(SignatureError::MissingHeader($name))
                    .and_then(|value| value
                        .to_str()
                        .map_err(|_| SignatureError::MalformedHeader($name))))
            };
        }

        let relay_id: RelayId = tryf!(extract_header!("X-Sentry-Relay-Id")
            .parse()
            .map_err(|_| SignatureError::MalformedHeader("X-Sentry-Relay-Id")));

        Hub::from_request(req).configure_scope(|scope| {
            // Dump out header value even if not string
            scope.set_tag("relay_id", relay_id.to_string());
        });

        let relay_sig = extract_header!("X-Sentry-Relay-Signature").to_owned();

        let future = RelayCache::from_registry()
            .send(GetRelay { relay_id })
            .map_err(Error::from)
            .and_then(|result| {
                result?
                    .relay
                    .ok_or_else(|| Error::from(SignatureError::UnknownRelay))
            })
            .join(RequestBody::new(req, MAX_JSON_SIZE).map_err(Error::from))
            .and_then(move |(relay, body)| {
                relay
                    .public_key
                    .unpack(&body, &relay_sig, None)
                    .map(|inner| SignedJson { inner, relay })
                    .map_err(|e| Error::from(SignatureError::from(e)))
            });

        Box::new(future)
    }
}
