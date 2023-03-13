use actix_web::actix::*;
use actix_web::http::StatusCode;
use actix_web::{Error, FromRequest, HttpMessage, HttpRequest, HttpResponse, ResponseError};
use futures::{FutureExt, TryFutureExt};
use relay_auth::{RelayId, UnpackError};
use relay_config::RelayInfo;
use relay_log::Hub;
use serde::de::DeserializeOwned;

use crate::actors::relays::{GetRelay, RelayCache};
use crate::body;
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

#[derive(Debug, thiserror::Error)]
enum SignatureError {
    #[error("invalid relay signature")]
    BadSignature(#[source] UnpackError),
    #[error("missing header: {0}")]
    MissingHeader(&'static str),
    #[error("malformed header: {0}")]
    MalformedHeader(&'static str),
    #[error("Unknown relay id")]
    UnknownRelay,
    #[error("invalid JSON data")]
    InvalidJson(#[source] serde_json::Error),
}

impl ResponseError for SignatureError {
    fn error_response(&self) -> HttpResponse {
        let status = match self {
            SignatureError::InvalidJson(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::UNAUTHORIZED,
        };

        HttpResponse::build(status).json(&ApiErrorResponse::from_error(self))
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

fn get_header<'a, S>(
    request: &'a HttpRequest<S>,
    name: &'static str,
) -> Result<&'a str, SignatureError> {
    let value = request
        .headers()
        .get(name)
        .ok_or(SignatureError::MissingHeader(name))?;

    value
        .to_str()
        .map_err(|_| SignatureError::MalformedHeader(name))
}

async fn signed_json<T, S>(request: HttpRequest<S>) -> Result<SignedJson<T>, Error>
where
    T: DeserializeOwned,
{
    let relay_id = get_header(&request, "x-sentry-relay-id")?
        .parse::<RelayId>()
        .map_err(|_| SignatureError::MalformedHeader("x-sentry-relay-id"))?;

    Hub::from_request(&request).configure_scope(|scope| {
        // Dump out header value even if not string
        scope.set_tag("relay_id", relay_id.to_string());
    });

    let signature = get_header(&request, "x-sentry-relay-signature")?.to_owned();

    let relay = RelayCache::from_registry()
        .send(GetRelay { relay_id })
        .await
        .map_err(|_| MailboxError::Closed)?
        .ok_or(SignatureError::UnknownRelay)?;

    let body = body::request_body(&request, MAX_JSON_SIZE).await?;

    let inner = relay
        .public_key
        .unpack(&body, &signature, None)
        .map_err(SignatureError::from)?;

    Ok(SignedJson { inner, relay })
}

impl<T: DeserializeOwned + 'static> FromRequest<ServiceState> for SignedJson<T> {
    type Config = ();
    type Result = ResponseFuture<Self, Error>;

    fn from_request(req: &HttpRequest<ServiceState>, _cfg: &Self::Config) -> Self::Result {
        Box::new(signed_json(req.clone()).boxed_local().compat())
    }
}
