use axum::extract::rejection::BytesRejection;
use axum::extract::FromRequest;
use axum::http::header::AsHeaderName;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{BoxError, RequestExt};
use bytes::Bytes;
use futures::{FutureExt, TryFutureExt};
use relay_auth::{RelayId, UnpackError};
use relay_config::RelayInfo;
use relay_log::Hub;
use relay_system::SendError;
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::actors::relays::{GetRelay, RelayCache};
use crate::body;
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
pub enum SignatureError {
    #[error("invalid relay signature")]
    BadSignature(#[source] UnpackError),
    #[error("missing header: {0}")]
    MissingHeader(&'static str),
    #[error("malformed header: {0}")]
    MalformedHeader(&'static str),
    #[error("unknown relay id")]
    UnknownRelay,
    #[error(transparent)]
    MalformedBody(#[from] BytesRejection),
    #[error("invalid JSON data")]
    InvalidJson(#[from] serde_json::Error),
    #[error("internal system is overloaded")]
    ServiceUnavailable(#[from] relay_system::SendError),
}

impl IntoResponse for SignatureError {
    fn into_response(self) -> Response {
        let status = match self {
            SignatureError::InvalidJson(_) => StatusCode::BAD_REQUEST,
            SignatureError::MalformedBody(_) => StatusCode::BAD_REQUEST,
            SignatureError::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::UNAUTHORIZED,
        };

        (status, ApiErrorResponse::from_error(&self)).into_response()
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

fn get_header<'a, B>(
    request: &'a Request<B>,
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

#[axum::async_trait]
impl<T, S, B> FromRequest<S, B> for SignedJson<T>
where
    T: DeserializeOwned,
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<axum::BoxError>,
    S: Send + Sync,
{
    type Rejection = SignatureError;

    async fn from_request(request: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let relay_id = get_header(&request, "x-sentry-relay-id")?
            .parse::<RelayId>()
            .map_err(|_| SignatureError::MalformedHeader("x-sentry-relay-id"))?;

        // TODO(ja): Integrate with Sentry hub.
        // Hub::from_request(&request).configure_scope(|scope| {
        //     // Dump out header value even if not string
        //     scope.set_tag("relay_id", relay_id.to_string());
        // });

        let signature = get_header(&request, "x-sentry-relay-signature")?.to_owned();

        let relay = RelayCache::from_registry()
            .send(GetRelay { relay_id })
            .await?
            .ok_or(SignatureError::UnknownRelay)?;

        // TODO(ja): Configure MAX_JSON_SIZE somehow
        let body = Bytes::from_request(request, state).await?;
        // let body = body::request_body(&request, MAX_JSON_SIZE).await?;

        let inner = relay.public_key.unpack(&body, &signature, None)?;
        Ok(SignedJson { inner, relay })
    }
}
