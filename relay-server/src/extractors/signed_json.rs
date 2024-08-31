use axum::extract::rejection::BytesRejection;
use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use relay_auth::{RelayId, UnpackError};
use relay_config::RelayInfo;
use serde::de::DeserializeOwned;

use crate::service::ServiceState;
use crate::services::relays::GetRelay;
use crate::utils::ApiErrorResponse;

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

#[derive(Debug)]
pub struct SignedBytes {
    pub body: Bytes,
    pub relay: RelayInfo,
}

#[axum::async_trait]
impl FromRequest<ServiceState> for SignedBytes {
    type Rejection = SignatureError;

    async fn from_request(request: Request, state: &ServiceState) -> Result<Self, Self::Rejection> {
        let relay_id = get_header(&request, "x-sentry-relay-id")?
            .parse::<RelayId>()
            .map_err(|_| SignatureError::MalformedHeader("x-sentry-relay-id"))?;

        relay_log::configure_scope(|s| s.set_tag("relay_id", relay_id));

        let signature = get_header(&request, "x-sentry-relay-signature")?.to_owned();

        let relay = state
            .relay_cache()
            .send(GetRelay { relay_id })
            .await?
            .ok_or(SignatureError::UnknownRelay)?;

        let body = Bytes::from_request(request, state).await?;
        if !relay.public_key.verify(&body, &signature) {
            Err(SignatureError::BadSignature(UnpackError::BadSignature))
        } else {
            Ok(SignedBytes { body, relay })
        }
    }
}

#[derive(Debug)]
pub struct SignedJson<T> {
    pub inner: T,
    pub relay: RelayInfo,
}

#[axum::async_trait]
impl<T> FromRequest<ServiceState> for SignedJson<T>
where
    T: DeserializeOwned,
{
    type Rejection = SignatureError;

    async fn from_request(request: Request, state: &ServiceState) -> Result<Self, Self::Rejection> {
        let SignedBytes { body, relay } = SignedBytes::from_request(request, state).await?;
        let inner = serde_json::from_slice(&body)?;
        Ok(SignedJson { inner, relay })
    }
}
