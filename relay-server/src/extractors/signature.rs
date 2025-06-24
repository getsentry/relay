use crate::extractors::SignatureError;
use crate::service::ServiceState;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use relay_auth::Signature;

impl FromRequestParts<ServiceState> for Signature {
    type Rejection = SignatureError;

    async fn from_request_parts(
        parts: &mut Parts,
        _: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let Some(header) = parts.headers.get("x-sentry-relay-signature") else {
            return Err(SignatureError::MissingHeader("x-sentry-relay-signature"));
        };
        Ok(Signature(header.as_bytes().to_vec()))
    }
}
