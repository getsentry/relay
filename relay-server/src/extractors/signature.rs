use axum::extract::OptionalFromRequestParts;
use axum::http::request::Parts;
use relay_auth::Signature;

use crate::extractors::SignatureError;
use crate::service::ServiceState;

impl OptionalFromRequestParts<ServiceState> for Signature {
    type Rejection = SignatureError;

    async fn from_request_parts(
        parts: &mut Parts,
        _: &ServiceState,
    ) -> Result<Option<Self>, Self::Rejection> {
        parts
            .headers
            .get("x-sentry-relay-signature")
            .map(|header| {
                header
                    .to_str()
                    .map_err(|_| SignatureError::MalformedHeader("x-sentry-relay-signature"))
                    .map(|sig| Signature(sig.to_owned()))
            })
            .transpose()
    }
}
