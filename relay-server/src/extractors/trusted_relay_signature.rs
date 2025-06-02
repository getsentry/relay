use crate::service::ServiceState;
use axum::extract::OptionalFromRequestParts;
use axum::http::request::Parts;
use relay_signature::{RelaySignature, TrustedRelaySignature, TrustedRelaySignatureErrors};
use std::convert::Infallible;

impl OptionalFromRequestParts<ServiceState> for RelaySignature {
    type Rejection = Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        _: &ServiceState,
    ) -> Result<Option<Self>, Self::Rejection> {
        match TrustedRelaySignature::from_headers(&parts.headers) {
            Ok(data) => Ok(Some(RelaySignature::Valid(data))),
            Err(TrustedRelaySignatureErrors::MissingSignature) => Ok(None),
            Err(e) => Ok(Some(RelaySignature::Invalid(e))),
        }
    }
}
