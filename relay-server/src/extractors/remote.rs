//! Extractors for types from other crates via [`Remote`].

use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use multer::Multipart;

use crate::service::ServiceState;
use crate::utils::{self, ApiErrorResponse};

/// A transparent wrapper around a remote type that implements [`FromRequest`] or [`IntoResponse`].
///
/// # Example
///
/// ```ignore
/// use std::convert::Infallible;
///
/// use axum::extract::{FromRequest, Request};
/// use axum::response::IntoResponse;
///
/// use crate::extractors::Remote;
///
/// // Derive `FromRequest` for `bool` for illustration purposes:
/// #[axum::async_trait]
/// impl<S> axum::extract::FromRequest<S> for Remote<bool> {
///     type Rejection = Remote<Infallible>;
///
///     async fn from_request(request: Request) -> Result<Self, Self::Rejection> {
///         Ok(Remote(true))
///     }
/// }
///
/// impl IntoResponse for Remote<Infallible> {
///    fn into_response(self) -> axum::response::Response {
///        match self.0 {}
///    }
/// }
/// ```
#[derive(Debug)]
pub struct Remote<T>(pub T);

impl<T> From<T> for Remote<T> {
    fn from(inner: T) -> Self {
        Self(inner)
    }
}

#[axum::async_trait]
impl FromRequest<ServiceState> for Remote<Multipart<'static>> {
    type Rejection = Remote<multer::Error>;

    async fn from_request(request: Request, state: &ServiceState) -> Result<Self, Self::Rejection> {
        utils::multipart_from_request(request, state.config())
            .map(Remote)
            .map_err(Remote)
    }
}

impl IntoResponse for Remote<multer::Error> {
    fn into_response(self) -> Response {
        let Self(ref error) = self;

        let status_code = match error {
            multer::Error::FieldSizeExceeded { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            multer::Error::StreamSizeExceeded { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            _ => StatusCode::BAD_REQUEST,
        };

        (status_code, ApiErrorResponse::from_error(error)).into_response()
    }
}
