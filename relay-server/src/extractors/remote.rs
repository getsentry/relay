//! Extractors for types from other crates via [`Xt`].

use axum::extract::{FromRequest, Request};
use axum::http::header;
use axum::response::IntoResponse;
use multer::{Constraints, Multipart, SizeLimit};

use crate::service::ServiceState;
use crate::utils::ApiErrorResponse;

/// A transparent wrapper around a type that implements [`FromRequest`] or [`IntoResponse`].
#[derive(Debug)]
pub struct Xt<T>(pub T);

impl<T> Xt<T> {
    /// Returns the inner value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> From<T> for Xt<T> {
    fn from(inner: T) -> Self {
        Self(inner)
    }
}

#[axum::async_trait]
impl FromRequest<ServiceState> for Xt<Multipart<'static>> {
    type Rejection = Xt<multer::Error>;

    async fn from_request(request: Request, state: &ServiceState) -> Result<Self, Self::Rejection> {
        let content_type = request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let boundary = multer::parse_boundary(content_type)?;

        let limits = SizeLimit::new()
            .whole_stream(state.config().max_attachments_size() as u64)
            .per_field(state.config().max_attachment_size() as u64);

        Ok(Self(Multipart::with_constraints(
            request.into_body().into_data_stream(),
            boundary,
            Constraints::new().size_limit(limits),
        )))
    }
}

impl IntoResponse for Xt<multer::Error> {
    fn into_response(self) -> axum::response::Response {
        ApiErrorResponse::from_error(&self.0).into_response()
    }
}
