use std::fmt;
use std::ops::Deref;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use crate::utils::ApiErrorResponse;

#[derive(Clone, Debug)]
pub struct Mime(mime::Mime);

impl fmt::Display for Mime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Deref for Mime {
    type Target = mime::Mime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct MimeError(mime::FromStrError);

impl IntoResponse for MimeError {
    fn into_response(self) -> axum::response::Response {
        (
            StatusCode::BAD_REQUEST,
            ApiErrorResponse::from_error(&self.0),
        )
            .into_response()
    }
}

#[axum::async_trait]
impl<S> FromRequestParts<S> for Mime {
    type Rejection = MimeError;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        let mime = parts
            .headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .parse()
            .map_err(MimeError)?;

        Ok(Self(mime))
    }
}
