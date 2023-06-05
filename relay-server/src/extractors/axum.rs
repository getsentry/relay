use axum::body::HttpBody;
use axum::extract::{BodyStream, FromRequest};
use axum::http::Request;
use axum::response::{IntoResponse, Response};
use axum::{BoxError, RequestExt};
use bytes::Bytes;
use multer::{parse_boundary, Multipart};
use reqwest::{header, StatusCode};

use crate::utils::ApiErrorResponse;

/// Adds the instrumentation on top of the [`bytes::Bytes`].
#[derive(Debug)]
pub struct InstrumentedBytes(pub Bytes);

#[axum::async_trait]
impl<S, B> FromRequest<S, B> for InstrumentedBytes
where
    Bytes: FromRequest<S, B>,
    B: Send + 'static,
    S: Send + Sync,
{
    type Rejection = <Bytes as FromRequest<S, B>>::Rejection;

    #[tracing::instrument(name = "middleware.axum.extractor", level = "trace", skip_all)]
    async fn from_request(req: axum::http::Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self(Bytes::from_request(req, state).await?))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MultipartError {
    #[error("missing Content-Type header")]
    MissingHeader,

    #[error(transparent)]
    MulterError(#[from] multer::Error),
}

impl IntoResponse for MultipartError {
    fn into_response(self) -> Response {
        let status = match self {
            MultipartError::MissingHeader | MultipartError::MulterError(_) => {
                StatusCode::BAD_REQUEST
            }
        };

        (status, ApiErrorResponse::from_error(&self)).into_response()
    }
}

/// Adds instrumentation on top of the [`axum::extract::Multipart`].
#[derive(Debug)]
pub struct InstrumentedMultipart(pub Multipart<'static>);

#[axum::async_trait]
impl<S, B> FromRequest<S, B> for InstrumentedMultipart
where
    B: HttpBody + Send + 'static,
    B::Data: Into<Bytes>,
    B::Error: Into<BoxError>,
    S: Send + Sync,
{
    type Rejection = MultipartError;

    #[tracing::instrument(name = "middleware.axum.extractor", level = "trace", skip_all)]
    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let header = req
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|s| s.to_str().ok())
            .ok_or(MultipartError::MissingHeader)?;

        let boundary = parse_boundary(header)?;

        let stream_result = match req.with_limited_body() {
            Ok(limited) => BodyStream::from_request(limited, state).await,
            Err(unlimited) => BodyStream::from_request(unlimited, state).await,
        };

        let stream = stream_result.unwrap_or_else(|err| match err {});
        let multipart = multer::Multipart::new(stream, boundary);

        Ok(Self(multipart))
    }
}
