//! TUS protocol upload endpoint for resumable uploads.
//!
//! Implements a subset of the TUS protocol v1.0.0, specifically "Creation With Upload"
//! which allows creating a resource and uploading data in a single POST request.
//!
//! Reference: <https://tus.io/protocols/resumable-upload#creation-with-upload>

mod tus;

use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use futures::StreamExt;
use relay_config::Config;

use crate::endpoints::common::BadStoreRequest;
use crate::middlewares;
use crate::service::ServiceState;

use self::tus::{TUS_RESUMABLE, TUS_VERSION, UPLOAD_OFFSET, validate_headers};

/// Error type for TUS upload requests.
#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    #[error("TUS protocol violation: {0}")]
    Tus(#[from] tus::Error),

    #[error("content length mismatch: expected {expected_length}, got {actual_length}")]
    ContentLengthMismatch {
        expected_length: usize,
        actual_length: usize,
    },

    #[error("failed to read request body: {0}")]
    BodyReadError(axum::Error),

    #[error("upload failed: {0}")]
    UploadFailed(#[from] BadStoreRequest),
}

impl IntoResponse for UploadError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            UploadError::Tus(_)
            | UploadError::ContentLengthMismatch { .. }
            | UploadError::BodyReadError(_) => StatusCode::BAD_REQUEST,
            UploadError::UploadFailed(inner) => {
                // Delegate to inner error for proper status code mapping
                return inner.to_string().into_response();
            }
        };

        let mut response = (status, self.to_string()).into_response();
        response
            .headers_mut()
            .insert(TUS_RESUMABLE, HeaderValue::from_static(TUS_VERSION));
        response
    }
}

/// Handles TUS upload requests (Creation With Upload).
///
/// This endpoint accepts POST requests with a body containing the complete upload data.
/// Unlike the full TUS protocol, this implementation only supports uploading all data
/// in a single request - partial uploads and resumption are not supported.
///
/// The body is processed as a stream to avoid loading the entire upload into memory.
async fn handle(headers: HeaderMap, body: Body) -> axum::response::Result<impl IntoResponse> {
    // Validate TUS protocol headers
    let expected_length = validate_headers(&headers).map_err(UploadError::from)?;

    // Stream the body and count bytes
    let mut stream = body.into_data_stream();
    let mut bytes_received: usize = 0;

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| UploadError::BodyReadError(e))?;
        bytes_received += chunk.len();

        if bytes_received > expected_length {
            return Err(UploadError::ContentLengthMismatch {
                expected_length,
                actual_length: bytes_received,
            })?;
        }

        // TODO: Process each chunk as it arrives
        // - Write to storage
        // - Update progress tracking
        // For now, we just count bytes to validate the upload length
    }

    // TODO: Implement actual upload handling
    // - Generate a unique upload ID
    // - Finalize the stored data
    // - Return the location of the created resource

    // Build success response with TUS headers
    let mut response_headers = HeaderMap::new();
    response_headers.insert(TUS_RESUMABLE, HeaderValue::from_static(TUS_VERSION));
    response_headers.insert(
        UPLOAD_OFFSET,
        HeaderValue::from_str(&bytes_received.to_string()).unwrap(),
    );
    // TODO: Add Location header with the upload URL once we have upload ID generation

    Ok((StatusCode::CREATED, response_headers, ""))
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle)
        // TODO: max_upload_size
        // .route_layer(DefaultBodyLimit::max(config.max_attachment_size()))
        .route_layer(axum::middleware::from_fn(middlewares::content_length))
}
