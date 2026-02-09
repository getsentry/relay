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

use crate::extractors::RequestMeta;
use crate::middlewares;
use crate::service::ServiceState;

use self::tus::{
    TUS_RESUMABLE, TUS_VERSION, TusUploadError, UPLOAD_OFFSET, extract_metadata,
    parse_upload_length,
};

/// Handles TUS upload requests (Creation With Upload).
///
/// This endpoint accepts POST requests with a body containing the complete upload data.
/// Unlike the full TUS protocol, this implementation only supports uploading all data
/// in a single request - partial uploads and resumption are not supported.
///
/// The body is processed as a stream to avoid loading the entire upload into memory.
async fn handle(
    _state: ServiceState,
    _meta: RequestMeta,
    headers: HeaderMap,
    body: Body,
) -> axum::response::Result<impl IntoResponse> {
    // Validate TUS protocol headers
    let expected_length = parse_upload_length(&headers)?;

    // Extract optional metadata
    let _metadata = extract_metadata(&headers);

    // Stream the body and count bytes
    let mut stream = body.into_data_stream();
    let mut bytes_received: usize = 0;

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| TusUploadError::BodyReadError(e.to_string()))?;
        bytes_received += chunk.len();

        // TODO: Process each chunk as it arrives
        // - Write to storage
        // - Update progress tracking
        // For now, we just count bytes to validate the upload length
    }

    // Validate total length matches Upload-Length header
    if bytes_received != expected_length {
        return Err(TusUploadError::ContentLengthMismatch {
            expected_length,
            actual_length: bytes_received,
        })?;
    }

    if bytes_received == 0 {
        return Err(TusUploadError::EmptyBody)?;
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
        .route_layer(DefaultBodyLimit::max(config.max_attachment_size()))
        .route_layer(axum::middleware::from_fn(middlewares::content_length))
}
