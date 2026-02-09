//! TUS protocol upload endpoint for resumable uploads.
//!
//! Implements a subset of the TUS protocol v1.0.0, specifically "Creation With Upload"
//! which allows creating a resource and uploading data in a single POST request.
//!
//! Reference: <https://tus.io/protocols/resumable-upload#creation-with-upload>

use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use data_encoding::BASE64;
use futures::StreamExt;
use relay_config::Config;

use crate::endpoints::common::BadStoreRequest;
use crate::extractors::RequestMeta;
use crate::middlewares;
use crate::service::ServiceState;

/// TUS protocol version supported by this endpoint.
const TUS_VERSION: &str = "1.0.0";

/// TUS protocol header for the protocol version.
const TUS_RESUMABLE_HEADER: &str = "Tus-Resumable";

/// TUS protocol header for the total upload length.
const UPLOAD_LENGTH_HEADER: &str = "Upload-Length";

/// TUS protocol header for the current upload offset.
const UPLOAD_OFFSET_HEADER: &str = "Upload-Offset";

/// TUS protocol header for upload metadata (base64-encoded key-value pairs).
const UPLOAD_METADATA_HEADER: &str = "Upload-Metadata";

/// Error type for TUS upload requests.
#[derive(Debug, thiserror::Error)]
pub enum TusUploadError {
    #[error("missing Tus-Resumable header")]
    MissingTusVersion,

    #[error("unsupported TUS protocol version: {0}")]
    UnsupportedTusVersion(String),

    #[error("missing Upload-Length header")]
    MissingUploadLength,

    #[error("invalid Upload-Length header: {0}")]
    InvalidUploadLength(String),

    #[error("content length mismatch: expected {expected_length}, got {actual_length}")]
    ContentLengthMismatch {
        expected_length: usize,
        actual_length: usize,
    },

    #[error("empty request body")]
    EmptyBody,

    #[error("failed to read request body: {0}")]
    BodyReadError(String),

    #[error("upload failed: {0}")]
    UploadFailed(#[from] BadStoreRequest),
}

impl IntoResponse for TusUploadError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            TusUploadError::MissingTusVersion
            | TusUploadError::UnsupportedTusVersion(_)
            | TusUploadError::MissingUploadLength
            | TusUploadError::InvalidUploadLength(_)
            | TusUploadError::ContentLengthMismatch { .. }
            | TusUploadError::EmptyBody => StatusCode::BAD_REQUEST,
            TusUploadError::BodyReadError(_) => StatusCode::BAD_REQUEST,
            TusUploadError::UploadFailed(inner) => {
                // Delegate to inner error for proper status code mapping
                return inner.to_string().into_response();
            }
        };

        let mut response = (status, self.to_string()).into_response();
        // Always include TUS version in error responses
        response
            .headers_mut()
            .insert(TUS_RESUMABLE_HEADER, HeaderValue::from_static(TUS_VERSION));
        response
    }
}

/// Parsed TUS upload metadata from the Upload-Metadata header.
#[derive(Debug, Default)]
pub struct UploadMetadata {
    /// The filename of the upload, if provided.
    pub filename: Option<String>,
    /// The content type of the upload, if provided.
    pub content_type: Option<String>,
}

impl UploadMetadata {
    /// Parse the Upload-Metadata header value.
    ///
    /// The format is: `key base64value, key2 base64value2`
    pub fn parse(header_value: &str) -> Self {
        let mut metadata = Self::default();

        for part in header_value.split(',') {
            let part = part.trim();
            let mut iter = part.splitn(2, ' ');

            let key = match iter.next() {
                Some(k) => k,
                None => continue,
            };

            let value = iter.next().and_then(|v| {
                BASE64
                    .decode(v.as_bytes())
                    .ok()
                    .and_then(|bytes| String::from_utf8(bytes).ok())
            });

            match key {
                "filename" => metadata.filename = value,
                "content_type" => metadata.content_type = value,
                _ => {} // Ignore unknown metadata keys
            }
        }

        metadata
    }
}

/// Validates TUS protocol headers and returns the expected upload length.
fn parse_upload_length(headers: &HeaderMap) -> Result<usize, TusUploadError> {
    // Validate Tus-Resumable header
    let tus_version = headers
        .get(TUS_RESUMABLE_HEADER)
        .ok_or(TusUploadError::MissingTusVersion)?
        .to_str()
        .map_err(|_| TusUploadError::UnsupportedTusVersion("invalid header value".into()))?;

    if tus_version != TUS_VERSION {
        return Err(TusUploadError::UnsupportedTusVersion(tus_version.into()));
    }

    // Validate Upload-Length header
    let upload_length = headers
        .get(UPLOAD_LENGTH_HEADER)
        .ok_or(TusUploadError::MissingUploadLength)?
        .to_str()
        .map_err(|_| TusUploadError::InvalidUploadLength("invalid header value".into()))?
        .parse::<usize>()
        .map_err(|_| TusUploadError::InvalidUploadLength("not a valid number".into()))?;

    Ok(upload_length)
}

/// Extracts optional upload metadata from headers.
fn extract_metadata(headers: &HeaderMap) -> UploadMetadata {
    headers
        .get(UPLOAD_METADATA_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(UploadMetadata::parse)
        .unwrap_or_default()
}

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
    response_headers.insert(TUS_RESUMABLE_HEADER, HeaderValue::from_static(TUS_VERSION));
    response_headers.insert(
        UPLOAD_OFFSET_HEADER,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_upload_metadata_empty() {
        let metadata = UploadMetadata::parse("");
        assert!(metadata.filename.is_none());
        assert!(metadata.content_type.is_none());
    }

    #[test]
    fn test_parse_upload_metadata_filename() {
        // "test.txt" base64-encoded is "dGVzdC50eHQ="
        let metadata = UploadMetadata::parse("filename dGVzdC50eHQ=");
        assert_eq!(metadata.filename, Some("test.txt".to_string()));
        assert!(metadata.content_type.is_none());
    }

    #[test]
    fn test_parse_upload_metadata_multiple() {
        // "test.txt" = "dGVzdC50eHQ="
        // "text/plain" = "dGV4dC9wbGFpbg=="
        let metadata =
            UploadMetadata::parse("filename dGVzdC50eHQ=, content_type dGV4dC9wbGFpbg==");
        assert_eq!(metadata.filename, Some("test.txt".to_string()));
        assert_eq!(metadata.content_type, Some("text/plain".to_string()));
    }

    #[test]
    fn test_parse_upload_metadata_unknown_keys() {
        let metadata = UploadMetadata::parse("unknown dGVzdA==, filename dGVzdC50eHQ=");
        assert_eq!(metadata.filename, Some("test.txt".to_string()));
    }

    #[test]
    fn test_validate_tus_headers_missing_version() {
        let headers = HeaderMap::new();
        let result = parse_upload_length(&headers);
        assert!(matches!(result, Err(TusUploadError::MissingTusVersion)));
    }

    #[test]
    fn test_validate_tus_headers_missing_length() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE_HEADER, HeaderValue::from_static("1.0.0"));
        let result = parse_upload_length(&headers);
        assert!(matches!(result, Err(TusUploadError::MissingUploadLength)));
    }

    #[test]
    fn test_validate_tus_headers_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE_HEADER, HeaderValue::from_static("1.0.0"));
        headers.insert(UPLOAD_LENGTH_HEADER, HeaderValue::from_static("1024"));
        let result = parse_upload_length(&headers);
        assert_eq!(result.unwrap(), 1024);
    }

    #[test]
    fn test_validate_tus_headers_unsupported_version() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE_HEADER, HeaderValue::from_static("0.2.0"));
        headers.insert(UPLOAD_LENGTH_HEADER, HeaderValue::from_static("1024"));
        let result = parse_upload_length(&headers);
        assert!(matches!(
            result,
            Err(TusUploadError::UnsupportedTusVersion(_))
        ));
    }
}
