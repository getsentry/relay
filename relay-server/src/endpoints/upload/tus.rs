//! TUS protocol types and helpers.
//!
//! Contains constants, error types, and parsing utilities for the TUS resumable upload protocol
//! v1.0.0.
//!
//! Reference: <https://tus.io/protocols/resumable-upload>

use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use data_encoding::BASE64;

use crate::endpoints::common::BadStoreRequest;

/// TUS protocol version supported by this endpoint.
pub const TUS_VERSION: &str = "1.0.0";

/// TUS protocol header for the protocol version.
pub const TUS_RESUMABLE: &str = "Tus-Resumable";

/// TUS protocol header for the total upload length.
pub const UPLOAD_LENGTH: &str = "Upload-Length";

/// TUS protocol header for the current upload offset.
pub const UPLOAD_OFFSET: &str = "Upload-Offset";

/// TUS protocol header for upload metadata (base64-encoded key-value pairs).
pub const UPLOAD_METADATA: &str = "Upload-Metadata";

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
            | TusUploadError::EmptyBody
            | TusUploadError::BodyReadError(_) => StatusCode::BAD_REQUEST,
            TusUploadError::UploadFailed(inner) => {
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
pub fn parse_upload_length(headers: &HeaderMap) -> Result<usize, TusUploadError> {
    let tus_version = headers
        .get(TUS_RESUMABLE)
        .ok_or(TusUploadError::MissingTusVersion)?
        .to_str()
        .map_err(|_| TusUploadError::UnsupportedTusVersion("invalid header value".into()))?;

    if tus_version != TUS_VERSION {
        return Err(TusUploadError::UnsupportedTusVersion(tus_version.into()));
    }

    let upload_length = headers
        .get(UPLOAD_LENGTH)
        .ok_or(TusUploadError::MissingUploadLength)?
        .to_str()
        .map_err(|_| TusUploadError::InvalidUploadLength("invalid header value".into()))?
        .parse::<usize>()
        .map_err(|_| TusUploadError::InvalidUploadLength("not a valid number".into()))?;

    Ok(upload_length)
}

/// Extracts optional upload metadata from headers.
pub fn extract_metadata(headers: &HeaderMap) -> UploadMetadata {
    headers
        .get(UPLOAD_METADATA)
        .and_then(|v| v.to_str().ok())
        .map(UploadMetadata::parse)
        .unwrap_or_default()
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
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        let result = parse_upload_length(&headers);
        assert!(matches!(result, Err(TusUploadError::MissingUploadLength)));
    }

    #[test]
    fn test_validate_tus_headers_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        let result = parse_upload_length(&headers);
        assert_eq!(result.unwrap(), 1024);
    }

    #[test]
    fn test_validate_tus_headers_unsupported_version() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("0.2.0"));
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        let result = parse_upload_length(&headers);
        assert!(matches!(
            result,
            Err(TusUploadError::UnsupportedTusVersion(_))
        ));
    }
}
