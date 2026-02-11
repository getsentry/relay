//! TUS protocol types and helpers.
//!
//! Contains constants, error types, and parsing utilities for the TUS resumable upload protocol
//! v1.0.0.
//!
//! Reference: <https://tus.io/protocols/resumable-upload>

use axum::http::HeaderMap;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The TUS version is missing or does not match the server's version.
    #[error("Version Mismatch: {0}")]
    Version(String),
    /// The `Upload-Length` header is missing or cannot be parsed.
    #[error("Invalid Upload-Length")]
    UploadLength,
    /// The `Content-Type` header is not what TUS expects.
    #[error("Invalid Content-Type")]
    ContentType,
}

/// TUS protocol version supported by this endpoint.
pub const TUS_VERSION: &str = "1.0.0";

/// TUS protocol header for the protocol version.
pub const TUS_RESUMABLE: &str = "Tus-Resumable";

/// TUS protocol header for the total upload length.
pub const UPLOAD_LENGTH: &str = "Upload-Length";

/// TUS protocol header for the current upload offset.
pub const UPLOAD_OFFSET: &str = "Upload-Offset";

/// Expected value of the content-type header.
pub const EXPECTED_CONTENT_TYPE: &str = "application/offset+octet-stream";

/// Validates TUS protocol headers and returns the expected upload length.
pub fn validate_headers(headers: &HeaderMap) -> Result<u64, Error> {
    let tus_version = headers
        .get(TUS_RESUMABLE)
        .ok_or_else(|| Error::Version("None".to_owned()))?
        .to_str()
        .map_err(|_| Error::Version("invalid header value".to_owned()))?;

    if tus_version != TUS_VERSION {
        return Err(Error::Version(tus_version.into()));
    }

    let content_type = headers.get(hyper::header::CONTENT_TYPE);
    if content_type.is_none_or(|ct| ct != EXPECTED_CONTENT_TYPE) {
        return Err(Error::ContentType);
    }

    let upload_length = headers
        .get(UPLOAD_LENGTH)
        .ok_or(Error::UploadLength)?
        .to_str()
        .map_err(|_| Error::UploadLength)?
        .parse()
        .map_err(|_| Error::UploadLength)?;

    Ok(upload_length)
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;

    use super::*;

    #[test]
    fn test_validate_tus_headers_missing_version() {
        let headers = HeaderMap::new();
        let result = validate_headers(&headers);
        assert!(matches!(result, Err(Error::Version(_))));
    }

    #[test]
    fn test_validate_tus_headers_missing_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        let result = validate_headers(&headers);
        assert!(matches!(result, Err(Error::ContentType)));
    }

    #[test]
    fn test_validate_tus_headers_missing_length() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(
            hyper::header::CONTENT_TYPE,
            HeaderValue::from_static(EXPECTED_CONTENT_TYPE),
        );
        let result = validate_headers(&headers);
        assert!(matches!(result, Err(Error::UploadLength)));
    }

    #[test]
    fn test_validate_tus_headers_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(
            hyper::header::CONTENT_TYPE,
            HeaderValue::from_static(EXPECTED_CONTENT_TYPE),
        );
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        let result = validate_headers(&headers);
        assert_eq!(result.unwrap(), 1024);
    }

    #[test]
    fn test_validate_tus_headers_unsupported_version() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("0.2.0"));
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        let result = validate_headers(&headers);
        assert!(matches!(result, Err(Error::Version(_))));
    }
}
