//! TUS protocol types and helpers.
//!
//! Contains constants, error types, and parsing utilities for the TUS resumable upload protocol
//! v1.0.0.
//!
//! Reference: <https://tus.io/protocols/resumable-upload>

use axum::http::HeaderMap;
use http::HeaderValue;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The TUS version is missing or does not match the server's version.
    #[error("Version Mismatch")]
    Version,
    /// The `Upload-Length` header is missing or cannot be parsed.
    #[error("Invalid Upload-Length")]
    UploadLength,
    /// The `Content-Type` header is not what TUS expects.
    #[error("Invalid Content-Type")]
    ContentType,
}

/// TUS protocol header for the protocol version.
///
/// See <https://tus.io/protocols/resumable-upload#tus-version>.
pub const TUS_RESUMABLE: &str = "Tus-Resumable";

/// TUS protocol header for supported extensions.
///
/// See <https://tus.io/protocols/resumable-upload#tus-extension>.
const TUS_EXTENSION: &str = "Tus-Extension";

const SUPPORTED_EXTENSIONS: HeaderValue = HeaderValue::from_static("creation-with-upload");

/// TUS protocol version supported by this endpoint.
pub const TUS_VERSION: HeaderValue = HeaderValue::from_static("1.0.0");

/// TUS protocol header for the total upload length.
///
/// See <https://tus.io/protocols/resumable-upload#upload-length>.
pub const UPLOAD_LENGTH: &str = "Upload-Length";

/// TUS protocol header for the current upload offset.
///
/// See <https://tus.io/protocols/resumable-upload#upload-offset>.
pub const UPLOAD_OFFSET: &str = "Upload-Offset";

/// Expected value of the content-type header.
pub const EXPECTED_CONTENT_TYPE: HeaderValue =
    HeaderValue::from_static("application/offset+octet-stream");

/// Validates TUS protocol headers and returns the expected upload length.
pub fn validate_headers(headers: &HeaderMap) -> Result<usize, Error> {
    let tus_version = headers.get(TUS_RESUMABLE);
    if tus_version != Some(&TUS_VERSION) {
        return Err(Error::Version);
    }

    let content_type = headers.get(http::header::CONTENT_TYPE);
    if content_type != Some(&EXPECTED_CONTENT_TYPE) {
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

/// Prepares the required TUS request headers for upstream requests.
pub fn request_headers(upload_length: usize) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(TUS_RESUMABLE, TUS_VERSION);
    headers.insert(http::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
    headers.insert(
        UPLOAD_LENGTH,
        HeaderValue::from_str(&upload_length.to_string())
            .expect("string from usize should always be a valid header"),
    );
    headers
}

/// Prepares the required TUS response headers.
pub fn response_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(TUS_RESUMABLE, TUS_VERSION);
    headers.insert(TUS_EXTENSION, SUPPORTED_EXTENSIONS);
    headers
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;

    use super::*;

    #[test]
    fn test_validate_tus_headers_missing_version() {
        let headers = HeaderMap::new();
        let result = validate_headers(&headers);
        assert!(matches!(result, Err(Error::Version)));
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
        headers.insert(hyper::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
        let result = validate_headers(&headers);
        assert!(matches!(result, Err(Error::UploadLength)));
    }

    #[test]
    fn test_validate_tus_headers_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(hyper::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
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
        assert!(matches!(result, Err(Error::Version)));
    }
}
