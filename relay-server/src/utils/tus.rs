//! TUS protocol types and helpers.
//!
//! Contains constants, error types, and parsing utilities for the TUS resumable upload protocol
//! v1.0.0.
//!
//! Reference: <https://tus.io/protocols/resumable-upload>

use std::str::FromStr;

use axum::http::HeaderMap;
use http::HeaderValue;
use http::header::AsHeaderName;

use crate::http::RequestBuilder;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The TUS version is missing or does not match the server's version.
    #[error("expected Tus-Resumable: 1.0.0, got: {}", .0.as_deref().unwrap_or("(missing)"))]
    Version(Option<String>),
    /// The `Upload-Length` and `Upload-Defer-Length` headers are both missing, incorrect, or cannot be parsed.
    #[error(
        "expected Upload-Length or Upload-Defer-Length=1, got Upload-Length={upload_length:?}, Upload-Defer-Length={upload_defer_length:?}"
    )]
    UploadLength {
        upload_length: Option<usize>,
        upload_defer_length: Option<usize>,
    },
    /// The `Upload-Offset` header is missing or invalid
    #[error("expected Upload-Offset: 0, got: {0:?}")]
    UploadOffset(Option<usize>),
    /// The `Upload-Defer-Length` header is not allowed for external/untrusted requests.
    #[error("Upload-Defer-Length not allowed")]
    DeferLengthNotAllowed,
    /// The `Content-Type` header is not what TUS expects.
    #[error("expected Content-Type: application/offset+octet-stream, got: {}", .0.as_deref().unwrap_or("(missing)"))]
    ContentType(Option<String>),
}

/// TUS protocol header for the protocol version.
///
/// See <https://tus.io/protocols/resumable-upload#tus-version>.
pub const TUS_RESUMABLE: &str = "Tus-Resumable";

/// TUS protocol header for supported extensions.
///
/// See <https://tus.io/protocols/resumable-upload#tus-extension>.
const TUS_EXTENSION: &str = "Tus-Extension";

const SUPPORTED_EXTENSIONS: HeaderValue = HeaderValue::from_static("creation,creation-with-upload");

/// TUS protocol version supported by this endpoint.
pub const TUS_VERSION: HeaderValue = HeaderValue::from_static("1.0.0");

/// TUS protocol header for the total upload length.
///
/// See <https://tus.io/protocols/resumable-upload#upload-length>.
pub const UPLOAD_LENGTH: &str = "Upload-Length";

/// TUS protocol header for the deferred upload length.
///
/// See <https://tus.io/protocols/resumable-upload#upload-defer-length>.
pub const UPLOAD_DEFER_LENGTH: &str = "Upload-Defer-Length";

/// TUS protocol header for the current upload offset.
///
/// See <https://tus.io/protocols/resumable-upload#upload-offset>.
pub const UPLOAD_OFFSET: &str = "Upload-Offset";

/// Expected value of the content-type header.
pub const EXPECTED_CONTENT_TYPE: HeaderValue =
    HeaderValue::from_static("application/offset+octet-stream");

/// Validates TUS protocol headers and returns a subset of parsed values.
///
/// Returns the declared `Upload-Length`.
pub fn validate_post_headers(
    headers: &HeaderMap,
    allow_defer_length: bool,
) -> Result<Option<usize>, Error> {
    let tus_version = headers.get(TUS_RESUMABLE);
    if tus_version != Some(&TUS_VERSION) {
        return Err(Error::Version(
            tus_version.and_then(|v| v.to_str().ok()).map(str::to_owned),
        ));
    }

    if let Some(ct) = headers.get(http::header::CONTENT_TYPE) {
        return Err(Error::ContentType(ct.to_str().ok().map(String::from)));
    }

    let upload_length: Option<usize> = parse_header(headers, UPLOAD_LENGTH);
    let upload_defer_length: Option<usize> = parse_header(headers, UPLOAD_DEFER_LENGTH);

    // Exactly one of Upload-Length and Upload-Defer-Length must be present.
    // Upload-Defer-Length is only accepted if its value is 1 (as demanded by the TUS protocol)
    // and `allow_defer_length` is true (i.e. the sender is trusted/internal).
    let upload_length = match (upload_length, upload_defer_length, allow_defer_length) {
        (Some(u), None, _) => Ok(Some(u)),
        (None, Some(1), true) => Ok(None),
        (None, Some(1), false) => Err(Error::DeferLengthNotAllowed),
        _ => Err(Error::UploadLength {
            upload_length,
            upload_defer_length,
        }),
    }?;

    Ok(upload_length)
}

/// Validates TUS protocol headers and returns the expected upload length.
pub fn validate_patch_headers(headers: &HeaderMap) -> Result<(), Error> {
    let tus_version = headers.get(TUS_RESUMABLE);
    if tus_version != Some(&TUS_VERSION) {
        return Err(Error::Version(
            tus_version.and_then(|v| v.to_str().ok()).map(str::to_owned),
        ));
    }

    let content_type = headers.get(http::header::CONTENT_TYPE);
    if content_type != Some(&EXPECTED_CONTENT_TYPE) {
        return Err(Error::ContentType(
            content_type
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned),
        ));
    }

    let upload_offset: usize =
        parse_header(headers, UPLOAD_OFFSET).ok_or(Error::UploadOffset(None))?;
    if upload_offset != 0 {
        // Only allow full uploads for now.
        return Err(Error::UploadOffset(Some(upload_offset)));
    }

    Ok(())
}

/// Prepares the required TUS request headers for upstream requests.
pub fn add_creation_headers(upload_length: Option<usize>, builder: &mut RequestBuilder) {
    builder.header(TUS_RESUMABLE, TUS_VERSION);
    if let Some(upload_length) = upload_length {
        builder.header(UPLOAD_LENGTH, HeaderValue::from(upload_length));
    } else {
        builder.header(UPLOAD_DEFER_LENGTH, "1");
    }
}

/// Prepares the required TUS request headers for upstream requests.
pub fn add_upload_headers(builder: &mut RequestBuilder) {
    builder.header(TUS_RESUMABLE, TUS_VERSION);
    builder.header(http::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
    builder.header(UPLOAD_OFFSET, "0"); // always zero until we implement retries / chunking
}

/// Prepares the required TUS response headers.
pub fn response_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(TUS_RESUMABLE, TUS_VERSION);
    headers.insert(TUS_EXTENSION, SUPPORTED_EXTENSIONS);
    headers
}

fn parse_header<K: AsHeaderName, V: FromStr>(headers: &HeaderMap, header_name: K) -> Option<V> {
    headers.get(header_name)?.to_str().ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use http::{HeaderValue, header};

    use super::*;

    #[test]
    fn test_validate_tus_headers_missing_version() {
        let headers = HeaderMap::new();
        let result = validate_post_headers(&headers, false);
        assert!(matches!(result, Err(Error::Version(_))));
    }

    #[test]
    fn test_validate_tus_headers_missing_length() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        let result = validate_post_headers(&headers, false);
        assert!(matches!(result, Err(Error::UploadLength { .. })));
    }

    #[test]
    fn test_validate_tus_headers_invalid_with_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(hyper::header::CONTENT_LENGTH, 1024.into());
        headers.insert(hyper::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        let result = validate_post_headers(&headers, false);
        assert!(matches!(result, Err(Error::ContentType(_))));
    }

    #[test]
    fn test_validate_tus_headers_valid_empty() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        let result = validate_post_headers(&headers, false);
        assert_eq!(result.unwrap(), Some(1024));
    }

    #[test]
    fn test_validate_tus_headers_valid_chunked() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(
            hyper::header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        let result = validate_post_headers(&headers, false);
        assert_eq!(result.unwrap(), Some(1024));
    }

    #[test]
    fn test_validate_tus_headers_unsupported_version() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("0.2.0"));
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        let result = validate_post_headers(&headers, false);
        assert!(matches!(result, Err(Error::Version(_))));
    }

    #[test]
    fn test_validate_tus_headers_valid_deferred_length_from_trusted_source() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        headers.insert(UPLOAD_DEFER_LENGTH, HeaderValue::from_static("1"));
        let result = validate_post_headers(&headers, true);
        assert!(matches!(result, Ok(None)));
    }

    #[test]
    fn test_validate_tus_headers_valid_deferred_length_from_untrusted_source() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(UPLOAD_DEFER_LENGTH, HeaderValue::from_static("1"));
        let result = validate_post_headers(&headers, false);
        assert!(matches!(result, Err(Error::DeferLengthNotAllowed)));
    }

    #[test]
    fn test_validate_tus_headers_invalid_deferred_length() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(UPLOAD_DEFER_LENGTH, HeaderValue::from_static("2"));
        let result = validate_post_headers(&headers, true);
        assert!(matches!(result, Err(Error::UploadLength { .. })));
    }

    #[test]
    fn test_validate_patch_headers_missing_version() {
        let headers = HeaderMap::new();
        let result = validate_patch_headers(&headers);
        assert!(matches!(result, Err(Error::Version(_))));
    }

    #[test]
    fn test_validate_patch_headers_wrong_version() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("0.2.0"));
        headers.insert(http::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
        headers.insert(UPLOAD_OFFSET, HeaderValue::from_static("0"));
        let result = validate_patch_headers(&headers);
        assert!(matches!(result, Err(Error::Version(_))));
    }

    #[test]
    fn test_validate_patch_headers_missing_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(UPLOAD_OFFSET, HeaderValue::from_static("0"));
        let result = validate_patch_headers(&headers);
        assert!(matches!(result, Err(Error::ContentType(_))));
    }

    #[test]
    fn test_validate_patch_headers_wrong_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        headers.insert(UPLOAD_OFFSET, HeaderValue::from_static("0"));
        let result = validate_patch_headers(&headers);
        assert!(matches!(result, Err(Error::ContentType(_))));
    }

    #[test]
    fn test_validate_patch_headers_missing_upload_offset() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(http::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
        let result = validate_patch_headers(&headers);
        assert!(matches!(result, Err(Error::UploadOffset(None))));
    }

    #[test]
    fn test_validate_patch_headers_nonzero_upload_offset() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(http::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
        headers.insert(UPLOAD_OFFSET, HeaderValue::from_static("512"));
        let result = validate_patch_headers(&headers);
        assert!(matches!(result, Err(Error::UploadOffset(Some(512)))));
    }

    #[test]
    fn test_validate_patch_headers_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(http::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
        headers.insert(UPLOAD_OFFSET, HeaderValue::from_static("0"));
        let result = validate_patch_headers(&headers);
        assert!(result.is_ok());
    }
}
