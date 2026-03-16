//! TUS protocol types and helpers.
//!
//! Contains constants, error types, and parsing utilities for the TUS resumable upload protocol
//! v1.0.0.
//!
//! Reference: <https://tus.io/protocols/resumable-upload>

use std::str::FromStr;

use axum::http::HeaderMap;
use http::header::AsHeaderName;
use http::{HeaderValue, header};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The TUS version is missing or does not match the server's version.
    #[error("Version Mismatch")]
    Version,
    /// The `Upload-Length` and `Upload-Defer-Length` headers are both missing, incorrect, or cannot be parsed.
    #[error("Invalid Upload-Length")]
    UploadLength,
    /// The `Upload-Offset` header is missing or invalid
    #[error("Invalid Upload-Offset: {0:?}")]
    UploadOffset(Option<usize>),
    /// The `Upload-Defer-Length` header is not allowed for external/untrusted requests.
    #[error("Upload-Defer-Length not allowed")]
    DeferLengthNotAllowed,
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

/// Parsed and validated header values.
#[derive(Debug, PartialEq, Eq)]
pub struct ParsedHeaders {
    /// Body type inferred from headers.
    pub body_type: BodyType,
    /// Value of the `Upload-Length` header, if given.
    pub upload_length: Option<usize>,
}

/// Body type inferred from `Content-Length` and `Transfer-Encoding` headers.
#[derive(Debug, PartialEq, Eq)]
pub enum BodyType {
    /// Content-Length: X
    Sized(usize),
    /// Transfer-Encoding: Chunked
    Chunked,
    /// An empty body.
    Empty,
}

impl BodyType {
    /// Whether the body has bytes to stream.
    pub fn has_bytes(&self) -> bool {
        !matches!(self, Self::Empty)
    }

    fn parse(headers: &HeaderMap) -> Result<Self, Error> {
        let content_length: Option<usize> = parse_header(headers, http::header::CONTENT_LENGTH);
        let transfer_encoding = headers
            .get(header::TRANSFER_ENCODING)
            .map(HeaderValue::as_bytes);

        match (content_length, transfer_encoding) {
            (Some(0) | None, None) => Ok(Self::Empty),
            (Some(size), None) => Ok(Self::Sized(size)),
            (None, Some(b"chunked")) => Ok(Self::Chunked),
            _ => return Err(Error::ContentType),
        }
    }
}

/// Validates TUS protocol headers and returns a subset of parsed values.
pub fn validate_post_headers(
    headers: &HeaderMap,
    allow_defer_length: bool,
) -> Result<ParsedHeaders, Error> {
    let tus_version = headers.get(TUS_RESUMABLE);
    if tus_version != Some(&TUS_VERSION) {
        return Err(Error::Version);
    }

    let body_type = BodyType::parse(headers)?;
    if body_type.has_bytes() {
        let content_type = headers.get(http::header::CONTENT_TYPE);
        if content_type != Some(&EXPECTED_CONTENT_TYPE) {
            return Err(Error::ContentType);
        }
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
        _ => Err(Error::UploadLength),
    }?;

    Ok(ParsedHeaders {
        body_type,
        upload_length,
    })
}

/// Validates TUS protocol headers and returns the expected upload length.
pub fn validate_patch_headers(headers: &HeaderMap) -> Result<(), Error> {
    let tus_version = headers.get(TUS_RESUMABLE);
    if tus_version != Some(&TUS_VERSION) {
        return Err(Error::Version);
    }

    let content_type = headers.get(http::header::CONTENT_TYPE);
    if content_type != Some(&EXPECTED_CONTENT_TYPE) {
        return Err(Error::ContentType);
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
pub fn request_headers(upload_length: Option<usize>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(TUS_RESUMABLE, TUS_VERSION);
    headers.insert(http::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
    if let Some(upload_length) = upload_length {
        headers.insert(UPLOAD_LENGTH, HeaderValue::from(upload_length));
    } else {
        headers.insert(UPLOAD_DEFER_LENGTH, HeaderValue::from(1));
    }
    headers
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
    use http::HeaderValue;

    use super::*;

    #[test]
    fn test_validate_tus_headers_missing_version() {
        let headers = HeaderMap::new();
        let result = validate_post_headers(&headers, false);
        assert!(matches!(result, Err(Error::Version)));
    }

    #[test]
    fn test_validate_tus_headers_missing_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("1024"));
        let result = validate_post_headers(&headers, false);
        assert!(matches!(result, Err(Error::ContentType)));
    }

    #[test]
    fn test_validate_tus_headers_missing_length() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(hyper::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
        let result = validate_post_headers(&headers, false);
        assert!(matches!(result, Err(Error::UploadLength)));
    }

    #[test]
    fn test_validate_tus_headers_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(hyper::header::CONTENT_LENGTH, 1024.into());
        headers.insert(hyper::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        let result = validate_post_headers(&headers, false);
        assert_eq!(
            result.unwrap(),
            ParsedHeaders {
                body_type: BodyType::Sized(1024),
                upload_length: Some(1024)
            }
        );
    }

    #[test]
    fn test_validate_tus_headers_valid_empty() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        let result = validate_post_headers(&headers, false);
        assert_eq!(
            result.unwrap(),
            ParsedHeaders {
                body_type: BodyType::Empty,
                upload_length: Some(1024)
            }
        );
    }

    #[test]
    fn test_validate_tus_headers_valid_chunked() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(
            hyper::header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        headers.insert(hyper::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        let result = validate_post_headers(&headers, false);
        assert_eq!(
            result.unwrap(),
            ParsedHeaders {
                body_type: BodyType::Chunked,
                upload_length: Some(1024)
            }
        );
    }

    #[test]
    fn test_validate_tus_headers_unsupported_version() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("0.2.0"));
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        let result = validate_post_headers(&headers, false);
        assert!(matches!(result, Err(Error::Version)));
    }

    #[test]
    fn test_validate_tus_headers_valid_deferred_length_from_trusted_source() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(hyper::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
        headers.insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        headers.insert(UPLOAD_DEFER_LENGTH, HeaderValue::from_static("1"));
        let result = validate_post_headers(&headers, true);
        assert!(matches!(
            result,
            Ok(ParsedHeaders {
                body_type: BodyType::Chunked,
                upload_length: None
            })
        ));
    }

    #[test]
    fn test_validate_tus_headers_valid_deferred_length_from_untrusted_source() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(hyper::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
        headers.insert(UPLOAD_DEFER_LENGTH, HeaderValue::from_static("1"));
        let result = validate_post_headers(&headers, false);
        assert!(matches!(result, Err(Error::DeferLengthNotAllowed)));
    }

    #[test]
    fn test_validate_tus_headers_invalid_deferred_length() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(hyper::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
        headers.insert(UPLOAD_DEFER_LENGTH, HeaderValue::from_static("2"));
        let result = validate_post_headers(&headers, true);
        assert!(matches!(result, Err(Error::UploadLength)));
    }
}
