//! TUS protocol types and helpers.
//!
//! Contains constants, error types, and parsing utilities for the TUS resumable upload protocol
//! v1.0.0.
//!
//! Reference: <https://tus.io/protocols/resumable-upload>

use std::str::FromStr;

use axum::http::HeaderMap;
use data_encoding::BASE64;
use http::HeaderValue;
use http::header::AsHeaderName;
use serde::{Deserialize, Serialize};

use crate::envelope::AttachmentType;
use crate::http::{HttpError, RequestBuilder};

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
    #[error("expected Upload-Offset >= 0")]
    UploadOffset,
    /// The `Content-Type` header is not what TUS expects.
    #[error("expected Content-Type: {expected}, got: {received}")]
    ContentType {
        expected: &'static str,
        received: String,
    },
    /// The `sentry` entry in `Upload-Metadata` is not valid base64.
    #[error("invalid base64 in `sentry` Upload-Metadata value: {0}")]
    InvalidMetadataBase64(#[from] data_encoding::DecodeError),
    /// The decoded `sentry` entry in `Upload-Metadata` is not valid JSON or does not match the schema.
    #[error("invalid `sentry` Upload-Metadata payload: {0}")]
    InvalidMetadata(#[from] serde_json::Error),
}

/// TUS protocol header for the protocol version.
///
/// See <https://tus.io/protocols/resumable-upload#tus-version>.
pub const TUS_RESUMABLE: &str = "Tus-Resumable";

/// TUS protocol header for supported extensions.
///
/// See <https://tus.io/protocols/resumable-upload#tus-extension>.
const TUS_EXTENSION: &str = "Tus-Extension";

const SUPPORTED_EXTENSIONS: HeaderValue = HeaderValue::from_static("creation");

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

/// TUS protocol header for metadata.
///
/// See <https://tus.io/protocols/resumable-upload#upload-metadata>.
pub const UPLOAD_METADATA: &str = "Upload-Metadata";

/// Expected value of the content-type header.
pub const EXPECTED_CONTENT_TYPE: HeaderValue = HeaderValue::from_static(EXPECTED_CONTENT_TYPE_STR);

const EXPECTED_CONTENT_TYPE_STR: &str = "application/offset+octet-stream";

/// Sentry-specific metadata extracted from the TUS `Upload-Metadata` header.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Metadata {
    /// The [`AttachmentType`] of the upload.
    ///
    /// Used for preliminary rate-limiting checks.
    #[serde(default)]
    pub attachment_type: AttachmentType,
}

/// Subset of TUS request headers.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Headers {
    /// The declared `Upload-Length`, or `None` if `Upload-Defer-Length: 1` was used.
    pub upload_length: Option<usize>,
    /// Sentry-specific `Upload-Metadata` entry.
    pub metadata: Option<Metadata>,
}

/// Validates TUS protocol headers and returns a subset of parsed values.
pub fn validate_post_headers(headers: &HeaderMap) -> Result<Headers, Error> {
    let tus_version = headers.get(TUS_RESUMABLE);
    if tus_version != Some(&TUS_VERSION) {
        return Err(Error::Version(
            tus_version.and_then(|v| v.to_str().ok()).map(str::to_owned),
        ));
    }

    if let Some(ct) = headers.get(http::header::CONTENT_TYPE) {
        return Err(Error::ContentType {
            expected: "none",
            received: ct.to_str().unwrap_or("").to_owned(),
        });
    }

    let upload_length: Option<usize> = parse_header(headers, UPLOAD_LENGTH);
    let upload_defer_length: Option<usize> = parse_header(headers, UPLOAD_DEFER_LENGTH);

    // Exactly one of Upload-Length and Upload-Defer-Length must be present.
    // Upload-Defer-Length is only accepted if its value is 1 (as demanded by the TUS protocol).
    let upload_length = match (upload_length, upload_defer_length) {
        (Some(u), None) => Ok(Some(u)),
        (None, Some(1)) => Ok(None),
        _ => Err(Error::UploadLength {
            upload_length,
            upload_defer_length,
        }),
    }?;

    let metadata = upload_metadata(headers)?;

    Ok(Headers {
        upload_length,
        metadata,
    })
}

/// Validates TUS protocol headers and returns the expected upload length.
///
/// Returns the offset from which the upload is resumed.
pub fn validate_patch_headers(headers: &HeaderMap) -> Result<usize, Error> {
    let tus_version = headers.get(TUS_RESUMABLE);
    if tus_version != Some(&TUS_VERSION) {
        return Err(Error::Version(
            tus_version.and_then(|v| v.to_str().ok()).map(str::to_owned),
        ));
    }

    let content_type = headers.get(http::header::CONTENT_TYPE);
    if content_type != Some(&EXPECTED_CONTENT_TYPE) {
        return Err(Error::ContentType {
            expected: EXPECTED_CONTENT_TYPE_STR,
            received: content_type
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned(),
        });
    }

    let upload_offset: usize = parse_header(headers, UPLOAD_OFFSET).ok_or(Error::UploadOffset)?;

    Ok(upload_offset)
}

/// Prepares the required TUS request headers for upstream requests.
pub fn add_creation_headers(
    upload_length: Option<usize>,
    attachment_type: Option<AttachmentType>,
    builder: &mut RequestBuilder,
) -> Result<(), HttpError> {
    if let Some(attachment_type) = attachment_type {
        let json = serde_json::to_vec(&Metadata { attachment_type })?;
        let header = HeaderValue::from_str(&format!("sentry {}", BASE64.encode(&json)))?;
        builder.header(UPLOAD_METADATA, header);
    }

    builder.header(TUS_RESUMABLE, TUS_VERSION);
    if let Some(upload_length) = upload_length {
        builder.header(UPLOAD_LENGTH, HeaderValue::from(upload_length));
    } else {
        builder.header(UPLOAD_DEFER_LENGTH, "1");
    };

    Ok(())
}

/// Prepares the required TUS request headers for upstream requests.
pub fn add_upload_headers(builder: &mut RequestBuilder, offset: usize) {
    builder.header(TUS_RESUMABLE, TUS_VERSION);
    builder.header(http::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
    builder.header(UPLOAD_OFFSET, offset.to_string());
}

/// Prepares the required TUS response headers.
pub fn response_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(TUS_RESUMABLE, TUS_VERSION);
    headers.insert(TUS_EXTENSION, SUPPORTED_EXTENSIONS);
    headers
}

/// Extracts the sentry metadata payload from the `Upload-Metadata` header.
fn upload_metadata(headers: &HeaderMap) -> Result<Option<Metadata>, Error> {
    let Some(payload) = parse_upload_metadata(headers) else {
        return Ok(None);
    };
    Some(parse_sentry_metadata(&payload)).transpose()
}

/// Extracts the raw (base64) sentry payload from the `Upload-Metadata` header.
fn parse_upload_metadata(headers: &HeaderMap) -> Option<String> {
    let metadata: String = parse_header(headers, UPLOAD_METADATA)?;
    metadata.split(',').find_map(|kv| match kv.split_once(' ') {
        Some(("sentry", value)) => Some(value.to_owned()),
        _ => None,
    })
}

/// Decodes and deserializes the sentry metadata payload.
fn parse_sentry_metadata(payload: &str) -> Result<Metadata, Error> {
    let decoded = BASE64.decode(payload.as_bytes())?;
    Ok(serde_json::from_slice(&decoded)?)
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
        let result = validate_post_headers(&headers);
        assert!(matches!(result, Err(Error::Version(_))));
    }

    #[test]
    fn test_validate_tus_headers_missing_length() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        let result = validate_post_headers(&headers);
        assert!(matches!(result, Err(Error::UploadLength { .. })));
    }

    #[test]
    fn test_validate_tus_headers_invalid_with_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(hyper::header::CONTENT_LENGTH, 1024.into());
        headers.insert(hyper::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        let result = validate_post_headers(&headers);
        assert!(matches!(result, Err(Error::ContentType { .. })));
    }

    #[test]
    fn test_validate_tus_headers_valid_empty() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        let result = validate_post_headers(&headers);
        assert_eq!(
            result.unwrap(),
            Headers {
                upload_length: Some(1024),
                metadata: None
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
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        let result = validate_post_headers(&headers);
        assert!(matches!(
            result,
            Ok(Headers {
                upload_length: Some(1024),
                metadata: None
            })
        ));
    }

    #[test]
    fn test_validate_tus_headers_unsupported_version() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("0.2.0"));
        headers.insert(UPLOAD_LENGTH, HeaderValue::from_static("1024"));
        let result = validate_post_headers(&headers);
        assert!(matches!(result, Err(Error::Version(_))));
    }

    #[test]
    fn test_validate_tus_headers_valid_deferred_length() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        headers.insert(UPLOAD_DEFER_LENGTH, HeaderValue::from_static("1"));
        let result = validate_post_headers(&headers);
        assert!(matches!(
            result,
            Ok(Headers {
                upload_length: None,
                metadata: None
            })
        ));
    }

    #[test]
    fn test_validate_tus_headers_invalid_deferred_length() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(UPLOAD_DEFER_LENGTH, HeaderValue::from_static("2"));
        let result = validate_post_headers(&headers);
        assert!(matches!(result, Err(Error::UploadLength { .. })));
    }

    #[test]
    fn test_validate_tus_headers_upload_meta() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(UPLOAD_DEFER_LENGTH, HeaderValue::from_static("1"));
        headers.insert(
            UPLOAD_METADATA,
            HeaderValue::from_static(
                "filename d29ybGRfZG9taW5hdGlvbl9wbGFuLnBkZg==,is_confidential,sentry eyJhdHRhY2htZW50X3R5cGUiOiAiZXZlbnQubWluaWR1bXAiLCAgInBhcmVudF90eXBlIjogImVycm9yIn0=",
            ),
        );
        let result = validate_post_headers(&headers);
        assert!(matches!(
            result,
            Ok(Headers {
                upload_length: None,
                metadata: Some(Metadata {
                    attachment_type: AttachmentType::Minidump
                })
            })
        ));
    }

    #[test]
    fn test_parse_sentry_metadata_valid() {
        let payload = "eyJhdHRhY2htZW50X3R5cGUiOiAiZXZlbnQubWluaWR1bXAifQ==";
        assert_eq!(
            parse_sentry_metadata(payload).unwrap(),
            Metadata {
                attachment_type: AttachmentType::Minidump
            }
        );
    }

    #[test]
    fn test_parse_sentry_metadata_invalid_base64() {
        assert!(matches!(
            parse_sentry_metadata("e="),
            Err(Error::InvalidMetadataBase64(_))
        ));
    }

    #[test]
    fn test_parse_sentry_metadata_invalid_json() {
        assert!(matches!(
            parse_sentry_metadata("ew=="),
            Err(Error::InvalidMetadata(_))
        ));
    }

    #[test]
    fn test_parse_sentry_metadata_default() {
        assert!(matches!(
            parse_sentry_metadata("e30="),
            Ok(Metadata {
                attachment_type: AttachmentType::Attachment
            })
        ));
    }

    #[test]
    fn test_upload_metadata_absent() {
        assert_eq!(upload_metadata(&HeaderMap::new()).unwrap(), None);
    }

    #[test]
    fn test_upload_metadata_no_sentry_entry() {
        let mut headers = HeaderMap::new();
        headers.insert(
            UPLOAD_METADATA,
            HeaderValue::from_static(
                "filename d29ybGRfZG9taW5hdGlvbl9wbGFuLnBkZg==,is_confidential",
            ),
        );
        assert_eq!(upload_metadata(&headers).unwrap(), None);
    }

    #[test]
    fn test_upload_metadata_with_sentry_entry() {
        let mut headers = HeaderMap::new();
        headers.insert(
            UPLOAD_METADATA,
            HeaderValue::from_static(
                "filename d29ybGRfZG9taW5hdGlvbl9wbGFuLnBkZg==,sentry eyJhdHRhY2htZW50X3R5cGUiOiAiZXZlbnQubWluaWR1bXAifQ==",
            ),
    );
        assert_eq!(
            upload_metadata(&headers).unwrap(),
            Some(Metadata {
                attachment_type: AttachmentType::Minidump
            })
        );
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
        assert!(matches!(result, Err(Error::ContentType { .. })));
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
        assert!(matches!(result, Err(Error::ContentType { .. })));
    }

    #[test]
    fn test_validate_patch_headers_missing_upload_offset() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(http::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
        let result = validate_patch_headers(&headers);
        assert!(matches!(result, Err(Error::UploadOffset)));
    }

    #[test]
    fn test_validate_patch_headers_noninteger_upload_offset() {
        let mut headers = HeaderMap::new();
        headers.insert(TUS_RESUMABLE, HeaderValue::from_static("1.0.0"));
        headers.insert(http::header::CONTENT_TYPE, EXPECTED_CONTENT_TYPE);
        headers.insert(UPLOAD_OFFSET, HeaderValue::from_static("adsf"));
        let result = validate_patch_headers(&headers);
        assert!(matches!(result, Err(Error::UploadOffset)));
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
