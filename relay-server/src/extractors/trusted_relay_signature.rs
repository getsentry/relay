use crate::service::ServiceState;
use axum::extract::OptionalFromRequestParts;
use axum::http::HeaderMap;
use axum::http::request::Parts;
use std::convert::Infallible;
use std::str::FromStr;

pub const SIGNATURE_DATA_HEADER: &str = "x-sentry-signature-headers";
pub const SIGNATURE_VERSION_HEADER: &str = "x-sentry-relay-signature-version";
pub const SIGNATURE_HEADER: &str = "x-sentry-relay-signature";

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum TrustedRelaySignatureErrors {
    #[error("missing header: {0}")]
    MissingHeader(String),
    #[error("malformed header: {0}")]
    MalformedHeader(String),
    #[error("invalid signature version")]
    InvalidSignatureVersion,
}

/// Signature version that describes how the signature is constructed.
///
/// Defaults to `V1`.
#[derive(Debug, Default, Clone, PartialEq)]
pub enum TrustedRelaySignatureVersion {
    #[default]
    V1,
}

impl TrustedRelaySignatureVersion {
    /// String representation of the version.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::V1 => "v1",
        }
    }

    /// Headers used for the signature, separated by ';' (semicolon).
    ///
    /// The value is concatenated in the specified order to create the signature.
    /// For example: x-foo-header;x-bar-header;x-time-header
    pub fn signature_data_headers(&self) -> &'static str {
        match self {
            Self::V1 => "Date",
        }
    }
}

impl FromStr for TrustedRelaySignatureVersion {
    type Err = TrustedRelaySignatureErrors;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "v1" => Ok(Self::V1),
            _ => Err(TrustedRelaySignatureErrors::InvalidSignatureVersion),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RelaySignature {
    Valid(TrustedRelaySignature),
    Invalid(TrustedRelaySignatureErrors),
}

impl From<TrustedRelaySignatureErrors> for RelaySignature {
    fn from(value: TrustedRelaySignatureErrors) -> Self {
        Self::Invalid(value)
    }
}

/// Contains data that is necessary for trusted relay signature verification.
#[derive(Debug, Clone, PartialEq)]
pub struct TrustedRelaySignature {
    /// The signature string from the header
    pub signature: String,
    /// The data that the signature was made from.
    pub signature_data: Vec<u8>,
}

impl OptionalFromRequestParts<ServiceState> for RelaySignature {
    type Rejection = Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        _: &ServiceState,
    ) -> Result<Option<Self>, Self::Rejection> {
        let version = match parts.headers.get(SIGNATURE_VERSION_HEADER) {
            Some(h) => match h
                .to_str()
                .map_err(|_| TrustedRelaySignatureErrors::InvalidSignatureVersion)
                .and_then(|h| h.parse())
            {
                Ok(version) => version,
                Err(_) => {
                    return Ok(Some(RelaySignature::Invalid(
                        TrustedRelaySignatureErrors::InvalidSignatureVersion,
                    )));
                }
            },
            None => TrustedRelaySignatureVersion::default(),
        };
        let signature = match parts.headers.get(SIGNATURE_HEADER) {
            Some(h) => match h.to_str() {
                Ok(signature) => signature,
                Err(_) => {
                    return Ok(Some(RelaySignature::Invalid(
                        TrustedRelaySignatureErrors::MalformedHeader(SIGNATURE_HEADER.to_owned()),
                    )));
                }
            },
            None => return Ok(None),
        };

        let signature_data = match version {
            TrustedRelaySignatureVersion::V1 => {
                let mut data = Vec::new();
                let data_headers = match get_header(&parts.headers, SIGNATURE_DATA_HEADER) {
                    Ok(headers) => headers,
                    Err(e) => return Ok(Some(RelaySignature::Invalid(e))),
                };
                for header in data_headers.split(";") {
                    let data_header = match get_header(&parts.headers, header) {
                        Ok(header) => header,
                        Err(e) => return Ok(Some(RelaySignature::Invalid(e))),
                    };
                    data.extend_from_slice(data_header.as_bytes());
                }
                data
            }
        };

        Ok(Some(RelaySignature::Valid(TrustedRelaySignature {
            signature: signature.to_owned(),
            signature_data,
        })))
    }
}

fn get_header<'a>(
    headers: &'a HeaderMap,
    name: &str,
) -> Result<&'a str, TrustedRelaySignatureErrors> {
    let header = headers
        .get(name)
        .ok_or(TrustedRelaySignatureErrors::MissingHeader(name.to_owned()))?;
    header
        .to_str()
        .map_err(|_| TrustedRelaySignatureErrors::MalformedHeader(name.to_owned()))
}
