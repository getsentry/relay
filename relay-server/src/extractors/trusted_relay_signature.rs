use axum::http::HeaderMap;
use std::str::FromStr;

pub const SIGNATURE_DATA_HEADER: &str = "x-sentry-signature-headers";
pub const SIGNATURE_VERSION_HEADER: &str = "x-sentry-relay-signature-version";
pub const SIGNATURE_HEADER: &str = "x-sentry-relay-signature";
pub const SIGNATURE_DATETIME_HEADER: &str = "x-sentry-relay-signature-date";

#[derive(Debug, Clone, thiserror::Error)]
pub enum TrustedRelaySignatureErrors {
    #[error("missing header: {0}")]
    MissingHeader(String),
    #[error("malformed header: {0}")]
    MalformedHeader(String),
    #[error("invalid signature version")]
    InvalidSignatureVersion,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TrustedRelaySignatureVersion {
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
            Self::V1 => SIGNATURE_DATETIME_HEADER,
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
pub struct TrustedRelaySignature {
    pub signature: String,
    pub signature_data: Vec<u8>,
    pub version: TrustedRelaySignatureVersion,
}

impl TrustedRelaySignature {
    pub fn from_headers(headers: &HeaderMap) -> Result<Self, TrustedRelaySignatureErrors> {
        let version = get_header(headers, SIGNATURE_VERSION_HEADER)?
            .parse()
            .map_err(|_| TrustedRelaySignatureErrors::InvalidSignatureVersion)?;
        let signature = get_header(headers, SIGNATURE_HEADER)?;

        let signature_data = match version {
            TrustedRelaySignatureVersion::V1 => {
                let mut data = Vec::new();
                let data_headers = get_header(headers, SIGNATURE_DATA_HEADER)?;
                for header in data_headers.split(";") {
                    let data_header = get_header(headers, header)?;
                    data.extend_from_slice(data_header.as_bytes());
                }
                data
            }
        };

        Ok(TrustedRelaySignature {
            signature: signature.to_owned(),
            version,
            signature_data,
        })
    }
}

fn get_header<'a>(
    headers: &'a HeaderMap,
    name: &str,
) -> Result<&'a str, TrustedRelaySignatureErrors> {
    let header = headers
        .get(name)
        .ok_or(TrustedRelaySignatureErrors::MissingHeader(name.to_string()))?;
    header
        .to_str()
        .map_err(|_| TrustedRelaySignatureErrors::MalformedHeader(name.to_string()))
}
