use axum::body::Bytes;
use axum::extract::OptionalFromRequestParts;
use axum::http::HeaderMap;
use axum::http::request::Parts;
use relay_auth::PublicKey;
use relay_config::Credentials;
use std::collections::HashMap;
use std::convert::Infallible;
use std::str::FromStr;

pub const SIGNATURE_DATA_HEADER: &str = "x-sentry-signature-headers";
pub const SIGNATURE_VERSION_HEADER: &str = "x-sentry-relay-signature-version";
pub const SIGNATURE_HEADER: &str = "x-sentry-relay-signature";

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RelaySignatureError {
    #[error("missing header: {0}")]
    MissingHeader(String),
    #[error("malformed header: {0}")]
    MalformedHeader(String),
    #[error("missing relay signature")]
    MissingSignature,
    #[error("invalid relay signature")]
    InvalidSignature,
    #[error("invalid signature version")]
    InvalidSignatureVersion,
    #[error("no credentials provided")]
    MissingCredentials,
    #[error("missing body")]
    MissingBody,
}

/// Signature version that describes how the signature is constructed.
#[derive(Debug, Clone, PartialEq)]
pub enum RelaySignatureVersion {
    EnvelopeSignature,
    Signed,
}

impl RelaySignatureVersion {
    /// String representation of the version.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::EnvelopeSignature => "envelope-signature",
            Self::Signed => "singed",
        }
    }
}

impl FromStr for RelaySignatureVersion {
    type Err = RelaySignatureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "envelope-signature" => Ok(Self::EnvelopeSignature),
            "singed" => Ok(Self::Signed),
            _ => Err(RelaySignatureError::InvalidSignatureVersion),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RelaySignature {
    Valid(RelaySignatureData),
    Invalid(RelaySignatureError),
}

impl From<RelaySignatureError> for RelaySignature {
    fn from(value: RelaySignatureError) -> Self {
        Self::Invalid(value)
    }
}

impl<S> OptionalFromRequestParts<S> for RelaySignature
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Option<Self>, Self::Rejection> {
        match RelaySignatureData::from_request_data(&parts.headers, None) {
            Ok(data) => Ok(Some(RelaySignature::Valid(data))),
            Err(RelaySignatureError::MissingSignature) => Ok(None),
            Err(e) => Ok(Some(RelaySignature::Invalid(e))),
        }
    }
}

/// Types of signatures that are supported by Relay.
#[derive(Debug)]
pub enum TrySign {
    /// Bytes of an envelope body that need are used to produce a signature.
    Body(Bytes),
    /// Envelope Signature that is used to verify if the request is coming from
    /// a trusted relay.
    RelayEnvelopeSign(String),
}

impl TrySign {
    /// Creates a signature with necessary additional data and returns it as a map.
    ///
    /// It will create different data depending on the variant of [`TrySign`].
    /// The data is meant to be used in HTTP headers, that's why it comes as a map.
    pub fn create_signature(
        self,
        credentials: Option<&Credentials>,
    ) -> Result<HashMap<&'static str, String>, RelaySignatureError> {
        match self {
            TrySign::Body(data) => {
                let credentials = credentials.ok_or(RelaySignatureError::MissingCredentials)?;
                Ok(HashMap::from([(
                    "X-Sentry-Relay-Signature",
                    credentials.secret_key.sign(data.as_ref()),
                )]))
            }
            TrySign::RelayEnvelopeSign(now) => {
                let Some(credentials) = credentials else {
                    return Ok(HashMap::new());
                };
                let data = now.as_bytes();
                let signature = credentials.secret_key.sign(data);
                Ok(HashMap::from([
                    ("X-Sentry-Relay-Signature", signature),
                    (SIGNATURE_DATA_HEADER, "Date".to_owned()),
                    (SIGNATURE_VERSION_HEADER, "envelope-signature".to_owned()),
                    ("Date", now),
                ]))
            }
        }
    }
}

/// Contains data that is necessary for trusted relay signature verification.
///
#[derive(Debug, Clone, PartialEq)]
pub struct RelaySignatureData {
    /// The signature string from the header
    pub signature: String,
    /// The data that the signature was made from.
    pub signature_data: Bytes,
    /// The version for this signature.
    pub version: RelaySignatureVersion,
}

impl RelaySignatureData {
    pub fn from_request_data(
        headers: &HeaderMap,
        body: Option<Bytes>,
    ) -> Result<Self, RelaySignatureError> {
        let signature = headers
            .get(SIGNATURE_HEADER)
            .ok_or(RelaySignatureError::MissingSignature)?
            .to_str()
            .map_err(|_| RelaySignatureError::InvalidSignature)?;
        let version = get_header(headers, SIGNATURE_VERSION_HEADER)
            .unwrap_or(RelaySignatureVersion::Signed.as_str())
            .parse()
            .map_err(|_| RelaySignatureError::InvalidSignatureVersion)?;
        let signature_data = match version {
            RelaySignatureVersion::EnvelopeSignature => {
                let mut data = Vec::new();
                for header in get_header(headers, SIGNATURE_DATA_HEADER)?.split(";") {
                    let data_header = get_header(headers, header)?;
                    data.extend_from_slice(data_header.as_bytes());
                }
                Bytes::from(data)
            }
            RelaySignatureVersion::Signed => body.ok_or(RelaySignatureError::MissingBody)?,
        };
        Ok(RelaySignatureData {
            signature: signature.to_owned(),
            signature_data,
            version,
        })
    }

    pub fn verify(&self, public_key: &PublicKey) -> bool {
        public_key.verify(&self.signature_data, &self.signature)
    }
}

fn get_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, RelaySignatureError> {
    let header = headers
        .get(name)
        .ok_or(RelaySignatureError::MissingHeader(name.to_owned()))?;
    header
        .to_str()
        .map_err(|_| RelaySignatureError::MalformedHeader(name.to_owned()))
}

/// Returns `true` if the signature could be verified with any public key of a trusted relay.
///
/// If the signature is missing, then it will return `false`.
pub fn check_trusted_relay_signature(
    signature: &RelaySignatureData,
    trusted_relays: &[PublicKey],
) -> bool {
    trusted_relays
        .iter()
        .any(|public_key| signature.verify(public_key))
}
