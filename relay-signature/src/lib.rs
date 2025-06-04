//! Relay module to create and verify signatures between Relays.
//!
//! Relays can have public and private key pairs which are used to sign requests and
//! add the resulting signature to an envelope header.
//!
//!
//!
use axum::body::Bytes;
use axum::extract::OptionalFromRequestParts;
use axum::http::HeaderMap;
use axum::http::request::Parts;
use relay_auth::PublicKey;
use relay_config::Credentials;
use std::collections::HashMap;
use std::convert::Infallible;
use std::str::FromStr;

/// Errors that can happen during the signature or verification process.
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

/// Signature version that describes what data the signature contains.
#[derive(Debug, Clone, PartialEq)]
pub enum RelaySignatureVersion {
    /// Signs a datetime with a Relay private key which the receiving Relay
    /// can verify with a stored public key to make sure the sending Relay is trusted.
    EnvelopeSignature,
    /// Used for authentication challenge when a relay registers itself upstream.
    Signed,
}

impl RelaySignatureVersion {
    /// String representation of the version.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::EnvelopeSignature => "envelope-signature",
            Self::Signed => "signed",
        }
    }
}

impl FromStr for RelaySignatureVersion {
    type Err = RelaySignatureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "envelope-signature" => Ok(Self::EnvelopeSignature),
            "signed" => Ok(Self::Signed),
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
        match RelaySignatureData::from_request_data(&parts.headers, Bytes::new()) {
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
    /// Used to verify if an envelope comes from a trusted relay.
    RelayEnvelopeSign,
}

impl TrySign {
    /// Creates a signature with necessary additional data and returns it as a map.
    ///
    /// The result contains all information necessary to reconstruct and verify the signature
    /// except the public key.
    ///
    /// This method can only fail if credentials are missing for a signature that is mandatory.
    pub fn create_signature_headers(
        self,
        credentials: Option<&Credentials>,
    ) -> Result<HashMap<&'static str, String>, RelaySignatureError> {
        match self {
            TrySign::Body(data) => {
                let credentials = credentials.ok_or(RelaySignatureError::MissingCredentials)?;
                let signature = credentials.secret_key.sign(data.as_ref());
                Ok(HashMap::from([
                    ("x-sentry-relay-signature", signature),
                    ("x-sentry-relay-signature-version", "signed".to_owned()),
                ]))
            }
            TrySign::RelayEnvelopeSign => {
                let Some(credentials) = credentials else {
                    return Ok(HashMap::new());
                };
                let signature = credentials.secret_key.sign(&[]);
                Ok(HashMap::from([
                    ("x-sentry-relay-signature", signature),
                    (
                        "x-sentry-relay-signature-version",
                        "envelope-signature".to_owned(),
                    ),
                ]))
            }
        }
    }
}

/// Contains information necessary to verify the correctness of the signature.
#[derive(Debug, Clone, PartialEq)]
pub struct RelaySignatureData {
    /// The signature string from the header.
    pub signature: String,
    /// The data that the signature was made from.
    pub signature_data: Bytes,
    /// The version for this signature.
    pub version: RelaySignatureVersion,
}

impl RelaySignatureData {
    /// Create [`RelaySignatureData`] based on request information.
    ///
    /// Depending on the signature version a body can be mandatory and will
    /// produce an error if it's missing.
    pub fn from_request_data(
        headers: &HeaderMap,
        data: Bytes,
    ) -> Result<Self, RelaySignatureError> {
        let signature = headers
            .get("x-sentry-relay-signature")
            .ok_or(RelaySignatureError::MissingSignature)?
            .to_str()
            .map_err(|_| RelaySignatureError::InvalidSignature)?;
        // Defaults to the authentication challenge signature version
        // if no explicit version is provided.
        let version: RelaySignatureVersion =
            get_header(headers, "x-sentry-relay-signature-version")
                .unwrap_or(RelaySignatureVersion::Signed.as_str())
                .parse()
                .map_err(|_| RelaySignatureError::InvalidSignatureVersion)?;
        Ok(RelaySignatureData {
            signature: signature.to_owned(),
            signature_data: data,
            version,
        })
    }

    /// Verifies the signature against a single public key.
    pub fn verify(&self, public_key: &PublicKey) -> bool {
        public_key.verify_timestamp(&self.signature_data, &self.signature, None)
    }

    /// Verifies the signature against any of the public keys and returns `true` if
    /// one of them can verify it successfully.
    pub fn verify_any(&self, public_keys: &[PublicKey]) -> bool {
        public_keys.iter().any(|public_key| self.verify(public_key))
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
