//! Relay module to create and verify signatures between Relays.
//!
//! Relays can have public and private key pairs which are used to sign requests and
//! add the resulting signature to an envelope header.
//!
//! It exposes different ways to create a signature through [`TrySign`] which uses
//! different data to create the signature.
//!
//! Provides [`RelaySignature`] which provides information if the signature was valid or not
//! if no immediate action wants to be done on an invalid signature.
//!
//! Contains a [`RelaySignatureData`] container which wraps the signature with the data
//! that the signature should be validated against if delayed verification is required.
//! It also provides a mechanism to verify a signature against multiple [`PublicKey`] and
//! will succeed if any of them successfully verifies.
//!
//! For ad-hoc verification it's also possible to use [`PublicKey::verify`] from
//! `relay-auth`, which is also what is called internally in [`RelaySignatureData::verify`].
//!
use axum::body::Bytes;
use chrono::Utc;
use relay_auth::{PublicKey, SignatureHeader};
use relay_config::Credentials;

/// Errors during the signature or verification process.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RelaySignatureError {
    #[error("invalid relay signature")]
    InvalidSignature,
    #[error("missing credentials")]
    MissingCredentials,
}

/// Signature within relay which can be valid or invalid.
///
/// Invalid does not refer to a failed verification but rather to a signature
/// that is not well formatted.
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

/// Types of signatures that are supported by Relay.
#[derive(Debug)]
pub enum TrySign {
    /// Bytes of an envelope body that need are used to produce a signature.
    Body(Bytes),
    /// No data is needed for this signature because we only want to see if
    /// the receiving relay can verify the signature correctly.
    RelayEnvelopeSign,
}

impl TrySign {
    /// Creates a signature based on the variant.
    ///
    /// Some variants might not produce a signature if credentials are missing, in which case
    /// `None` is returned.
    ///
    /// If credentials are mandatory, it will return `RelaySignatureError::MissingCredentials`.
    pub fn create_signature(
        self,
        credentials: Option<&Credentials>,
    ) -> Result<Option<String>, RelaySignatureError> {
        match self {
            TrySign::Body(data) => {
                let credentials = credentials.ok_or(RelaySignatureError::MissingCredentials)?;
                Ok(Some(credentials.secret_key.sign(data.as_ref())))
            }
            TrySign::RelayEnvelopeSign => {
                let Some(credentials) = credentials else {
                    return Ok(None);
                };
                let header = SignatureHeader {
                    timestamp: Some(Utc::now()),
                };
                Ok(Some(credentials.secret_key.sign_with_header(&[], &header)))
            }
        }
    }
}

/// Contains information necessary to verify the correctness of the signature.
#[derive(Debug, Clone, PartialEq)]
pub struct RelaySignatureData {
    /// The signature string.
    pub signature: String,
    /// The data which the signature will be checked against.
    pub signature_data: Bytes,
}

impl RelaySignatureData {
    /// Creates a new [`RelaySignatureData`] container.
    pub fn new(signature: String, signature_data: Bytes) -> Self {
        Self {
            signature,
            signature_data,
        }
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
