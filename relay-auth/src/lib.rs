//! Authentication and crypto for Relay.
//!
//! This library contains the [`PublicKey`] and [`SecretKey`] types, which can be used to validate
//! and sign traffic between Relays in authenticated endpoints. Additionally, Relays identify via a
//! [`RelayId`], which is included in the request signature and headers.
//!
//! Relay uses Ed25519 at the moment. This is considered an implementation detail and is subject to
//! change at any time. Do not rely on a specific signing mechanism.
//!
//! # Generating Credentials
//!
//! Use the [`generate_relay_id`] and [`generate_key_pair`] function to generate credentials:
//!
//! ```
//! let relay_id = relay_auth::generate_relay_id();
//! let (private_key, public_key) = relay_auth::generate_key_pair();
//! ```

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use std::fmt;
use std::fmt::Display;
use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use data_encoding::BASE64URL_NOPAD;
use ed25519_dalek::{Digest, DigestSigner, DigestVerifier, Signer, Verifier};
use hmac::{Hmac, Mac};
use rand::rngs::OsRng;
use rand::{RngCore as _, TryRngCore as _};
use relay_common::time::UnixTimestamp;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::Sha512;
use uuid::Uuid;

include!(concat!(env!("OUT_DIR"), "/constants.gen.rs"));

/// The latest Relay version known to this Relay. This is the current version.
const LATEST_VERSION: RelayVersion = RelayVersion::new(VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH);

/// The oldest downstream Relay version still supported by this Relay.
const OLDEST_VERSION: RelayVersion = RelayVersion::new(0, 0, 0); // support all

/// Alias for Relay IDs (UUIDs).
pub type RelayId = Uuid;

/// The version of a Relay.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct RelayVersion {
    major: u8,
    minor: u8,
    patch: u8,
}

impl RelayVersion {
    /// Returns the current Relay version.
    pub fn current() -> Self {
        LATEST_VERSION
    }

    /// Returns the oldest compatible Relay version.
    ///
    /// Relays older than this cannot authenticate with this Relay. It is possible for newer Relays
    /// to authenticate.
    pub fn oldest() -> Self {
        OLDEST_VERSION
    }

    /// Creates a new version with the given components.
    pub const fn new(major: u8, minor: u8, patch: u8) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Returns `true` if this version is still supported.
    pub fn supported(self) -> bool {
        self >= Self::oldest()
    }

    /// Returns `true` if this version is older than the current version.
    pub fn outdated(self) -> bool {
        self < Self::current()
    }
}

impl fmt::Display for RelayVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Raised if Relay cannot parse the provided version.
#[derive(Clone, Copy, Debug, Default, thiserror::Error)]
#[error("invalid relay version string")]
pub struct ParseRelayVersionError;

impl FromStr for RelayVersion {
    type Err = ParseRelayVersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut iter = s
            .split(&['.', '-'][..])
            .map(|s| s.parse().map_err(|_| ParseRelayVersionError));

        let major = iter.next().ok_or(ParseRelayVersionError)??;
        let minor = iter.next().ok_or(ParseRelayVersionError)??;
        let patch = iter.next().ok_or(ParseRelayVersionError)??;

        Ok(Self::new(major, minor, patch))
    }
}

relay_common::impl_str_serde!(RelayVersion, "a version string");

/// Raised if a key could not be parsed.
#[derive(Debug, Eq, Hash, PartialEq, thiserror::Error)]
pub enum KeyParseError {
    /// Invalid key encoding.
    #[error("bad key encoding")]
    BadEncoding,
    /// Invalid key data.
    #[error("bad key data")]
    BadKey,
}

/// Raised to indicate failure on unpacking.
#[derive(Debug, thiserror::Error)]
pub enum UnpackError {
    /// Raised if the signature is invalid.
    #[error("invalid signature on data")]
    BadSignature,
    /// Invalid key encoding.
    #[error("bad key encoding")]
    BadEncoding,
    /// Raised if deserializing of data failed.
    #[error("could not deserialize payload")]
    BadPayload(#[source] serde_json::Error),
    /// Raised on unpacking if the data is too old.
    #[error("signature is too old")]
    SignatureExpired,
}

/// Used to tell which algorithm was used for signature creation.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SignatureAlgorithm {
    /// Regular signature creation which clones the data internally.
    #[serde(rename = "v0")]
    Regular,
    /// Pre-hashed signature which allows incremental hashing.
    #[serde(rename = "v1")]
    Prehashed,
}

/// A wrapper around packed data that adds a timestamp.
///
/// This is internally automatically used when data is signed.
#[derive(Serialize, Deserialize, Debug)]
pub struct SignatureHeader {
    /// The timestamp of when the data was packed and signed.
    #[serde(rename = "t", skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<DateTime<Utc>>,

    /// Represents how this signature was created and how it needs to be verified.
    ///
    /// Defaults to [`SignatureAlgorithm::Regular`] because that was used before the introduction
    /// of this field.
    #[serde(rename = "a", skip_serializing_if = "Option::is_none")]
    pub signature_algorithm: Option<SignatureAlgorithm>,
}

impl SignatureHeader {
    /// Checks if the signature expired.
    pub fn expired(&self, max_age: Duration) -> bool {
        if let Some(ts) = self.timestamp {
            ts < (Utc::now() - max_age)
        } else {
            false
        }
    }
}

impl Default for SignatureHeader {
    fn default() -> SignatureHeader {
        SignatureHeader {
            timestamp: Some(Utc::now()),
            signature_algorithm: None,
        }
    }
}

/// Represents the secret key of an Relay.
///
/// Secret keys are based on ed25519 but this should be considered an
/// implementation detail for now.  We only ever represent public keys
/// on the wire as opaque ascii encoded strings of arbitrary format or length.
#[derive(Clone)]
pub struct SecretKey {
    inner: ed25519_dalek::SigningKey,
}

/// Represents the final registration.
#[derive(Serialize, Deserialize, Debug)]
pub struct Registration {
    relay_id: RelayId,
}

/// Creates a digest for signature verification/signing.
fn create_digest(header: &[u8], data: &[u8]) -> Sha512 {
    let mut digest = Sha512::default();
    digest.update(header);
    digest.update(b"\x00");
    digest.update(data);
    digest
}

impl SecretKey {
    /// Signs some data with the secret key and returns the signature.
    ///
    /// This is will sign with the default header.
    pub fn sign(&self, data: &[u8]) -> Signature {
        self.sign_with_header(data, &SignatureHeader::default())
    }

    /// Signs some data with the secret key and a specific header and
    /// then returns the signature.
    ///
    /// The default behavior is to attach the timestamp in the header to the
    /// signature so that old signatures on verification can be rejected.
    pub fn sign_with_header(&self, data: &[u8], sig_header: &SignatureHeader) -> Signature {
        let mut header =
            serde_json::to_vec(&sig_header).expect("attempted to pack non json safe header");
        let header_encoded = BASE64URL_NOPAD.encode(&header);
        let sig = match sig_header
            .signature_algorithm
            .unwrap_or(SignatureAlgorithm::Regular)
        {
            SignatureAlgorithm::Regular => {
                header.push(b'\x00');
                header.extend_from_slice(data);
                self.inner.sign(&header)
            }
            SignatureAlgorithm::Prehashed => {
                let digest = create_digest(&header, data);
                self.inner.sign_digest(digest)
            }
        };

        let mut sig_encoded = BASE64URL_NOPAD.encode(&sig.to_bytes());
        sig_encoded.push('.');
        sig_encoded.push_str(&header_encoded);
        Signature(sig_encoded)
    }

    /// Packs some serializable data into JSON and signs it with the default header.
    pub fn pack<S: Serialize>(&self, data: S) -> (Vec<u8>, Signature) {
        self.pack_with_header(data, &SignatureHeader::default())
    }

    /// Packs some serializable data into JSON and signs it with the specified header.
    pub fn pack_with_header<S: Serialize>(
        &self,
        data: S,
        header: &SignatureHeader,
    ) -> (Vec<u8>, Signature) {
        // this can only fail if we deal with badly formed data.  In that case we
        // consider that a panic.  Should not happen.
        let json = serde_json::to_vec(&data).expect("attempted to pack non json safe data");
        let sig = self.sign_with_header(&json, header);
        (json, sig)
    }
}

impl PartialEq for SecretKey {
    fn eq(&self, other: &SecretKey) -> bool {
        self.inner.to_keypair_bytes() == other.inner.to_keypair_bytes()
    }
}

impl Eq for SecretKey {}

impl FromStr for SecretKey {
    type Err = KeyParseError;

    fn from_str(s: &str) -> Result<SecretKey, KeyParseError> {
        let bytes = match BASE64URL_NOPAD.decode(s.as_bytes()) {
            Ok(bytes) => bytes,
            _ => return Err(KeyParseError::BadEncoding),
        };

        let inner = if let Ok(keypair) = bytes.as_slice().try_into() {
            ed25519_dalek::SigningKey::from_keypair_bytes(&keypair)
                .map_err(|_| KeyParseError::BadKey)?
        } else if let Ok(secret_key) = bytes.try_into() {
            ed25519_dalek::SigningKey::from_bytes(&secret_key)
        } else {
            return Err(KeyParseError::BadKey);
        };

        Ok(SecretKey { inner })
    }
}

impl fmt::Display for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(
                f,
                "{}",
                BASE64URL_NOPAD.encode(&self.inner.to_keypair_bytes())
            )
        } else {
            write!(f, "{}", BASE64URL_NOPAD.encode(&self.inner.to_bytes()))
        }
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SecretKey(\"{self}\")")
    }
}

relay_common::impl_str_serde!(SecretKey, "a secret key");

/// Represents the public key of a Relay.
///
/// Public keys are based on ed25519 but this should be considered an
/// implementation detail for now.  We only ever represent public keys
/// on the wire as opaque ascii encoded strings of arbitrary format or length.
#[derive(Clone, Eq, PartialEq)]
pub struct PublicKey {
    inner: ed25519_dalek::VerifyingKey,
}

impl PublicKey {
    /// Verifies the signature and returns the embedded signature
    /// header.
    pub fn verify_meta(&self, data: &[u8], sig: SignatureRef<'_>) -> Option<SignatureHeader> {
        let mut iter = sig.0.splitn(2, '.');
        let sig_bytes = match iter.next() {
            Some(sig_encoded) => BASE64URL_NOPAD.decode(sig_encoded.as_bytes()).ok()?,
            None => return None,
        };
        let sig = ed25519_dalek::Signature::from_slice(&sig_bytes).ok()?;

        let header = match iter.next() {
            Some(header_encoded) => BASE64URL_NOPAD.decode(header_encoded.as_bytes()).ok()?,
            None => return None,
        };
        let parsed: SignatureHeader = serde_json::from_slice(&header).ok()?;

        let verification_result = match parsed
            .signature_algorithm
            .unwrap_or(SignatureAlgorithm::Regular)
        {
            SignatureAlgorithm::Regular => {
                let mut to_verify = header.clone();
                to_verify.push(b'\x00');
                to_verify.extend_from_slice(data);
                self.inner.verify(&to_verify, &sig)
            }
            SignatureAlgorithm::Prehashed => {
                let digest = create_digest(&header, data);
                self.inner.verify_digest(digest, &sig)
            }
        };
        if verification_result.is_ok() {
            Some(parsed)
        } else {
            None
        }
    }

    /// Verifies a signature but discards the header.
    pub fn verify(&self, data: &[u8], sig: SignatureRef<'_>) -> bool {
        self.verify_meta(data, sig).is_some()
    }

    /// Verifies a signature and checks the timestamp.
    pub fn verify_timestamp(
        &self,
        data: &[u8],
        sig: SignatureRef<'_>,
        max_age: Option<Duration>,
    ) -> bool {
        self.verify_meta(data, sig)
            .map(|header| max_age.is_none() || !header.expired(max_age.unwrap()))
            .unwrap_or(false)
    }

    /// Unpacks signed data and returns it with header.
    pub fn unpack_meta<D: DeserializeOwned>(
        &self,
        data: &[u8],
        signature: SignatureRef<'_>,
    ) -> Result<(SignatureHeader, D), UnpackError> {
        if let Some(header) = self.verify_meta(data, signature) {
            serde_json::from_slice(data)
                .map(|data| (header, data))
                .map_err(UnpackError::BadPayload)
        } else {
            Err(UnpackError::BadSignature)
        }
    }

    /// Unpacks the data and verifies that it's not too old, then
    /// throws away the wrapper.
    ///
    /// If no `max_age` is set, the embedded timestamp does not get validated.
    pub fn unpack<D: DeserializeOwned>(
        &self,
        data: &[u8],
        signature: SignatureRef<'_>,
        max_age: Option<Duration>,
    ) -> Result<D, UnpackError> {
        let (header, data) = self.unpack_meta(data, signature)?;
        if max_age.is_none() || !header.expired(max_age.unwrap()) {
            Ok(data)
        } else {
            Err(UnpackError::SignatureExpired)
        }
    }
}

impl FromStr for PublicKey {
    type Err = KeyParseError;

    fn from_str(s: &str) -> Result<PublicKey, KeyParseError> {
        let Ok(bytes) = BASE64URL_NOPAD.decode(s.as_bytes()) else {
            return Err(KeyParseError::BadEncoding);
        };

        let inner = match bytes.try_into() {
            Ok(bytes) => ed25519_dalek::VerifyingKey::from_bytes(&bytes)
                .map_err(|_| KeyParseError::BadKey)?,
            Err(_) => return Err(KeyParseError::BadKey),
        };

        Ok(PublicKey { inner })
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", BASE64URL_NOPAD.encode(&self.inner.to_bytes()))
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PublicKey(\"{self}\")")
    }
}

relay_common::impl_str_serde!(PublicKey, "a public key");

/// Generates an Relay ID.
pub fn generate_relay_id() -> RelayId {
    Uuid::new_v4()
}

/// Generates a secret + public key pair.
pub fn generate_key_pair() -> (SecretKey, PublicKey) {
    let mut csprng = OsRng;
    let mut secret = [0; 32];
    csprng
        .try_fill_bytes(&mut secret)
        .expect("os rng should be available");
    let kp = ed25519_dalek::SigningKey::from_bytes(&secret);
    let pk = kp.verifying_key();
    (SecretKey { inner: kp }, PublicKey { inner: pk })
}

/// An encoded and signed `RegisterState`.
///
/// This signature can be used by the upstream server to ensure that the downstream client did not
/// tamper with the token without keeping state between requests. For more information, see
/// `RegisterState`.
///
/// The format and contents of `SignedRegisterState` are intentionally opaque. Downstream clients
/// do not need to interpret it, and the upstream can change its contents at any time. Parsing and
/// validation is only performed on the upstream.
///
/// In the current implementation, the serialized state has the format `{state}:{signature}`, where
/// each component is:
///  - `state`: A URL-safe base64 encoding of the JSON serialized `RegisterState`.
///  - `signature`: A URL-safe base64 encoding of the SHA512 HMAC of the encoded state.
///
/// To create a signed state, use `RegisterChallenge::sign`. To validate the signature and read
/// the state, use `SignedRegisterChallenge::unpack`. In both cases, a secret for signing has to be
/// supplied.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SignedRegisterState(String);

impl SignedRegisterState {
    /// Creates an Hmac instance for signing the `RegisterState`.
    fn mac(secret: &[u8]) -> Hmac<Sha512> {
        Hmac::new_from_slice(secret).expect("HMAC takes variable keys")
    }

    /// Signs the given `RegisterState` and serializes it into a single string.
    fn sign(state: RegisterState, secret: &[u8]) -> Self {
        let json = serde_json::to_string(&state).expect("relay register state serializes to JSON");
        let token = BASE64URL_NOPAD.encode(json.as_bytes());

        let mut mac = Self::mac(secret);
        mac.update(token.as_bytes());
        let signature = BASE64URL_NOPAD.encode(&mac.finalize().into_bytes());

        Self(format!("{token}:{signature}"))
    }

    /// Splits the signed state into the encoded state and encoded signature.
    fn split(&self) -> (&str, &str) {
        let mut split = self.as_str().splitn(2, ':');
        (split.next().unwrap_or(""), split.next().unwrap_or(""))
    }

    /// Returns the string representation of the token.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Unpacks the encoded state and validates the signature.
    ///
    /// If `max_age` is specified, then the timestamp in the state is validated against the current
    /// time stamp. If the stored timestamp is too old, `UnpackError::SignatureExpired` is returned.
    pub fn unpack(
        &self,
        secret: &[u8],
        max_age: Option<Duration>,
    ) -> Result<RegisterState, UnpackError> {
        let (token, signature) = self.split();
        let code = BASE64URL_NOPAD
            .decode(signature.as_bytes())
            .map_err(|_| UnpackError::BadEncoding)?;

        let mut mac = Self::mac(secret);
        mac.update(token.as_bytes());
        mac.verify_slice(&code)
            .map_err(|_| UnpackError::BadSignature)?;

        let json = BASE64URL_NOPAD
            .decode(token.as_bytes())
            .map_err(|_| UnpackError::BadEncoding)?;
        let state =
            serde_json::from_slice::<RegisterState>(&json).map_err(UnpackError::BadPayload)?;

        if let Some(max_age) = max_age {
            let secs = state.timestamp().as_secs() as i64;
            if secs + max_age.num_seconds() < Utc::now().timestamp() {
                return Err(UnpackError::SignatureExpired);
            }
        }

        Ok(state)
    }
}

impl fmt::Display for SignedRegisterState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

/// A state structure containing relevant information from `RegisterRequest`.
///
/// This structure is used to carry over information between the downstream register request and
/// register response. In addition to identifying information, it contains a random bit to avoid
/// replay attacks.
#[derive(Clone, Deserialize, Serialize)]
pub struct RegisterState {
    timestamp: UnixTimestamp,
    relay_id: RelayId,
    public_key: PublicKey,
    rand: String,
}

impl RegisterState {
    /// Returns the timestamp at which the challenge was created.
    pub fn timestamp(&self) -> UnixTimestamp {
        self.timestamp
    }

    /// Returns the identifier of the requesting downstream Relay.
    pub fn relay_id(&self) -> RelayId {
        self.relay_id
    }

    /// Returns the public key of the requesting downstream Relay.
    pub fn public_key(&self) -> &PublicKey {
        &self.public_key
    }
}

/// Generates a new random token for the register state.
fn nonce() -> String {
    let mut rng = rand::rng();
    let mut bytes = vec![0u8; 64];
    rng.fill_bytes(&mut bytes);
    BASE64URL_NOPAD.encode(&bytes)
}

/// Represents a request for registration with the upstream.
///
/// This is created if the Relay signs in for the first time.  The server needs
/// to respond to this request with a unique token that is then used to sign
/// the response.
#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterRequest {
    relay_id: RelayId,
    public_key: PublicKey,
    #[serde(default)]
    version: RelayVersion,
}

impl RegisterRequest {
    /// Creates a new request to register an Relay upstream.
    pub fn new(relay_id: &RelayId, public_key: &PublicKey) -> RegisterRequest {
        RegisterRequest {
            relay_id: *relay_id,
            public_key: public_key.clone(),
            version: RelayVersion::current(),
        }
    }

    /// Unpacks a signed register request for bootstrapping.
    ///
    /// This unpacks the embedded public key first, then verifies if the
    /// self signature was made by that public key.  If all is well then
    /// the data is returned.
    pub fn bootstrap_unpack(
        data: &[u8],
        signature: SignatureRef<'_>,
        max_age: Option<Duration>,
    ) -> Result<RegisterRequest, UnpackError> {
        let req: RegisterRequest = serde_json::from_slice(data).map_err(UnpackError::BadPayload)?;
        let pk = req.public_key();
        pk.unpack(data, signature, max_age)
    }

    /// Returns the Relay ID of the registering Relay.
    pub fn relay_id(&self) -> RelayId {
        self.relay_id
    }

    /// Returns the new public key of registering Relay.
    pub fn public_key(&self) -> &PublicKey {
        &self.public_key
    }

    /// Creates a register challenge for this request.
    pub fn into_challenge(self, secret: &[u8]) -> RegisterChallenge {
        let state = RegisterState {
            timestamp: UnixTimestamp::now(),
            relay_id: self.relay_id,
            public_key: self.public_key,
            rand: nonce(),
        };

        RegisterChallenge {
            relay_id: self.relay_id,
            token: SignedRegisterState::sign(state, secret),
        }
    }
}

/// Represents the response the server is supposed to send to a register request.
#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterChallenge {
    relay_id: RelayId,
    token: SignedRegisterState,
}

impl RegisterChallenge {
    /// Returns the Relay ID of the registering Relay.
    pub fn relay_id(&self) -> &RelayId {
        &self.relay_id
    }

    /// Returns the token that needs signing.
    pub fn token(&self) -> &str {
        self.token.as_str()
    }

    /// Creates a register response.
    pub fn into_response(self) -> RegisterResponse {
        RegisterResponse {
            relay_id: self.relay_id,
            token: self.token,
            version: RelayVersion::current(),
        }
    }
}

/// Represents a response to a register challenge.
///
/// The response contains the same data as the register challenge. By signing this payload
/// successfully, this Relay authenticates with the upstream.
#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterResponse {
    relay_id: RelayId,
    token: SignedRegisterState,
    #[serde(default)]
    version: RelayVersion,
}

impl RegisterResponse {
    /// Unpacks the register response and validates signatures.
    pub fn unpack(
        data: &[u8],
        signature: SignatureRef<'_>,
        secret: &[u8],
        max_age: Option<Duration>,
    ) -> Result<(Self, RegisterState), UnpackError> {
        let response: Self = serde_json::from_slice(data).map_err(UnpackError::BadPayload)?;
        let state = response.token.unpack(secret, max_age)?;

        if let Some(header) = state.public_key().verify_meta(data, signature) {
            if max_age.is_some_and(|m| header.expired(m)) {
                return Err(UnpackError::SignatureExpired);
            }
        } else {
            return Err(UnpackError::BadSignature);
        }

        Ok((response, state))
    }

    /// Returns the Relay ID of the registering Relay.
    pub fn relay_id(&self) -> RelayId {
        self.relay_id
    }

    /// Returns the token that needs signing.
    pub fn token(&self) -> &str {
        self.token.as_str()
    }

    /// Returns the version of the registering Relay.
    pub fn version(&self) -> RelayVersion {
        self.version
    }
}

/// A wrapper around a String that represents a signature.
#[derive(Debug, Clone, PartialEq)]
pub struct Signature(pub String);

impl Display for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Signature {
    /// Verifies the signature against any of the provided public keys.
    ///
    /// Returns `true` if the signature is valid with one of the given
    /// public keys and satisfies the timestamp constraints defined by `start_time`
    /// and `max_age`.
    pub fn verify_any(
        &self,
        public_key: &[PublicKey],
        start_time: DateTime<Utc>,
        max_age: Duration,
    ) -> bool {
        public_key
            .iter()
            .any(|p| self.verify(p, start_time, max_age))
    }

    /// Verifies the signature using the specified public key.
    ///
    /// The signature is considered valid if it can be verified using the given
    /// public key and its embedded timestamp falls within the valid time range,
    /// starting from `start_time` and not exceeding `max_age`.
    pub fn verify(
        &self,
        public_key: &PublicKey,
        start_time: DateTime<Utc>,
        max_age: Duration,
    ) -> bool {
        let Some(header) = public_key.verify_meta(&[], self.as_signature_ref()) else {
            return false;
        };
        let Some(timestamp) = header.timestamp else {
            return false;
        };
        let elapsed = start_time - timestamp;
        elapsed >= Duration::zero() && elapsed <= max_age
    }

    /// Verifies the signature against the given data and public key.
    ///
    /// Returns `true` if the signature is valid for the provided `data`
    /// when verified with the given public key.
    pub fn verify_bytes(&self, data: &[u8], public_key: &PublicKey) -> bool {
        public_key
            .verify_meta(data, self.as_signature_ref())
            .is_some()
    }

    /// Returns a borrowed view of the signature as a `SignatureRef`.
    ///
    /// This method provides a lightweight reference wrapper over the internal
    /// signature data.
    pub fn as_signature_ref(&self) -> SignatureRef<'_> {
        SignatureRef(self.0.as_str())
    }
}

/// A borrowed reference to a signature string used for validation.
///
/// `SignatureRef` provides a view into the signature data as a string slice,
/// allowing verification to work with borrowed data without unnecessary allocations.
/// This type is typically obtained by borrowing from an owned [`Signature`].
pub struct SignatureRef<'a>(pub &'a str);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keys() {
        let sk: SecretKey =
        "OvXFVm1tIUi8xDTuyHX1SSqdMc8nCt2qU9IUaH5p7oUk5pHZsdnfXNiMWiMLtSE86J3N9Peo5CBP1YQHDUkApQ"
            .parse()
            .unwrap();
        let pk: PublicKey = "JOaR2bHZ31zYjFojC7UhPOidzfT3qOQgT9WEBw1JAKU"
            .parse()
            .unwrap();

        assert_eq!(
            sk.to_string(),
            "OvXFVm1tIUi8xDTuyHX1SSqdMc8nCt2qU9IUaH5p7oU"
        );
        assert_eq!(
            format!("{sk:#}"),
            "OvXFVm1tIUi8xDTuyHX1SSqdMc8nCt2qU9IUaH5p7oUk5pHZsdnfXNiMWiMLtSE86J3N9Peo5CBP1YQHDUkApQ"
        );
        assert_eq!(
            pk.to_string(),
            "JOaR2bHZ31zYjFojC7UhPOidzfT3qOQgT9WEBw1JAKU"
        );

        assert_eq!(
            "bad data".parse::<SecretKey>(),
            Err(KeyParseError::BadEncoding)
        );
        assert_eq!("OvXF".parse::<SecretKey>(), Err(KeyParseError::BadKey));

        assert_eq!(
            "bad data".parse::<PublicKey>(),
            Err(KeyParseError::BadEncoding)
        );
        assert_eq!("OvXF".parse::<PublicKey>(), Err(KeyParseError::BadKey));
    }

    #[test]
    fn test_serializing() {
        let sk: SecretKey =
        "OvXFVm1tIUi8xDTuyHX1SSqdMc8nCt2qU9IUaH5p7oUk5pHZsdnfXNiMWiMLtSE86J3N9Peo5CBP1YQHDUkApQ"
            .parse()
            .unwrap();
        let pk: PublicKey = "JOaR2bHZ31zYjFojC7UhPOidzfT3qOQgT9WEBw1JAKU"
            .parse()
            .unwrap();

        let sk_json = serde_json::to_string(&sk).unwrap();
        assert_eq!(sk_json, "\"OvXFVm1tIUi8xDTuyHX1SSqdMc8nCt2qU9IUaH5p7oU\"");

        let pk_json = serde_json::to_string(&pk).unwrap();
        assert_eq!(pk_json, "\"JOaR2bHZ31zYjFojC7UhPOidzfT3qOQgT9WEBw1JAKU\"");

        assert_eq!(serde_json::from_str::<SecretKey>(&sk_json).unwrap(), sk);
        assert_eq!(serde_json::from_str::<PublicKey>(&pk_json).unwrap(), pk);
    }

    #[test]
    fn test_signatures() {
        let (sk, pk) = generate_key_pair();
        let data = b"Hello World!";

        let sig = sk.sign(data);
        assert!(pk.verify(data, sig.as_signature_ref()));

        let bad_sig = "jgubwSf2wb2wuiRpgt2H9_bdDSMr88hXLp5zVuhbr65EGkSxOfT5ILIWr623twLgLd0bDgHg6xzOaUCX7XvUCw";
        assert!(!pk.verify(data, SignatureRef(bad_sig)));
    }

    #[test]
    fn test_registration() {
        let max_age = Duration::minutes(15);

        // initial setup
        let relay_id = generate_relay_id();
        let (sk, pk) = generate_key_pair();

        // create a register request
        let request = RegisterRequest::new(&relay_id, &pk);

        // sign it
        let (request_bytes, request_sig) = sk.pack(request);

        // attempt to get the data through bootstrap unpacking.
        let request = RegisterRequest::bootstrap_unpack(
            &request_bytes,
            request_sig.as_signature_ref(),
            Some(max_age),
        )
        .unwrap();
        assert_eq!(request.relay_id(), relay_id);
        assert_eq!(request.public_key(), &pk);

        let upstream_secret = b"secret";

        // create a challenge
        let challenge = request.into_challenge(upstream_secret);
        let challenge_token = challenge.token().to_owned();
        assert_eq!(challenge.relay_id(), &relay_id);
        assert!(challenge.token().len() > 40);

        // check the challenge contains the expected info
        let state = SignedRegisterState(challenge_token.clone());
        let register_state = state.unpack(upstream_secret, None).unwrap();
        assert_eq!(register_state.public_key, pk);
        assert_eq!(register_state.relay_id, relay_id);

        // create a response from the challenge
        let response = challenge.into_response();

        // sign and unsign it
        let (response_bytes, response_sig) = sk.pack(response);
        let (response, _) = RegisterResponse::unpack(
            &response_bytes,
            response_sig.as_signature_ref(),
            upstream_secret,
            Some(max_age),
        )
        .unwrap();

        assert_eq!(response.relay_id(), relay_id);
        assert_eq!(response.token(), challenge_token);
        assert_eq!(response.version, LATEST_VERSION);
    }

    /// This is a pseudo-test to easily generate the strings used by test_auth.py
    /// You can copy the output to the top of the test_auth.py when there are changes in the
    /// exchanged authentication structures.
    /// It follows test_registration but instead of asserting it prints the strings
    #[test]
    #[allow(clippy::print_stdout, reason = "helper test to generate output")]
    fn test_generate_strings_for_test_auth_py() {
        let max_age = Duration::minutes(15);
        println!("Generating test data for test_auth.py...");

        // initial setup
        let relay_id = generate_relay_id();
        println!("RELAY_ID = b\"{relay_id}\"");
        let (sk, pk) = generate_key_pair();
        println!("RELAY_KEY = b\"{pk}\"");

        // create a register request
        let request = RegisterRequest::new(&relay_id, &pk);
        println!("REQUEST = b'{}'", serde_json::to_string(&request).unwrap());

        // sign it
        let (request_bytes, request_sig) = sk.pack(&request);
        println!("REQUEST_SIG = \"{request_sig}\"");

        // attempt to get the data through bootstrap unpacking.
        let request = RegisterRequest::bootstrap_unpack(
            &request_bytes,
            request_sig.as_signature_ref(),
            Some(max_age),
        )
        .unwrap();

        let upstream_secret = b"secret";

        // create a challenge
        let challenge = request.into_challenge(upstream_secret);
        let challenge_token = challenge.token().to_owned();
        println!("TOKEN = \"{challenge_token}\"");

        // create a response from the challenge
        let response = challenge.into_response();
        let serialized_response = serde_json::to_string(&response).unwrap();
        let (_, response_sig) = sk.pack(&response);

        println!("RESPONSE = b'{serialized_response}'");
        println!("RESPONSE_SIG = \"{response_sig}\"");

        println!("RELAY_VERSION = \"{LATEST_VERSION}\"");
    }

    /// Test we can still deserialize an old response that does not contain the version
    #[test]
    fn test_deserialize_old_response() {
        let serialized_challenge = "{\"relay_id\":\"6b7d15b8-cee2-4354-9fee-dae7ef43e434\",\"token\":\"eyJ0aW1lc3RhbXAiOjE1OTg5Njc0MzQsInJlbGF5X2lkIjoiNmI3ZDE1YjgtY2VlMi00MzU0LTlmZWUtZGFlN2VmNDNlNDM0IiwicHVibGljX2tleSI6ImtNcEdieWRIWlN2b2h6ZU1sZ2hjV3dIZDhNa3JlS0d6bF9uY2RrWlNPTWciLCJyYW5kIjoiLUViNG9Hal80dUZYOUNRRzFBVmdqTjRmdGxaNU9DSFlNOFl2d1podmlyVXhUY0tFSWYtQzhHaldsZmgwQTNlMzYxWE01dVh0RHhvN00tbWhZeXpWUWcifQ:KJUDXlwvibKNQmex-_Cu1U0FArlmoDkyqP7bYIDGrLXudfjGfCjH-UjNsUHWVDnbM28YdQ-R2MBSyF51aRLQcw\"}";
        let result: RegisterResponse = serde_json::from_str(serialized_challenge).unwrap();
        assert_eq!(
            result.relay_id,
            Uuid::parse_str("6b7d15b8-cee2-4354-9fee-dae7ef43e434").unwrap()
        )
    }

    #[test]
    fn test_relay_version_current() {
        assert_eq!(
            env!("CARGO_PKG_VERSION"),
            RelayVersion::current().to_string()
        );
    }

    #[test]
    fn test_relay_version_oldest() {
        // Regression test against unintentional changes.
        assert_eq!("0.0.0", RelayVersion::oldest().to_string());
    }

    #[test]
    fn test_relay_version_parse() {
        assert_eq!(
            RelayVersion::new(20, 7, 0),
            "20.7.0-beta.0".parse().unwrap()
        );
    }

    #[test]
    fn test_relay_version_oldest_supported() {
        assert!(RelayVersion::oldest().supported());
    }

    #[test]
    fn test_relay_version_any_supported() {
        // Every version must be supported at the moment.
        // This test can be changed when dropping support for older versions.
        assert!(RelayVersion::default().supported());
    }

    #[test]
    fn test_relay_version_from_str() {
        assert_eq!(RelayVersion::new(20, 7, 0), "20.7.0".parse().unwrap());
    }

    #[test]
    fn test_verify_any() {
        let pair1 = generate_key_pair();
        let pair2 = generate_key_pair();
        let pair3 = generate_key_pair();

        let signature = pair3.0.sign(&[]);
        assert!(signature.verify_any(
            &[pair1.1, pair2.1, pair3.1],
            Utc::now(),
            Duration::seconds(10)
        ));
    }

    #[test]
    fn test_verify_max_age() {
        let pair = generate_key_pair();
        let signature = pair.0.sign(&[]);
        let start_time = Utc::now();
        // The signature is valid in general
        assert!(signature.verify(&pair.1, start_time, Duration::seconds(10)));
        // Signature is no longer valid because too much time elapsed
        assert!(!signature.verify(
            &pair.1,
            start_time - Duration::seconds(1),
            Duration::milliseconds(500)
        ))
    }

    #[test]
    fn test_verify_any_max_age() {
        let start_time = Utc::now();
        let pair1 = generate_key_pair();
        let pair2 = generate_key_pair();
        let pair3 = generate_key_pair();

        let header = SignatureHeader {
            timestamp: Some(start_time),
            signature_algorithm: Some(SignatureAlgorithm::Regular),
        };
        let signature = pair3.0.sign_with_header(&[], &header);

        let public_keys = &[pair1.1, pair2.1, pair3.1];

        // Signature still valid after 1 second
        assert!(signature.verify_any(
            public_keys,
            start_time + Duration::seconds(1),
            Duration::seconds(2)
        ));
        // Signature is no longer valid because too much time elapsed
        assert!(!signature.verify_any(
            public_keys,
            start_time + Duration::seconds(3),
            Duration::seconds(2)
        ))
    }

    #[test]
    fn test_regular_algorithm() {
        let (secret, public) = generate_key_pair();
        let signature = secret.sign(&[]);
        assert!(signature.verify(&public, Utc::now(), Duration::seconds(10)));
    }

    #[test]
    fn test_prehashed_algorithm() {
        let (secret, public) = generate_key_pair();
        let header = SignatureHeader {
            timestamp: Some(Utc::now()),
            signature_algorithm: Some(SignatureAlgorithm::Prehashed),
        };
        let signature = secret.sign_with_header(&[], &header);
        assert!(signature.verify(&public, Utc::now(), Duration::seconds(10)));
    }

    #[test]
    fn test_legacy_signature_can_be_verified() {
        // TestHeader struct is used to mimic old version that do not have
        // the `signature_variant` fields.
        #[derive(Serialize)]
        struct TestHeader {
            #[serde(rename = "t")]
            timestamp: Option<DateTime<Utc>>,
        }
        let header = serde_json::to_string(&TestHeader {
            timestamp: Some(Utc::now()),
        })
        .unwrap();

        let data: &[u8] = &[];
        let (secret, public) = generate_key_pair();
        let mut to_sign = header.clone().into_bytes();
        to_sign.push(b'\x00');
        to_sign.extend_from_slice(data);
        let sig = secret.inner.sign(to_sign.as_slice());
        let mut sig_encoded = BASE64URL_NOPAD.encode(sig.to_bytes().as_slice());
        sig_encoded.push('.');
        sig_encoded.push_str(BASE64URL_NOPAD.encode(header.as_bytes()).as_str());

        assert!(public.verify(data, SignatureRef(sig_encoded.as_str())));
    }
}
