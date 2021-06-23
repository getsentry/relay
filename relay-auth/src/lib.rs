#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Duration, TimeZone, Utc};
use failure::Fail;
use hmac::{Hmac, Mac};
use rand::{rngs::OsRng, thread_rng, RngCore};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::Sha512;

use relay_common::{UnixTimestamp, Uuid};

include!(concat!(env!("OUT_DIR"), "/constants.gen.rs"));

/// The latest Relay version known to this Relay. This is the current version.
const LATEST_VERSION: RelayVersion = RelayVersion::new(VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH);

/// The oldest downstream Relay version still supported by this Relay.
const OLDEST_VERSION: RelayVersion = RelayVersion::new(0, 0, 0); // support all

/// Alias for relay IDs (UUIDs).
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

#[derive(Clone, Copy, Debug, Default, Fail)]
#[fail(display = "hello")]
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
#[derive(Debug, Fail, PartialEq, Eq, Hash)]
pub enum KeyParseError {
    /// Invalid key encoding.
    #[fail(display = "bad key encoding")]
    BadEncoding,
    /// Invalid key data.
    #[fail(display = "bad key data")]
    BadKey,
}

/// Raised to indicate failure on unpacking.
#[derive(Debug, Fail)]
pub enum UnpackError {
    /// Raised if the signature is invalid.
    #[fail(display = "invalid signature on data")]
    BadSignature,
    /// Invalid key encoding.
    #[fail(display = "bad key encoding")]
    BadEncoding,
    /// Raised if deserializing of data failed.
    #[fail(display = "could not deserialize payload")]
    BadPayload(#[cause] serde_json::Error),
    /// Raised on unpacking if the data is too old.
    #[fail(display = "signature is too old")]
    SignatureExpired,
}

/// A wrapper around packed data that adds a timestamp.
///
/// This is internally automatically used when data is signed.
#[derive(Serialize, Deserialize, Debug)]
pub struct SignatureHeader {
    /// The timestamp of when the data was packed and signed.
    #[serde(rename = "t", skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<DateTime<Utc>>,
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
        }
    }
}

/// Represents the public key of an relay.
///
/// Public keys are based on ed25519 but this should be considered an
/// implementation detail for now.  We only ever represent public keys
/// on the wire as opaque ascii encoded strings of arbitrary format or length.
#[derive(Clone)]
pub struct PublicKey {
    inner: ed25519_dalek::PublicKey,
}

/// Represents the secret key of an relay.
///
/// Secret keys are based on ed25519 but this should be considered an
/// implementation detail for now.  We only ever represent public keys
/// on the wire as opaque ascii encoded strings of arbitrary format or length.
pub struct SecretKey {
    inner: ed25519_dalek::Keypair,
}

impl Clone for SecretKey {
    fn clone(&self) -> SecretKey {
        SecretKey {
            inner: ed25519_dalek::Keypair::from_bytes(&self.inner.to_bytes()[..]).unwrap(),
        }
    }
}

/// Represents the final registration.
#[derive(Serialize, Deserialize, Debug)]
pub struct Registration {
    relay_id: RelayId,
}

impl SecretKey {
    /// Signs some data with the secret key and returns the signature.
    ///
    /// This is will sign with the default header.
    pub fn sign(&self, data: &[u8]) -> String {
        self.sign_with_header(data, &SignatureHeader::default())
    }

    /// Signs some data with the seret key and a specific header and
    /// then returns the signature.
    ///
    /// The default behavior is to attach the timestamp in the header to the
    /// signature so that old signatures on verification can be rejected.
    pub fn sign_with_header(&self, data: &[u8], header: &SignatureHeader) -> String {
        let mut header =
            serde_json::to_vec(&header).expect("attempted to pack non json safe header");
        let header_encoded = base64::encode_config(&header[..], base64::URL_SAFE_NO_PAD);
        header.push(b'\x00');
        header.extend_from_slice(data);
        let sig = self.inner.sign::<Sha512>(&header);
        let mut sig_encoded = base64::encode_config(&sig.to_bytes()[..], base64::URL_SAFE_NO_PAD);
        sig_encoded.push('.');
        sig_encoded.push_str(&header_encoded);
        sig_encoded
    }

    /// Packs some serializable data into JSON and signs it with the default header.
    pub fn pack<S: Serialize>(&self, data: S) -> (Vec<u8>, String) {
        self.pack_with_header(data, &SignatureHeader::default())
    }

    /// Packs some serializable data into JSON and signs it with the specified header.
    pub fn pack_with_header<S: Serialize>(
        &self,
        data: S,
        header: &SignatureHeader,
    ) -> (Vec<u8>, String) {
        // this can only fail if we deal with badly formed data.  In that case we
        // consider that a panic.  Should not happen.
        let json = serde_json::to_vec(&data).expect("attempted to pack non json safe data");
        let sig = self.sign_with_header(&json, &header);
        (json, sig)
    }
}

impl PartialEq for SecretKey {
    fn eq(&self, other: &SecretKey) -> bool {
        self.inner.to_bytes()[..] == other.inner.to_bytes()[..]
    }
}

impl Eq for SecretKey {}

impl FromStr for SecretKey {
    type Err = KeyParseError;

    fn from_str(s: &str) -> Result<SecretKey, KeyParseError> {
        let bytes = match base64::decode_config(s, base64::URL_SAFE_NO_PAD) {
            Ok(bytes) => bytes,
            _ => return Err(KeyParseError::BadEncoding),
        };

        Ok(SecretKey {
            inner: if bytes.len() == 64 {
                ed25519_dalek::Keypair::from_bytes(&bytes).map_err(|_| KeyParseError::BadKey)?
            } else {
                let secret = ed25519_dalek::SecretKey::from_bytes(&bytes)
                    .map_err(|_| KeyParseError::BadKey)?;
                let public = ed25519_dalek::PublicKey::from_secret::<Sha512>(&secret);
                ed25519_dalek::Keypair { secret, public }
            },
        })
    }
}

impl fmt::Display for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(
                f,
                "{}",
                base64::encode_config(&self.inner.to_bytes()[..], base64::URL_SAFE_NO_PAD)
            )
        } else {
            write!(
                f,
                "{}",
                base64::encode_config(&self.inner.secret.to_bytes()[..], base64::URL_SAFE_NO_PAD)
            )
        }
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SecretKey(\"{}\")", self)
    }
}

relay_common::impl_str_serde!(SecretKey, "a secret key");

impl PublicKey {
    /// Verifies the signature and returns the embedded signature
    /// header.
    pub fn verify_meta(&self, data: &[u8], sig: &str) -> Option<SignatureHeader> {
        let mut iter = sig.splitn(2, '.');
        let sig_bytes = match iter.next() {
            Some(sig_encoded) => {
                base64::decode_config(sig_encoded, base64::URL_SAFE_NO_PAD).ok()?
            }
            None => return None,
        };
        let sig = ed25519_dalek::Signature::from_bytes(&sig_bytes).ok()?;

        let header = match iter.next() {
            Some(header_encoded) => {
                base64::decode_config(header_encoded, base64::URL_SAFE_NO_PAD).ok()?
            }
            None => return None,
        };
        let mut to_verify = header.clone();
        to_verify.push(b'\x00');
        to_verify.extend_from_slice(data);
        if self.inner.verify::<Sha512>(&to_verify, &sig).is_ok() {
            serde_json::from_slice(&header).ok()
        } else {
            None
        }
    }

    /// Verifies a signature but discards the header.
    pub fn verify(&self, data: &[u8], sig: &str) -> bool {
        self.verify_meta(data, sig).is_some()
    }

    /// Verifies a signature and checks the timestamp.
    pub fn verify_timestamp(&self, data: &[u8], sig: &str, max_age: Option<Duration>) -> bool {
        self.verify_meta(data, sig)
            .map(|header| max_age.is_none() || !header.expired(max_age.unwrap()))
            .unwrap_or(false)
    }

    /// Unpacks signed data and returns it with header.
    pub fn unpack_meta<D: DeserializeOwned>(
        &self,
        data: &[u8],
        signature: &str,
    ) -> Result<(SignatureHeader, D), UnpackError> {
        if let Some(header) = self.verify_meta(&data, signature) {
            serde_json::from_slice(&data)
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
        signature: &str,
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

impl PartialEq for PublicKey {
    fn eq(&self, other: &PublicKey) -> bool {
        self.inner.to_bytes()[..] == other.inner.to_bytes()[..]
    }
}

impl Eq for PublicKey {}

impl FromStr for PublicKey {
    type Err = KeyParseError;

    fn from_str(s: &str) -> Result<PublicKey, KeyParseError> {
        let bytes = match base64::decode_config(s, base64::URL_SAFE_NO_PAD) {
            Ok(bytes) => bytes,
            _ => return Err(KeyParseError::BadEncoding),
        };
        Ok(PublicKey {
            inner: ed25519_dalek::PublicKey::from_bytes(&bytes)
                .map_err(|_| KeyParseError::BadKey)?,
        })
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            base64::encode_config(&self.inner.to_bytes()[..], base64::URL_SAFE_NO_PAD)
        )
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PublicKey(\"{}\")", self)
    }
}

relay_common::impl_str_serde!(PublicKey, "a public key");

/// Generates an relay ID.
pub fn generate_relay_id() -> RelayId {
    Uuid::new_v4()
}

/// Generates a secret + public key pair.
pub fn generate_key_pair() -> (SecretKey, PublicKey) {
    let mut csprng = OsRng::new().unwrap();
    let kp = ed25519_dalek::Keypair::generate::<Sha512, _>(&mut csprng);
    let pk = ed25519_dalek::PublicKey::from_bytes(&kp.public.as_bytes()[..]).unwrap();
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
        Hmac::new_varkey(secret).expect("HMAC takes variable keys")
    }

    /// Signs the given `RegisterState` and serializes it into a single string.
    fn sign(state: RegisterState, secret: &[u8]) -> Self {
        let json = serde_json::to_string(&state).expect("relay register state serializes to JSON");
        let token = base64::encode_config(&json, base64::URL_SAFE_NO_PAD);

        let mut mac = Self::mac(secret);
        mac.input(token.as_bytes());
        let signature = base64::encode_config(&mac.result().code(), base64::URL_SAFE_NO_PAD);

        Self(format!("{}:{}", token, signature))
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
        let code = base64::decode_config(signature, base64::URL_SAFE_NO_PAD)
            .map_err(|_| UnpackError::BadEncoding)?;

        let mut mac = Self::mac(secret);
        mac.input(token.as_bytes());
        mac.verify(&code).map_err(|_| UnpackError::BadSignature)?;

        let json = base64::decode_config(token, base64::URL_SAFE_NO_PAD)
            .map_err(|_| UnpackError::BadEncoding)?;
        let state =
            serde_json::from_slice::<RegisterState>(&json).map_err(UnpackError::BadPayload)?;

        if let Some(max_age) = max_age {
            let secs = state.timestamp().as_secs() as i64;
            if Utc.timestamp(secs, 0) + max_age < Utc::now() {
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
/// This struct is used to carry over information between the downstream register request and
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
    let mut rng = thread_rng();
    let mut bytes = vec![0u8; 64];
    rng.fill_bytes(&mut bytes);
    base64::encode_config(&bytes, base64::URL_SAFE_NO_PAD)
}

/// Represents a request for registration with the upstream.
///
/// This is created if the relay signs in for the first time.  The server needs
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
    /// Creates a new request to register an relay upstream.
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
        signature: &str,
        max_age: Option<Duration>,
    ) -> Result<RegisterRequest, UnpackError> {
        let req: RegisterRequest = serde_json::from_slice(data).map_err(UnpackError::BadPayload)?;
        let pk = req.public_key();
        pk.unpack(data, signature, max_age)
    }

    /// Returns the relay ID of the registering relay.
    pub fn relay_id(&self) -> RelayId {
        self.relay_id
    }

    /// Returns the new public key of registering relay.
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
    /// Returns the relay ID of the registering relay.
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
        signature: &str,
        secret: &[u8],
        max_age: Option<Duration>,
    ) -> Result<(Self, RegisterState), UnpackError> {
        let response: Self = serde_json::from_slice(data).map_err(UnpackError::BadPayload)?;
        let state = response.token.unpack(secret, max_age)?;

        if let Some(header) = state.public_key().verify_meta(data, signature) {
            if max_age.map_or(false, |m| header.expired(m)) {
                return Err(UnpackError::SignatureExpired);
            }
        } else {
            return Err(UnpackError::BadSignature);
        }

        Ok((response, state))
    }

    /// Returns the relay ID of the registering relay.
    pub fn relay_id(&self) -> RelayId {
        self.relay_id
    }

    /// Returns the token that needs signing.
    pub fn token(&self) -> &str {
        self.token.as_str()
    }

    pub fn version(&self) -> RelayVersion {
        self.version
    }
}

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
        format!("{:#}", sk),
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
    assert!(pk.verify(data, &sig));

    let bad_sig =
        "jgubwSf2wb2wuiRpgt2H9_bdDSMr88hXLp5zVuhbr65EGkSxOfT5ILIWr623twLgLd0bDgHg6xzOaUCX7XvUCw";
    assert!(!pk.verify(data, &bad_sig));
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
    let (request_bytes, request_sig) = sk.pack(&request);

    // attempt to get the data through bootstrap unpacking.
    let request =
        RegisterRequest::bootstrap_unpack(&request_bytes, &request_sig, Some(max_age)).unwrap();
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
    let (response_bytes, response_sig) = sk.pack(&response);
    let (response, _) = RegisterResponse::unpack(
        &response_bytes,
        &response_sig,
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
fn test_generate_strings_for_test_auth_py() {
    let max_age = Duration::minutes(15);
    println!("Generating test data for test_auth.py...");

    // initial setup
    let relay_id = generate_relay_id();
    println!("RELAY_ID = b\"{}\"", relay_id);
    let (sk, pk) = generate_key_pair();
    println!("RELAY_KEY = b\"{}\"", pk);

    // create a register request
    let request = RegisterRequest::new(&relay_id, &pk);
    println!("REQUEST = b'{}'", serde_json::to_string(&request).unwrap());

    // sign it
    let (request_bytes, request_sig) = sk.pack(&request);
    println!("REQUEST_SIG = \"{}\"", request_sig);

    // attempt to get the data through bootstrap unpacking.
    let request =
        RegisterRequest::bootstrap_unpack(&request_bytes, &request_sig, Some(max_age)).unwrap();

    let upstream_secret = b"secret";

    // create a challenge
    let challenge = request.into_challenge(upstream_secret);
    let challenge_token = challenge.token().to_owned();
    println!("TOKEN = \"{}\"", challenge_token);

    // create a response from the challenge
    let response = challenge.into_response();
    let serialized_response = serde_json::to_string(&response).unwrap();
    let (_, response_sig) = sk.pack(&response);

    println!("RESPONSE = b'{}'", serialized_response);
    println!("RESPONSE_SIG = \"{}\"", response_sig);

    println!("RELAY_VERSION = \"{}\"", &LATEST_VERSION);
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
