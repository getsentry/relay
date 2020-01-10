use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use failure::Fail;
use rand::{rngs::OsRng, thread_rng, RngCore};
use sentry_types::Uuid;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::Sha512;

/// Alias for relay IDs (UUIDs)
pub type RelayId = Uuid;

/// Raised if a key could not be parsed.
#[derive(Debug, Fail, PartialEq, Eq, Hash)]
pub enum KeyParseError {
    /// Invalid key encoding
    #[fail(display = "bad key encoding")]
    BadEncoding,
    /// Invalid key data
    #[fail(display = "bad key data")]
    BadKey,
}

/// Raised to indicate failure on unpacking.
#[derive(Debug, Fail)]
pub enum UnpackError {
    /// Raised if the signature is invalid.
    #[fail(display = "invalid signature on data")]
    BadSignature,
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

/// Reprensents the final registration.
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

impl_str_serialization!(SecretKey, "a secret key");

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

impl_str_serialization!(PublicKey, "a public key");

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

/// Represents a challenge request.
///
/// This is created if the relay signs in for the first time.  The server needs
/// to respond to this challenge with a unique token that is then used to sign
/// the response.
#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterRequest {
    relay_id: RelayId,
    public_key: PublicKey,
}

impl RegisterRequest {
    /// Creates a new request to register an relay upstream.
    pub fn new(relay_id: &RelayId, public_key: &PublicKey) -> RegisterRequest {
        RegisterRequest {
            relay_id: *relay_id,
            public_key: public_key.clone(),
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
    pub fn relay_id(&self) -> &RelayId {
        &self.relay_id
    }

    /// Returns the new public key of registering relay.
    pub fn public_key(&self) -> &PublicKey {
        &self.public_key
    }

    /// Creates a register challenge for this request.
    pub fn create_challenge(&self) -> RegisterChallenge {
        let mut rng = thread_rng();
        let mut bytes = vec![0u8; 64];
        rng.fill_bytes(&mut bytes);
        let token = base64::encode_config(&bytes, base64::URL_SAFE_NO_PAD);
        RegisterChallenge {
            relay_id: self.relay_id,
            token,
        }
    }
}

/// Represents the response the server is supposed to send to a register request.
#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterChallenge {
    relay_id: RelayId,
    token: String,
}

impl RegisterChallenge {
    /// Returns the relay ID of the registering relay.
    pub fn relay_id(&self) -> &RelayId {
        &self.relay_id
    }

    /// Returns the token that needs signing.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Creates a register response.
    pub fn create_response(&self) -> RegisterResponse {
        RegisterResponse {
            relay_id: self.relay_id,
            token: self.token.clone(),
        }
    }
}

/// Represents a response to a register challenge
#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterResponse {
    relay_id: RelayId,
    token: String,
}

impl RegisterResponse {
    /// Loads the register response without validating.
    pub fn unpack_unsafe(data: &[u8]) -> Result<RegisterResponse, UnpackError> {
        serde_json::from_slice(data).map_err(UnpackError::BadPayload)
    }

    /// Returns the relay ID of the registering relay.
    pub fn relay_id(&self) -> &RelayId {
        &self.relay_id
    }

    /// Returns the token that needs signing.
    pub fn token(&self) -> &str {
        &self.token
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
    use serde_json;

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
    assert_eq!(pk.verify(data, &sig), true);

    let bad_sig =
        "jgubwSf2wb2wuiRpgt2H9_bdDSMr88hXLp5zVuhbr65EGkSxOfT5ILIWr623twLgLd0bDgHg6xzOaUCX7XvUCw";
    assert_eq!(pk.verify(data, &bad_sig), false);
}

#[test]
fn test_registration() {
    let max_age = Duration::minutes(15);

    // initial setup
    let relay_id = generate_relay_id();
    let (sk, pk) = generate_key_pair();

    // create a register request
    let reg_req = RegisterRequest::new(&relay_id, &pk);

    // sign it
    let (reg_req_bytes, reg_req_sig) = sk.pack(&reg_req);

    // attempt to get the data through bootstrap unpacking.
    let reg_req =
        RegisterRequest::bootstrap_unpack(&reg_req_bytes, &reg_req_sig, Some(max_age)).unwrap();
    assert_eq!(reg_req.relay_id(), &relay_id);
    assert_eq!(reg_req.public_key(), &pk);

    // create a challenge
    let challenge = reg_req.create_challenge();
    assert_eq!(challenge.relay_id(), &relay_id);
    assert!(challenge.token().len() > 40);

    // create a response from the challenge
    let reg_resp = challenge.create_response();

    // sign and unsign it
    let (reg_resp_bytes, reg_resp_sig) = sk.pack(&reg_resp);
    let reg_resp: RegisterResponse = pk
        .unpack(&reg_resp_bytes, &reg_resp_sig, Some(max_age))
        .unwrap();
    assert_eq!(reg_resp.relay_id(), &relay_id);
    assert_eq!(reg_resp.token(), challenge.token());
}
