use std::fmt;
use std::str::FromStr;
use std::sync::{Once, ONCE_INIT};

use rand::{thread_rng, Rng};
use base64;
use uuid::Uuid;
use serde::ser::Serialize;
use serde::de::DeserializeOwned;
use serde_json;
use sodiumoxide;
use sodiumoxide::crypto::sign::ed25519 as sign_backend;
use chrono::{DateTime, Duration, Utc};

static INIT_SODIUMOXIDE_RNG: Once = ONCE_INIT;

/// Alias for relay IDs (UUIDs)
pub type RelayId = Uuid;

/// Calls to sodiumoxide that need the RNG need to go through this
/// wrapper as otherwise thread safety cannot be guarnateed.
fn with_sodiumoxide_rng<T, F: FnOnce() -> T>(cb: F) -> T {
    INIT_SODIUMOXIDE_RNG.call_once(|| {
        sodiumoxide::init();
    });
    cb()
}

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
pub struct Packed<D> {
    /// The timestamp of when the data was packed and signed.
    pub timestamp: DateTime<Utc>,
    /// The payload of the packed structure.
    pub data: D,
}

/// Represents the public key of an relay.
///
/// Public keys are based on ed25519 but this should be considered an
/// implementation detail for now.  We only ever represent public keys
/// on the wire as opaque ascii encoded strings of arbitrary format or length.
#[derive(Clone)]
pub struct PublicKey {
    inner: sign_backend::PublicKey,
}

/// Represents the secret key of an relay.
///
/// Secret keys are based on ed25519 but this should be considered an
/// implementation detail for now.  We only ever represent public keys
/// on the wire as opaque ascii encoded strings of arbitrary format or length.
#[derive(Clone)]
pub struct SecretKey {
    inner: sign_backend::SecretKey,
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

/// Represents the response the server is supposed to send to a register request.
#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterChallenge {
    relay_id: RelayId,
    token: String,
}

/// Represents a response to a register challenge
#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterResponse {
    relay_id: RelayId,
    token: String,
}

impl SecretKey {
    /// Signs some data and returns the signature as string.
    pub fn sign(&self, data: &[u8]) -> String {
        let sig = sign_backend::sign_detached(data, &self.inner);
        base64::encode_config(&sig.0[..], base64::URL_SAFE_NO_PAD)
    }

    /// Signs some serializable data and packs it.
    ///
    /// This puts a wrapper with a timestamp around the data, encodes it to
    /// json, serializes and signs it.  The return value is
    /// `(packed, signature)`.
    pub fn pack<S: Serialize>(&self, data: S) -> (Vec<u8>, String) {
        // this can only fail if we deal with badly formed data.  In that case we
        // consider that a panic.  Should not happen.
        let packed = Packed {
            timestamp: Utc::now(),
            data: data,
        };
        let json = serde_json::to_vec(&packed).expect("attempted to pack non json safe data");
        let sig = self.sign(&json);
        (json, sig)
    }
}

impl PartialEq for SecretKey {
    fn eq(&self, other: &SecretKey) -> bool {
        &self.inner.0[..] == &other.inner.0[..]
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
            inner: sign_backend::SecretKey::from_slice(&bytes).ok_or(KeyParseError::BadKey)?,
        })
    }
}

impl fmt::Display for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            base64::encode_config(&self.inner.0[..], base64::URL_SAFE_NO_PAD)
        )
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SecretKey(\"{}\")", self)
    }
}

impl_str_serialization!(SecretKey, "a secret key");

impl PublicKey {
    /// Verifies a signature.
    pub fn verify(&self, data: &[u8], sig: &str) -> bool {
        let mut sig_arr = [0u8; sign_backend::SIGNATUREBYTES];
        let sig_bytes = match base64::decode_config(sig, base64::URL_SAFE_NO_PAD) {
            Ok(bytes) => bytes,
            _ => return false,
        };
        if sig_bytes.len() != sig_arr.len() {
            return false;
        }
        sig_arr.clone_from_slice(&sig_bytes);
        sign_backend::verify_detached(&sign_backend::Signature(sig_arr), data, &self.inner)
    }

    /// Unpacks signed data and returns it with the packed wrapper.
    pub fn unpack_wrapper<D: DeserializeOwned>(
        &self,
        data: &[u8],
        signature: &str,
    ) -> Result<Packed<D>, UnpackError> {
        if self.verify(&data, signature) {
            serde_json::from_slice(&data).map_err(UnpackError::BadPayload)
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
        let rv: Packed<D> = self.unpack_wrapper(data, signature)?;
        if max_age.is_none() || rv.timestamp >= (Utc::now() - max_age.unwrap()) {
            Ok(rv.data)
        } else {
            Err(UnpackError::SignatureExpired)
        }
    }
}

impl<D: DeserializeOwned> Packed<D> {
    /// Returns the data unsafe without verifying signature.
    ///
    /// This can be used for bootstrapping purposes where looking into the
    /// payload is necessary as the public key is not known yet.
    pub fn unpack_unsafe(data: &[u8]) -> Result<Packed<D>, UnpackError> {
        serde_json::from_slice(&data).map_err(UnpackError::BadPayload)
    }
}

impl PartialEq for PublicKey {
    fn eq(&self, other: &PublicKey) -> bool {
        &self.inner.0[..] == &other.inner.0[..]
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
            inner: sign_backend::PublicKey::from_slice(&bytes).ok_or(KeyParseError::BadKey)?,
        })
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            base64::encode_config(&self.inner.0[..], base64::URL_SAFE_NO_PAD)
        )
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
    with_sodiumoxide_rng(|| {
        let (pk, sk) = sign_backend::gen_keypair();
        (SecretKey { inner: sk }, PublicKey { inner: pk })
    })
}

impl RegisterRequest {
    /// Creates a new request to register an relay upstream.
    pub fn new(relay_id: &RelayId, public_key: &PublicKey) -> RegisterRequest {
        RegisterRequest {
            relay_id: relay_id.clone(),
            public_key: public_key.clone(),
        }
    }

    /// Unpacks a signed register request for bootstrapping.
    ///
    /// This unpacks the embedded public key first, then verifies if the
    /// self signature was made by that public key.  If all is well then
    /// the data is returned.
    pub fn bootstrap_unpack(data: &[u8], signature: &str, max_age: Option<Duration>)
        -> Result<RegisterRequest, UnpackError>
    {
        let packed = Packed::<RegisterRequest>::unpack_unsafe(data)?;
        let pk = packed.data.public_key();
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
            relay_id: self.relay_id.clone(),
            token: token,
        }
    }
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
            relay_id: self.relay_id.clone(),
            token: self.token.clone(),
        }
    }
}

impl RegisterResponse {
    /// Loads the register response without validating.
    pub fn unpack_unsafe(data: &[u8]) -> Result<RegisterResponse, UnpackError> {
        let packed = Packed::<RegisterResponse>::unpack_unsafe(data)?;
        Ok(packed.data)
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
    assert_eq!(
        sk_json,
        "\"OvXFVm1tIUi8xDTuyHX1SSqdMc8nCt2qU9IUaH5p7oUk5pHZsdnfXNiMW\
         iMLtSE86J3N9Peo5CBP1YQHDUkApQ\""
    );

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

    // attempt to get the data unsigned unsafe
    let reg_req_packed = Packed::<RegisterRequest>::unpack_unsafe(&reg_req_bytes).unwrap();
    assert_eq!(reg_req_packed.data.relay_id(), &relay_id);
    assert_eq!(reg_req_packed.data.public_key(), &pk);

    // unsign it properly now
    let reg_req: RegisterRequest = pk.unpack(&reg_req_bytes, &reg_req_sig, Some(max_age)).unwrap();
    assert_eq!(reg_req.relay_id(), &relay_id);
    assert_eq!(reg_req.public_key(), &pk);

    // use the shortcut.
    let reg_req = RegisterRequest::bootstrap_unpack(&reg_req_bytes, &reg_req_sig, Some(max_age)).unwrap();
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
    let reg_resp: RegisterResponse = pk.unpack(&reg_resp_bytes, &reg_resp_sig, Some(max_age)).unwrap();
    assert_eq!(reg_resp.relay_id(), &relay_id);
    assert_eq!(reg_resp.token(), challenge.token());
}
