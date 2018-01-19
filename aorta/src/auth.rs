use std::fmt;
use std::str::FromStr;
use std::sync::{Once, ONCE_INIT};

use rand::{thread_rng, Rng};
use base64;
use uuid::Uuid;
use serde::ser::{Serialize, Serializer};
use serde::de::{self, Deserialize, DeserializeOwned, Deserializer, Visitor};
use serde_json;
use sodiumoxide;
use sodiumoxide::crypto::sign::ed25519 as sign_backend;
use chrono::{DateTime, Utc};

static INIT_SODIUMOXIDE_RNG: Once = ONCE_INIT;

/// Alias for agent IDs (UUIDs)
pub type AgentId = Uuid;

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
    /// Raised if signature or payload are missing
    #[fail(display = "could not unpack because of bad data")]
    BadData,
    /// Raised if the signature is invalid.
    #[fail(display = "invalid signature on data")]
    BadSignature,
    /// Raised if deserializing of data failed.
    #[fail(display = "could not deserialize payload")]
    BadPayload(serde_json::Error),
}

/// Represents the public key of an agent.
///
/// Public keys are based on ed25519 but this should be considered an
/// implementation detail for now.  We only ever represent public keys
/// on the wire as opaque ascii encoded strings of arbitrary format or length.
#[derive(Clone)]
pub struct PublicKey {
    inner: sign_backend::PublicKey,
}

/// Represents the secret key of an agent.
///
/// Secret keys are based on ed25519 but this should be considered an
/// implementation detail for now.  We only ever represent public keys
/// on the wire as opaque ascii encoded strings of arbitrary format or length.
pub struct SecretKey {
    inner: sign_backend::SecretKey,
}

/// Represents a challenge request.
///
/// This is created if the agent signs in for the first time.  The server needs
/// to respond to this challenge with a unique token that is then used to sign
/// the response.
#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterRequest {
    agent_id: AgentId,
    public_key: PublicKey,
    timestamp: DateTime<Utc>,
}

/// Represents the response the server is supposed to send to a register request.
#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterChallenge {
    agent_id: AgentId,
    timestamp: DateTime<Utc>,
    token: String,
}

/// Represents a response to a register challenge
#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterResponse {
    agent_id: AgentId,
    timestamp: DateTime<Utc>,
    token: String,
}

impl SecretKey {
    /// Signs some data and returns the signature as string.
    pub fn sign(&self, data: &[u8]) -> String {
        let sig = sign_backend::sign_detached(data, &self.inner);
        base64::encode_config(&sig.0[..], base64::URL_SAFE_NO_PAD)
    }

    /// Signs some serializable data and packs it.
    pub fn pack<S: Serialize>(&self, data: S) -> String {
        // this can only fail if we deal with badly formed data.  In that case we
        // consider that a panic.  Should not happen.
        let json = serde_json::to_vec(&data).expect("attempted to pack non json safe data");
        let sig = self.sign(&json);
        format!(
            "{}.{}",
            sig,
            base64::encode_config(&json, base64::URL_SAFE_NO_PAD)
        )
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

impl Serialize for SecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for SecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = SecretKey;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a secret key")
            }

            fn visit_str<E>(self, value: &str) -> Result<SecretKey, E>
            where
                E: de::Error,
            {
                match value.parse() {
                    Ok(value) => Ok(value),
                    Err(_) => Err(de::Error::invalid_value(de::Unexpected::Str(value), &self)),
                }
            }
        }

        deserializer.deserialize_str(V)
    }
}

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

    /// Unpacks signed data.
    pub fn unpack<D: DeserializeOwned>(&self, data: &str) -> Result<D, UnpackError> {
        let mut pieces = data.splitn(2, '.');
        let sig = pieces.next().ok_or(UnpackError::BadData)?;
        let payload = pieces.next().ok_or(UnpackError::BadData)?;
        let payload_bytes = base64::decode_config(payload, base64::URL_SAFE_NO_PAD)
            .map_err(|_| UnpackError::BadData)?;
        if self.verify(&payload_bytes, sig) {
            serde_json::from_slice(&payload_bytes).map_err(UnpackError::BadPayload)
        } else {
            Err(UnpackError::BadSignature)
        }
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

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = PublicKey;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a secret key")
            }

            fn visit_str<E>(self, value: &str) -> Result<PublicKey, E>
            where
                E: de::Error,
            {
                match value.parse() {
                    Ok(value) => Ok(value),
                    Err(_) => Err(de::Error::invalid_value(de::Unexpected::Str(value), &self)),
                }
            }
        }

        deserializer.deserialize_str(V)
    }
}

/// Generates an agent ID.
pub fn generate_agent_id() -> AgentId {
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
    /// Creates a new request to register an agent upstream.
    pub fn new(agent_id: &AgentId, public_key: &PublicKey) -> RegisterRequest {
        RegisterRequest {
            agent_id: agent_id.clone(),
            public_key: public_key.clone(),
            timestamp: Utc::now(),
        }
    }

    /// Returns the agent ID of the registering agent.
    pub fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    /// Returns the new public key of registering agent.
    pub fn public_key(&self) -> &PublicKey {
        &self.public_key
    }

    /// Returns the timestamp of when the registration started.
    pub fn timestamp(&self) -> &DateTime<Utc> {
        &self.timestamp
    }

    /// Creates a register challenge for this request.
    pub fn create_challenge(&self) -> RegisterChallenge {
        let mut rng = thread_rng();
        let mut bytes = vec![0u8; 64];
        rng.fill_bytes(&mut bytes);
        let token = base64::encode_config(&bytes, base64::URL_SAFE_NO_PAD);
        RegisterChallenge {
            agent_id: self.agent_id.clone(),
            timestamp: Utc::now(),
            token: token,
        }
    }
}

impl RegisterChallenge {
    /// Returns the agent ID of the registering agent.
    pub fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    /// Returns the timestamp of when the register challenge was created.
    pub fn timestamp(&self) -> &DateTime<Utc> {
        &self.timestamp
    }

    /// Returns the token that needs signing.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Creates a register response.
    pub fn create_response(&self) -> RegisterResponse {
        RegisterResponse { 
            agent_id: self.agent_id.clone(),
            timestamp: Utc::now(),
            token: self.token.clone(),
        }
    }
}

impl RegisterResponse {
    /// Returns the agent ID of the registering agent.
    pub fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    /// Returns the timestamp of when the register response was created.
    pub fn timestamp(&self) -> &DateTime<Utc> {
        &self.timestamp
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
    assert_eq!(sk_json, "\"OvXFVm1tIUi8xDTuyHX1SSqdMc8nCt2qU9IUaH5p7oUk5pHZsdnfXNiMWiMLtSE86J3N9Peo5CBP1YQHDUkApQ\"");

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
    // initial setup
    let agent_id = generate_agent_id();
    let (sk, pk) = generate_key_pair();

    // create a register request
    let reg_req = RegisterRequest::new(&agent_id, &pk);

    // sign and unsign it
    let signed_reg_req = sk.pack(&reg_req);
    let reg_req: RegisterRequest = pk.unpack(&signed_reg_req).unwrap();
    assert_eq!(reg_req.agent_id(), &agent_id);
    assert_eq!(reg_req.public_key(), &pk);

    // create a challenge
    let challenge = reg_req.create_challenge();
    assert_eq!(challenge.agent_id(), &agent_id);
    assert!(challenge.token().len() > 40);

    // create a response from the challenge
    let reg_resp = challenge.create_response();
    let signed_reg_resp = sk.pack(&reg_resp);
    let reg_resp: RegisterResponse = pk.unpack(&signed_reg_resp).unwrap();
    assert_eq!(reg_resp.agent_id(), &agent_id);
    assert_eq!(reg_resp.token(), challenge.token());
}
