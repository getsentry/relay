use std::fmt;
use std::str::FromStr;
use std::sync::{Once, ONCE_INIT};

use base64;
use uuid::Uuid;
use serde::ser::{Serialize, Serializer};
use serde::de::{self, Deserialize, Deserializer, Visitor};
use sodiumoxide;
use sodiumoxide::crypto::sign::ed25519 as sign_backend;

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

/// Represents the public key of an agent.
///
/// Public keys are based on ed25519 but this should be considered an
/// implementation detail for now.  We only ever represent public keys
/// on the wire as opaque ascii encoded strings of arbitrary format or length.
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

impl SecretKey {
    /// Signs some data and returns the signature as string.
    pub fn sign(&self, data: &[u8]) -> String {
        let sig = sign_backend::sign_detached(data, &self.inner);
        base64::encode_config(&sig.0[..], base64::URL_SAFE_NO_PAD)
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
