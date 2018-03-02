use uuid::Uuid;
use serde_json;
use chrono::Duration;

use smith_aorta::{PublicKey, SecretKey, RegisterRequest, RegisterResponse,
                  generate_key_pair, generate_relay_id};

use core::{SmithBuf, SmithStr, SmithUuid};

/// Represents a public key in smith.
pub struct SmithPublicKey;

/// Represents a secret key in smith.
pub struct SmithSecretKey;

/// Represents a key pair from key generation.
#[repr(C)]
pub struct SmithKeyPair {
    pub public_key: *mut SmithPublicKey,
    pub secret_key: *mut SmithSecretKey,
}

/// Represents a register request.
pub struct SmithRegisterRequest;

ffi_fn! {
    /// Parses a public key from a string.
    unsafe fn smith_publickey_parse(s: &SmithStr) -> Result<*mut SmithPublicKey> {
        let public_key: PublicKey = s.as_str().parse()?;
        Ok(Box::into_raw(Box::new(public_key)) as *mut SmithPublicKey)
    }
}

ffi_fn! {
    /// Frees a public key.
    unsafe fn smith_publickey_free(spk: *mut SmithPublicKey) {
        if !spk.is_null() {
            let pk = spk as *mut PublicKey;
            Box::from_raw(pk);
        }
    }
}

ffi_fn! {
    /// Converts a public key into a string.
    unsafe fn smith_publickey_to_string(spk: *const SmithPublicKey) -> Result<SmithStr> {
        let pk = spk as *const PublicKey;
        Ok(SmithStr::from_string((*pk).to_string()))
    }
}

ffi_fn! {
    /// Verifies a signature
    unsafe fn smith_publickey_verify(spk: *const SmithPublicKey, data: *const SmithBuf, sig: *const SmithStr) -> Result<bool> {
        let pk = spk as *const PublicKey;
        Ok((*pk).verify((*data).as_bytes(), (*sig).as_str()))
    }
}

ffi_fn! {
    /// Verifies a signature
    unsafe fn smith_publickey_verify_timestamp(spk: *const SmithPublicKey, data: *const SmithBuf, sig: *const SmithStr, max_age: u32) -> Result<bool> {
        let pk = spk as *const PublicKey;
        let max_age = Some(Duration::seconds(max_age as i64));
        Ok((*pk).verify_timestamp((*data).as_bytes(), (*sig).as_str(), max_age))
    }
}

ffi_fn! {
    /// Parses a secret key from a string.
    unsafe fn smith_secretkey_parse(s: &SmithStr) -> Result<*mut SmithSecretKey> {
        let secret_key: SecretKey = s.as_str().parse()?;
        Ok(Box::into_raw(Box::new(secret_key)) as *mut SmithSecretKey)
    }
}

ffi_fn! {
    /// Frees a secret key.
    unsafe fn smith_secretkey_free(spk: *mut SmithSecretKey) {
        if !spk.is_null() {
            let pk = spk as *mut SecretKey;
            Box::from_raw(pk);
        }
    }
}

ffi_fn! {
    /// Converts a secret key into a string.
    unsafe fn smith_secretkey_to_string(spk: *const SmithSecretKey) -> Result<SmithStr> {
        let pk = spk as *const SecretKey;
        Ok(SmithStr::from_string((*pk).to_string()))
    }
}

ffi_fn! {
    /// Verifies a signature
    unsafe fn smith_secretkey_sign(spk: *const SmithSecretKey, data: *const SmithBuf) -> Result<SmithStr> {
        let pk = spk as *const SecretKey;
        Ok(SmithStr::from_string((*pk).sign((*data).as_bytes())))
    }
}

ffi_fn! {
    /// Generates a secret, public key pair.
    unsafe fn smith_generate_key_pair() -> Result<SmithKeyPair> {
        let (sk, pk) = generate_key_pair();
        Ok(SmithKeyPair {
            secret_key: Box::into_raw(Box::new(sk)) as *mut SmithSecretKey,
            public_key: Box::into_raw(Box::new(pk)) as *mut SmithPublicKey,
        })
    }
}

ffi_fn! {
    /// Randomly generates an relay id
    unsafe fn smith_generate_relay_id() -> Result<SmithUuid> {
        let relay_id = generate_relay_id();
        Ok(SmithUuid::new(relay_id))
    }
}

#[derive(Serialize)]
struct SmithChallengeResult {
    pub relay_id: Uuid,
    pub public_key: PublicKey,
    pub token: String,
}

ffi_fn! {
    /// Creates a challenge from a register request and returns JSON.
    unsafe fn smith_create_register_challenge(data: *const SmithBuf,
                                              signature: *const SmithStr,
                                              max_age: u32)
        -> Result<SmithStr>
    {
        let max_age = Duration::seconds(max_age as i64);
        let req = RegisterRequest::bootstrap_unpack(
            (*data).as_bytes(), (*signature).as_str(), Some(max_age))?;
        let challenge = req.create_challenge();
        Ok(SmithStr::from_string(serde_json::to_string(&SmithChallengeResult {
            relay_id: req.relay_id().clone(),
            public_key: req.public_key().clone(),
            token: challenge.token().to_string(),
        })?))
    }
}

ffi_fn! {
    /// Given just the data from a register response returns the
    /// conained relay id without validating the signature.
    unsafe fn smith_get_register_response_relay_id(data: *const SmithBuf)
        -> Result<SmithUuid>
    {
        Ok(SmithUuid::new(*RegisterResponse::unpack_unsafe((*data).as_bytes())?.relay_id()))
    }
}

#[derive(Serialize)]
struct SmithRegisterResponse {
    pub relay_id: Uuid,
    pub token: String,
}

ffi_fn! {
    /// Validates a register response.
    unsafe fn smith_validate_register_response(pk: *const SmithPublicKey,
                                               data: *const SmithBuf,
                                               signature: *const SmithStr,
                                               max_age: u32)
        -> Result<SmithStr>
    {
        let max_age = Duration::seconds(max_age as i64);
        let pk = &*(pk as *const PublicKey);
        let reg_resp: RegisterResponse = pk.unpack(
            (*data).as_bytes(), (*signature).as_str(), Some(max_age))?;
        Ok(SmithStr::from_string(serde_json::to_string(&SmithRegisterResponse {
            relay_id: reg_resp.relay_id().clone(),
            token: reg_resp.token().to_string(),
        })?))
    }
}
