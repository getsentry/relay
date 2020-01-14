use chrono::Duration;
use serde::Serialize;

use relay_auth::{
    generate_key_pair, generate_relay_id, PublicKey, RegisterRequest, RegisterResponse, SecretKey,
};
use relay_common::Uuid;

use crate::core::{RelayBuf, RelayStr, RelayUuid};

/// Represents a public key in relay.
pub struct RelayPublicKey;

/// Represents a secret key in relay.
pub struct RelaySecretKey;

/// Represents a key pair from key generation.
#[repr(C)]
pub struct RelayKeyPair {
    pub public_key: *mut RelayPublicKey,
    pub secret_key: *mut RelaySecretKey,
}

/// Represents a register request.
pub struct RelayRegisterRequest;

ffi_fn! {
    /// Parses a public key from a string.
    unsafe fn relay_publickey_parse(s: *const RelayStr) -> Result<*mut RelayPublicKey> {
        let public_key: PublicKey = (*s).as_str().parse()?;
        Ok(Box::into_raw(Box::new(public_key)) as *mut RelayPublicKey)
    }
}

ffi_fn! {
    /// Frees a public key.
    unsafe fn relay_publickey_free(spk: *mut RelayPublicKey) {
        if !spk.is_null() {
            let pk = spk as *mut PublicKey;
            Box::from_raw(pk);
        }
    }
}

ffi_fn! {
    /// Converts a public key into a string.
    unsafe fn relay_publickey_to_string(spk: *const RelayPublicKey)
        -> Result<RelayStr>
    {
        let pk = spk as *const PublicKey;
        Ok(RelayStr::from_string((*pk).to_string()))
    }
}

ffi_fn! {
    /// Verifies a signature
    unsafe fn relay_publickey_verify(spk: *const RelayPublicKey,
                                         data: *const RelayBuf,
                                         sig: *const RelayStr) -> Result<bool> {
        let pk = spk as *const PublicKey;
        Ok((*pk).verify((*data).as_bytes(), (*sig).as_str()))
    }
}

ffi_fn! {
    /// Verifies a signature
    unsafe fn relay_publickey_verify_timestamp(spk: *const RelayPublicKey,
                                                   data: *const RelayBuf,
                                                   sig: *const RelayStr,
                                                   max_age: u32) -> Result<bool> {
        let pk = spk as *const PublicKey;
        let max_age = Some(Duration::seconds(i64::from(max_age)));
        Ok((*pk).verify_timestamp((*data).as_bytes(), (*sig).as_str(), max_age))
    }
}

ffi_fn! {
    /// Parses a secret key from a string.
    unsafe fn relay_secretkey_parse(s: &RelayStr) -> Result<*mut RelaySecretKey> {
        let secret_key: SecretKey = s.as_str().parse()?;
        Ok(Box::into_raw(Box::new(secret_key)) as *mut RelaySecretKey)
    }
}

ffi_fn! {
    /// Frees a secret key.
    unsafe fn relay_secretkey_free(spk: *mut RelaySecretKey) {
        if !spk.is_null() {
            let pk = spk as *mut SecretKey;
            Box::from_raw(pk);
        }
    }
}

ffi_fn! {
    /// Converts a secret key into a string.
    unsafe fn relay_secretkey_to_string(spk: *const RelaySecretKey)
        -> Result<RelayStr>
    {
        let pk = spk as *const SecretKey;
        Ok(RelayStr::from_string((*pk).to_string()))
    }
}

ffi_fn! {
    /// Verifies a signature
    unsafe fn relay_secretkey_sign(spk: *const RelaySecretKey,
                                       data: *const RelayBuf) -> Result<RelayStr> {
        let pk = spk as *const SecretKey;
        Ok(RelayStr::from_string((*pk).sign((*data).as_bytes())))
    }
}

ffi_fn! {
    /// Generates a secret, public key pair.
    unsafe fn relay_generate_key_pair() -> Result<RelayKeyPair> {
        let (sk, pk) = generate_key_pair();
        Ok(RelayKeyPair {
            secret_key: Box::into_raw(Box::new(sk)) as *mut RelaySecretKey,
            public_key: Box::into_raw(Box::new(pk)) as *mut RelayPublicKey,
        })
    }
}

ffi_fn! {
    /// Randomly generates an relay id
    unsafe fn relay_generate_relay_id() -> Result<RelayUuid> {
        let relay_id = generate_relay_id();
        Ok(RelayUuid::new(relay_id))
    }
}

#[derive(Serialize)]
struct RelayChallengeResult {
    pub relay_id: Uuid,
    pub public_key: PublicKey,
    pub token: String,
}

ffi_fn! {
    /// Creates a challenge from a register request and returns JSON.
    unsafe fn relay_create_register_challenge(data: *const RelayBuf,
                                              signature: *const RelayStr,
                                              max_age: u32)
        -> Result<RelayStr>
    {
        let max_age = Duration::seconds(i64::from(max_age));
        let req = RegisterRequest::bootstrap_unpack(
            (*data).as_bytes(), (*signature).as_str(), Some(max_age))?;
        let challenge = req.create_challenge();
        Ok(RelayStr::from_string(serde_json::to_string(&RelayChallengeResult {
            relay_id: *req.relay_id(),
            public_key: req.public_key().clone(),
            token: challenge.token().to_string(),
        })?))
    }
}

ffi_fn! {
    /// Given just the data from a register response returns the
    /// conained relay id without validating the signature.
    unsafe fn relay_get_register_response_relay_id(data: *const RelayBuf)
        -> Result<RelayUuid>
    {
        Ok(RelayUuid::new(*RegisterResponse::unpack_unsafe((*data).as_bytes())?.relay_id()))
    }
}

#[derive(Serialize)]
struct RelayRegisterResponse {
    pub relay_id: Uuid,
    pub token: String,
}

ffi_fn! {
    /// Validates a register response.
    unsafe fn relay_validate_register_response(pk: *const RelayPublicKey,
                                               data: *const RelayBuf,
                                               signature: *const RelayStr,
                                               max_age: u32)
        -> Result<RelayStr>
    {
        let max_age = Duration::seconds(i64::from(max_age));
        let pk = &*(pk as *const PublicKey);
        let reg_resp: RegisterResponse = pk.unpack(
            (*data).as_bytes(), (*signature).as_str(), Some(max_age))?;
        Ok(RelayStr::from_string(serde_json::to_string(&RelayRegisterResponse {
            relay_id: *reg_resp.relay_id(),
            token: reg_resp.token().to_string(),
        })?))
    }
}
