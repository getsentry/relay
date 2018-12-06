use chrono::Duration;
use serde_json;

use semaphore_common::{
    generate_key_pair, generate_relay_id, PublicKey, RegisterRequest, RegisterResponse, SecretKey,
    Uuid,
};

use crate::core::{SemaphoreBuf, SemaphoreStr, SemaphoreUuid};

/// Represents a public key in semaphore.
pub struct SemaphorePublicKey;

/// Represents a secret key in semaphore.
pub struct SemaphoreSecretKey;

/// Represents a key pair from key generation.
#[repr(C)]
pub struct SemaphoreKeyPair {
    pub public_key: *mut SemaphorePublicKey,
    pub secret_key: *mut SemaphoreSecretKey,
}

/// Represents a register request.
pub struct SemaphoreRegisterRequest;

ffi_fn! {
    /// Parses a public key from a string.
    unsafe fn semaphore_publickey_parse(s: *const SemaphoreStr) -> Result<*mut SemaphorePublicKey> {
        let public_key: PublicKey = (*s).as_str().parse()?;
        Ok(Box::into_raw(Box::new(public_key)) as *mut SemaphorePublicKey)
    }
}

ffi_fn! {
    /// Frees a public key.
    unsafe fn semaphore_publickey_free(spk: *mut SemaphorePublicKey) {
        if !spk.is_null() {
            let pk = spk as *mut PublicKey;
            Box::from_raw(pk);
        }
    }
}

ffi_fn! {
    /// Converts a public key into a string.
    unsafe fn semaphore_publickey_to_string(spk: *const SemaphorePublicKey)
        -> Result<SemaphoreStr>
    {
        let pk = spk as *const PublicKey;
        Ok(SemaphoreStr::from_string((*pk).to_string()))
    }
}

ffi_fn! {
    /// Verifies a signature
    unsafe fn semaphore_publickey_verify(spk: *const SemaphorePublicKey,
                                         data: *const SemaphoreBuf,
                                         sig: *const SemaphoreStr) -> Result<bool> {
        let pk = spk as *const PublicKey;
        Ok((*pk).verify((*data).as_bytes(), (*sig).as_str()))
    }
}

ffi_fn! {
    /// Verifies a signature
    unsafe fn semaphore_publickey_verify_timestamp(spk: *const SemaphorePublicKey,
                                                   data: *const SemaphoreBuf,
                                                   sig: *const SemaphoreStr,
                                                   max_age: u32) -> Result<bool> {
        let pk = spk as *const PublicKey;
        let max_age = Some(Duration::seconds(i64::from(max_age)));
        Ok((*pk).verify_timestamp((*data).as_bytes(), (*sig).as_str(), max_age))
    }
}

ffi_fn! {
    /// Parses a secret key from a string.
    unsafe fn semaphore_secretkey_parse(s: &SemaphoreStr) -> Result<*mut SemaphoreSecretKey> {
        let secret_key: SecretKey = s.as_str().parse()?;
        Ok(Box::into_raw(Box::new(secret_key)) as *mut SemaphoreSecretKey)
    }
}

ffi_fn! {
    /// Frees a secret key.
    unsafe fn semaphore_secretkey_free(spk: *mut SemaphoreSecretKey) {
        if !spk.is_null() {
            let pk = spk as *mut SecretKey;
            Box::from_raw(pk);
        }
    }
}

ffi_fn! {
    /// Converts a secret key into a string.
    unsafe fn semaphore_secretkey_to_string(spk: *const SemaphoreSecretKey)
        -> Result<SemaphoreStr>
    {
        let pk = spk as *const SecretKey;
        Ok(SemaphoreStr::from_string((*pk).to_string()))
    }
}

ffi_fn! {
    /// Verifies a signature
    unsafe fn semaphore_secretkey_sign(spk: *const SemaphoreSecretKey,
                                       data: *const SemaphoreBuf) -> Result<SemaphoreStr> {
        let pk = spk as *const SecretKey;
        Ok(SemaphoreStr::from_string((*pk).sign((*data).as_bytes())))
    }
}

ffi_fn! {
    /// Generates a secret, public key pair.
    unsafe fn semaphore_generate_key_pair() -> Result<SemaphoreKeyPair> {
        let (sk, pk) = generate_key_pair();
        Ok(SemaphoreKeyPair {
            secret_key: Box::into_raw(Box::new(sk)) as *mut SemaphoreSecretKey,
            public_key: Box::into_raw(Box::new(pk)) as *mut SemaphorePublicKey,
        })
    }
}

ffi_fn! {
    /// Randomly generates an relay id
    unsafe fn semaphore_generate_relay_id() -> Result<SemaphoreUuid> {
        let relay_id = generate_relay_id();
        Ok(SemaphoreUuid::new(relay_id))
    }
}

#[derive(Serialize)]
struct SemaphoreChallengeResult {
    pub relay_id: Uuid,
    pub public_key: PublicKey,
    pub token: String,
}

ffi_fn! {
    /// Creates a challenge from a register request and returns JSON.
    unsafe fn semaphore_create_register_challenge(data: *const SemaphoreBuf,
                                              signature: *const SemaphoreStr,
                                              max_age: u32)
        -> Result<SemaphoreStr>
    {
        let max_age = Duration::seconds(i64::from(max_age));
        let req = RegisterRequest::bootstrap_unpack(
            (*data).as_bytes(), (*signature).as_str(), Some(max_age))?;
        let challenge = req.create_challenge();
        Ok(SemaphoreStr::from_string(serde_json::to_string(&SemaphoreChallengeResult {
            relay_id: *req.relay_id(),
            public_key: req.public_key().clone(),
            token: challenge.token().to_string(),
        })?))
    }
}

ffi_fn! {
    /// Given just the data from a register response returns the
    /// conained relay id without validating the signature.
    unsafe fn semaphore_get_register_response_relay_id(data: *const SemaphoreBuf)
        -> Result<SemaphoreUuid>
    {
        Ok(SemaphoreUuid::new(*RegisterResponse::unpack_unsafe((*data).as_bytes())?.relay_id()))
    }
}

#[derive(Serialize)]
struct SemaphoreRegisterResponse {
    pub relay_id: Uuid,
    pub token: String,
}

ffi_fn! {
    /// Validates a register response.
    unsafe fn semaphore_validate_register_response(pk: *const SemaphorePublicKey,
                                               data: *const SemaphoreBuf,
                                               signature: *const SemaphoreStr,
                                               max_age: u32)
        -> Result<SemaphoreStr>
    {
        let max_age = Duration::seconds(i64::from(max_age));
        let pk = &*(pk as *const PublicKey);
        let reg_resp: RegisterResponse = pk.unpack(
            (*data).as_bytes(), (*signature).as_str(), Some(max_age))?;
        Ok(SemaphoreStr::from_string(serde_json::to_string(&SemaphoreRegisterResponse {
            relay_id: *reg_resp.relay_id(),
            token: reg_resp.token().to_string(),
        })?))
    }
}
