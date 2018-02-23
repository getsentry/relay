use uuid::Uuid;
use serde_json;
use chrono::Duration;

use smith_aorta::{PublicKey, SecretKey, RegisterRequest, RegisterResponse,
                  generate_key_pair, generate_agent_id};

use core::{SmithBuf, SmithStr, SmithUuid};

pub struct SmithAgentId;

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
    /// Randomly generates an agent id
    unsafe fn smith_generate_agent_id() -> Result<SmithUuid> {
        let agent_id = generate_agent_id();
        Ok(SmithUuid::new(agent_id))
    }
}

#[derive(Serialize)]
struct CombinedChallengeResult {
    pub agent_id: Uuid,
    pub public_key: PublicKey,
    pub token: String,
}

ffi_fn! {
    /// Creates a challenge from a register request and returns JSON.
    unsafe fn smith_create_register_challenge(signed_req: *const SmithStr,
                                              max_age: u32)
        -> Result<SmithStr>
    {
        let max_age = Duration::seconds(max_age as i64);
        let req = RegisterRequest::bootstrap_unpack((*signed_req).as_str(), Some(max_age))?;
        let challenge = req.create_challenge();
        Ok(SmithStr::from_string(serde_json::to_string(&CombinedChallengeResult {
            agent_id: req.agent_id().clone(),
            public_key: req.public_key().clone(),
            token: challenge.token().to_string(),
        })?))
    }
}

ffi_fn! {
    /// Validates a register response.
    unsafe fn smith_validate_register_response(pk: *const SmithPublicKey,
                                               signed_resp: *const SmithStr,
                                               max_age: u32)
        -> Result<SmithStr>
    {
        let max_age = Duration::seconds(max_age as i64);
        let pk = &*(pk as *const PublicKey);
        let reg_resp: RegisterResponse = pk.unpack((*signed_resp).as_str(), Some(max_age))?;
        Ok(SmithStr::from_string(serde_json::to_string(&reg_resp)?))
    }
}
