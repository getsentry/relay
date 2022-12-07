use chrono::Duration;
use serde::Serialize;

use relay_auth::{
    generate_key_pair, generate_relay_id, PublicKey, RegisterRequest, RegisterResponse, RelayId,
    RelayVersion, SecretKey,
};

use crate::core::{RelayBuf, RelayStr, RelayUuid};

/// Represents a public key in Relay.
pub struct RelayPublicKey;

/// Represents a secret key in Relay.
pub struct RelaySecretKey;

/// Represents a key pair from key generation.
#[repr(C)]
pub struct RelayKeyPair {
    /// The public key used for verifying Relay signatures.
    pub public_key: *mut RelayPublicKey,
    /// The secret key used for signing Relay requests.
    pub secret_key: *mut RelaySecretKey,
}

/// Represents a register request.
pub struct RelayRegisterRequest;

/// Parses a public key from a string.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_publickey_parse(s: *const RelayStr) -> *mut RelayPublicKey {
    let public_key: PublicKey = (*s).as_str().parse()?;
    Box::into_raw(Box::new(public_key)) as *mut RelayPublicKey
}

/// Frees a public key.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_publickey_free(spk: *mut RelayPublicKey) {
    if !spk.is_null() {
        let pk = spk as *mut PublicKey;
        let _dropped = Box::from_raw(pk);
    }
}

/// Converts a public key into a string.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_publickey_to_string(spk: *const RelayPublicKey) -> RelayStr {
    let pk = spk as *const PublicKey;
    RelayStr::from_string((*pk).to_string())
}

/// Verifies a signature
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_publickey_verify(
    spk: *const RelayPublicKey,
    data: *const RelayBuf,
    sig: *const RelayStr,
) -> bool {
    let pk = spk as *const PublicKey;
    (*pk).verify((*data).as_bytes(), (*sig).as_str())
}

/// Verifies a signature
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_publickey_verify_timestamp(
    spk: *const RelayPublicKey,
    data: *const RelayBuf,
    sig: *const RelayStr,
    max_age: u32,
) -> bool {
    let pk = spk as *const PublicKey;
    let max_age = Some(Duration::seconds(i64::from(max_age)));
    (*pk).verify_timestamp((*data).as_bytes(), (*sig).as_str(), max_age)
}

/// Parses a secret key from a string.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_secretkey_parse(s: &RelayStr) -> *mut RelaySecretKey {
    let secret_key: SecretKey = s.as_str().parse()?;
    Box::into_raw(Box::new(secret_key)) as *mut RelaySecretKey
}

/// Frees a secret key.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_secretkey_free(spk: *mut RelaySecretKey) {
    if !spk.is_null() {
        let pk = spk as *mut SecretKey;
        let _dropped = Box::from_raw(pk);
    }
}

/// Converts a secret key into a string.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_secretkey_to_string(spk: *const RelaySecretKey) -> RelayStr {
    let pk = spk as *const SecretKey;
    RelayStr::from_string((*pk).to_string())
}

/// Verifies a signature
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_secretkey_sign(
    spk: *const RelaySecretKey,
    data: *const RelayBuf,
) -> RelayStr {
    let pk = spk as *const SecretKey;
    RelayStr::from_string((*pk).sign((*data).as_bytes()))
}

/// Generates a secret, public key pair.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_generate_key_pair() -> RelayKeyPair {
    let (sk, pk) = generate_key_pair();
    RelayKeyPair {
        secret_key: Box::into_raw(Box::new(sk)) as *mut RelaySecretKey,
        public_key: Box::into_raw(Box::new(pk)) as *mut RelayPublicKey,
    }
}

/// Randomly generates an relay id
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_generate_relay_id() -> RelayUuid {
    let relay_id = generate_relay_id();
    RelayUuid::new(relay_id)
}

/// Creates a challenge from a register request and returns JSON.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_create_register_challenge(
    data: *const RelayBuf,
    signature: *const RelayStr,
    secret: *const RelayStr,
    max_age: u32,
) -> RelayStr {
    let max_age = match max_age {
        0 => None,
        m => Some(Duration::seconds(i64::from(m))),
    };

    let req =
        RegisterRequest::bootstrap_unpack((*data).as_bytes(), (*signature).as_str(), max_age)?;

    let challenge = req.into_challenge((*secret).as_str().as_bytes());
    RelayStr::from_string(serde_json::to_string(&challenge)?)
}

#[derive(Serialize)]
struct RelayRegisterResponse<'a> {
    pub relay_id: RelayId,
    pub token: &'a str,
    pub public_key: &'a PublicKey,
    pub version: RelayVersion,
}

/// Validates a register response.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_validate_register_response(
    data: *const RelayBuf,
    signature: *const RelayStr,
    secret: *const RelayStr,
    max_age: u32,
) -> RelayStr {
    let max_age = match max_age {
        0 => None,
        m => Some(Duration::seconds(i64::from(m))),
    };

    let (response, state) = RegisterResponse::unpack(
        (*data).as_bytes(),
        (*signature).as_str(),
        (*secret).as_str().as_bytes(),
        max_age,
    )?;

    let relay_response = RelayRegisterResponse {
        relay_id: response.relay_id(),
        token: response.token(),
        public_key: state.public_key(),
        version: response.version(),
    };

    let json = serde_json::to_string(&relay_response)?;
    RelayStr::from_string(json)
}

/// Returns true if the given version is supported by this library.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_version_supported(version: &RelayStr) -> bool {
    let relay_version = match version.as_str() {
        "" => RelayVersion::default(),
        s => s.parse::<RelayVersion>()?,
    };

    relay_version.supported()
}
