use smith_aorta::{PublicKey, SecretKey, generate_key_pair};

use core::{SmithBuf, SmithStr};

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
