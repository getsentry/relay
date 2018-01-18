use std::sync::{Once, ONCE_INIT};

use base64;
use sodiumoxide;
use sodiumoxide::crypto::sign::ed25519 as sign_backend;


static INIT_SODIUMOXIDE: Once = ONCE_INIT;


fn with_sodiumoxide<T, F: FnOnce() -> T>(cb: F) -> T {
    INIT_SODIUMOXIDE.call_once(|| {
        sodiumoxide::init();
    });
    cb()
}


/// Represents the public key of an agent.
#[derive(Debug)]
pub struct PublicKey {
    inner: sign_backend::PublicKey,
}

/// Represents the secret key of an agent.
#[derive(Debug)]
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

impl PublicKey {
    /// Verifies a signature.
    pub fn verify(&self, data: &[u8], sig: &str) -> bool {
        let mut sig_arr = [0u8; sign_backend::SIGNATUREBYTES];
        let sig_bytes = match base64::decode_config(sig, base64::URL_SAFE_NO_PAD) {
            Ok(bytes) => bytes,
            _ => return false
        };
        if sig_bytes.len() != sig_arr.len() {
            return false;
        }
        sig_arr.clone_from_slice(&sig_bytes);
        sign_backend::verify_detached(&sign_backend::Signature(sig_arr),
                                      data, &self.inner)
    }
}

/// Generates a secret + public key pair.
pub fn generate_key_pair() -> (SecretKey, PublicKey) {
    with_sodiumoxide(|| {
        let (pk, sk) = sign_backend::gen_keypair();
        (SecretKey { inner: sk }, PublicKey { inner: pk })
    })
}

#[test]
fn test_signatures() {
    let (sk, pk) = generate_key_pair();
    let data = b"Hello World!";

    let mut sig = sk.sign(data);
    assert_eq!(pk.verify(data, &sig), true);

    let bad_sig = "jgubwSf2wb2wuiRpgt2H9_bdDSMr88hXLp5zVuhbr65EGkSxOfT5ILIWr623twLgLd0bDgHg6xzOaUCX7XvUCw";
    assert_eq!(pk.verify(data, &bad_sig), false);
}
