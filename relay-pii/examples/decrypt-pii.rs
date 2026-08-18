//! Unseals an `_encrypted_pii` payload with the org's secret key.
//!
//! Relay only ever holds the public half, so this is the other side of `EncryptProcessor::seal`:
//! the org supplies the secret key it kept from `process-event --keygen` and gets the original,
//! unscrubbed values back as a JSON object keyed by event path.
//!
//! Usage:
//!
//! ```text
//! cargo run -p relay-pii --example decrypt-pii -- <secret-key-file|secret-key> [ciphertext]
//! ```
//!
//! The key may be a file path or the base64 key itself. The ciphertext is read from stdin when
//! it is not given as an argument.

use std::collections::BTreeMap;
use std::io::Read;
use std::{env, fs, process};

fn main() {
    let mut args = env::args().skip(1);

    let Some(key_arg) = args.next() else {
        eprintln!("usage: decrypt-pii <secret-key-file|secret-key> [ciphertext]");
        process::exit(2);
    };

    let ciphertext = match args.next() {
        Some(arg) => arg,
        None => {
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf).expect("read stdin");
            buf
        }
    };

    // Accept either a path to a key file or the base64 key itself, so a key pasted from a
    // terminal scrollback works without a detour through a temp file.
    let key_b64 = match fs::read_to_string(&key_arg) {
        Ok(contents) => contents,
        Err(_) => key_arg.clone(),
    };
    let key_bytes = data_encoding::BASE64
        .decode(key_b64.trim().as_bytes())
        .expect("secret key is not valid base64");
    let secret = crypto_box::SecretKey::from_slice(&key_bytes).expect("secret key is not 32 bytes");

    // The public half is printed so a wrong-key failure is easy to tell apart from a corrupt
    // ciphertext: compare it against `vars.publicKey` in the project's PII config.
    eprintln!(
        "public key for this secret: {}",
        data_encoding::BASE64.encode(secret.public_key().as_bytes())
    );

    let sealed = data_encoding::BASE64
        .decode(ciphertext.trim().as_bytes())
        .expect("ciphertext is not valid base64");

    let opened = match secret.unseal(&sealed) {
        Ok(opened) => opened,
        Err(_) => {
            eprintln!("failed to unseal: wrong secret key, or the ciphertext is truncated");
            process::exit(1);
        }
    };

    let values: BTreeMap<String, String> =
        serde_json::from_slice(&opened).expect("sealed payload is not a JSON string map");

    println!(
        "{}",
        serde_json::to_string_pretty(&values).expect("serialize")
    );
}
