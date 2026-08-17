//! Emits a real sealed payload for cross-language interop checks.
//!
//! Ignored by default; run explicitly to regenerate fixtures for `scripts/decrypt-pii.py`:
//!
//! ```sh
//! cargo test -p relay-pii --test interop -- --ignored --nocapture
//! ```
#![allow(clippy::print_stdout, reason = "developer-facing fixture generator")]

use relay_event_schema::processor::{self, ProcessingState};
use relay_event_schema::protocol::Event;
use relay_pii::{ENCRYPTED_PII_KEY, EncryptProcessor, PiiConfig, PiiProcessor, generate_keypair};
use relay_protocol::{Annotated, FromValue, Value};

#[test]
#[ignore = "writes fixtures to /tmp for the python interop check"]
fn emit_interop_fixture() {
    let (public_key, secret_key) = generate_keypair();

    let config: PiiConfig = serde_json::from_value(serde_json::json!({
        "vars": {"publicKey": public_key},
        "rules": {"recoverable": {"type": "anything", "redaction": {"method": "encrypt"}}},
        "applications": {"$user.email": ["recoverable"], "$user.username": ["recoverable"]}
    }))
    .unwrap();

    let mut event = Event::from_value(
        serde_json::json!({
            "event_id": "7b9e89cf79ee451986112e0425fa9fd4",
            "user": {"email": "bruno@example.com", "username": "bruno", "id": "42"}
        })
        .into(),
    );

    let compiled = config.compiled();

    let mut encrypt = EncryptProcessor::new(compiled);
    processor::process_value(&mut event, &mut encrypt, ProcessingState::root()).unwrap();
    let sealed = encrypt.seal().unwrap().unwrap();

    let mut pii = PiiProcessor::new(compiled);
    processor::process_value(&mut event, &mut pii, ProcessingState::root()).unwrap();

    // Mirror what the server does: attach the sealed payload after scrubbing.
    event.value_mut().as_mut().unwrap().other.insert(
        ENCRYPTED_PII_KEY.to_owned(),
        Annotated::new(Value::String(sealed)),
    );

    std::fs::write("/tmp/pii-event.json", event.to_json_pretty().unwrap()).unwrap();
    std::fs::write("/tmp/pii-secret.key", secret_key).unwrap();

    println!("wrote /tmp/pii-event.json and /tmp/pii-secret.key");
}
