//! End-to-end check that `encrypt` rules preserve scrubbing behaviour while making the original
//! values recoverable with the private key.

use std::collections::BTreeMap;

use relay_event_schema::processor::{self, ProcessingState};
use relay_event_schema::protocol::Event;
use relay_pii::{EncryptProcessor, PiiConfig, PiiProcessor, generate_keypair};
use relay_protocol::FromValue;

/// Decrypts a sealed payload the way an org would, returning the recovered path -> value map.
fn unseal(secret_key: &str, sealed: &str) -> BTreeMap<String, String> {
    let secret = crypto_box::SecretKey::from_slice(
        &data_encoding::BASE64.decode(secret_key.as_bytes()).unwrap(),
    )
    .unwrap();
    let opened = secret
        .unseal(&data_encoding::BASE64.decode(sealed.as_bytes()).unwrap())
        .unwrap();
    serde_json::from_slice(&opened).unwrap()
}

#[test]
fn test_encrypt_preserves_scrubbing() {
    let (public_key, secret_key) = generate_keypair();

    // Encrypt the user's email, but keep scrubbing the credit card in `extra` the old way. The two
    // redactions coexist in one config.
    let config: PiiConfig = serde_json::from_value(serde_json::json!({
        "vars": {"publicKey": public_key},
        "rules": {
            "recoverable": {"type": "anything", "redaction": {"method": "encrypt"}}
        },
        "applications": {
            "$user.email": ["recoverable"],
            "extra.**": ["@creditcard:mask"]
        }
    }))
    .unwrap();

    let mut event = Event::from_value(
        serde_json::json!({
            "event_id": "7b9e89cf79ee451986112e0425fa9fd4",
            "user": {"email": "bruno@example.com", "id": "42"},
            "extra": {"card": "4111111111111111"}
        })
        .into(),
    );

    let compiled = config.compiled();
    assert!(EncryptProcessor::is_enabled(compiled));

    let mut encrypt = EncryptProcessor::new(compiled);
    processor::process_value(&mut event, &mut encrypt, ProcessingState::root()).unwrap();
    let sealed = encrypt.seal().unwrap().expect("email was collected");

    let mut pii = PiiProcessor::new(compiled);
    processor::process_value(&mut event, &mut pii, ProcessingState::root()).unwrap();

    let json = event.to_json().unwrap();

    // The email is gone from the event body, replaced by a placeholder.
    assert!(!json.contains("bruno@example.com"));
    assert!(json.contains("[Encrypted]"));

    // The unrelated masking rule is untouched by any of this.
    assert!(!json.contains("4111111111111111"));
    assert!(json.contains("****************"));

    // Only the field the encrypt rule targeted was captured -- not the whole event.
    let recovered = unseal(&secret_key, &sealed);
    assert_eq!(
        recovered.get("user.email").map(String::as_str),
        Some("bruno@example.com")
    );
    assert_eq!(recovered.len(), 1, "recovered: {recovered:?}");
}

#[test]
fn test_encrypt_without_key_still_scrubs() {
    // Same rule, no public key: the value must still be destroyed. Failing open here would be the
    // worst possible outcome, so it is worth asserting explicitly.
    let config: PiiConfig = serde_json::from_value(serde_json::json!({
        "rules": {"recoverable": {"type": "anything", "redaction": {"method": "encrypt"}}},
        "applications": {"$user.email": ["recoverable"]}
    }))
    .unwrap();

    let mut event =
        Event::from_value(serde_json::json!({"user": {"email": "bruno@example.com"}}).into());

    let compiled = config.compiled();
    assert!(!EncryptProcessor::is_enabled(compiled));

    let mut pii = PiiProcessor::new(compiled);
    processor::process_value(&mut event, &mut pii, ProcessingState::root()).unwrap();

    let json = event.to_json().unwrap();
    assert!(!json.contains("bruno@example.com"));
    assert!(json.contains("[Encrypted]"));
}

#[test]
fn test_unknown_method_fails_closed() {
    // An older Relay that predates `encrypt` deserializes the method as `Redaction::Other`. It must
    // still destroy the value rather than pass it through, otherwise rolling this out to a mixed
    // fleet would leak PII through the older hop.
    let config: PiiConfig = serde_json::from_value(serde_json::json!({
        "rules": {"future": {"type": "anything", "redaction": {"method": "not_yet_invented"}}},
        "applications": {"$user.email": ["future"]}
    }))
    .unwrap();

    let mut event =
        Event::from_value(serde_json::json!({"user": {"email": "bruno@example.com"}}).into());

    let mut pii = PiiProcessor::new(config.compiled());
    processor::process_value(&mut event, &mut pii, ProcessingState::root()).unwrap();

    assert!(!event.to_json().unwrap().contains("bruno@example.com"));
}
