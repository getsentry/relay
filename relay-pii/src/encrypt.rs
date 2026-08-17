//! Encryption of PII values against an org-held public key.
//!
//! Scrubbing destroys data. Encryption keeps it, but only for whoever holds the private key. This
//! module implements the second option as a strict *addition* to the first: values matched by an
//! `encrypt` rule are still scrubbed in place exactly as before, and the original is collected
//! separately, sealed against the org's public key, and attached to the event under
//! `_encrypted_pii`.
//!
//! Keeping the ciphertext out of the event body is what makes this safe to do with ordinary
//! randomized encryption. If the ciphertext replaced the value in place, every occurrence of the
//! same value would look different to everything downstream, and anything that counts distinct
//! values -- unique users, most notably -- would silently start counting events instead. Because the
//! scrubbed field keeps whatever it had before, none of that changes.
//!
//! Relay only ever holds the public half of the keypair, so it can write these values and never read
//! them back.

use std::collections::BTreeMap;

use crypto_box::PublicKey;
use crypto_box::aead::OsRng;
use relay_event_schema::processor::{ProcessingResult, ProcessingState, Processor};
use relay_protocol::Meta;

use crate::compiledconfig::CompiledPiiConfig;
use crate::redactions::Redaction;

/// Key under which the sealed payload is attached to the event.
pub const ENCRYPTED_PII_KEY: &str = "_encrypted_pii";

/// Failure to seal collected values against a public key.
#[derive(Debug, thiserror::Error)]
pub enum EncryptError {
    /// The config carried no usable public key.
    ///
    /// Either none was configured, or it failed to decode when the config was compiled.
    #[error("no valid public key configured")]
    MissingPublicKey,

    /// The collected values could not be serialized.
    #[error("failed to serialize collected values")]
    Serialize(#[source] serde_json::Error),

    /// The sealing operation itself failed.
    #[error("failed to seal values")]
    Seal,
}

/// Collects the full, unscrubbed values of every field targeted by an `encrypt` rule.
///
/// Run this over the event *before* [`PiiProcessor`](crate::PiiProcessor), while the originals are
/// still present. It only reads; the event is left untouched.
///
/// Unlike scrubbing -- which usually replaces just the matched substring -- this captures the whole
/// field value. That means the sealed payload can contain more than the PII that triggered it (the
/// entire log message, not only the email address inside it), which is deliberate: it gives the org
/// back something readable, and it keeps the client side a simple map lookup instead of splicing
/// fragments back in at recorded offsets.
pub struct EncryptProcessor<'a> {
    compiled_config: &'a CompiledPiiConfig,
    collected: BTreeMap<String, String>,
}

impl<'a> EncryptProcessor<'a> {
    /// Creates a new processor based on a compiled config.
    pub fn new(compiled_config: &'a CompiledPiiConfig) -> Self {
        Self {
            compiled_config,
            collected: BTreeMap::new(),
        }
    }

    /// Returns `true` if this config has any `encrypt` rule and a key to seal with.
    ///
    /// Use this to skip the extra walk over the event entirely, which is the common case.
    pub fn is_enabled(compiled_config: &CompiledPiiConfig) -> bool {
        compiled_config.public_key.is_some()
            && compiled_config
                .applications
                .iter()
                .any(|(_, rules)| rules.iter().any(|r| r.redaction == Redaction::Encrypt))
    }

    /// Seals everything collected so far, returning a base64-encoded sealed box.
    ///
    /// Returns `Ok(None)` when nothing was collected, so callers can skip attaching an empty
    /// payload.
    pub fn seal(&self) -> Result<Option<String>, EncryptError> {
        if self.collected.is_empty() {
            return Ok(None);
        }

        let key = self
            .compiled_config
            .public_key
            .ok_or(EncryptError::MissingPublicKey)?;

        let plaintext = serde_json::to_vec(&self.collected).map_err(EncryptError::Serialize)?;

        // A libsodium-compatible sealed box: an ephemeral keypair is generated per call, so the
        // same input yields different output every time, and the ephemeral secret is discarded
        // immediately. Nothing here can reverse it.
        let sealed = PublicKey::from(key)
            .seal(&mut OsRng, &plaintext)
            .map_err(|_| EncryptError::Seal)?;

        Ok(Some(data_encoding::BASE64.encode(&sealed)))
    }

    /// Returns `true` if any `encrypt` rule applies at the current path.
    fn should_encrypt(&self, state: &ProcessingState<'_>) -> bool {
        self.compiled_config
            .applications
            .iter()
            .filter(|(selector, _)| selector.matches_path(&state.path()))
            .any(|(_, rules)| rules.iter().any(|r| r.redaction == Redaction::Encrypt))
    }
}

impl Processor for EncryptProcessor<'_> {
    fn process_string(
        &mut self,
        value: &mut String,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        // Mirrors `PiiProcessor::process_string`: these never carry PII and would only bloat the
        // payload.
        if let "" | "true" | "false" | "null" | "undefined" = value.as_str() {
            return Ok(());
        }

        if self.should_encrypt(state) {
            self.collected
                .insert(state.path().to_string(), value.clone());
        }

        Ok(())
    }
}

/// Generates a fresh X25519 keypair, returning `(public, secret)` base64-encoded.
///
/// Provided so the org can produce a keypair without extra tooling; the secret half must never be
/// given to Relay.
pub fn generate_keypair() -> (String, String) {
    let secret = crypto_box::SecretKey::generate(&mut OsRng);
    let public = secret.public_key();

    (
        data_encoding::BASE64.encode(public.as_bytes()),
        data_encoding::BASE64.encode(&secret.to_bytes()),
    )
}

#[cfg(test)]
mod tests {
    use relay_event_schema::processor::process_value;
    use relay_event_schema::protocol::Event;
    use relay_protocol::{Annotated, FromValue};
    use serde_json::json;

    use super::*;
    use crate::{PiiConfig, PiiProcessor};

    /// Builds a config that encrypts `extra` and scrubs the user's email.
    fn config(public_key: &str) -> PiiConfig {
        serde_json::from_value(json!({
            "vars": {"publicKey": public_key},
            "rules": {
                "keep_it": {"type": "anything", "redaction": {"method": "encrypt"}}
            },
            "applications": {
                "$string": ["keep_it"]
            }
        }))
        .unwrap()
    }

    fn event() -> Annotated<Event> {
        Event::from_value(
            json!({
                "message": "login failed for bruno@example.com",
                "extra": {"card": "4111111111111111"}
            })
            .into(),
        )
    }

    #[test]
    fn test_roundtrip() {
        let (public_key, secret_key) = generate_keypair();
        let config = config(&public_key);
        let compiled = config.compiled();
        assert!(EncryptProcessor::is_enabled(compiled));

        let mut event = event();

        // Capture before scrubbing, while the originals are intact.
        let mut encrypt = EncryptProcessor::new(compiled);
        process_value(&mut event, &mut encrypt, ProcessingState::root()).unwrap();
        let sealed = encrypt.seal().unwrap().expect("something was collected");

        // Scrubbing still runs and still destroys the values in place.
        let mut pii = PiiProcessor::new(compiled);
        process_value(&mut event, &mut pii, ProcessingState::root()).unwrap();
        let scrubbed = event.to_json().unwrap();
        assert!(!scrubbed.contains("bruno@example.com"));
        assert!(!scrubbed.contains("4111111111111111"));

        // The org, holding the secret key, gets the originals back.
        let secret = crypto_box::SecretKey::from_slice(
            &data_encoding::BASE64.decode(secret_key.as_bytes()).unwrap(),
        )
        .unwrap();
        let opened = secret
            .unseal(&data_encoding::BASE64.decode(sealed.as_bytes()).unwrap())
            .unwrap();
        let recovered: BTreeMap<String, String> = serde_json::from_slice(&opened).unwrap();

        assert_eq!(
            recovered.get("logentry.formatted").map(String::as_str),
            Some("login failed for bruno@example.com")
        );
        assert_eq!(
            recovered.get("extra.card").map(String::as_str),
            Some("4111111111111111")
        );
    }

    #[test]
    fn test_nondeterministic() {
        let (public_key, _) = generate_keypair();
        let config = config(&public_key);

        let seal_once = || {
            let mut event = event();
            let mut encrypt = EncryptProcessor::new(config.compiled());
            process_value(&mut event, &mut encrypt, ProcessingState::root()).unwrap();
            encrypt.seal().unwrap().unwrap()
        };

        // Identical input, different ciphertext: no equality leak across events.
        assert_ne!(seal_once(), seal_once());
    }

    #[test]
    fn test_disabled_without_key() {
        let config: PiiConfig = serde_json::from_value(json!({
            "rules": {"keep_it": {"type": "anything", "redaction": {"method": "encrypt"}}},
            "applications": {"$string": ["keep_it"]}
        }))
        .unwrap();

        assert!(!EncryptProcessor::is_enabled(config.compiled()));
    }

    #[test]
    fn test_disabled_without_encrypt_rule() {
        let (public_key, _) = generate_keypair();
        let config: PiiConfig = serde_json::from_value(json!({
            "vars": {"publicKey": public_key},
            "applications": {"$string": ["@anything:remove"]}
        }))
        .unwrap();

        assert!(!EncryptProcessor::is_enabled(config.compiled()));
    }

    #[test]
    fn test_invalid_key_is_ignored() {
        let config = config("not-base64!!");
        assert!(!EncryptProcessor::is_enabled(config.compiled()));
    }
}
