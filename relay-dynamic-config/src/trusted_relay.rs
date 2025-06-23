use serde::{Deserialize, Serialize};

/// Configuration to control communication from trusted relays.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TrustedRelayConfig {
    /// Checks the signature of an event and rejects it if enabled.
    #[serde(skip_serializing_if = "SignatureVerification::is_default")]
    pub verify_signature: SignatureVerification,
}

impl TrustedRelayConfig {
    /// Checks whether the config can be considered empty.
    ///
    /// Empty here means that all values are equal to their default values.
    pub fn is_empty(&self) -> bool {
        self.verify_signature == SignatureVerification::default()
    }
}

/// Types of verification that can be performed on the signature.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SignatureVerification {
    /// Checks the signature for validity and verifies that the embedded timestamp
    /// is not too old.
    WithTimestamp,
    /// Does not perform any validation on the signature.
    #[default]
    Disabled,
}

impl SignatureVerification {
    /// Checks if it is the default variant.
    pub fn is_default(&self) -> bool {
        *self == SignatureVerification::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_with_timestamp() {
        let json = r#"{"verifySignature":"withTimestamp"}"#;
        let result: TrustedRelayConfig = serde_json::from_str(json).unwrap();
        let serialized = serde_json::to_string(&result).unwrap();
        assert_eq!(json, serialized);
    }

    #[test]
    fn test_with_timestamp() {
        let json = r#"{"verifySignature":"withTimestamp"}"#;
        let result: TrustedRelayConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            result.verify_signature,
            SignatureVerification::WithTimestamp
        );
    }

    #[test]
    fn test_disabled() {
        let json = r#"{"verifySignature":"disabled"}"#;
        let result: TrustedRelayConfig = serde_json::from_str(json).unwrap();
        assert_eq!(result.verify_signature, SignatureVerification::Disabled);
    }

    #[test]
    fn test_default() {
        let config = TrustedRelayConfig::default();
        assert_eq!(config.verify_signature, SignatureVerification::Disabled);
    }

    #[test]
    fn test_default_serialize() {
        let serialized = serde_json::to_string(&TrustedRelayConfig::default()).unwrap();
        assert_eq!(serialized, r#"{}"#);
    }
}
