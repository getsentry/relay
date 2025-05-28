use serde::{Deserialize, Serialize};

/// Configuration to control communication from trusted relays.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TrustedRelayConfig {
    /// Checks the signature of an event and rejects it if enabled.
    #[serde(skip_serializing_if = "is_false")]
    pub verify_signature: bool,
}

impl TrustedRelayConfig {
    pub fn is_empty(&self) -> bool {
        !self.verify_signature
    }
}

fn is_false(b: &bool) -> bool {
    !b
}

#[cfg(test)]
mod tests {
    use crate::trusted_relay::TrustedRelayConfig;

    #[test]
    fn test_serialize() {
        let json = r#"{"verifySignature":true}"#;
        let result: TrustedRelayConfig = serde_json::from_str(json).unwrap();
        let serialized = serde_json::to_string(&result).unwrap();
        assert_eq!(json, serialized);
    }

    #[test]
    fn test_default() {
        let config = TrustedRelayConfig::default();
        assert_eq!(config.verify_signature, false);
    }

    #[test]
    fn test_default_serialize() {
        let serialized = serde_json::to_string(&TrustedRelayConfig::default()).unwrap();
        assert_eq!(serialized, r#"{}"#);
    }
}
