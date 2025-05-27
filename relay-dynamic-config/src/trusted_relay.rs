use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TrustedRelayConfig {
    #[serde(skip_serializing_if = "is_false")]
    pub verify_signature: bool,
}

fn is_false(b: &bool) -> bool {
    !b
}

#[cfg(test)]
mod tests {
    use crate::trusted_relay::TrustedRelayConfig;

    #[test]
    fn test_serialize() {
        let json = r#"{"verify_signature":true}"#;
        let result: TrustedRelayConfig = serde_json::from_str(json).unwrap();
        let serialized = serde_json::to_string(&result).unwrap();
        assert_eq!(json, serialized);
    }
}
