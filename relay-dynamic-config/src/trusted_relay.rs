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
