use serde::{Deserialize, Serialize};

use relay_general::protocol::EventId;

use crate::utils::deserialize_number_from_string;
use crate::ProfileError;

#[derive(Debug, Serialize, Deserialize)]
struct TypescriptProfile {
    device_is_emulator: bool,
    device_locale: String,
    device_manufacturer: String,
    device_model: String,
    device_os_build_number: Option<String>,
    device_os_name: String,
    device_os_version: String,

    #[serde(deserialize_with = "deserialize_number_from_string")]
    duration_ns: u64,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    environment: String,

    platform: String,
    profile_id: EventId,
    profile: Vec<serde_json::Value>,
    trace_id: EventId,
    transaction_id: EventId,
    transaction_name: String,
    version_code: String,
    version_name: String,
}

pub fn parse_typescript_profile(payload: &[u8]) -> Result<Vec<u8>, ProfileError> {
    let profile: TypescriptProfile =
        serde_json::from_slice(payload).map_err(ProfileError::InvalidJson)?;

    if profile.profile.is_empty() {
        return Err(ProfileError::NotEnoughSamples);
    }

    match serde_json::to_vec(&profile) {
        Ok(payload) => Ok(payload),
        Err(_) => Err(ProfileError::CannotSerializePayload),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_typescript() {
        let payload = include_bytes!("../tests/fixtures/profiles/typescript.json");
        let data = parse_typescript_profile(payload);
        assert!(data.is_ok());
        assert!(parse_typescript_profile(&data.unwrap()[..]).is_ok());
    }
}
