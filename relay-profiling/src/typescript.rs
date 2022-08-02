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
    use serde::Deserialize;

    use crate::parse_typescript_profile;
    use crate::ProfileError;

    #[test]
    fn test_roundtrip_typescript() {
        let payload = include_bytes!("../tests/fixtures/profiles/typescript.json");
        let data = parse_typescript_profile(payload);
        assert!(data.is_ok());
        assert!(parse_typescript_profile(&data.unwrap()[..]).is_ok());
    }

    #[derive(Debug, Deserialize)]
    struct MinimalTypescriptProfile {
        duration_ns: u64,
    }

    fn minimal_profile_from_json(data: &[u8]) -> Result<MinimalTypescriptProfile, ProfileError> {
        serde_json::from_slice(data).map_err(ProfileError::InvalidJson)
    }

    #[test]
    fn test_normalization() {
        let payload = r#"
        {
            "profile": [
                {
                    "name": "process_name",
                    "args": {
                        "name": "tsc"
                    },
                    "cat": "__metadata",
                    "ph": "M",
                    "ts": 189644.04201507568,
                    "pid": 1,
                    "tid": 1
                }
            ],
            "device_locale": "en_CA.UTF-8",
            "device_manufacturer": "GitHub",
            "device_model": "GitHub Actions",
            "device_os_name": "darwin",
            "device_os_version": "21.4.0",
            "device_is_emulator": false,
            "transaction_name": "typescript.compile",
            "version_code": "1",
            "version_name": "0.1",
            "duration_ns": "87880000000",
            "trace_id": "a882b1448a1c446a910063f6e9e374c2",
            "transaction_id": "616fd15cb7ff4143a5d8d8e1cb74d141",
            "platform": "typescript",
            "environment": "ci",
            "profile_id": "74c735d8d1f342559bf6b387703c2fd6"
        }
        "#;
        let data = parse_typescript_profile(payload.as_bytes());
        let minimal_profile = minimal_profile_from_json(&data.unwrap()[..]);
        assert!(minimal_profile.is_ok());
        assert!(minimal_profile.unwrap().duration_ns == 87880000000);
    }
}
