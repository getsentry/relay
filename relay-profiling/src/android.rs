use android_trace_log::AndroidTraceLog;
use serde::{Deserialize, Serialize};

use relay_general::protocol::EventId;

use crate::utils::deserialize_number_from_string;
use crate::ProfileError;

#[derive(Debug, Serialize, Deserialize)]
struct AndroidProfile {
    android_api_level: u16,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    build_id: String,

    device_cpu_frequencies: Vec<u32>,
    device_is_emulator: bool,
    device_locale: String,
    device_manufacturer: String,
    device_model: String,
    device_os_name: String,
    device_os_version: String,

    #[serde(deserialize_with = "deserialize_number_from_string")]
    device_physical_memory_bytes: u64,

    #[serde(deserialize_with = "deserialize_number_from_string")]
    duration_ns: u64,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    environment: String,

    platform: String,
    profile_id: EventId,
    trace_id: EventId,
    transaction_id: EventId,
    transaction_name: String,
    version_code: String,
    version_name: String,

    #[serde(default, skip_serializing)]
    sampled_profile: String,

    #[serde(default)]
    profile: Option<AndroidTraceLog>,
}

impl AndroidProfile {
    fn parse(&mut self) -> Result<(), ProfileError> {
        let profile_bytes = match base64::decode(&self.sampled_profile) {
            Ok(profile) => profile,
            Err(_) => return Err(ProfileError::InvalidBase64Value),
        };
        self.profile = match android_trace_log::parse(&profile_bytes) {
            Ok(profile) => Some(profile),
            Err(_) => return Err(ProfileError::InvalidSampledProfile),
        };
        Ok(())
    }
}

pub fn parse_android_profile(payload: &[u8]) -> Result<Vec<u8>, ProfileError> {
    let mut profile: AndroidProfile =
        serde_json::from_slice(payload).map_err(ProfileError::InvalidJson)?;

    if profile.sampled_profile.is_empty() {
        return Ok(Vec::new());
    }

    profile.parse()?;

    if profile.profile.as_ref().unwrap().events.len() < 2 {
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
    fn test_roundtrip_android() {
        let payload = include_bytes!("../tests/fixtures/profiles/android.json");
        let data = parse_android_profile(payload);
        assert!(data.is_ok());
        assert!(parse_android_profile(&(data.unwrap())[..]).is_ok());
    }
}
