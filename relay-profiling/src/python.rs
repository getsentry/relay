use relay_general::protocol::EventId;
use serde::{Deserialize, Serialize};

use crate::utils::deserialize_number_from_string;
use crate::ProfileError;

#[derive(Debug, Serialize, Deserialize)]
struct Frame {
    line: u32,
    name: String,
    file: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Sample {
    frames: Vec<u32>,

    #[serde(deserialize_with = "deserialize_number_from_string")]
    relative_timestamp_ns: u64,

    #[serde(deserialize_with = "deserialize_number_from_string")]
    thread_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Profile {
    samples: Vec<Sample>,
    frames: Vec<Frame>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PythonProfile {
    device_os_name: String,
    device_os_version: String,

    #[serde(deserialize_with = "deserialize_number_from_string")]
    duration_ns: u64,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    environment: String,

    platform: String,
    platform_version: String,
    profile_id: EventId,
    profile: Profile,
    trace_id: EventId,
    transaction_id: EventId,
    transaction_name: String,
    version_code: String,
    version_name: String,
}

pub fn parse_python_profile(payload: &[u8]) -> Result<Vec<u8>, ProfileError> {
    let profile: PythonProfile =
        serde_json::from_slice(payload).map_err(ProfileError::InvalidJson)?;

    if profile.profile.samples.len() < 2 {
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
    fn test_roundtrip_python() {
        let payload = include_bytes!("../tests/fixtures/profiles/python.json");
        let data = parse_python_profile(payload);
        assert!(data.is_ok());
        assert!(parse_python_profile(&data.unwrap()[..]).is_ok());
    }
}
