use failure::Fail;

use android_trace_log::AndroidTraceLog;
use serde::{Deserialize, Serialize};

use crate::envelope::{ContentType, Item};

#[derive(Debug, Deserialize)]
pub struct MinimalProfile {
    #[serde(default)]
    pub platform: String,
}

pub fn minimal_profile_from_json(data: &[u8]) -> Result<MinimalProfile, ProfileError> {
    serde_json::from_slice(data).map_err(ProfileError::InvalidJson)
}

#[derive(Debug, Serialize, Deserialize)]
struct AndroidProfile {
    android_api_level: u16,
    build_id: String,
    device_cpu_frequencies: Vec<u32>,
    device_is_emulator: bool,
    device_locale: String,
    device_manufacturer: String,
    device_model: String,
    device_os_name: String,
    device_os_version: String,
    device_physical_memory_bytes: String,
    duration_ns: String,
    environment: Option<String>,
    platform: String,
    profile_id: String,
    trace_id: String,
    transaction_id: String,
    transaction_name: String,
    version_code: String,
    version_name: String,

    #[serde(skip_serializing)]
    sampled_profile: String,
    #[serde(skip_deserializing, default)]
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

#[derive(Debug, Fail)]
pub enum ProfileError {
    #[fail(display = "invalid json in profile")]
    InvalidJson(#[cause] serde_json::Error),
    #[fail(display = "invalid base64 value")]
    InvalidBase64Value,
    #[fail(display = "invalid sampled profile")]
    InvalidSampledProfile,
    #[fail(display = "cannot serialize payload")]
    CannotSerializePayload,
}

pub fn parse_android_profile(item: &mut Item) -> Result<(), ProfileError> {
    let mut profile: AndroidProfile =
        serde_json::from_slice(&item.payload()).map_err(ProfileError::InvalidJson)?;

    profile.parse()?;

    match serde_json::to_vec(&profile) {
        Ok(payload) => item.set_payload(ContentType::Json, &payload[..]),
        Err(_) => return Err(ProfileError::CannotSerializePayload),
    };

    Ok(())
}
