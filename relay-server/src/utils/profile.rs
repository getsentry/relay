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
    device_locale: String,
    device_manufacturer: String,
    device_model: String,
    device_os_name: String,
    device_os_version: String,
    duration_ns: String,
    environment: String,
    platform: String,
    stacktrace: String,
    stacktrace_id: String,
    trace_id: String,
    transaction_id: String,
    transaction_name: String,
    version_code: String,
    version_name: String,

    #[serde(default)]
    android_trace: Option<AndroidTraceLog>,
}

impl AndroidProfile {
    fn parse_stack_trace(&mut self) -> Result<(), ProfileError> {
        let stacktrace_bytes = match base64::decode(&self.stacktrace) {
            Ok(stacktrace) => stacktrace,
            Err(_) => return Err(ProfileError::InvalidBase64Value),
        };
        self.android_trace = match android_trace_log::parse(&stacktrace_bytes) {
            Ok(trace) => Some(trace),
            Err(_) => return Err(ProfileError::InvalidStackTrace),
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
    #[fail(display = "invalid stack trace in profile")]
    InvalidStackTrace,
    #[fail(display = "cannot serialize payload")]
    CannotSerializePayload,
}

pub fn parse_android_profile(item: &mut Item) -> Result<(), ProfileError> {
    let mut profile: AndroidProfile =
        serde_json::from_slice(&item.payload()).map_err(ProfileError::InvalidJson)?;

    profile.parse_stack_trace()?;

    match serde_json::to_vec(&profile) {
        Ok(payload) => item.set_payload(ContentType::Json, &payload[..]),
        Err(_) => return Err(ProfileError::CannotSerializePayload),
    };

    Ok(())
}
