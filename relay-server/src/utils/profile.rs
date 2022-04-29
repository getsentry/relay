use failure::Fail;
use std::collections::HashMap;

use android_trace_log::AndroidTraceLog;
use serde::{de, Deserialize, Serialize};
use uuid::Uuid;

use crate::envelope::{ContentType, Item};

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
    #[fail(display = "not enough samples")]
    NotEnoughSamples,
    #[fail(display = "platform not supported")]
    PlatformNotSupported,
}

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
    build_id: Option<Uuid>,
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
    profile_id: Uuid,
    trace_id: Uuid,
    transaction_id: Uuid,
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

pub fn parse_android_profile(item: &mut Item) -> Result<(), ProfileError> {
    let mut profile: AndroidProfile =
        serde_json::from_slice(&item.payload()).map_err(ProfileError::InvalidJson)?;

    profile.parse()?;

    if profile.profile.as_ref().unwrap().events.len() < 2 {
        return Err(ProfileError::NotEnoughSamples);
    }

    match serde_json::to_vec(&profile) {
        Ok(payload) => item.set_payload(ContentType::Json, &payload[..]),
        Err(_) => return Err(ProfileError::CannotSerializePayload),
    };

    Ok(())
}

fn strip_pointer_authentication_code<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    let address_without_prefix = s.trim_start_matches("0x");
    match u64::from_str_radix(address_without_prefix, 16) {
        // https://github.com/microsoft/plcrashreporter/blob/748087386cfc517936315c107f722b146b0ad1ab/Source/PLCrashAsyncThread_arm.c#L84
        Ok(address) => Ok(format!("{:#x}", address & 0x0000000FFFFFFFFF)),
        Err(err) => Err(de::Error::custom(format!(
            "failed to strip pointer authentication code: {}",
            err,
        ))),
    }
}

pub fn deserialize_u64_from_string<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    match s.parse::<u64>() {
        Ok(n) => Ok(n),
        Err(err) => Err(serde::de::Error::custom(format!(
            "failed to deserialize u64: {}",
            err,
        ))),
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Frame {
    function: String,

    #[serde(deserialize_with = "strip_pointer_authentication_code")]
    instruction_addr: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Sample {
    frames: Vec<Frame>,
    queue_address: Option<String>,

    #[serde(deserialize_with = "deserialize_u64_from_string")]
    relative_timestamp_ns: u64,

    thread_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ThreadMetadata {
    name: String,
    priority: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct QueueMetadata {
    label: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct SampledProfile {
    samples: Vec<Sample>,
    thread_metadata: HashMap<String, ThreadMetadata>,
    queue_metadata: HashMap<String, QueueMetadata>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Image {
    image_addr: String,
    image_size: u32,
    image_vmaddr: String,
    name: String,

    #[serde(rename = "type")]
    image_type: String,

    #[serde(rename(serialize = "debug_id"))]
    uuid: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
struct DebugMeta {
    images: Image,
}

#[derive(Debug, Serialize, Deserialize)]
struct CocoaProfile {
    debug_meta: DebugMeta,
    device_is_emulator: bool,
    device_locale: String,
    device_manufacturer: String,
    device_model: String,
    device_os_build_number: String,
    device_os_name: String,
    device_os_version: String,
    duration_ns: String,
    environment: Option<String>,
    platform: String,
    profile_id: Uuid,
    sampled_profile: SampledProfile,
    trace_id: Uuid,
    transaction_id: Uuid,
    transaction_name: String,
    version_code: String,
    version_name: String,
}

pub fn parse_cocoa_profile(item: &mut Item) -> Result<(), ProfileError> {
    let profile: CocoaProfile =
        serde_json::from_slice(&item.payload()).map_err(ProfileError::InvalidJson)?;

    if profile.sampled_profile.samples.len() < 2 {
        return Err(ProfileError::NotEnoughSamples);
    }

    match serde_json::to_vec(&profile) {
        Ok(payload) => item.set_payload(ContentType::Json, &payload[..]),
        Err(_) => return Err(ProfileError::CannotSerializePayload),
    };

    Ok(())
}

pub fn parse_typescript_profile(item: &mut Item) -> Result<(), ProfileError> {
    let profile: Vec<serde_json::Value> =
        serde_json::from_slice(&item.payload()).map_err(ProfileError::InvalidJson)?;
    if profile.is_empty() {
        return Err(ProfileError::NotEnoughSamples);
    }

    Ok(())
}
