//! Android Format
//!
//! Relay is expecting a JSON object with some mandatory metadata and a `sampled_profile` key
//! containing the raw Android profile.
//!
//! `android` has a specific binary representation of its profile and Relay is responsible to
//! unpack it before it's forwarded down the line.
//!
use std::collections::HashMap;

use android_trace_log::chrono::Utc;
use android_trace_log::{AndroidTraceLog, Clock, Vm};
use data_encoding::BASE64_NOPAD;
use relay_event_schema::protocol::EventId;
use serde::{Deserialize, Serialize};

use crate::debug_image::get_proguard_image;
use crate::measurements::ChunkMeasurement;
use crate::sample::v2::ProfileData;
use crate::types::{ClientSdk, DebugMeta};
use crate::{MAX_PROFILE_CHUNK_DURATION, ProfileError};

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    build_id: String,
    chunk_id: EventId,
    profiler_id: EventId,

    client_sdk: ClientSdk,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    environment: String,
    platform: String,
    release: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    debug_meta: Option<DebugMeta>,

    #[serde(default)]
    duration_ns: u64,
    timestamp: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Chunk {
    #[serde(flatten)]
    metadata: Metadata,

    #[serde(default, skip_serializing)]
    sampled_profile: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    js_profile: Option<ProfileData>,

    #[serde(default = "Chunk::default")]
    profile: AndroidTraceLog,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    measurements: Option<HashMap<String, ChunkMeasurement>>,
}

impl Chunk {
    fn default() -> AndroidTraceLog {
        AndroidTraceLog {
            data_file_overflow: Default::default(),
            clock: Clock::Global,
            elapsed_time: Default::default(),
            total_method_calls: Default::default(),
            clock_call_overhead: Default::default(),
            vm: Vm::Dalvik,
            start_time: Utc::now(),
            pid: Default::default(),
            gc_trace: Default::default(),
            threads: Default::default(),
            methods: Default::default(),
            events: Default::default(),
        }
    }

    fn parse(&mut self) -> Result<(), ProfileError> {
        let profile_bytes = match BASE64_NOPAD.decode(self.sampled_profile.as_bytes()) {
            Ok(profile) => profile,
            Err(_) => return Err(ProfileError::InvalidBase64Value),
        };
        self.profile = match android_trace_log::parse(&profile_bytes) {
            Ok(profile) => profile,
            Err(_) => return Err(ProfileError::InvalidSampledProfile),
        };
        Ok(())
    }
}

fn parse_chunk(payload: &[u8]) -> Result<Chunk, ProfileError> {
    let d = &mut serde_json::Deserializer::from_slice(payload);
    let mut profile: Chunk =
        serde_path_to_error::deserialize(d).map_err(ProfileError::InvalidJson)?;

    if let Some(ref mut js_profile) = profile.js_profile {
        js_profile.normalize(profile.metadata.platform.as_str())?;
    }

    if !profile.sampled_profile.is_empty() {
        profile.parse()?;
    }

    if profile.profile.events.is_empty() {
        return Err(ProfileError::NotEnoughSamples);
    }

    if profile.profile.elapsed_time > MAX_PROFILE_CHUNK_DURATION {
        return Err(ProfileError::DurationIsTooLong);
    }

    if profile.profile.elapsed_time.is_zero() {
        return Err(ProfileError::DurationIsZero);
    }

    // Use duration given by the profiler and not reported by the SDK.
    profile.metadata.duration_ns = profile.profile.elapsed_time.as_nanos() as u64;

    // If build_id is not empty but we don't have any DebugImage set,
    // we create the proper Proguard image and set the uuid.
    if !profile.metadata.build_id.is_empty() && profile.metadata.debug_meta.is_none() {
        profile.metadata.debug_meta = Some(DebugMeta {
            images: vec![get_proguard_image(&profile.metadata.build_id)?],
        })
    }

    Ok(profile)
}

pub fn parse(payload: &[u8]) -> Result<Vec<u8>, ProfileError> {
    let profile = parse_chunk(payload)?;

    serde_json::to_vec(&profile).map_err(|_| ProfileError::CannotSerializePayload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let payload = include_bytes!("../../tests/fixtures/android/chunk/valid.json");
        let profile = parse_chunk(payload);
        assert!(profile.is_ok());
        let data = serde_json::to_vec(&profile.unwrap());
        assert!(parse_chunk(&(data.unwrap())[..]).is_ok());
    }

    #[test]
    fn test_roundtrip_react_native() {
        let payload = include_bytes!("../../tests/fixtures/android/chunk/valid-rn.json");
        let profile = parse_chunk(payload);
        assert!(profile.is_ok());
        let data = serde_json::to_vec(&profile.unwrap());
        assert!(parse_chunk(&(data.unwrap())[..]).is_ok());
    }

    #[test]
    fn test_remove_invalid_events() {
        let payload =
            include_bytes!("../../tests/fixtures/android/chunk/remove_invalid_events.json");
        let data = parse(payload);
        assert!(data.is_err());
    }
}
