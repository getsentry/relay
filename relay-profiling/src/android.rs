use std::collections::{HashMap, HashSet};

use android_trace_log::chrono::{DateTime, Utc};
use android_trace_log::{AndroidTraceLog, Clock, Time};
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

    /// Removes an event with a duration of 0
    fn remove_events_with_no_duration(&mut self) {
        let mut android_profile = self.profile.as_ref().unwrap().clone();
        let mut events = android_profile.events;
        let clock = android_profile.clock;
        let start_time = android_profile.start_time;
        let mut timestamps_per_thread_id: HashMap<u16, HashSet<u64>> = HashMap::new();

        for event in events.iter() {
            let event_time = get_timestamp(clock, start_time, event.time);
            timestamps_per_thread_id
                .entry(event.thread_id)
                .or_default()
                .insert(event_time);
        }

        timestamps_per_thread_id.retain(|_, timestamps| timestamps.len() > 1);
        events.retain(|event| timestamps_per_thread_id.contains_key(&event.thread_id));

        android_profile.events = events;
        self.profile = Some(android_profile);
    }
}

pub fn parse_android_profile(payload: &[u8]) -> Result<Vec<u8>, ProfileError> {
    let mut profile: AndroidProfile =
        serde_json::from_slice(payload).map_err(ProfileError::InvalidJson)?;

    if profile.sampled_profile.is_empty() {
        return Ok(Vec::new());
    }

    profile.parse()?;
    profile.remove_events_with_no_duration();

    if profile.profile.as_ref().unwrap().events.is_empty() {
        return Err(ProfileError::NotEnoughSamples);
    }

    match serde_json::to_vec(&profile) {
        Ok(payload) => Ok(payload),
        Err(_) => Err(ProfileError::CannotSerializePayload),
    }
}

fn get_timestamp(clock: Clock, start_time: DateTime<Utc>, event_time: Time) -> u64 {
    match (clock, event_time) {
        (Clock::Global, Time::Global(time)) => {
            time.as_nanos() as u64 - start_time.timestamp_nanos() as u64
        }
        (
            Clock::Cpu,
            Time::Monotonic {
                cpu: Some(cpu),
                wall: None,
            },
        ) => cpu.as_nanos() as u64,
        (
            Clock::Wall,
            Time::Monotonic {
                cpu: None,
                wall: Some(wall),
            },
        ) => wall.as_nanos() as u64,
        (
            Clock::Dual,
            Time::Monotonic {
                cpu: Some(_),
                wall: Some(wall),
            },
        ) => wall.as_nanos() as u64,
        _ => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_android() {
        let payload = include_bytes!("../tests/fixtures/profiles/android/roundtrip.json");
        let data = parse_android_profile(payload);
        assert!(data.is_ok());
        assert!(parse_android_profile(&(data.unwrap())[..]).is_ok());
    }

    #[test]
    fn test_remove_invalid_events() {
        let payload =
            include_bytes!("../tests/fixtures/profiles/android/remove_invalid_events.json");
        let data = parse_android_profile(payload);
        assert!(data.is_err());
    }
}
