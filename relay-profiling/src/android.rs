use std::collections::{HashMap, HashSet};

use android_trace_log::chrono::{DateTime, Utc};
use android_trace_log::{AndroidTraceLog, Clock, Time, Vm};
use data_encoding::BASE64;
use serde::{Deserialize, Serialize};

use relay_general::protocol::EventId;

use crate::measurements::Measurement;
use crate::transaction_metadata::TransactionMetadata;
use crate::utils::{deserialize_number_from_string, is_zero};
use crate::ProfileError;

#[derive(Debug, Serialize, Deserialize, Clone)]
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

    #[serde(default, skip_serializing_if = "String::is_empty")]
    environment: String,

    platform: String,
    profile_id: EventId,
    version_code: String,
    version_name: String,

    #[serde(default, skip_serializing_if = "EventId::is_nil")]
    transaction_id: EventId,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    transaction_name: String,
    #[serde(default, skip_serializing_if = "EventId::is_nil")]
    trace_id: EventId,
    #[serde(
        default,
        deserialize_with = "deserialize_number_from_string",
        skip_serializing_if = "is_zero"
    )]
    duration_ns: u64,
    #[serde(
        default,
        deserialize_with = "deserialize_number_from_string",
        skip_serializing_if = "is_zero"
    )]
    active_thread_id: u64,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    transactions: Vec<TransactionMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction: Option<TransactionMetadata>,

    #[serde(default, skip_serializing)]
    sampled_profile: String,

    #[serde(default = "AndroidProfile::default")]
    profile: AndroidTraceLog,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    measurements: Option<HashMap<String, Measurement>>,
}

impl AndroidProfile {
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
        let profile_bytes = match BASE64.decode(self.sampled_profile.as_bytes()) {
            Ok(profile) => profile,
            Err(_) => return Err(ProfileError::InvalidBase64Value),
        };
        self.profile = match android_trace_log::parse(&profile_bytes) {
            Ok(profile) => profile,
            Err(_) => return Err(ProfileError::InvalidSampledProfile),
        };
        Ok(())
    }

    fn has_transaction_metadata(&self) -> bool {
        !self.transaction_name.is_empty() && self.duration_ns > 0
    }

    /// Removes an event with a duration of 0
    fn remove_events_with_no_duration(&mut self) {
        let android_trace = &mut self.profile;
        let events = &mut android_trace.events;
        let clock = android_trace.clock;
        let start_time = android_trace.start_time;
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
    }
}

fn parse_profile(payload: &[u8]) -> Result<AndroidProfile, ProfileError> {
    let mut profile: AndroidProfile =
        serde_json::from_slice(payload).map_err(ProfileError::InvalidJson)?;

    let transaction_opt = profile.transactions.drain(..).next();
    if let Some(transaction) = transaction_opt {
        if !transaction.valid() {
            return Err(ProfileError::InvalidTransactionMetadata);
        }

        // this is for compatibility
        profile.active_thread_id = transaction.active_thread_id;
        profile.duration_ns = transaction.duration_ns();
        profile.trace_id = transaction.trace_id;
        profile.transaction_id = transaction.id;
        profile.transaction_name = transaction.name.clone();

        profile.transaction = Some(transaction);
    } else if !profile.has_transaction_metadata() {
        return Err(ProfileError::NoTransactionAssociated);
    }

    if !profile.sampled_profile.is_empty() {
        profile.parse()?;
        profile.remove_events_with_no_duration();
    }

    if profile.profile.events.is_empty() {
        return Err(ProfileError::NotEnoughSamples);
    }

    Ok(profile)
}

pub fn parse_android_profile(payload: &[u8]) -> Result<Vec<u8>, ProfileError> {
    let profile = parse_profile(payload)?;
    serde_json::to_vec(&profile).map_err(|_| ProfileError::CannotSerializePayload)
}

fn get_timestamp(clock: Clock, start_time: DateTime<Utc>, event_time: Time) -> u64 {
    match (clock, event_time) {
        (Clock::Global, Time::Global(time)) => {
            let time_ns = time.as_nanos() as u64;
            let start_time_ns = start_time.timestamp_nanos() as u64;
            if time_ns >= start_time_ns {
                time_ns - start_time_ns
            } else {
                0
            }
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
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_android() {
        let payload = include_bytes!("../tests/fixtures/profiles/android/roundtrip.json");
        let profile = parse_profile(payload);
        assert!(profile.is_ok());
        let data = serde_json::to_vec(&profile.unwrap());
        assert!(parse_android_profile(&(data.unwrap())[..]).is_ok());
    }

    #[test]
    fn test_no_transaction() {
        let payload = include_bytes!("../tests/fixtures/profiles/android/no_transaction.json");
        let data = parse_android_profile(payload);
        assert!(data.is_err());
    }

    #[test]
    fn test_remove_invalid_events() {
        let payload =
            include_bytes!("../tests/fixtures/profiles/android/remove_invalid_events.json");
        let data = parse_android_profile(payload);
        assert!(data.is_err());
    }

    #[test]
    fn test_transactions_to_top_level() {
        let payload =
            include_bytes!("../tests/fixtures/profiles/android/multiple_transactions.json");

        let profile = match parse_profile(payload) {
            Err(err) => panic!("cannot parse profile: {err:?}"),
            Ok(profile) => profile,
        };
        assert_eq!(
            profile.transaction_id,
            "7d6db784355e4d7dbf98671c69cbcc77".parse().unwrap()
        );
        assert_eq!(
            profile.trace_id,
            "7d6db784355e4d7dbf98671c69cbcc77".parse().unwrap()
        );
        assert_eq!(profile.active_thread_id, 12345);
        assert_eq!(profile.transaction_name, "transaction1");
        assert_eq!(profile.duration_ns, 1000000000);
    }
}
