use std::collections::{HashMap, HashSet};
use std::time::Duration;

use android_trace_log::chrono::{DateTime, Utc};
use android_trace_log::{AndroidTraceLog, Clock, Time, Vm};
use serde::{Deserialize, Serialize};

use relay_general::protocol::EventId;

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

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    transactions: Vec<TransactionMetadata>,

    #[serde(default, skip_serializing)]
    sampled_profile: String,

    #[serde(default = "AndroidProfile::default")]
    profile: AndroidTraceLog,
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
        let profile_bytes = match base64::decode(&self.sampled_profile) {
            Ok(profile) => profile,
            Err(_) => return Err(ProfileError::InvalidBase64Value),
        };
        self.profile = match android_trace_log::parse(&profile_bytes) {
            Ok(profile) => profile,
            Err(_) => return Err(ProfileError::InvalidSampledProfile),
        };
        Ok(())
    }

    fn set_transaction(&mut self, transaction: &TransactionMetadata) {
        self.transaction_name = transaction.name.clone();
        self.transaction_id = transaction.id;
        self.trace_id = transaction.trace_id;
        self.duration_ns = transaction.relative_end_ns - transaction.relative_start_ns;
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

pub fn expand_android_profile(payload: &[u8]) -> Result<Vec<Vec<u8>>, ProfileError> {
    let profile = parse_android_profile(payload)?;
    let mut items: Vec<Vec<u8>> = Vec::new();

    if profile.transactions.is_empty() && profile.has_transaction_metadata() {
        match serde_json::to_vec(&profile) {
            Ok(payload) => items.push(payload),
            Err(_) => {
                return Err(ProfileError::CannotSerializePayload);
            }
        };

        return Ok(items);
    }

    for transaction in &profile.transactions {
        let mut new_profile = profile.clone();

        new_profile.set_transaction(transaction);
        new_profile.transactions.clear();

        let clock = new_profile.profile.clock;
        let start_time = new_profile.profile.start_time;

        new_profile.profile.events.retain_mut(|event| {
            let event_timestamp = get_timestamp(clock, start_time, event.time);
            if transaction.relative_start_ns <= event_timestamp
                && event_timestamp <= transaction.relative_end_ns
            {
                let relative_start = match clock {
                    Clock::Cpu => Duration::from_millis(transaction.relative_cpu_start_ms),
                    Clock::Wall | Clock::Dual | Clock::Global => {
                        Duration::from_nanos(transaction.relative_start_ns)
                    }
                };
                event.time = substract_to_timestamp(event.time, relative_start);
                true
            } else {
                false
            }
        });

        if new_profile.profile.events.is_empty() {
            continue;
        }

        match serde_json::to_vec(&new_profile) {
            Ok(payload) => items.push(payload),
            Err(_) => {
                return Err(ProfileError::CannotSerializePayload);
            }
        };
    }

    Ok(items)
}

fn substract_to_timestamp(time: Time, duration: Duration) -> Time {
    match time {
        Time::Global(time) => Time::Global(time - duration),
        Time::Monotonic {
            cpu: Some(time),
            wall: None,
        } => Time::Monotonic {
            cpu: time
                .checked_sub(duration)
                .or_else(|| Some(Duration::default())),
            wall: None,
        },
        Time::Monotonic {
            wall: Some(time),
            cpu: None,
        } => Time::Monotonic {
            wall: time
                .checked_sub(duration)
                .or_else(|| Some(Duration::default())),
            cpu: None,
        },
        Time::Monotonic {
            cpu: Some(cpu),
            wall: Some(wall),
        } => Time::Monotonic {
            cpu: cpu
                .checked_sub(duration)
                .or_else(|| Some(Duration::default())),
            wall: wall
                .checked_sub(duration)
                .or_else(|| Some(Duration::default())),
        },
        _ => unimplemented!(),
    }
}

fn get_timestamp(clock: Clock, start_time: DateTime<Utc>, event_time: Time) -> u64 {
    match (clock, event_time) {
        (Clock::Global, Time::Global(time)) => {
            let time_ns = time.as_nanos() as u64;
            let start_time_ns = start_time.timestamp_nanos() as u64;
            if time_ns >= start_time_ns {
                time.as_nanos() as u64 - start_time.timestamp_nanos() as u64
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

fn parse_android_profile(payload: &[u8]) -> Result<AndroidProfile, ProfileError> {
    let mut profile: AndroidProfile =
        serde_json::from_slice(payload).map_err(ProfileError::InvalidJson)?;

    if profile.transactions.is_empty() && !profile.has_transaction_metadata() {
        return Err(ProfileError::NoTransactionAssociated);
    }

    for transaction in &profile.transactions {
        if !transaction.valid() {
            return Err(ProfileError::InvalidTransactionMetadata);
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_android() {
        let payload = include_bytes!("../tests/fixtures/profiles/android/roundtrip.json");
        let profile = parse_android_profile(payload);
        assert!(profile.is_ok());
        let data = serde_json::to_vec(&profile.unwrap());
        assert!(parse_android_profile(&(data.unwrap())[..]).is_ok());
    }

    #[test]
    fn test_multiple_transactions() {
        let payload =
            include_bytes!("../tests/fixtures/profiles/android/multiple_transactions.json");
        let data = parse_android_profile(payload);
        assert!(data.is_ok());
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
    fn test_expand_multiple_transactions() {
        let payload =
            include_bytes!("../tests/fixtures/profiles/android/multiple_transactions.json");
        let data = expand_android_profile(payload);
        assert!(data.is_ok());
        assert_eq!(data.as_ref().unwrap().len(), 3);

        let profile = match parse_android_profile(&data.as_ref().unwrap()[0][..]) {
            Err(err) => panic!("cannot parse profile: {:?}", err),
            Ok(profile) => profile,
        };
        assert_eq!(
            profile.transaction_id,
            "7d6db784355e4d7dbf98671c69cbcc77".parse().unwrap()
        );
        assert_eq!(profile.duration_ns, 1000000000);
        assert_eq!(profile.profile.events.len(), 8453);

        for event in &profile.profile.events {
            assert!(
                get_timestamp(
                    profile.profile.clock,
                    profile.profile.start_time,
                    event.time
                ) < profile.duration_ns
            );
        }
    }

    #[test]
    fn test_substract_to_timestamp() {
        let now = Utc::now();
        let timestamps: Vec<(Clock, Time)> = vec![
            (
                Clock::Global,
                Time::Global(Duration::from_nanos(now.timestamp_nanos() as u64 + 100)),
            ),
            (
                Clock::Cpu,
                Time::Monotonic {
                    cpu: Some(Duration::from_nanos(100)),
                    wall: None,
                },
            ),
            (
                Clock::Wall,
                Time::Monotonic {
                    wall: Some(Duration::from_nanos(100)),
                    cpu: None,
                },
            ),
            (
                Clock::Dual,
                Time::Monotonic {
                    cpu: Some(Duration::from_nanos(20)),
                    wall: Some(Duration::from_nanos(100)),
                },
            ),
        ];
        for (clock, timestamp) in timestamps {
            let result = substract_to_timestamp(timestamp, Duration::from_nanos(50));
            assert_eq!(get_timestamp(clock, now, result), 50);
        }
    }
}
