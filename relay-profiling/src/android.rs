use std::collections::{BTreeMap, HashMap, HashSet};

use android_trace_log::chrono::{DateTime, Utc};
use android_trace_log::{AndroidTraceLog, Clock, Time, Vm};
use data_encoding::BASE64_NOPAD;
use relay_event_schema::protocol::EventId;
use serde::{Deserialize, Serialize};

use crate::measurements::Measurement;
use crate::transaction_metadata::TransactionMetadata;
use crate::utils::{deserialize_number_from_string, is_zero};
use crate::{ProfileError, MAX_PROFILE_DURATION};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProfileMetadata {
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

    #[serde(default, skip_serializing_if = "Option::is_none")]
    timestamp: Option<DateTime<Utc>>,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    release: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    dist: String,

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

    #[serde(default, skip_serializing_if = "Option::is_none")]
    measurements: Option<HashMap<String, Measurement>>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    transaction_metadata: BTreeMap<String, String>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    transaction_tags: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct AndroidProfile {
    #[serde(flatten)]
    metadata: ProfileMetadata,

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

    fn has_transaction_metadata(&self) -> bool {
        !self.metadata.transaction_name.is_empty() && self.metadata.duration_ns > 0
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

    let transaction_opt = profile.metadata.transactions.drain(..).next();
    if let Some(transaction) = transaction_opt {
        if !transaction.valid() {
            return Err(ProfileError::InvalidTransactionMetadata);
        }

        // this is for compatibility
        profile.metadata.active_thread_id = transaction.active_thread_id;
        profile.metadata.duration_ns = transaction.duration_ns();
        profile.metadata.trace_id = transaction.trace_id;
        profile.metadata.transaction_id = transaction.id;
        profile.metadata.transaction_name = transaction.name.clone();

        profile.metadata.transaction = Some(transaction);
    } else if profile.has_transaction_metadata() {
        profile.metadata.transaction = Some(TransactionMetadata {
            active_thread_id: profile.metadata.active_thread_id,
            id: profile.metadata.transaction_id,
            name: profile.metadata.transaction_name.clone(),
            trace_id: profile.metadata.trace_id,
            ..Default::default()
        });
    } else {
        return Err(ProfileError::NoTransactionAssociated);
    }

    if !profile.sampled_profile.is_empty() {
        profile.parse()?;
        profile.remove_events_with_no_duration();
    }

    if profile.profile.events.is_empty() {
        return Err(ProfileError::NotEnoughSamples);
    }

    if profile.profile.elapsed_time > MAX_PROFILE_DURATION {
        return Err(ProfileError::DurationIsTooLong);
    }

    Ok(profile)
}

pub fn parse_android_profile(
    payload: &[u8],
    transaction_metadata: BTreeMap<String, String>,
    transaction_tags: BTreeMap<String, String>,
) -> Result<Vec<u8>, ProfileError> {
    let mut profile = parse_profile(payload)?;

    if let Some(transaction_name) = transaction_metadata.get("transaction") {
        profile.metadata.transaction_name = transaction_name.to_owned();

        if let Some(ref mut transaction) = profile.metadata.transaction {
            transaction.name = profile.metadata.transaction_name.to_owned();
        }
    }

    if let Some(release) = transaction_metadata.get("release") {
        profile.metadata.release = release.to_owned();
    }

    if let Some(dist) = transaction_metadata.get("dist") {
        profile.metadata.dist = dist.to_owned();
    }

    if let Some(environment) = transaction_metadata.get("environment") {
        profile.metadata.environment = environment.to_owned();
    }

    profile.metadata.transaction_metadata = transaction_metadata;
    profile.metadata.transaction_tags = transaction_tags;

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
        assert!(
            parse_android_profile(&(data.unwrap())[..], BTreeMap::new(), BTreeMap::new()).is_ok()
        );
    }

    #[test]
    fn test_no_transaction() {
        let payload = include_bytes!("../tests/fixtures/profiles/android/no_transaction.json");
        let data = parse_android_profile(payload, BTreeMap::new(), BTreeMap::new());
        assert!(data.is_err());
    }

    #[test]
    fn test_remove_invalid_events() {
        let payload =
            include_bytes!("../tests/fixtures/profiles/android/remove_invalid_events.json");
        let data = parse_android_profile(payload, BTreeMap::new(), BTreeMap::new());
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
            profile.metadata.transaction_id,
            "7d6db784355e4d7dbf98671c69cbcc77".parse().unwrap()
        );
        assert_eq!(
            profile.metadata.trace_id,
            "7d6db784355e4d7dbf98671c69cbcc77".parse().unwrap()
        );
        assert_eq!(profile.metadata.active_thread_id, 12345);
        assert_eq!(profile.metadata.transaction_name, "transaction1");
        assert_eq!(profile.metadata.duration_ns, 1000000000);
    }

    #[test]
    fn test_extract_transaction_metadata() {
        let transaction_metadata = BTreeMap::from([
            ("release".to_string(), "some-random-release".to_string()),
            (
                "transaction".to_string(),
                "some-random-transaction".to_string(),
            ),
        ]);

        let payload = include_bytes!("../tests/fixtures/profiles/android/valid.json");
        let profile_json = parse_android_profile(payload, transaction_metadata, BTreeMap::new());
        assert!(profile_json.is_ok());

        let output: AndroidProfile = serde_json::from_slice(&profile_json.unwrap()[..])
            .map_err(ProfileError::InvalidJson)
            .unwrap();
        assert_eq!(output.metadata.release, "some-random-release".to_string());
        assert_eq!(
            output.metadata.transaction_name,
            "some-random-transaction".to_string()
        );

        if let Some(transaction) = output.metadata.transaction {
            assert_eq!(transaction.name, "some-random-transaction".to_string());
        }
    }

    #[test]
    fn test_timestamp() {
        let payload = include_bytes!("../tests/fixtures/profiles/android/roundtrip.json");
        let profile = parse_profile(payload);

        assert!(profile.is_ok());
        assert!(profile.as_ref().unwrap().metadata.timestamp.is_none());

        let mut ap = profile.unwrap();
        let now = Some(Utc::now());
        ap.metadata.timestamp = now;
        let data = serde_json::to_vec(&ap);
        let updated = parse_profile(&(data.unwrap())[..]);

        assert!(updated.is_ok());
        assert_eq!(updated.unwrap().metadata.timestamp, now);
    }
}
