//! Android Format
//!
//! Relay is expecting a JSON object with some mandatory metadata and a `sampled_profile` key
//! containing the raw Android profile.
//!
//! `android` has a specific binary representation of its profile and Relay is responsible to
//! unpack it before it's forwarded down the line.
//!
use std::collections::{BTreeMap, HashMap};

use android_trace_log::chrono::{DateTime, Utc};
use android_trace_log::{AndroidTraceLog, Clock, Vm};
use data_encoding::BASE64_NOPAD;
use relay_event_schema::protocol::EventId;
use serde::{Deserialize, Serialize};

use crate::debug_image::get_proguard_image;
use crate::measurements::LegacyMeasurement;
use crate::sample::v1::SampleProfile;
use crate::transaction_metadata::TransactionMetadata;
use crate::types::{ClientSdk, DebugMeta};
use crate::utils::{default_client_sdk, deserialize_number_from_string, is_zero};
use crate::{MAX_PROFILE_DURATION, ProfileError};

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

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    transaction_metadata: BTreeMap<String, String>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    transaction_tags: BTreeMap<String, String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    debug_meta: Option<DebugMeta>,

    #[serde(skip_serializing_if = "Option::is_none")]
    client_sdk: Option<ClientSdk>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct AndroidProfilingEvent {
    #[serde(flatten)]
    metadata: ProfileMetadata,

    #[serde(default, skip_serializing)]
    sampled_profile: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    js_profile: Option<SampleProfile>,

    #[serde(default = "AndroidProfilingEvent::default")]
    profile: AndroidTraceLog,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    measurements: Option<HashMap<String, LegacyMeasurement>>,
}

impl AndroidProfilingEvent {
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
        !self.metadata.transaction_name.is_empty()
    }
}

fn parse_profile(payload: &[u8]) -> Result<AndroidProfilingEvent, ProfileError> {
    let d = &mut serde_json::Deserializer::from_slice(payload);
    let mut profile: AndroidProfilingEvent =
        serde_path_to_error::deserialize(d).map_err(ProfileError::InvalidJson)?;

    let transaction_opt = profile.metadata.transactions.drain(..).next();
    if let Some(transaction) = transaction_opt {
        if !transaction.valid() {
            return Err(ProfileError::InvalidTransactionMetadata);
        }

        // this is for compatibility
        profile.metadata.active_thread_id = transaction.active_thread_id;
        profile.metadata.trace_id = transaction.trace_id;
        profile.metadata.transaction_id = transaction.id;
        profile
            .metadata
            .transaction_name
            .clone_from(&transaction.name);

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

    if let Some(ref mut js_profile) = profile.js_profile {
        js_profile.normalize(profile.metadata.platform.as_str(), "")?;
    }

    if !profile.sampled_profile.is_empty() {
        profile.parse()?;
    }

    if profile.profile.events.is_empty() {
        return Err(ProfileError::NotEnoughSamples);
    }

    if profile.profile.elapsed_time > MAX_PROFILE_DURATION {
        return Err(ProfileError::DurationIsTooLong);
    }

    if profile.profile.elapsed_time.is_zero() {
        return Err(ProfileError::DurationIsZero);
    }

    // Use duration given by the profiler and not reported by the SDK.
    profile.metadata.duration_ns = profile.profile.elapsed_time.as_nanos() as u64;

    Ok(profile)
}

pub fn parse_android_profile(
    payload: &[u8],
    transaction_metadata: BTreeMap<String, String>,
    transaction_tags: BTreeMap<String, String>,
) -> Result<Vec<u8>, ProfileError> {
    let mut profile = parse_profile(payload)?;

    if let Some(transaction_name) = transaction_metadata.get("transaction") {
        transaction_name.clone_into(&mut profile.metadata.transaction_name);

        if let Some(ref mut transaction) = profile.metadata.transaction {
            profile
                .metadata
                .transaction_name
                .clone_into(&mut transaction.name);
        }
    }

    if let Some(release) = transaction_metadata.get("release") {
        release.clone_into(&mut profile.metadata.release);
    }

    if let Some(dist) = transaction_metadata.get("dist") {
        dist.clone_into(&mut profile.metadata.dist);
    }

    if let Some(environment) = transaction_metadata.get("environment") {
        environment.clone_into(&mut profile.metadata.environment);
    }

    if let Some(segment_id) = transaction_metadata
        .get("segment_id")
        .and_then(|segment_id| segment_id.parse().ok())
    {
        if let Some(transaction_metadata) = profile.metadata.transaction.as_mut() {
            transaction_metadata.segment_id = Some(segment_id);
        }
    }

    profile.metadata.client_sdk = match (
        transaction_metadata.get("client_sdk.name"),
        transaction_metadata.get("client_sdk.version"),
    ) {
        (Some(name), Some(version)) => Some(ClientSdk {
            name: name.to_owned(),
            version: version.to_owned(),
        }),
        _ => default_client_sdk(profile.metadata.platform.as_str()),
    };
    profile.metadata.transaction_metadata = transaction_metadata;
    profile.metadata.transaction_tags = transaction_tags;

    // If build_id is not empty but we don't have any DebugImage set,
    // we create the proper Proguard image and set the uuid.
    if !profile.metadata.build_id.is_empty() && profile.metadata.debug_meta.is_none() {
        profile.metadata.debug_meta = Some(DebugMeta {
            images: vec![get_proguard_image(&profile.metadata.build_id)?],
        })
    }

    serde_json::to_vec(&profile).map_err(|_| ProfileError::CannotSerializePayload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_android() {
        let payload = include_bytes!("../../tests/fixtures/android/legacy/roundtrip.json");
        let profile = parse_profile(payload);
        assert!(profile.is_ok());
        let data = serde_json::to_vec(&profile.unwrap());
        assert!(
            parse_android_profile(&(data.unwrap())[..], BTreeMap::new(), BTreeMap::new()).is_ok()
        );
    }

    #[test]
    fn test_roundtrip_react_native() {
        let payload = include_bytes!("../../tests/fixtures/android/legacy/roundtrip.rn.json");
        let profile = parse_profile(payload);
        assert!(profile.is_ok());
        let data = serde_json::to_vec(&profile.unwrap());
        assert!(
            parse_android_profile(&(data.unwrap())[..], BTreeMap::new(), BTreeMap::new()).is_ok()
        );
    }

    #[test]
    fn test_no_transaction() {
        let payload = include_bytes!("../../tests/fixtures/android/legacy/no_transaction.json");
        let data = parse_android_profile(payload, BTreeMap::new(), BTreeMap::new());
        assert!(data.is_err());
    }

    #[test]
    fn test_remove_invalid_events() {
        let payload =
            include_bytes!("../../tests/fixtures/android/legacy/remove_invalid_events.json");
        let data = parse_android_profile(payload, BTreeMap::new(), BTreeMap::new());
        assert!(data.is_err());
    }

    #[test]
    fn test_transactions_to_top_level() {
        let payload =
            include_bytes!("../../tests/fixtures/android/legacy/multiple_transactions.json");

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
    }

    #[test]
    fn test_extract_transaction_metadata() {
        let transaction_metadata = BTreeMap::from([
            ("release".to_owned(), "some-random-release".to_owned()),
            (
                "transaction".to_owned(),
                "some-random-transaction".to_owned(),
            ),
        ]);

        let payload = include_bytes!("../../tests/fixtures/android/legacy/valid.json");
        let profile_json = parse_android_profile(payload, transaction_metadata, BTreeMap::new());
        assert!(profile_json.is_ok());

        let payload = profile_json.unwrap();
        let d = &mut serde_json::Deserializer::from_slice(&payload[..]);
        let output: AndroidProfilingEvent = serde_path_to_error::deserialize(d)
            .map_err(ProfileError::InvalidJson)
            .unwrap();
        assert_eq!(output.metadata.release, "some-random-release".to_owned());
        assert_eq!(
            output.metadata.transaction_name,
            "some-random-transaction".to_owned()
        );

        if let Some(transaction) = output.metadata.transaction {
            assert_eq!(transaction.name, "some-random-transaction".to_owned());
        }
    }

    #[test]
    fn test_timestamp() {
        let payload = include_bytes!("../../tests/fixtures/android/legacy/roundtrip.json");
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
