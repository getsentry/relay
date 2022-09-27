use std::collections::HashMap;

use serde::{de, Deserialize, Serialize};

use relay_general::protocol::{Addr, EventId};

use crate::error::ProfileError;
use crate::native_debug_image::NativeDebugImage;
use crate::transaction_metadata::TransactionMetadata;
use crate::utils::{deserialize_number_from_string, is_zero};

fn strip_pointer_authentication_code<'de, D>(deserializer: D) -> Result<Addr, D::Error>
where
    D: de::Deserializer<'de>,
{
    let addr: Addr = Deserialize::deserialize(deserializer)?;
    // https://github.com/microsoft/plcrashreporter/blob/748087386cfc517936315c107f722b146b0ad1ab/Source/PLCrashAsyncThread_arm.c#L84
    Ok(Addr(addr.0 & 0x0000000FFFFFFFFF))
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Frame {
    #[serde(deserialize_with = "strip_pointer_authentication_code")]
    instruction_addr: Addr,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Sample {
    frames: Vec<Frame>,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    queue_address: String,

    #[serde(deserialize_with = "deserialize_number_from_string")]
    relative_timestamp_ns: u64,

    #[serde(deserialize_with = "deserialize_number_from_string")]
    thread_id: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ThreadMetadata {
    #[serde(default)]
    is_main_thread: bool,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    priority: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct QueueMetadata {
    label: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SampledProfile {
    samples: Vec<Sample>,

    #[serde(default)]
    thread_metadata: HashMap<String, ThreadMetadata>,

    #[serde(default)]
    queue_metadata: HashMap<String, QueueMetadata>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct DebugMeta {
    images: Vec<NativeDebugImage>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CocoaProfile {
    debug_meta: DebugMeta,
    device_is_emulator: bool,
    device_locale: String,
    device_manufacturer: String,
    device_model: String,
    device_os_build_number: String,
    device_os_name: String,
    device_os_version: String,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    environment: String,

    platform: String,
    profile_id: EventId,
    sampled_profile: SampledProfile,
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
}

impl CocoaProfile {
    fn set_transaction(&mut self, transaction: &TransactionMetadata) {
        self.transaction_name = transaction.name.clone();
        self.transaction_id = transaction.id;
        self.trace_id = transaction.trace_id;
        self.duration_ns = transaction.duration_ns();
    }

    fn has_transaction_metadata(&self) -> bool {
        !self.transaction_name.is_empty() && self.duration_ns > 0
    }

    /// Removes a sample when it's the only sample on its thread
    fn remove_single_samples_per_thread(&mut self) {
        let mut sample_count_by_thread_id: HashMap<u64, u32> = HashMap::new();

        for sample in self.sampled_profile.samples.iter() {
            *sample_count_by_thread_id
                .entry(sample.thread_id)
                .or_default() += 1;
        }

        // Only keep data from threads with more than 1 sample so we can calculate a duration
        sample_count_by_thread_id.retain(|_, count| *count > 1);

        self.sampled_profile
            .samples
            .retain(|sample| sample_count_by_thread_id.contains_key(&sample.thread_id));
    }
}

pub fn expand_cocoa_profile(payload: &[u8]) -> Result<Vec<Vec<u8>>, ProfileError> {
    let profile = parse_cocoa_profile(payload)?;
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

        new_profile.profile_id = EventId::new();
        new_profile.set_transaction(transaction);
        new_profile.transactions.clear();

        new_profile.sampled_profile.samples.retain_mut(|sample| {
            if transaction.relative_start_ns <= sample.relative_timestamp_ns
                && sample.relative_timestamp_ns <= transaction.relative_end_ns
            {
                sample.relative_timestamp_ns -= transaction.relative_start_ns;
                true
            } else {
                false
            }
        });

        match serde_json::to_vec(&new_profile) {
            Ok(payload) => items.push(payload),
            Err(_) => {
                return Err(ProfileError::CannotSerializePayload);
            }
        };
    }

    Ok(items)
}

fn parse_cocoa_profile(payload: &[u8]) -> Result<CocoaProfile, ProfileError> {
    let mut profile: CocoaProfile =
        serde_json::from_slice(payload).map_err(ProfileError::InvalidJson)?;

    profile.remove_single_samples_per_thread();

    if profile.sampled_profile.samples.is_empty() {
        return Err(ProfileError::NotEnoughSamples);
    }

    if profile.transactions.is_empty() && !profile.has_transaction_metadata() {
        return Err(ProfileError::NoTransactionAssociated);
    }

    for transaction in profile.transactions.iter() {
        if !transaction.valid() {
            return Err(ProfileError::InvalidTransactionMetadata);
        }
    }

    Ok(profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_cocoa() {
        let payload = include_bytes!("../tests/fixtures/profiles/cocoa/roundtrip.json");
        let profile = parse_cocoa_profile(payload);
        assert!(profile.is_ok());
        let data = serde_json::to_vec(&profile.unwrap());
        assert!(parse_cocoa_profile(&data.unwrap()[..]).is_ok());
    }

    #[test]
    fn test_parse_multiple_transactions() {
        let payload = include_bytes!("../tests/fixtures/profiles/cocoa/multiple_transactions.json");
        let data = parse_cocoa_profile(payload);
        assert!(data.is_ok());
    }

    #[test]
    fn test_no_transaction() {
        let payload = include_bytes!("../tests/fixtures/profiles/cocoa/no_transaction.json");
        let data = parse_cocoa_profile(payload);
        assert!(data.is_err());
    }

    fn generate_profile() -> CocoaProfile {
        CocoaProfile {
            debug_meta: DebugMeta { images: Vec::new() },
            device_is_emulator: true,
            device_locale: "en_US".to_string(),
            device_manufacturer: "Apple".to_string(),
            device_model: "iPhome11,3".to_string(),
            device_os_build_number: "H3110".to_string(),
            device_os_name: "iOS".to_string(),
            device_os_version: "16.0".to_string(),
            duration_ns: 1337,
            environment: "testing".to_string(),
            platform: "cocoa".to_string(),
            profile_id: EventId::new(),
            sampled_profile: SampledProfile {
                thread_metadata: HashMap::new(),
                samples: Vec::new(),
                queue_metadata: HashMap::new(),
            },
            trace_id: EventId::new(),
            transaction_id: EventId::new(),
            transaction_name: "test".to_string(),
            transactions: Vec::new(),
            version_code: "9999".to_string(),
            version_name: "1.0".to_string(),
        }
    }

    #[test]
    fn test_filter_samples() {
        let mut profile = generate_profile();

        profile.sampled_profile.samples.extend(vec![
            Sample {
                frames: Vec::new(),
                queue_address: "0xdeadbeef".to_string(),
                relative_timestamp_ns: 1,
                thread_id: 1,
            },
            Sample {
                frames: Vec::new(),
                queue_address: "0xdeadbeef".to_string(),
                relative_timestamp_ns: 1,
                thread_id: 1,
            },
            Sample {
                frames: Vec::new(),
                queue_address: "0xdeadbeef".to_string(),
                relative_timestamp_ns: 1,
                thread_id: 2,
            },
            Sample {
                frames: Vec::new(),
                queue_address: "0xdeadbeef".to_string(),
                relative_timestamp_ns: 1,
                thread_id: 3,
            },
        ]);

        profile.remove_single_samples_per_thread();

        assert!(profile.sampled_profile.samples.len() == 2);
    }

    #[test]
    fn test_parse_profile_with_all_samples_filtered() {
        let mut profile = generate_profile();
        profile.sampled_profile.samples.extend(vec![
            Sample {
                frames: Vec::new(),
                queue_address: "0xdeadbeef".to_string(),
                relative_timestamp_ns: 1,
                thread_id: 1,
            },
            Sample {
                frames: Vec::new(),
                queue_address: "0xdeadbeef".to_string(),
                relative_timestamp_ns: 1,
                thread_id: 2,
            },
            Sample {
                frames: Vec::new(),
                queue_address: "0xdeadbeef".to_string(),
                relative_timestamp_ns: 1,
                thread_id: 3,
            },
            Sample {
                frames: Vec::new(),
                queue_address: "0xdeadbeef".to_string(),
                relative_timestamp_ns: 1,
                thread_id: 4,
            },
        ]);

        let payload = serde_json::to_vec(&profile).unwrap();
        assert!(parse_cocoa_profile(&payload[..]).is_err());
    }

    #[test]
    fn test_expand_multiple_transactions() {
        let payload = include_bytes!("../tests/fixtures/profiles/cocoa/multiple_transactions.json");
        let data = expand_cocoa_profile(payload);
        assert!(data.is_ok());
        assert_eq!(data.as_ref().unwrap().len(), 2);

        let profile = match parse_cocoa_profile(&data.as_ref().unwrap()[0][..]) {
            Err(err) => panic!("cannot parse profile: {:?}", err),
            Ok(profile) => profile,
        };
        assert_eq!(
            profile.transaction_id,
            "30976f2ddbe04ac9b6bffe6e35d4710c".parse().unwrap()
        );
        assert_eq!(profile.duration_ns, 8);
        assert_eq!(profile.sampled_profile.samples.len(), 2);

        for sample in &profile.sampled_profile.samples {
            assert!(sample.relative_timestamp_ns < profile.duration_ns);
        }
    }
}
