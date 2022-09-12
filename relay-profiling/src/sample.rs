use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use relay_general::protocol::{Addr, EventId};

use crate::error::ProfileError;
use crate::native_debug_image::NativeDebugImage;
use crate::transaction_metadata::TransactionMetadata;
use crate::utils::{deserialize_number_from_string, is_zero};
use crate::Platform;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Frame {
    instruction_addr: Addr,
}

impl Frame {
    fn strip_pointer_authentication_code(&mut self, addr: u64) {
        self.instruction_addr = Addr(self.instruction_addr.0 & addr);
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Sample {
    stack_id: u32,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    thread_id: u64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    relative_timestamp_ns: u64,

    // cocoa only
    #[serde(default, skip_serializing_if = "Option::is_none")]
    queue_address: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ThreadMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    is_main: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    is_active: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    priority: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct QueueMetadata {
    label: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Stack {
    frames: Vec<Frame>,
}

impl Stack {
    fn strip_pointer_authentication_code(&mut self, addr: u64) {
        for frame in &mut self.frames {
            frame.strip_pointer_authentication_code(addr);
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Profile {
    samples: Vec<Sample>,
    stacks: Vec<Stack>,
    thread_metadata: HashMap<String, ThreadMetadata>,

    // cocoa only
    #[serde(default, skip_serializing_if = "Option::is_none")]
    queue_metadata: Option<HashMap<String, QueueMetadata>>,
}

impl Profile {
    fn strip_pointer_authentication_code(&mut self, platform: &Platform, architecture: &str) {
        let addr = match (platform, architecture) {
            // https://github.com/microsoft/plcrashreporter/blob/748087386cfc517936315c107f722b146b0ad1ab/Source/PLCrashAsyncThread_arm.c#L84
            (Platform::Cocoa, "arm64") | (Platform::Cocoa, "arm64e") => 0x0000000FFFFFFFFF,
            _ => return,
        };
        for stack in &mut self.stacks {
            stack.strip_pointer_authentication_code(addr);
        }
    }
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
struct DebugMeta {
    images: Vec<NativeDebugImage>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct OSMetadata {
    name: String,
    version: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    build_number: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RuntimeMetadata {
    name: String,
    version: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct DeviceMetadata {
    architecture: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    is_emulator: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    locale: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    manufacturer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
enum Version {
    #[serde(rename = "1")]
    V1,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SampleProfile {
    version: Version,

    #[serde(skip_serializing_if = "Option::is_none")]
    debug_meta: Option<DebugMeta>,

    device: DeviceMetadata,
    os: OSMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime: Option<RuntimeMetadata>,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    environment: String,
    #[serde(alias = "profile_id")]
    event_id: EventId,
    platform: Platform,
    profile: Profile,
    release: String,
    timestamp: DateTime<Utc>,

    #[serde(
        default,
        deserialize_with = "deserialize_number_from_string",
        skip_serializing_if = "is_zero"
    )]
    duration_ns: u64,
    #[serde(default, skip_serializing_if = "EventId::is_nil")]
    trace_id: EventId,
    #[serde(default, skip_serializing_if = "EventId::is_nil")]
    transaction_id: EventId,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    transaction_name: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    transactions: Vec<TransactionMetadata>,
}

impl SampleProfile {
    fn valid(&self) -> bool {
        match self.platform {
            Platform::Cocoa => {
                self.os.build_number.is_some()
                    && self.device.is_emulator.is_some()
                    && self.device.locale.is_some()
                    && self.device.manufacturer.is_some()
                    && self.device.model.is_some()
            }
            Platform::Python => self.runtime.is_some(),
            Platform::Node => self.runtime.is_some(),
            _ => true,
        }
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

    /// Removes a sample when it's the only sample on its thread
    fn remove_single_samples_per_thread(&mut self) {
        let mut sample_count_by_thread_id: HashMap<u64, u32> = HashMap::new();

        for sample in self.profile.samples.iter() {
            *sample_count_by_thread_id
                .entry(sample.thread_id)
                .or_default() += 1;
        }

        // Only keep data from threads with more than 1 sample so we can calculate a duration
        sample_count_by_thread_id.retain(|_, count| *count > 1);

        self.profile
            .samples
            .retain(|sample| sample_count_by_thread_id.contains_key(&sample.thread_id));
    }

    fn strip_pointer_authentication_code(&mut self) {
        self.profile
            .strip_pointer_authentication_code(&self.platform, &self.device.architecture);
    }
}

pub fn expand_sample_profile(payload: &[u8]) -> Result<Vec<Vec<u8>>, ProfileError> {
    let profile = parse_profile(payload)?;

    if !profile.valid() {
        return Err(ProfileError::MissingProfileMetadata);
    }

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

        new_profile.profile.samples.retain_mut(|sample| {
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

fn parse_profile(payload: &[u8]) -> Result<SampleProfile, ProfileError> {
    let mut profile: SampleProfile =
        serde_json::from_slice(payload).map_err(ProfileError::InvalidJson)?;

    profile.remove_single_samples_per_thread();

    if profile.profile.samples.is_empty() {
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

    profile.strip_pointer_authentication_code();

    Ok(profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let payload = include_bytes!("../tests/fixtures/profiles/sample/roundtrip.json");
        let profile = parse_profile(payload);
        assert!(profile.is_ok());
        let data = serde_json::to_vec(&profile.unwrap());
        assert!(parse_profile(&data.unwrap()[..]).is_ok());
    }

    #[test]
    fn test_expand() {
        let payload = include_bytes!("../tests/fixtures/profiles/sample/roundtrip.json");
        let profile = expand_sample_profile(payload);
        assert!(profile.is_ok());
    }

    #[test]
    fn test_parse_multiple_transactions() {
        let payload =
            include_bytes!("../tests/fixtures/profiles/sample/multiple_transactions.json");
        let data = parse_profile(payload);
        assert!(data.is_ok());
    }

    #[test]
    fn test_no_transaction() {
        let payload = include_bytes!("../tests/fixtures/profiles/sample/no_transaction.json");
        let data = parse_profile(payload);
        assert!(data.is_err());
    }

    fn generate_profile() -> SampleProfile {
        SampleProfile {
            debug_meta: Option::None,
            version: Version::V1,
            timestamp: Utc::now(),
            runtime: Option::None,
            device: DeviceMetadata {
                architecture: "arm64e".to_string(),
                is_emulator: Some(true),
                locale: Some("en_US".to_string()),
                manufacturer: Some("Apple".to_string()),
                model: Some("iPhome11,3".to_string()),
            },
            os: OSMetadata {
                build_number: Some("H3110".to_string()),
                name: "iOS".to_string(),
                version: "16.0".to_string(),
            },
            duration_ns: 1337,
            environment: "testing".to_string(),
            platform: Platform::Cocoa,
            event_id: EventId::new(),
            profile: Profile {
                queue_metadata: Some(HashMap::new()),
                samples: Vec::new(),
                stacks: Vec::new(),
                thread_metadata: HashMap::new(),
            },
            trace_id: EventId::new(),
            transaction_id: EventId::new(),
            transaction_name: "test".to_string(),
            transactions: Vec::new(),
            release: "1.0 (9999)".to_string(),
        }
    }

    #[test]
    fn test_filter_samples() {
        let mut profile = generate_profile();

        profile.profile.stacks.push(Stack { frames: Vec::new() });
        profile.profile.samples.extend(vec![
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                relative_timestamp_ns: 1,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                relative_timestamp_ns: 1,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                relative_timestamp_ns: 1,
                thread_id: 2,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                relative_timestamp_ns: 1,
                thread_id: 3,
            },
        ]);

        profile.remove_single_samples_per_thread();

        assert!(profile.profile.samples.len() == 2);
    }

    #[test]
    fn test_parse_profile_with_all_samples_filtered() {
        let mut profile = generate_profile();

        profile.profile.stacks.push(Stack { frames: Vec::new() });
        profile.profile.samples.extend(vec![
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                relative_timestamp_ns: 1,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                relative_timestamp_ns: 1,
                thread_id: 2,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                relative_timestamp_ns: 1,
                thread_id: 3,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                relative_timestamp_ns: 1,
                thread_id: 4,
            },
        ]);

        let payload = serde_json::to_vec(&profile).unwrap();
        assert!(parse_profile(&payload[..]).is_err());
    }

    #[test]
    fn test_expand_multiple_transactions() {
        let payload =
            include_bytes!("../tests/fixtures/profiles/sample/multiple_transactions.json");
        let data = expand_sample_profile(payload);
        assert!(data.is_ok());
        assert_eq!(data.as_ref().unwrap().len(), 2);

        let profile = match parse_profile(&data.as_ref().unwrap()[0][..]) {
            Err(err) => panic!("cannot parse profile: {:?}", err),
            Ok(profile) => profile,
        };
        assert_eq!(
            profile.transaction_id,
            "30976f2ddbe04ac9b6bffe6e35d4710c".parse().unwrap()
        );
        assert_eq!(profile.duration_ns, 50000000);
        assert_eq!(profile.profile.samples.len(), 4);

        for sample in &profile.profile.samples {
            assert!(sample.relative_timestamp_ns < profile.duration_ns);
        }
    }
}
