use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use relay_general::protocol::{Addr, EventId};

use crate::error::ProfileError;
use crate::measurements::Measurement;
use crate::native_debug_image::NativeDebugImage;
use crate::transaction_metadata::TransactionMetadata;
use crate::utils::deserialize_number_from_string;
use crate::Platform;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Frame {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    instruction_addr: Option<Addr>,
    #[serde(alias = "name", default, skip_serializing_if = "Option::is_none")]
    function: Option<String>,
    #[serde(alias = "line", default, skip_serializing_if = "Option::is_none")]
    lineno: Option<u32>,
    #[serde(alias = "column", default, skip_serializing_if = "Option::is_none")]
    colno: Option<u32>,
    #[serde(alias = "file", default, skip_serializing_if = "Option::is_none")]
    filename: Option<String>,
}

impl Frame {
    fn strip_pointer_authentication_code(&mut self, pac_code: u64) {
        if let Some(address) = self.instruction_addr {
            self.instruction_addr = Some(Addr(address.0 & pac_code));
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Sample {
    stack_id: u32,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    thread_id: u64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    elapsed_since_start_ns: u64,

    // cocoa only
    #[serde(default, skip_serializing_if = "Option::is_none")]
    queue_address: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ThreadMetadata {
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
struct Profile {
    samples: Vec<Sample>,
    stacks: Vec<Vec<u32>>,
    frames: Vec<Frame>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    thread_metadata: Option<HashMap<String, ThreadMetadata>>,

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
        for frame in &mut self.frames {
            frame.strip_pointer_authentication_code(addr);
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

#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub enum Version {
    #[default]
    Unknown,
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

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    transactions: Vec<TransactionMetadata>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    measurements: Option<HashMap<String, Vec<Measurement>>>,
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

    /// Removes a sample when it's the only sample on its thread
    fn remove_single_samples_per_thread(&mut self) {
        let mut sample_count_by_thread_id: HashMap<u64, u32> = HashMap::new();

        for sample in &self.profile.samples {
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

    // As we're getting one profile for multiple transactions and our backend doesn't support this,
    // we need to duplicate the profile to have one profile for one transaction, filter the samples
    // to match the transaction bounds and generate a new profile ID.
    for transaction in &profile.transactions {
        let mut new_profile = profile.clone();

        new_profile.event_id = EventId::new();
        new_profile.transactions.clear();
        new_profile.transactions.push(transaction.clone());

        new_profile.profile.samples.retain_mut(|sample| {
            if transaction.relative_start_ns <= sample.elapsed_since_start_ns
                && sample.elapsed_since_start_ns <= transaction.relative_end_ns
            {
                sample.elapsed_since_start_ns -= transaction.relative_start_ns;
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

    profile
        .transactions
        .retain(|transaction| transaction.valid());

    if profile.transactions.is_empty() {
        return Err(ProfileError::NoTransactionAssociated);
    }

    profile.remove_single_samples_per_thread();

    if profile.profile.samples.is_empty() {
        return Err(ProfileError::NotEnoughSamples);
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
            environment: "testing".to_string(),
            platform: Platform::Cocoa,
            event_id: EventId::new(),
            profile: Profile {
                queue_metadata: Some(HashMap::new()),
                samples: Vec::new(),
                stacks: Vec::new(),
                frames: Vec::new(),
                thread_metadata: Some(HashMap::new()),
            },
            transactions: Vec::new(),
            release: "1.0 (9999)".to_string(),
            measurements: None,
        }
    }

    #[test]
    fn test_filter_samples() {
        let mut profile = generate_profile();

        profile.profile.stacks.push(Vec::new());
        profile.profile.samples.extend(vec![
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 1,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 1,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 1,
                thread_id: 2,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 1,
                thread_id: 3,
            },
        ]);

        profile.remove_single_samples_per_thread();

        assert!(profile.profile.samples.len() == 2);
    }

    #[test]
    fn test_parse_profile_with_all_samples_filtered() {
        let mut profile = generate_profile();

        profile.profile.stacks.push(Vec::new());
        profile.profile.samples.extend(vec![
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 1,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 1,
                thread_id: 2,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 1,
                thread_id: 3,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 1,
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

        let original_profile = match parse_profile(payload) {
            Err(err) => panic!("cannot parse profile: {:?}", err),
            Ok(profile) => profile,
        };
        let expanded_profile = match parse_profile(&data.as_ref().unwrap()[0][..]) {
            Err(err) => panic!("cannot parse profile: {:?}", err),
            Ok(profile) => profile,
        };
        assert_eq!(
            original_profile.transactions[0],
            expanded_profile.transactions[0]
        );
        assert_eq!(expanded_profile.profile.samples.len(), 4);

        for sample in &expanded_profile.profile.samples {
            assert!(sample.elapsed_since_start_ns < expanded_profile.transactions[0].duration_ns());
        }
    }
}
