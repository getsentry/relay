use std::collections::HashMap;

use serde::{de, Deserialize, Serialize};

use relay_general::protocol::{Addr, DebugId, EventId, NativeImagePath};

use crate::transaction_metadata::TransactionMetadata;
use crate::utils::{deserialize_number_from_string, is_zero};
use crate::ProfileError;

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

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
enum ImageType {
    MachO,
    Symbolic,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct NativeDebugImage {
    #[serde(alias = "name")]
    code_file: NativeImagePath,
    #[serde(alias = "id")]
    debug_id: DebugId,
    image_addr: Addr,

    #[serde(default)]
    image_vmaddr: Addr,

    #[serde(deserialize_with = "deserialize_number_from_string")]
    image_size: u64,

    #[serde(rename = "type")]
    image_type: ImageType,
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
    fn set_transaction(&mut self, transaction: TransactionMetadata) {
        self.transaction_name = transaction.name;
        self.transaction_id = transaction.id;
        self.trace_id = transaction.trace_id;
        self.duration_ns = transaction.relative_end_ns - transaction.relative_start_ns;
    }

    fn has_transaction_metadata(&self) -> bool {
        !self.transaction_name.is_empty() && self.duration_ns > 0
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

        new_profile.set_transaction(transaction.clone());
        new_profile.transactions.clear();

        new_profile.sampled_profile.samples.retain(|sample| {
            transaction.relative_start_ns <= sample.relative_timestamp_ns
                && sample.relative_timestamp_ns <= transaction.relative_end_ns
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
    let profile: CocoaProfile =
        serde_json::from_slice(payload).map_err(ProfileError::InvalidJson)?;

    if profile.sampled_profile.samples.len() < 2 {
        return Err(ProfileError::NotEnoughSamples);
    }

    if profile.transactions.is_empty() && !profile.has_transaction_metadata() {
        return Err(ProfileError::NoTransactionAssociated);
    }

    for transaction in profile.transactions.iter() {
        if !transaction.clone().valid() {
            return Err(ProfileError::InvalidTransactionMetadata);
        }
    }

    Ok(profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    use relay_general::protocol::{DebugImage, NativeDebugImage as RelayNativeDebugImage};
    use relay_general::types::{Annotated, Map};

    #[test]
    fn test_roundtrip_cocoa() {
        let payload = include_bytes!("../tests/fixtures/profiles/cocoa/valid.json");
        let profile = parse_cocoa_profile(payload);
        assert!(profile.is_ok());
        let data = serde_json::to_vec(&profile.unwrap());
        assert!(parse_cocoa_profile(&data.unwrap()[..]).is_ok());
    }

    #[test]
    fn test_ios_debug_image_compatibility() {
        let image_json = r#"{"debug_id":"32420279-25E2-34E6-8BC7-8A006A8F2425","image_addr":"0x000000010258c000","code_file":"/private/var/containers/Bundle/Application/C3511752-DD67-4FE8-9DA2-ACE18ADFAA61/TrendingMovies.app/TrendingMovies","type":"macho","image_size":1720320,"image_vmaddr":"0x0000000100000000"}"#;
        let image: NativeDebugImage = serde_json::from_str(image_json).unwrap();
        let json = serde_json::to_string(&image).unwrap();
        let annotated = Annotated::from_json(&json[..]).unwrap();
        assert_eq!(
            Annotated::new(DebugImage::MachO(Box::new(RelayNativeDebugImage {
                arch: Annotated::empty(),
                code_file: Annotated::new("/private/var/containers/Bundle/Application/C3511752-DD67-4FE8-9DA2-ACE18ADFAA61/TrendingMovies.app/TrendingMovies".into()),
                code_id: Annotated::empty(),
                debug_file: Annotated::empty(),
                debug_id: Annotated::new("32420279-25E2-34E6-8BC7-8A006A8F2425".parse().unwrap()),
                image_addr: Annotated::new(Addr(4334338048)),
                image_size: Annotated::new(1720320),
                image_vmaddr: Annotated::new(Addr(4294967296)),
                other: Map::new(),
            }))), annotated);
    }

    #[test]
    fn test_multiple_transactions() {
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
}
