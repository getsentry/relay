use std::collections::HashMap;

use serde::{de, Deserialize, Serialize};

use relay_general::protocol::{Addr, DebugId, EventId, NativeImagePath};

use crate::utils::deserialize_number_from_string;
use crate::ProfileError;

fn strip_pointer_authentication_code<'de, D>(deserializer: D) -> Result<Addr, D::Error>
where
    D: de::Deserializer<'de>,
{
    let addr: Addr = Deserialize::deserialize(deserializer)?;
    // https://github.com/microsoft/plcrashreporter/blob/748087386cfc517936315c107f722b146b0ad1ab/Source/PLCrashAsyncThread_arm.c#L84
    Ok(Addr(addr.0 & 0x0000000FFFFFFFFF))
}

#[derive(Debug, Serialize, Deserialize)]
struct Frame {
    #[serde(deserialize_with = "strip_pointer_authentication_code")]
    instruction_addr: Addr,
}

#[derive(Debug, Serialize, Deserialize)]
struct Sample {
    frames: Vec<Frame>,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    queue_address: String,

    #[serde(deserialize_with = "deserialize_number_from_string")]
    relative_timestamp_ns: u64,

    #[serde(deserialize_with = "deserialize_number_from_string")]
    thread_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ThreadMetadata {
    #[serde(default)]
    is_main_thread: bool,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    priority: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
struct QueueMetadata {
    label: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct SampledProfile {
    samples: Vec<Sample>,

    #[serde(default)]
    thread_metadata: HashMap<String, ThreadMetadata>,

    #[serde(default)]
    queue_metadata: HashMap<String, QueueMetadata>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum ImageType {
    MachO,
    Symbolic,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
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

#[derive(Debug, Serialize, Deserialize)]
struct DebugMeta {
    images: Vec<NativeDebugImage>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CocoaProfile {
    debug_meta: DebugMeta,
    device_is_emulator: bool,
    device_locale: String,
    device_manufacturer: String,
    device_model: String,
    device_os_build_number: String,
    device_os_name: String,
    device_os_version: String,

    #[serde(deserialize_with = "deserialize_number_from_string")]
    duration_ns: u64,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    environment: String,

    platform: String,
    profile_id: EventId,
    sampled_profile: SampledProfile,
    trace_id: EventId,
    transaction_id: EventId,
    transaction_name: String,
    version_code: String,
    version_name: String,
}

impl CocoaProfile {
    fn filter_samples(&mut self) {
        let mut sample_count_by_thread_id: HashMap<u64, u32> = HashMap::new();

        for sample in self.sampled_profile.samples.iter() {
            *sample_count_by_thread_id
                .entry(sample.thread_id)
                .or_default() += 1;
        }

        sample_count_by_thread_id.retain(|_, count| *count > 1);

        self.sampled_profile
            .samples
            .retain(|sample| sample_count_by_thread_id.contains_key(&sample.thread_id));
    }
}

pub fn parse_cocoa_profile(payload: &[u8]) -> Result<Vec<u8>, ProfileError> {
    let mut profile: CocoaProfile =
        serde_json::from_slice(payload).map_err(ProfileError::InvalidJson)?;

    profile.filter_samples();

    if profile.sampled_profile.samples.is_empty() {
        return Err(ProfileError::NotEnoughSamples);
    }

    match serde_json::to_vec(&profile) {
        Ok(payload) => Ok(payload),
        Err(_) => Err(ProfileError::CannotSerializePayload),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use relay_general::protocol::{DebugImage, NativeDebugImage as RelayNativeDebugImage};
    use relay_general::types::{Annotated, Map};

    #[test]
    fn test_roundtrip_cocoa() {
        let payload = include_bytes!("../tests/fixtures/profiles/cocoa.json");
        let data = parse_cocoa_profile(payload);
        assert!(data.is_ok());
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

        profile.filter_samples();

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
}
