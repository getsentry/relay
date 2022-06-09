use failure::Fail;
use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;

use android_trace_log::AndroidTraceLog;
use serde::{de, Deserialize, Serialize};

use crate::envelope::{ContentType, Item};
use relay_general::protocol::{Addr, DebugId, EventId, NativeImagePath};

#[derive(Debug, Fail)]
pub enum ProfileError {
    #[fail(display = "invalid json in profile")]
    InvalidJson(#[cause] serde_json::Error),
    #[fail(display = "invalid base64 value")]
    InvalidBase64Value,
    #[fail(display = "invalid sampled profile")]
    InvalidSampledProfile,
    #[fail(display = "cannot serialize payload")]
    CannotSerializePayload,
    #[fail(display = "not enough samples")]
    NotEnoughSamples,
    #[fail(display = "platform not supported")]
    PlatformNotSupported,
}

#[derive(Debug, Deserialize)]
pub struct MinimalProfile {
    pub platform: String,
}

pub fn minimal_profile_from_json(data: &[u8]) -> Result<MinimalProfile, ProfileError> {
    serde_json::from_slice(data).map_err(ProfileError::InvalidJson)
}

pub fn deserialize_number_from_string<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    D: de::Deserializer<'de>,
    T: FromStr + Deserialize<'de>,
    <T as FromStr>::Err: Display,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrInt<T> {
        String(String),
        Number(T),
    }

    match StringOrInt::<T>::deserialize(deserializer)? {
        StringOrInt::String(s) => s.parse::<T>().map_err(serde::de::Error::custom),
        StringOrInt::Number(i) => Ok(i),
    }
}

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
}

pub fn parse_android_profile(item: &mut Item) -> Result<(), ProfileError> {
    let mut profile: AndroidProfile =
        serde_json::from_slice(&item.payload()).map_err(ProfileError::InvalidJson)?;

    if profile.sampled_profile.is_empty() {
        return Ok(());
    }

    profile.parse()?;

    if profile.profile.as_ref().unwrap().events.len() < 2 {
        return Err(ProfileError::NotEnoughSamples);
    }

    match serde_json::to_vec(&profile) {
        Ok(payload) => item.set_payload(ContentType::Json, &payload[..]),
        Err(_) => return Err(ProfileError::CannotSerializePayload),
    };

    Ok(())
}

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
    #[serde(default, skip_serializing_if = "String::is_empty")]
    name: String,
    priority: u32,
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
struct NativeDebugImage {
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

pub fn parse_cocoa_profile(item: &mut Item) -> Result<(), ProfileError> {
    let profile: CocoaProfile =
        serde_json::from_slice(&item.payload()).map_err(ProfileError::InvalidJson)?;

    if profile.sampled_profile.samples.len() < 2 {
        return Err(ProfileError::NotEnoughSamples);
    }

    match serde_json::to_vec(&profile) {
        Ok(payload) => item.set_payload(ContentType::Json, &payload[..]),
        Err(_) => return Err(ProfileError::CannotSerializePayload),
    };

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct TypescriptProfile {
    device_is_emulator: bool,
    device_locale: String,
    device_manufacturer: String,
    device_model: String,
    device_os_build_number: Option<String>,
    device_os_name: String,
    device_os_version: String,

    #[serde(deserialize_with = "deserialize_number_from_string")]
    duration_ns: u64,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    environment: String,

    platform: String,
    profile_id: EventId,
    profile: Vec<serde_json::Value>,
    trace_id: EventId,
    transaction_id: EventId,
    transaction_name: String,
    version_code: String,
    version_name: String,
}

pub fn parse_typescript_profile(item: &mut Item) -> Result<(), ProfileError> {
    let profile: TypescriptProfile =
        serde_json::from_slice(&item.payload()).map_err(ProfileError::InvalidJson)?;

    if profile.profile.is_empty() {
        return Err(ProfileError::NotEnoughSamples);
    }

    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
struct RustFrame {
    instruction_addr: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct RustSample {
    frames: Vec<RustFrame>,
    thread_name: String,
    thread_id: u64,
    nanos_relative_to_start: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct RustSampledProfile {
    start_time_nanos: u64,
    start_time_secs: u64,
    duration_nanos: u64,
    samples: Vec<RustSample>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RustDebugMeta {
    images: Vec<NativeDebugImage>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RustProfile {
    duration_ns: u64,
    platform: String,
    architecture: String,
    trace_id: EventId,
    transaction_name: String,
    transaction_id: EventId,
    profile_id: EventId,
    sampled_profile: RustSampledProfile,
    device_os_name: String,
    device_os_version: String,
    version_name: String,
    version_code: String,
    debug_meta: RustDebugMeta,
}

pub fn parse_rust_profile(item: &mut Item) -> Result<(), ProfileError> {
    let profile: RustProfile =
        serde_json::from_slice(&item.payload()).map_err(ProfileError::InvalidJson)?;

    if profile.sampled_profile.samples.is_empty() {
        return Err(ProfileError::NotEnoughSamples);
    }

    match serde_json::to_vec(&profile) {
        Ok(payload) => item.set_payload(ContentType::Json, &payload[..]),
        Err(_) => return Err(ProfileError::CannotSerializePayload),
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    use relay_general::protocol::{DebugImage, NativeDebugImage as RelayNativeDebugImage};
    use relay_general::types::{Annotated, Map};

    use crate::envelope::{ContentType, Item, ItemType};

    #[test]
    fn test_roundtrip_cocoa() {
        let mut item = Item::new(ItemType::Profile);
        let payload = Bytes::from(&include_bytes!("../../tests/fixtures/profiles/cocoa.json")[..]);
        item.set_payload(ContentType::Json, payload);

        assert!(parse_cocoa_profile(&mut item).is_ok());

        item.set_payload(ContentType::Json, item.payload());

        assert!(parse_cocoa_profile(&mut item).is_ok());
    }

    #[test]
    fn test_roundtrip_android() {
        let mut item = Item::new(ItemType::Profile);
        let payload =
            Bytes::from(&include_bytes!("../../tests/fixtures/profiles/android.json")[..]);

        item.set_payload(ContentType::Json, payload);

        assert!(parse_android_profile(&mut item).is_ok());

        item.set_payload(ContentType::Json, item.payload());

        assert!(parse_android_profile(&mut item).is_ok());
    }

    #[test]
    fn test_roundtrip_typescript() {
        let mut item = Item::new(ItemType::Profile);
        let payload =
            Bytes::from(&include_bytes!("../../tests/fixtures/profiles/typescript.json")[..]);
        item.set_payload(ContentType::Json, payload);

        assert!(parse_typescript_profile(&mut item).is_ok());

        item.set_payload(ContentType::Json, item.payload());

        assert!(parse_typescript_profile(&mut item).is_ok());
    }

    #[test]
    fn test_roundtrip_rust() {
        let mut item = Item::new(ItemType::Profile);
        let payload = Bytes::from(&include_bytes!("../../tests/fixtures/profiles/rust.json")[..]);

        item.set_payload(ContentType::Json, payload);

        assert!(parse_rust_profile(&mut item).is_ok());

        item.set_payload(ContentType::Json, item.payload());

        assert!(parse_rust_profile(&mut item).is_ok());
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
    fn test_rust_debug_image_compatibility() {
        let image_json = r#"{"type": "symbolic","name": "/Users/vigliasentry/Documents/dev/rustfib/target/release/rustfib","image_addr": "0x104c6c000","image_size": 557056,"image_vmaddr": "0x100000000","id": "e5fd8c72-6f8f-3ad2-9c52-5ae133138e0c","code_id": "e5fd8c726f8f3ad29c525ae133138e0c"}"#;
        let image: NativeDebugImage = serde_json::from_str(image_json).unwrap();
        let json = serde_json::to_string(&image).unwrap();
        let annotated = Annotated::from_json(&json[..]).unwrap();
        assert_eq!(
            Annotated::new(DebugImage::Symbolic(Box::new(RelayNativeDebugImage {
                arch: Annotated::empty(),
                code_file: Annotated::new(
                    "/Users/vigliasentry/Documents/dev/rustfib/target/release/rustfib".into()
                ),
                code_id: Annotated::empty(),
                debug_file: Annotated::empty(),
                debug_id: Annotated::new("e5fd8c72-6f8f-3ad2-9c52-5ae133138e0c".parse().unwrap()),
                image_addr: Annotated::new(Addr(4375101440)),
                image_size: Annotated::new(557056),
                image_vmaddr: Annotated::new(Addr(4294967296)),
                other: Map::new(),
            }))),
            annotated
        );
    }
}
