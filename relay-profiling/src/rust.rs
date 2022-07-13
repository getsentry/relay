use crate::cocoa::NativeDebugImage;
use crate::ProfileError;
use relay_general::protocol::EventId;
use serde::{Deserialize, Serialize};

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

pub fn parse_rust_profile(payload: &[u8]) -> Result<Vec<u8>, ProfileError> {
    let profile: RustProfile =
        serde_json::from_slice(payload).map_err(ProfileError::InvalidJson)?;

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

    use relay_general::protocol::{Addr, DebugImage, NativeDebugImage as RelayNativeDebugImage};
    use relay_general::types::{Annotated, Map};

    #[test]
    fn test_roundtrip_rust() {
        let payload = include_bytes!("../tests/fixtures/profiles/rust.json");
        let data = parse_rust_profile(payload);
        assert!(data.is_ok());
        assert!(parse_rust_profile(&data.unwrap()[..]).is_ok());
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
