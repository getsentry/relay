//! Profiling protocol and processing for Sentry.
//!
//! Profiles are captured during the life of a transaction on Sentry clients and sent to Relay for
//! ingestion as JSON objects with some metadata. They are sent alongside a transaction (usually in
//! the same envelope) and are usually big objects (average size around 300KB and it's not unusual
//! to see several MB). Their size is linked to the amount of code executed during the transaction
//! (so the duration of the transaction influences the size of the profile).
//!
//! It's preferrable to have a profile with a transaction. If a transaction is dropped, the profile
//! should be dropped as well.
//!
//! # Envelope
//!
//! To send a profile to Relay, the profile is enclosed in an item of type `profile`:
//! ```json
//! {"type": "profile", "size": ...}
//! { ... }
//! ```
//!
//! # Protocol
//!
//! Relay is expecting a JSON object with some mandatory metadata and a `sampled_profile` key
//! containing the raw profile. Each platform has their own schema for the profile.
//!
//! `android` has a specific binary representation of its profile and Relay is responsible to
//! unpack it before it's forwarded down the line.
//!
//! The mandatory metadata can vary a bit depending on the platform. For mobile platform, it looks
//! like this:
//! ```json
//! {
//!     "debug_meta": { ... },
//!     "device_is_emulator": true,
//!     "device_locale": "en_US",
//!     "device_manufacturer": "Apple",
//!     "device_model": "iPhone14,3",
//!     "device_os_build_number": "21E258",
//!     "device_os_name": "iOS",
//!     "device_os_version": "15.2",
//!     "device_physical_memory_bytes": 34359738368,
//!     "duration_ns": "6634284250",
//!     "platform": "cocoa",
//!     "profile_id": "ee6851adf6014de8af8ca517217ac481",
//!     "sampled_profile": { ... },
//!     "trace_id": "4b45d297ef404fb89e8fdf418c8f38a2",
//!     "transaction_id": "8c3e0bc0518540b3ad1aa70d08f1ca7a",
//!     "transaction_name": "iOS_Swift.ViewController",
//!     "version_code": "1",
//!     "version_name": "7.14.0"
//! }
//! ```
//!
//! These are the custom attributes for mobile platforms:
//! - `device_os_build_number`
//! - `android_api_level`
//! - `device_is_emulator`
//! - `device_locale`
//! - `device_model`
//! - `device_manufacturer`
//! - `device_physical_memory_bytes`
//!
//! These are the custom attributes for the `android` platform:
//! - `build_id`
//! - `device_cpu_frequencies`
//!
//! These are the custom attributes for backend platforms:
//! - `architecture`
//!
//! # Ingestion
//!
//! Relay will forward those profiles encoded with `msgpack` after unpacking them if needed and push a message on Kafka looking
//! like this for the `cocoa` platform:
//! ```json
//! {
//!     "debug_meta": { ... },
//!     "device_is_emulator": true,
//!     "device_locale": "en_US",
//!     "device_manufacturer": "Apple",
//!     "device_model": "iPhone14,3",
//!     "device_os_build_number": "21E258",
//!     "device_os_name": "iOS",
//!     "device_os_version": "15.2",
//!     "device_physical_memory_bytes": 34359738368,
//!     "duration_ns": 6634284250,
//!     "platform": "cocoa",
//!     "profile_id": "ee6851adf6014de8af8ca517217ac481",
//!     "sampled_profile": { ... },
//!     "trace_id": "4b45d297ef404fb89e8fdf418c8f38a2",
//!     "transaction_id": "8c3e0bc0518540b3ad1aa70d08f1ca7a",
//!     "transaction_name": "iOS_Swift.ViewController",
//!     "version_code": "1",
//!     "version_name": "7.14.0"
//! }
//! ```

use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::Duration;

mod android;
mod error;
mod extract_from_transaction;
mod measurements;
mod native_debug_image;
mod outcomes;
mod sample;
mod transaction_metadata;
mod utils;

use relay_event_schema::protocol::{Event, EventId};

use crate::extract_from_transaction::{extract_transaction_metadata, extract_transaction_tags};

pub use crate::error::ProfileError;
pub use crate::outcomes::discard_reason;

const MAX_PROFILE_DURATION: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize)]
struct MinimalProfile {
    #[serde(alias = "profile_id")]
    event_id: EventId,
    platform: String,
    #[serde(default)]
    version: sample::Version,
}

fn minimal_profile_from_json(payload: &[u8]) -> Result<MinimalProfile, ProfileError> {
    serde_json::from_slice(payload).map_err(ProfileError::InvalidJson)
}

pub fn parse_metadata(payload: &[u8]) -> Result<(), ProfileError> {
    let profile = match minimal_profile_from_json(payload) {
        Ok(profile) => profile,
        Err(err) => return Err(err),
    };
    match profile.version {
        sample::Version::V1 => {
            let _: sample::ProfileMetadata = match serde_json::from_slice(payload) {
                Ok(profile) => profile,
                Err(err) => return Err(ProfileError::InvalidJson(err)),
            };
        }
        _ => match profile.platform.as_str() {
            "android" => {
                let _: android::ProfileMetadata = match serde_json::from_slice(payload) {
                    Ok(profile) => profile,
                    Err(err) => return Err(ProfileError::InvalidJson(err)),
                };
            }
            _ => return Err(ProfileError::PlatformNotSupported),
        },
    };
    Ok(())
}

pub fn expand_profile(
    payload: &[u8],
    event: Option<&Event>,
) -> Result<(EventId, Vec<u8>), ProfileError> {
    let profile = match minimal_profile_from_json(payload) {
        Ok(profile) => profile,
        Err(err) => return Err(err),
    };
    let (transaction_metadata, transaction_tags) = match event {
        Some(event) => (
            extract_transaction_metadata(event),
            extract_transaction_tags(event),
        ),
        _ => (BTreeMap::new(), BTreeMap::new()),
    };
    let processed_payload = match profile.version {
        sample::Version::V1 => {
            sample::parse_sample_profile(payload, transaction_metadata, transaction_tags)
        }
        sample::Version::Unknown => match profile.platform.as_str() {
            "android" => {
                android::parse_android_profile(payload, transaction_metadata, transaction_tags)
            }
            _ => return Err(ProfileError::PlatformNotSupported),
        },
    };
    processed_payload.map(|payload| (profile.event_id, payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minimal_profile_with_version() {
        let data = r#"{"version":"1","platform":"cocoa","event_id":"751fff80-a266-467b-a6f5-eeeef65f4f84"}"#;
        let profile = minimal_profile_from_json(data.as_bytes());
        assert!(profile.is_ok());
        assert_eq!(profile.unwrap().version, sample::Version::V1);
    }

    #[test]
    fn test_minimal_profile_without_version() {
        let data = r#"{"platform":"android","event_id":"751fff80-a266-467b-a6f5-eeeef65f4f84"}"#;
        let profile = minimal_profile_from_json(data.as_bytes());
        assert!(profile.is_ok());
        assert_eq!(profile.unwrap().version, sample::Version::Unknown);
    }

    #[test]
    fn test_expand_profile_with_version() {
        let payload = include_bytes!("../tests/fixtures/profiles/sample/roundtrip.json");
        assert!(expand_profile(payload, Some(&Event::default())).is_ok());
    }

    #[test]
    fn test_expand_profile_without_version() {
        let payload = include_bytes!("../tests/fixtures/profiles/android/roundtrip.json");
        assert!(expand_profile(payload, Some(&Event::default())).is_ok());
    }
}
