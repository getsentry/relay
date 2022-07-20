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

mod android;
mod cocoa;
mod error;
mod python;
mod rust;
mod typescript;
mod utils;

use crate::android::parse_android_profile;
use crate::cocoa::parse_cocoa_profile;
use crate::error::ProfileError;
use crate::python::parse_python_profile;
use crate::rust::parse_rust_profile;
use crate::typescript::parse_typescript_profile;

#[derive(Debug, Deserialize)]
struct MinimalProfile {
    platform: String,
}

fn minimal_profile_from_json(data: &[u8]) -> Result<MinimalProfile, ProfileError> {
    serde_json::from_slice(data).map_err(ProfileError::InvalidJson)
}

pub fn parse_profile(payload: &[u8]) -> Result<Vec<u8>, ProfileError> {
    let minimal_profile: MinimalProfile = minimal_profile_from_json(payload)?;
    return match minimal_profile.platform.as_str() {
        "android" => parse_android_profile(payload),
        "cocoa" => parse_cocoa_profile(payload),
        "python" => parse_python_profile(payload),
        "rust" => parse_rust_profile(payload),
        "typescript" => parse_typescript_profile(payload),
        _ => Err(ProfileError::PlatformNotSupported),
    };
}
