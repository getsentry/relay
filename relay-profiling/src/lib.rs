use serde::Deserialize;

mod android;
mod cocoa;
mod error;
mod rust;
mod typescript;
mod utils;

use crate::android::parse_android_profile;
use crate::cocoa::parse_cocoa_profile;
use crate::error::ProfileError;
use crate::rust::parse_rust_profile;
use crate::typescript::parse_typescript_profile;

#[derive(Debug, Deserialize)]
struct MinimalProfile {
    pub platform: String,
}

fn minimal_profile_from_json(data: &[u8]) -> Result<MinimalProfile, ProfileError> {
    serde_json::from_slice(data).map_err(ProfileError::InvalidJson)
}

pub fn parse_profile(payload: &[u8]) -> Result<Vec<u8>, ProfileError> {
    let minimal_profile: MinimalProfile = minimal_profile_from_json(payload)?;
    return match minimal_profile.platform.as_str() {
        "android" => parse_android_profile(payload),
        "cocoa" => parse_cocoa_profile(payload),
        "rust" => parse_rust_profile(payload),
        "typescript" => parse_typescript_profile(payload),
        _ => Err(ProfileError::PlatformNotSupported),
    };
}
