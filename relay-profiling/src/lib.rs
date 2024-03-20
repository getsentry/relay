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
//! ## Transaction Profiling
//!
//! To send a profile of a transaction to Relay, the profile is enclosed in an item of type
//! `profile`:
//! ```json
//! {"type": "profile", "size": ...}
//! { ... }
//! ```
//! ## Continuous Profiling
//!
//! For continuous profiling, we expect to receive chunks of profile in an item of type
//! `profile_chunk`:
//! ```json
//! {"type": "profile_chunk"}
//! { ... }
//! ```
//!
//! # Protocol
//!
//! Each item type expects a different format.
//!
//! For `Profile` item type, we expect the Sample format v1 or Android format.
//! For `ProfileChunk` item type, we expect the Sample format v2.
//!
//! # Ingestion
//!
//! Relay will forward those profiles encoded with `msgpack` after unpacking them if needed and push a message on Kafka.

use std::error::Error;
use std::time::Duration;

use relay_base_schema::project::ProjectId;
use relay_event_schema::protocol::{Event, EventId};
use serde::Deserialize;
use serde_json::Deserializer;

use crate::extract_from_transaction::{extract_transaction_metadata, extract_transaction_tags};

pub use crate::error::ProfileError;
pub use crate::outcomes::discard_reason;

mod android;
mod error;
mod extract_from_transaction;
mod measurements;
mod native_debug_image;
mod outcomes;
mod sample;
mod transaction_metadata;
mod utils;

const MAX_PROFILE_DURATION: Duration = Duration::from_secs(30);

/// Unique identifier for a profile.
///
/// Same format as event IDs.
pub type ProfileId = EventId;

#[derive(Debug, Deserialize)]
struct MinimalProfile {
    #[serde(alias = "profile_id")]
    event_id: ProfileId,
    platform: String,
    #[serde(default)]
    version: sample::Version,
}

fn minimal_profile_from_json(
    payload: &[u8],
) -> Result<MinimalProfile, serde_path_to_error::Error<serde_json::Error>> {
    let d = &mut Deserializer::from_slice(payload);
    serde_path_to_error::deserialize(d)
}

pub fn parse_metadata(payload: &[u8], project_id: ProjectId) -> Result<ProfileId, ProfileError> {
    let profile = match minimal_profile_from_json(payload) {
        Ok(profile) => profile,
        Err(err) => {
            relay_log::warn!(
                error = &err as &dyn Error,
                from = "minimal",
                project_id = project_id.value(),
            );
            return Err(ProfileError::InvalidJson(err));
        }
    };
    match profile.version {
        sample::Version::V1 => {
            let d = &mut Deserializer::from_slice(payload);
            let _: sample::v1::ProfileMetadata = match serde_path_to_error::deserialize(d) {
                Ok(profile) => profile,
                Err(err) => {
                    relay_log::warn!(
                        error = &err as &dyn Error,
                        from = "metadata",
                        platform = profile.platform,
                        project_id = project_id.value(),
                        "invalid profile",
                    );
                    return Err(ProfileError::InvalidJson(err));
                }
            };
        }
        _ => match profile.platform.as_str() {
            "android" => {
                let d = &mut Deserializer::from_slice(payload);
                let _: android::ProfileMetadata = match serde_path_to_error::deserialize(d) {
                    Ok(profile) => profile,
                    Err(err) => {
                        relay_log::warn!(
                            error = &err as &dyn Error,
                            from = "metadata",
                            platform = "android",
                            project_id = project_id.value(),
                            "invalid profile",
                        );
                        return Err(ProfileError::InvalidJson(err));
                    }
                };
            }
            _ => return Err(ProfileError::PlatformNotSupported),
        },
    };
    Ok(profile.event_id)
}

pub fn expand_profile(payload: &[u8], event: &Event) -> Result<(ProfileId, Vec<u8>), ProfileError> {
    let profile = match minimal_profile_from_json(payload) {
        Ok(profile) => profile,
        Err(err) => {
            relay_log::warn!(
                error = &err as &dyn Error,
                from = "minimal",
                platform = event.platform.as_str(),
                project_id = event.project.value().unwrap_or(&0),
                sdk_name = event.sdk_name(),
                sdk_version = event.sdk_version(),
                transaction_id = ?event.id.value(),
                "invalid profile",
            );
            return Err(ProfileError::InvalidJson(err));
        }
    };
    let transaction_metadata = extract_transaction_metadata(event);
    let transaction_tags = extract_transaction_tags(event);
    let processed_payload = match profile.version {
        sample::Version::V1 => {
            sample::v1::parse_sample_profile(payload, transaction_metadata, transaction_tags)
        }
        _ => match profile.platform.as_str() {
            "android" => {
                android::parse_android_profile(payload, transaction_metadata, transaction_tags)
            }
            _ => return Err(ProfileError::PlatformNotSupported),
        },
    };
    match processed_payload {
        Ok(payload) => Ok((profile.event_id, payload)),
        Err(err) => match err {
            ProfileError::InvalidJson(err) => {
                relay_log::warn!(
                    error = &err as &dyn Error,
                    from = "parsing",
                    platform = profile.platform,
                    project_id = event.project.value().unwrap_or(&0),
                    sdk_name = event.sdk_name(),
                    sdk_version = event.sdk_version(),
                    transaction_id = ?event.id.value(),
                    "invalid profile",
                );
                Err(ProfileError::InvalidJson(err))
            }
            _ => {
                relay_log::warn!(
                    error = &err as &dyn Error,
                    from = "parsing",
                    platform = profile.platform,
                    project_id = event.project.value().unwrap_or(&0),
                    sdk_name = event.sdk_name(),
                    sdk_version = event.sdk_version(),
                    transaction_id = ?event.id.value(),
                    "invalid profile",
                );
                Err(err)
            }
        },
    }
}

pub fn expand_profile_chunk(payload: &[u8]) -> Result<Vec<u8>, ProfileError> {
    let profile = match minimal_profile_from_json(payload) {
        Ok(profile) => profile,
        Err(err) => {
            relay_log::warn!(
                error = &err as &dyn Error,
                from = "minimal",
                "invalid profile chunk",
            );
            return Err(ProfileError::InvalidJson(err));
        }
    };
    match profile.version {
        sample::Version::V2 => {
            let mut profile = sample::v2::parse(payload)?;
            profile.normalize()?;
            serde_json::to_vec(&profile).map_err(|_| ProfileError::CannotSerializePayload)
        }
        _ => Err(ProfileError::PlatformNotSupported),
    }
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
        let payload = include_bytes!("../tests/fixtures/sample/v1/valid.json");
        assert!(expand_profile(payload, &Event::default()).is_ok());
    }

    #[test]
    fn test_expand_profile_with_version_and_segment_id() {
        let payload = include_bytes!("../tests/fixtures/sample/v1/segment_id.json");
        assert!(expand_profile(payload, &Event::default()).is_ok());
    }

    #[test]
    fn test_expand_profile_without_version() {
        let payload = include_bytes!("../tests/fixtures/android/roundtrip.json");
        assert!(expand_profile(payload, &Event::default()).is_ok());
    }
}
