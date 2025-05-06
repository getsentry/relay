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
use std::net::IpAddr;
use std::time::Duration;

use bytes::Bytes;
use url::Url;

use relay_base_schema::project::ProjectId;
use relay_dynamic_config::GlobalConfig;
use relay_event_schema::protocol::{Csp, Event, EventId, Exception, LogEntry, Values};
use relay_filter::{Filterable, ProjectFiltersConfig};
use relay_protocol::{Getter, Val};
use serde::Deserialize;
use serde_json::Deserializer;

use crate::extract_from_transaction::{extract_transaction_metadata, extract_transaction_tags};

pub use crate::error::ProfileError;
pub use crate::outcomes::discard_reason;

mod android;
mod debug_image;
mod error;
mod extract_from_transaction;
mod measurements;
mod outcomes;
mod sample;
mod transaction_metadata;
mod types;
mod utils;

const MAX_PROFILE_DURATION: Duration = Duration::from_secs(30);
/// For continuous profiles, each chunk can be at most 1 minute.
/// In certain circumstances (e.g. high cpu load) the profiler
/// the profiler may be stopped slightly after 60, hence here we
/// give it a bit more room to handle such cases (66 instead of 60)
const MAX_PROFILE_CHUNK_DURATION: Duration = Duration::from_secs(66);

/// Unique identifier for a profile.
///
/// Same format as event IDs.
pub type ProfileId = EventId;

/// Determines the type/use of a [`ProfileChunk`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileType {
    /// A backend profile.
    Backend,
    /// A UI profile.
    Ui,
}

impl ProfileType {
    /// Converts a platform to a [`ProfileType`].
    ///
    /// The profile type is currently determined based on the contained profile
    /// platform. It determines the data category this profile chunk belongs to.
    ///
    /// This needs to be synchronized with the implementation in Sentry:
    /// <https://github.com/getsentry/sentry/blob/ed2e1c8bcd0d633e6f828fcfbeefbbdd98ef3dba/src/sentry/profiles/task.py#L995>
    pub fn from_platform(platform: &str) -> Self {
        match platform {
            "cocoa" | "android" | "javascript" => Self::Ui,
            _ => Self::Backend,
        }
    }
}

#[derive(Debug, Deserialize)]
struct MinimalProfile {
    #[serde(alias = "profile_id", alias = "chunk_id")]
    event_id: ProfileId,
    platform: String,
    release: Option<String>,
    #[serde(default)]
    version: sample::Version,
}

impl Filterable for MinimalProfile {
    fn csp(&self) -> Option<&Csp> {
        None
    }

    fn exceptions(&self) -> Option<&Values<Exception>> {
        None
    }

    fn ip_addr(&self) -> Option<&str> {
        None
    }

    fn logentry(&self) -> Option<&LogEntry> {
        None
    }

    fn release(&self) -> Option<&str> {
        self.release.as_deref()
    }

    fn transaction(&self) -> Option<&str> {
        None
    }

    fn url(&self) -> Option<Url> {
        None
    }

    fn user_agent(&self) -> Option<&str> {
        None
    }

    fn header(&self, _: &str) -> Option<&str> {
        None
    }
}

impl Getter for MinimalProfile {
    fn get_value(&self, path: &str) -> Option<Val<'_>> {
        match path.strip_prefix("event.")? {
            "release" => self.release.as_deref().map(|release| release.into()),
            "platform" => Some(self.platform.as_str().into()),
            _ => None,
        }
    }
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
                let _: android::legacy::ProfileMetadata = match serde_path_to_error::deserialize(d)
                {
                    Ok(profile) => profile,
                    Err(err) => {
                        relay_log::debug!(
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

pub fn expand_profile(
    payload: &[u8],
    event: &Event,
    client_ip: Option<IpAddr>,
    filter_settings: &ProjectFiltersConfig,
    global_config: &GlobalConfig,
) -> Result<(ProfileId, Vec<u8>), ProfileError> {
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

    if let Err(filter_stat_key) = relay_filter::should_filter(
        &profile,
        client_ip,
        filter_settings,
        global_config.filters(),
    ) {
        return Err(ProfileError::Filtered(filter_stat_key));
    }

    let transaction_metadata = extract_transaction_metadata(event);
    let transaction_tags = extract_transaction_tags(event);
    let processed_payload = match (profile.platform.as_str(), profile.version) {
        (_, sample::Version::V1) => {
            sample::v1::parse_sample_profile(payload, transaction_metadata, transaction_tags)
        }
        ("android", _) => {
            android::legacy::parse_android_profile(payload, transaction_metadata, transaction_tags)
        }
        (_, _) => return Err(ProfileError::PlatformNotSupported),
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
                relay_log::debug!(
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

/// Intermediate type for all processing on a profile chunk.
pub struct ProfileChunk {
    profile: MinimalProfile,
    payload: Bytes,
}

impl ProfileChunk {
    /// Parses a new [`Self`] from raw bytes.
    pub fn new(payload: Bytes) -> Result<Self, ProfileError> {
        match minimal_profile_from_json(&payload) {
            Ok(profile) => Ok(Self { profile, payload }),
            Err(err) => {
                relay_log::debug!(
                    error = &err as &dyn Error,
                    from = "minimal",
                    "invalid profile chunk",
                );
                Err(ProfileError::InvalidJson(err))
            }
        }
    }

    /// Returns the [`ProfileType`] this chunk belongs to.
    ///
    /// This is currently determined from the platform via [`ProfileType::from_platform`].
    pub fn profile_type(&self) -> ProfileType {
        ProfileType::from_platform(&self.profile.platform)
    }

    /// Applies inbound filters to the profile chunk.
    ///
    /// The profile needs to be filtered (rejected) when this returns an error.
    pub fn filter(
        &self,
        client_ip: Option<IpAddr>,
        filter_settings: &ProjectFiltersConfig,
        global_config: &GlobalConfig,
    ) -> Result<(), ProfileError> {
        relay_filter::should_filter(
            &self.profile,
            client_ip,
            filter_settings,
            global_config.filters(),
        )
        .map_err(ProfileError::Filtered)
    }

    /// Normalizes and 'expands' the profile chunk into its normalized form Sentry expects.
    pub fn expand(&self) -> Result<Vec<u8>, ProfileError> {
        match (self.profile.platform.as_str(), self.profile.version) {
            ("android", _) => android::chunk::parse(&self.payload),
            (_, sample::Version::V2) => {
                let mut profile = sample::v2::parse(&self.payload)?;
                profile.normalize()?;
                Ok(serde_json::to_vec(&profile)
                    .map_err(|_| ProfileError::CannotSerializePayload)?)
            }
            (_, _) => Err(ProfileError::PlatformNotSupported),
        }
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
        assert!(
            expand_profile(
                payload,
                &Event::default(),
                None,
                &ProjectFiltersConfig::default(),
                &GlobalConfig::default()
            )
            .is_ok()
        );
    }

    #[test]
    fn test_expand_profile_with_version_and_segment_id() {
        let payload = include_bytes!("../tests/fixtures/sample/v1/segment_id.json");
        assert!(
            expand_profile(
                payload,
                &Event::default(),
                None,
                &ProjectFiltersConfig::default(),
                &GlobalConfig::default()
            )
            .is_ok()
        );
    }

    #[test]
    fn test_expand_profile_without_version() {
        let payload = include_bytes!("../tests/fixtures/android/legacy/roundtrip.json");
        assert!(
            expand_profile(
                payload,
                &Event::default(),
                None,
                &ProjectFiltersConfig::default(),
                &GlobalConfig::default()
            )
            .is_ok()
        );
    }
}
