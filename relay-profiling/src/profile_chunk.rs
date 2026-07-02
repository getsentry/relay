use serde::Deserialize;

use crate::{
    AndroidProfileChunk, PerfettoProfileChunk, ProfileError, ProfileType, V2ProfileChunk, sample,
};

/// Minimum interface all profile chunk types must implement.
pub trait ProfileChunk {
    /// Returns the platform this profile chunk is associated with.
    fn platform(&self) -> &str;

    /// Returns the [`ProfileType`] of this profile chunk.
    ///
    /// By default this is inferred from the [`Self::platform`].
    fn profile_type(&self) -> ProfileType {
        ProfileType::from_platform(self.platform())
    }

    /// Normalizes the profile chunk.
    fn normalize(&mut self) -> Result<(), ProfileError>;
}

/// Supported profile chunks for continous profiling.
#[derive(Debug)]
pub enum AnyProfileChunk {
    Android(Box<AndroidProfileChunk>),
    Perfetto(Box<PerfettoProfileChunk>),
    V2(Box<V2ProfileChunk>),
}

impl From<Box<V2ProfileChunk>> for AnyProfileChunk {
    fn from(chunk: Box<V2ProfileChunk>) -> Self {
        Self::V2(chunk)
    }
}

impl From<Box<AndroidProfileChunk>> for AnyProfileChunk {
    fn from(chunk: Box<AndroidProfileChunk>) -> Self {
        Self::Android(chunk)
    }
}

impl From<Box<PerfettoProfileChunk>> for AnyProfileChunk {
    fn from(chunk: Box<PerfettoProfileChunk>) -> Self {
        Self::Perfetto(chunk)
    }
}

impl From<AndroidOrV2ProfileChunk> for AnyProfileChunk {
    fn from(chunk: AndroidOrV2ProfileChunk) -> Self {
        match chunk {
            AndroidOrV2ProfileChunk::Android(c) => Self::Android(c),
            AndroidOrV2ProfileChunk::V2(c) => Self::V2(c),
        }
    }
}

impl ProfileChunk for AnyProfileChunk {
    fn platform(&self) -> &str {
        match self {
            AnyProfileChunk::Android(chunk) => chunk.platform(),
            AnyProfileChunk::Perfetto(chunk) => chunk.platform(),
            AnyProfileChunk::V2(chunk) => chunk.platform(),
        }
    }

    fn normalize(&mut self) -> Result<(), ProfileError> {
        match self {
            AnyProfileChunk::Android(chunk) => chunk.normalize(),
            AnyProfileChunk::Perfetto(chunk) => chunk.normalize(),
            AnyProfileChunk::V2(chunk) => chunk.normalize(),
        }
    }
}

impl relay_protocol::Getter for AnyProfileChunk {
    fn get_value(&self, path: &str) -> Option<relay_protocol::Val<'_>> {
        match self {
            AnyProfileChunk::Android(chunk) => chunk.get_value(path),
            AnyProfileChunk::Perfetto(chunk) => chunk.get_value(path),
            AnyProfileChunk::V2(chunk) => chunk.get_value(path),
        }
    }
}

impl relay_filter::Filterable for AnyProfileChunk {
    fn release(&self) -> Option<&str> {
        match self {
            AnyProfileChunk::Android(chunk) => chunk.release(),
            AnyProfileChunk::Perfetto(chunk) => chunk.release(),
            AnyProfileChunk::V2(chunk) => chunk.release(),
        }
    }
}

/// Either an [`AndroidProfileChunk`] or a [`V2ProfileChunk`].
#[derive(Debug)]
pub enum AndroidOrV2ProfileChunk {
    Android(Box<AndroidProfileChunk>),
    V2(Box<V2ProfileChunk>),
}

impl ProfileChunk for AndroidOrV2ProfileChunk {
    fn platform(&self) -> &str {
        match self {
            AndroidOrV2ProfileChunk::Android(chunk) => chunk.platform(),
            AndroidOrV2ProfileChunk::V2(chunk) => chunk.platform(),
        }
    }

    fn normalize(&mut self) -> Result<(), ProfileError> {
        match self {
            AndroidOrV2ProfileChunk::Android(chunk) => chunk.normalize(),
            AndroidOrV2ProfileChunk::V2(chunk) => chunk.normalize(),
        }
    }
}

impl AndroidOrV2ProfileChunk {
    /// Parses either a [`AndroidOrV2ProfileChunk`] or [`ProfileChunk`] from a slice of bytes.
    pub fn parse(data: &[u8]) -> Result<Self, ProfileError> {
        #[derive(Debug, Deserialize)]
        struct MinimalProfile {
            platform: String,
            #[serde(default)]
            version: sample::Version,
            #[serde(default)]
            sampled_profile: Option<serde::de::IgnoredAny>,
        }

        let minimal: MinimalProfile = {
            let d = &mut serde_json::Deserializer::from_slice(data);
            serde_path_to_error::deserialize(d)
        }?;

        let is_android_trace_profile = minimal.platform == "android"
            && matches!(
                minimal.version,
                sample::Version::V2 | sample::Version::V2AndroidTrace
            )
            && minimal.sampled_profile.is_some();

        match (minimal.version, is_android_trace_profile) {
            // Android SDKs produce two profile_chunk types that pass through this method: trace
            // profiles and Application-Not-Responding (ANR) profiles. They come in multiple
            // varieties, each of which needs to be accounted for.

            // Trace profiles:
            // ---------------
            // Versions: 2 (incorrect), 2.android-trace (corrected)
            // Platform: android
            // Content field: sampled_profile (i.e., Android Runtime's event-based format, aka
            //   "traces")
            // Destination type: AndroidProfileChunk

            // ANR profiles:
            // ---------------
            // Version: 2
            // Platform: java (incorrect), android (corrected)
            // Content field: profile (i.e., standardized stacks/frames/samples format)
            // Destination type: V2ProfileChunk

            // First handle Android trace profiles...
            (_, true) => AndroidProfileChunk::parse(data)
                .map(Box::new)
                .map(Self::Android),

            // ...then handle everything else (Android ANRs, Cocoa profiles, etc.).
            (sample::Version::V2, _) => V2ProfileChunk::parse(data).map(Box::new).map(Self::V2),
            (
                sample::Version::V2AndroidTrace | sample::Version::V1 | sample::Version::Unknown,
                _,
            ) => Err(ProfileError::PlatformNotSupported),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    #[test]
    fn test_parse_android_trace_profile_into_android_profile_chunk() {
        let base_payload: Value =
            serde_json::from_slice(include_bytes!("../tests/fixtures/android/chunk/valid.json"))
                .unwrap();

        for version in ["2", "2.android-trace"] {
            let mut payload = base_payload.clone();
            payload["version"] = json!(version);
            let data = serde_json::to_vec(&payload).unwrap();

            let chunk = AndroidOrV2ProfileChunk::parse(&data).unwrap();

            assert!(
                matches!(chunk, AndroidOrV2ProfileChunk::Android(_)),
                "expected Android profile chunk for version {version:?}"
            );
        }
    }

    #[test]
    fn test_return_error_for_android_trace_profile_without_version() {
        for (fixture, payload) in [
            (
                "android trace format",
                &include_bytes!("../tests/fixtures/android/chunk/valid.json")[..],
            ),
            (
                "react native android trace format",
                &include_bytes!("../tests/fixtures/android/chunk/valid-rn.json")[..],
            ),
        ] {
            let mut payload: Value = serde_json::from_slice(payload).unwrap();
            payload.as_object_mut().unwrap().remove("version");
            let data = serde_json::to_vec(&payload).unwrap();

            let err = AndroidOrV2ProfileChunk::parse(&data).unwrap_err();
            assert!(
                matches!(err, ProfileError::PlatformNotSupported),
                "expected unsupported platform error for {fixture}"
            );
        }
    }

    #[test]
    fn test_parse_sample_v2_profile_into_v2_profile_chunk() {
        let base_payload: Value =
            serde_json::from_slice(include_bytes!("../tests/fixtures/sample/v2/valid.json"))
                .unwrap();

        for platform in ["android", "cocoa"] {
            let mut payload = base_payload.clone();
            payload["platform"] = json!(platform);
            let data = serde_json::to_vec(&payload).unwrap();

            let chunk = AndroidOrV2ProfileChunk::parse(&data).unwrap();

            assert!(
                matches!(chunk, AndroidOrV2ProfileChunk::V2(_)),
                "expected v2 profile chunk for platform {platform:?}"
            );
        }
    }

    #[test]
    fn test_return_error_for_android_trace_version_without_android_trace_payload() {
        let base_payload: Value =
            serde_json::from_slice(include_bytes!("../tests/fixtures/sample/v2/valid.json"))
                .unwrap();

        for platform in ["android", "cocoa"] {
            let mut payload = base_payload.clone();
            payload["platform"] = json!(platform);
            payload["version"] = json!("2.android-trace");
            let data = serde_json::to_vec(&payload).unwrap();

            let err = AndroidOrV2ProfileChunk::parse(&data).unwrap_err();
            assert!(
                matches!(err, ProfileError::PlatformNotSupported),
                "expected unsupported platform error for platform {platform:?}"
            );
        }
    }

    #[test]
    fn test_return_error_for_version_1_profile() {
        for (fixture, payload) in [
            (
                "sample v2 format",
                &include_bytes!("../tests/fixtures/sample/v2/valid.json")[..],
            ),
            (
                "android trace format",
                &include_bytes!("../tests/fixtures/android/chunk/valid.json")[..],
            ),
        ] {
            let mut payload: Value = serde_json::from_slice(payload).unwrap();
            payload["version"] = json!("1");
            let data = serde_json::to_vec(&payload).unwrap();

            let err = AndroidOrV2ProfileChunk::parse(&data).unwrap_err();
            assert!(
                matches!(err, ProfileError::PlatformNotSupported),
                "expected unsupported platform error for {fixture}"
            );
        }
    }

    #[test]
    fn test_return_error_for_unknown_version_profile() {
        let base_payload: Value =
            serde_json::from_slice(include_bytes!("../tests/fixtures/sample/v2/valid.json"))
                .unwrap();

        for platform in ["android", "cocoa"] {
            let mut payload = base_payload.clone();
            payload["platform"] = json!(platform);
            payload.as_object_mut().unwrap().remove("version");
            let data = serde_json::to_vec(&payload).unwrap();

            let err = AndroidOrV2ProfileChunk::parse(&data).unwrap_err();
            assert!(
                matches!(err, ProfileError::PlatformNotSupported),
                "expected unsupported platform error for platform {platform:?}"
            );
        }
    }
}
