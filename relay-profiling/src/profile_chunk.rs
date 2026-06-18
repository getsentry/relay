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
            // Present only on legacy android profiles: the raw, unparsed profile.
            // Ignored here, we only care whether it's set.
            sampled_profile: Option<serde::de::IgnoredAny>,
        }

        let minimal: MinimalProfile = {
            let d = &mut serde_json::Deserializer::from_slice(data);
            serde_path_to_error::deserialize(d)
        }?;

        // Android profiles used to always be parsed with higher priority than `v2`,
        // but that misroutes android profiles which are actually in the sample `v2`
        // format.
        // The `sampled_profile` check is an interim fix: the Android SDK
        // sends legacy profiles with version "2" even though they don't follow the
        // sample `v2` format, so we keep routing those to the legacy android format.
        let is_legacy_android = minimal.platform == "android"
            && (minimal.version == sample::Version::Unknown || minimal.sampled_profile.is_some());
        if is_legacy_android {
            return AndroidProfileChunk::parse(data)
                .map(Box::new)
                .map(Self::Android);
        }

        match minimal.version {
            sample::Version::V2 => V2ProfileChunk::parse(data).map(Box::new).map(Self::V2),
            sample::Version::V1 => Err(ProfileError::PlatformNotSupported),
            sample::Version::Unknown => Err(ProfileError::PlatformNotSupported),
        }
    }
}
