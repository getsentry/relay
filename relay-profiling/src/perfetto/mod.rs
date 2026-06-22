use bytes::Bytes;

use crate::sample::v2;
use crate::{ProfileError, V2ProfileChunk};

mod convert;
#[allow(dead_code)]
mod proto;

/// A parsed Perfetto profiling chunk.
#[derive(Debug)]
pub struct Chunk {
    inner: v2::ProfileChunk,
    perfetto: Bytes,
}

impl Chunk {
    /// Parses a [`Chunk`] from the required [`v2::ProfileChunk`] and a Perfetto profile.
    ///
    /// A Perfetto profile always requires an associated [`v2::ProfileChunk`] for additional
    /// metadata. The resulting [`Chunk`] contains all metadata from the [`v2::ProfileChunk`]
    /// and samples from the `perfetto` profile.
    ///
    /// Note: if the parsed `sample` already contains profiling information, the frames in the
    /// Perfetto profile are not extracted again.
    pub fn parse(sample: &[u8], perfetto: Bytes) -> Result<Self, ProfileError> {
        let mut inner: v2::ProfileChunk = {
            let deserializer = &mut serde_json::Deserializer::from_slice(sample);
            serde_path_to_error::deserialize(deserializer).map_err(ProfileError::InvalidJson)?
        };

        if inner.profile.is_empty() {
            let (profile_data, debug_images) = convert::convert(&perfetto)?;
            inner.profile = profile_data;
            inner.metadata.debug_meta.images = debug_images;
        }

        Ok(Self { inner, perfetto })
    }

    /// Returns the Perfetto profile this [`Chunk`] was parsed from.
    pub fn perfetto(&self) -> &Bytes {
        &self.perfetto
    }

    /// Returns the combined metadata and Perfetto profile as a [`V2ProfileChunk`].
    pub fn as_v2(&self) -> &V2ProfileChunk {
        &self.inner
    }
}

impl crate::profile_chunk::ProfileChunk for Chunk {
    fn platform(&self) -> &str {
        &self.inner.metadata.platform
    }

    fn normalize(&mut self) -> Result<(), ProfileError> {
        self.inner.normalize()
    }
}

impl relay_filter::Filterable for Chunk {
    fn release(&self) -> Option<&str> {
        self.inner.metadata.release.as_deref()
    }
}

impl relay_protocol::Getter for Chunk {
    fn get_value(&self, path: &str) -> Option<relay_protocol::Val<'_>> {
        self.inner.get_value(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{ProfileChunk, ProfileType};

    const PERFETTO_ANDROID: Bytes = Bytes::from_static(include_bytes!(
        "../../tests/fixtures/android/perfetto/android.pftrace"
    ));

    #[test]
    fn test_parse_perfetto() {
        let metadata_json = serde_json::json!({
            "version": "2",
            "chunk_id": "0432a0a4c25f4697bf9f0a2fcbe6a814",
            "profiler_id": "4d229f1d3807421ba62a5f8bc295d836",
            "platform": "android",
            "content_type": "perfetto",
            "client_sdk": {"name": "sentry-android", "version": "1.0"},
        });
        let metadata_bytes = serde_json::to_vec(&metadata_json).unwrap();

        let chunk = Chunk::parse(&metadata_bytes, PERFETTO_ANDROID).unwrap();

        assert_eq!(chunk.inner.metadata.platform, "android");
        assert_eq!(chunk.profile_type(), ProfileType::Ui);

        insta::assert_json_snapshot!(chunk.inner);
    }

    #[test]
    fn test_parse_perfetto_invalid_metadata() {
        let result = Chunk::parse(b"not json", PERFETTO_ANDROID);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_perfetto_empty_trace() {
        // Valid metadata but no profiling samples in the binary → should fail.
        let metadata_bytes = serde_json::to_vec(&serde_json::json!({
            "version": "2",
            "chunk_id": "0432a0a4c25f4697bf9f0a2fcbe6a814",
            "profiler_id": "4d229f1d3807421ba62a5f8bc295d836",
            "platform": "android",
            "content_type": "perfetto",
            "client_sdk": {"name": "sentry-android", "version": "1.0"},
        }))
        .unwrap();

        let _ = Chunk::parse(&metadata_bytes, Bytes::from_static(b"")).unwrap_err();
    }

    #[test]
    fn test_parse_perfetto_missing_required_field() {
        // metadata is missing the required `chunk_id` field → de-serialization error.
        let metadata_bytes = serde_json::to_vec(&serde_json::json!({
            "version": "2",
            "profiler_id": "4d229f1d3807421ba62a5f8bc295d836",
            "platform": "android",
            "client_sdk": {"name": "sentry-android", "version": "1.0"},
        }))
        .unwrap();

        let result = Chunk::parse(&metadata_bytes, PERFETTO_ANDROID);
        assert!(
            matches!(result, Err(ProfileError::InvalidJson(_))),
            "expected InvalidJson, got {result:?}"
        );
    }
}
