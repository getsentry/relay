use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::error::ProfileError;
use crate::measurements::Measurement;
use crate::sample::{DebugMeta, Frame, ThreadMetadata, Version};

#[derive(Debug, Serialize, Deserialize)]
pub struct ProfileMetadata {
    /// Random UUID identifying a chunk
    pub chunk_id: String,
    /// Random UUID for each profiler session
    pub profiler_id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub debug_meta: Option<DebugMeta>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment: Option<String>,
    pub platform: String,
    pub release: String,

    /// Hard-coded string containing "2" to indicate the format version.
    pub version: Version,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Sample {
    /// Unix timestamp in seconds with millisecond precision when the sample
    /// was captured.
    pub timestamp: f64,
    pub stack_id: usize,
    pub thread_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProfileChunk {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measurements: Option<BTreeMap<String, Measurement>>,
    #[serde(flatten)]
    pub metadata: ProfileMetadata,
    pub profile: ProfileData,
}

impl ProfileChunk {
    pub fn normalize(&mut self) -> Result<(), ProfileError> {
        self.profile.normalize(self.metadata.platform.as_str())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProfileData {
    pub samples: Vec<Sample>,
    pub stacks: Vec<Vec<usize>>,
    pub frames: Vec<Frame>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_metadata: Option<BTreeMap<String, ThreadMetadata>>,
}

impl ProfileData {
    /// Ensures valid profile chunk or returns an error.
    ///
    /// Mutates the profile chunk. Removes invalid samples and threads.
    /// Throws an error if the profile chunk is malformed.
    /// Removes extra metadata that are not referenced in the samples.
    ///
    /// profile.normalize("cocoa", "arm64e")
    pub fn normalize(&mut self, platform: &str) -> Result<(), ProfileError> {
        if self.samples.is_empty() {
            return Err(ProfileError::NotEnoughSamples);
        }

        if !self.all_stacks_referenced_by_samples_exist() {
            return Err(ProfileError::MalformedSamples);
        }

        if !self.all_frames_referenced_by_stacks_exist() {
            return Err(ProfileError::MalformedStacks);
        }

        self.strip_pointer_authentication_code(platform);
        self.remove_unreferenced_threads();

        Ok(())
    }

    fn strip_pointer_authentication_code(&mut self, platform: &str) {
        let addr = match platform {
            // https://github.com/microsoft/plcrashreporter/blob/748087386cfc517936315c107f722b146b0ad1ab/Source/PLCrashAsyncThread_arm.c#L84
            "cocoa" => 0x0000000FFFFFFFFF,
            _ => return,
        };
        for frame in &mut self.frames {
            frame.strip_pointer_authentication_code(addr);
        }
    }

    /// Checks that all stacks referenced by the samples exist in the stacks.
    fn all_stacks_referenced_by_samples_exist(&self) -> bool {
        self.samples
            .iter()
            .all(|sample| self.stacks.get(sample.stack_id).is_some())
    }

    /// Checks that all frames referenced by the stacks exist in the frames.
    fn all_frames_referenced_by_stacks_exist(&self) -> bool {
        self.stacks.iter().all(|stack| {
            stack
                .iter()
                .all(|frame_id| self.frames.get(*frame_id).is_some())
        })
    }

    fn remove_unreferenced_threads(&mut self) {
        if let Some(thread_metadata) = &mut self.thread_metadata {
            let thread_ids = self
                .samples
                .iter()
                .map(|sample| sample.thread_id.clone())
                .collect::<HashSet<_>>();
            thread_metadata.retain(|thread_id, _| thread_ids.contains(thread_id));
        }
    }
}

pub fn parse(payload: &[u8]) -> Result<ProfileChunk, ProfileError> {
    let d = &mut serde_json::Deserializer::from_slice(payload);
    serde_path_to_error::deserialize(d).map_err(ProfileError::InvalidJson)
}

#[cfg(test)]
mod tests {
    use crate::sample::v2::parse;

    #[test]
    fn test_roundtrip() {
        let first_payload = include_bytes!("../../tests/fixtures/sample/v2/valid.json");
        let first_parse = parse(first_payload);
        assert!(first_parse.is_ok(), "{:#?}", first_parse);
        let second_payload = serde_json::to_vec(&first_parse.unwrap()).unwrap();
        let second_parse = parse(&second_payload[..]);
        assert!(second_parse.is_ok(), "{:#?}", second_parse);
    }
}
