//! Sample Format V2
//!
//! This version of the sample format expects a collection of samples to be sent with no reference
//! to the events collected while the profiler was running.
//!
//! We collect a profiler ID, meaning to be a random identifier for this specific instance of the
//! profiler and not a persistent ID. It only needs to be valid from the start of the profiler to
//! when it stops and will be useful to then group samples on the backend.
//!
//! Spans are expected to carry the profiler ID to know which samples are associated with them.
//!
use std::collections::{BTreeMap, HashSet};

use relay_event_schema::protocol::EventId;
use serde::{Deserialize, Serialize};

use crate::error::ProfileError;
use crate::measurements::Measurement;
use crate::sample::{DebugMeta, Frame, ThreadMetadata, Version};

#[derive(Debug, Serialize, Deserialize)]
pub struct ProfileMetadata {
    /// Random UUID identifying a chunk
    pub chunk_id: EventId,
    /// Random UUID for each profiler session
    pub profiler_id: EventId,

    #[serde(default, skip_serializing_if = "DebugMeta::is_empty")]
    pub debug_meta: DebugMeta,

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
    /// Index of the stack in the `stacks` field of the profile.
    pub stack_id: usize,
    /// Thread or queue identifier
    pub thread_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProfileChunk {
    // `measurements` contains CPU/memory measurements we do during the capture of the chunk.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub measurements: BTreeMap<String, Measurement>,
    /// This struct contains all the metadata related to the chunk but all fields are expected to
    /// be at the top-level of the object.
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
    /// `samples` contains the list of samples referencing a stack and thread identifier.
    /// If 2 stack of frames captured at 2 different timestamps are identical, you're expected to
    /// reference the same `stack_id`.
    pub samples: Vec<Sample>,
    /// `stacks` contains a list of stacks indicating the index of the frame in the `frames` field.
    /// We do this to not have to repeat frames in different stacks.
    pub stacks: Vec<Vec<usize>>,
    /// `frames` contains a list of unique frames found in the profile.
    pub frames: Vec<Frame>,

    /// `thread_metadata` contains information about the thread or the queue. The identifier is a
    /// string and can be any unique identifier for the thread or stack (an integer or an address
    /// for example).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub thread_metadata: BTreeMap<String, ThreadMetadata>,
}

impl ProfileData {
    /// Ensures valid profile chunk or returns an error.
    ///
    /// Mutates the profile chunk. Removes invalid samples and threads.
    /// Throws an error if the profile chunk is malformed.
    /// Removes extra metadata that are not referenced in the samples.
    pub fn normalize(&mut self, platform: &str) -> Result<(), ProfileError> {
        if self.samples.is_empty() {
            return Err(ProfileError::NotEnoughSamples);
        }

        // Remove NaN as a side-effect
        self.sort_samples_by_timestamp();

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
        let thread_ids = self
            .samples
            .iter()
            .map(|sample| sample.thread_id.clone())
            .collect::<HashSet<_>>();
        self.thread_metadata
            .retain(|thread_id, _| thread_ids.contains(thread_id));
    }

    fn sort_samples_by_timestamp(&mut self) {
        self.samples.retain(|s| !s.timestamp.is_nan());
        self.samples
            .sort_by(|a, b| a.timestamp.partial_cmp(&b.timestamp).unwrap());
    }
}

pub fn parse(payload: &[u8]) -> Result<ProfileChunk, ProfileError> {
    let d = &mut serde_json::Deserializer::from_slice(payload);
    serde_path_to_error::deserialize(d).map_err(ProfileError::InvalidJson)
}

#[cfg(test)]
mod tests {
    use crate::sample::v2::{parse, Frame, ProfileData, Sample};

    #[test]
    fn test_roundtrip() {
        let first_payload = include_bytes!("../../tests/fixtures/sample/v2/valid.json");
        let first_parse = parse(first_payload);
        assert!(first_parse.is_ok(), "{:#?}", first_parse);
        let second_payload = serde_json::to_vec(&first_parse.unwrap()).unwrap();
        let second_parse = parse(&second_payload[..]);
        assert!(second_parse.is_ok(), "{:#?}", second_parse);
    }

    #[test]
    fn test_samples_sorted() {
        let mut chunk = ProfileData {
            samples: vec![
                Sample {
                    stack_id: 0,
                    thread_id: "1".to_string(),
                    timestamp: 2000.0,
                },
                Sample {
                    stack_id: 0,
                    thread_id: "1".to_string(),
                    timestamp: 1000.0,
                },
                Sample {
                    stack_id: 0,
                    thread_id: "1".to_string(),
                    timestamp: f64::NAN,
                },
            ],
            stacks: vec![vec![0]],
            frames: vec![Frame {
                ..Default::default()
            }],
            thread_metadata: Default::default(),
        };

        chunk.sort_samples_by_timestamp();

        let timestamps: Vec<f64> = chunk.samples.iter().map(|s| s.timestamp).collect();

        assert_eq!(chunk.samples.len(), 2);
        assert_eq!(timestamps, vec![1000.0, 2000.0]);
    }
}
