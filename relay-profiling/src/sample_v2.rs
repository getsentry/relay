use std::collections::{BTreeMap, HashSet};
use std::ops::Range;

use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::error::ProfileError;
use crate::measurements::Measurement;
use crate::sample::{DebugMeta, Frame, ThreadMetadata, Version};

const MAX_PROFILE_CHUNK_DURATION_MS: f64 = 10000f64;

#[derive(Debug, Serialize, Deserialize)]
pub struct ProfileMetadata {
    chunk_id: String,
    profiler_id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    debug_meta: Option<DebugMeta>,

    #[serde(skip_serializing_if = "Option::is_none")]
    environment: Option<String>,
    platform: String,
    release: String,

    timestamp: DateTime<Utc>,
    version: Version,
}

#[derive(Debug, Serialize, Deserialize)]
struct Sample {
    timestamp_ms: f64,
    stack_id: usize,
    thread_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProfileChunk {
    #[serde(skip_serializing_if = "Option::is_none")]
    measurements: Option<BTreeMap<String, Measurement>>,
    #[serde(flatten)]
    metadata: ProfileMetadata,
    profile: ProfileData,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProfileData {
    samples: Vec<Sample>,
    stacks: Vec<Vec<usize>>,
    frames: Vec<Frame>,

    #[serde(skip_serializing_if = "Option::is_none")]
    thread_metadata: Option<BTreeMap<String, ThreadMetadata>>,
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
        // Clean samples before running the checks.
        self.remove_idle_samples_at_the_edge();
        self.remove_single_samples_per_thread();

        if self.samples.is_empty() {
            return Err(ProfileError::NotEnoughSamples);
        }

        if !self.all_stacks_referenced_by_samples_exist() {
            return Err(ProfileError::MalformedSamples);
        }

        if !self.all_frames_referenced_by_stacks_exist() {
            return Err(ProfileError::MalformedStacks);
        }

        if self.is_above_max_duration() {
            return Err(ProfileError::DurationIsTooLong);
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

    fn remove_idle_samples_at_the_edge(&mut self) {
        let mut active_ranges: BTreeMap<String, Range<usize>> = BTreeMap::new();

        for (i, sample) in self.samples.iter().enumerate() {
            if !self
                .stacks
                .get(sample.stack_id)
                .is_some_and(|stack| !stack.is_empty())
            {
                continue;
            }

            active_ranges
                .entry(sample.thread_id.clone())
                .and_modify(|range| range.end = i + 1)
                .or_insert(i..i + 1);
        }

        self.samples = self
            .samples
            .drain(..)
            .enumerate()
            .filter(|(i, sample)| {
                active_ranges
                    .get(&sample.thread_id)
                    .is_some_and(|range| range.contains(i))
            })
            .map(|(_, sample)| sample)
            .collect();
    }

    /// Removes a sample when it's the only sample on its thread
    fn remove_single_samples_per_thread(&mut self) {
        let sample_count_by_thread_id = &self
            .samples
            .iter()
            .counts_by(|sample| sample.thread_id.clone())
            // Only keep data from threads with more than 1 sample so we can calculate a duration
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .collect::<BTreeMap<_, _>>();

        self.samples
            .retain(|sample| sample_count_by_thread_id.contains_key(&sample.thread_id));
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

    /// Checks if the last sample was recorded within the max profile duration.
    fn is_above_max_duration(&self) -> bool {
        self.samples.last().map_or(false, |sample| {
            sample.timestamp_ms > MAX_PROFILE_CHUNK_DURATION_MS
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

fn parse_profile(payload: &[u8]) -> Result<ProfileChunk, ProfileError> {
    let d = &mut serde_json::Deserializer::from_slice(payload);
    let mut profile: ProfileChunk =
        serde_path_to_error::deserialize(d).map_err(ProfileError::InvalidJson)?;

    profile
        .profile
        .normalize(profile.metadata.platform.as_str())?;

    Ok(profile)
}

pub fn parse(payload: &[u8]) -> Result<Vec<u8>, ProfileError> {
    let profile = parse_profile(payload)?;
    serde_json::to_vec(&profile).map_err(|_| ProfileError::CannotSerializePayload)
}

#[cfg(test)]
mod tests {
    use crate::sample_v2::{parse, parse_profile};

    #[test]
    fn test_roundtrip() {
        let first_payload = include_bytes!("../tests/fixtures/profiles/sample_v2/valid.json");
        let first_parse = parse_profile(first_payload);
        assert!(first_parse.is_ok(), "{:#?}", first_parse);
        let second_payload = serde_json::to_vec(&first_parse.unwrap()).unwrap();
        let second_parse = parse_profile(&second_payload[..]);
        assert!(second_parse.is_ok(), "{:#?}", second_parse);
    }

    #[test]
    fn test_expand() {
        let payload = include_bytes!("../tests/fixtures/profiles/sample_v2/valid.json");
        let profile = parse(payload);
        assert!(profile.is_ok(), "{:#?}", profile);
    }
}
