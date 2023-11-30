use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Range;

use chrono::{DateTime, Utc};
use itertools::Itertools;
use relay_event_schema::protocol::{Addr, EventId};
use serde::{Deserialize, Serialize};

use crate::error::ProfileError;
use crate::measurements::Measurement;
use crate::native_debug_image::NativeDebugImage;
use crate::transaction_metadata::TransactionMetadata;
use crate::utils::{deserialize_number_from_string, string_is_null_or_empty};
use crate::MAX_PROFILE_DURATION;

const MAX_PROFILE_DURATION_NS: u64 = MAX_PROFILE_DURATION.as_nanos() as u64;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Frame {
    #[serde(skip_serializing_if = "Option::is_none")]
    abs_path: Option<String>,
    #[serde(alias = "column", skip_serializing_if = "Option::is_none")]
    colno: Option<u32>,
    #[serde(alias = "file", skip_serializing_if = "Option::is_none")]
    filename: Option<String>,
    #[serde(alias = "name", skip_serializing_if = "Option::is_none")]
    function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_app: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    instruction_addr: Option<Addr>,
    #[serde(alias = "line", skip_serializing_if = "Option::is_none")]
    lineno: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    module: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    platform: Option<String>,
}

impl Frame {
    fn strip_pointer_authentication_code(&mut self, pac_code: u64) {
        if let Some(address) = self.instruction_addr {
            self.instruction_addr = Some(Addr(address.0 & pac_code));
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Sample {
    stack_id: usize,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    thread_id: u64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    elapsed_since_start_ns: u64,

    // cocoa only
    #[serde(default, skip_serializing_if = "Option::is_none")]
    queue_address: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ThreadMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    priority: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct QueueMetadata {
    label: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SampleProfile {
    samples: Vec<Sample>,
    stacks: Vec<Vec<usize>>,
    frames: Vec<Frame>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    thread_metadata: Option<HashMap<String, ThreadMetadata>>,

    // cocoa only
    #[serde(default, skip_serializing_if = "Option::is_none")]
    queue_metadata: Option<HashMap<String, QueueMetadata>>,
}

impl SampleProfile {
    /// Ensures valid profiler or returns an error.
    ///
    /// Mutates the profile. Removes invalid samples and threads.
    /// Throws an error if the profile is malformed.
    /// Removes extra metadata that are not referenced in the samples.
    ///
    /// profile.normalize("cocoa", "arm64e")
    pub fn normalize(&mut self, platform: &str, architecture: &str) -> Result<(), ProfileError> {
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

        self.strip_pointer_authentication_code(platform, architecture);
        self.remove_unreferenced_threads();
        self.remove_unreferenced_queues();

        Ok(())
    }

    fn strip_pointer_authentication_code(&mut self, platform: &str, architecture: &str) {
        let addr = match (platform, architecture) {
            // https://github.com/microsoft/plcrashreporter/blob/748087386cfc517936315c107f722b146b0ad1ab/Source/PLCrashAsyncThread_arm.c#L84
            ("cocoa", "arm64") | ("cocoa", "arm64e") => 0x0000000FFFFFFFFF,
            _ => return,
        };
        for frame in &mut self.frames {
            frame.strip_pointer_authentication_code(addr);
        }
    }

    fn remove_idle_samples_at_the_edge(&mut self) {
        let mut active_ranges: HashMap<u64, Range<usize>> = HashMap::new();

        for (i, sample) in self.samples.iter().enumerate() {
            if !self
                .stacks
                .get(sample.stack_id)
                .is_some_and(|stack| !stack.is_empty())
            {
                continue;
            }

            active_ranges
                .entry(sample.thread_id)
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
            .counts_by(|sample| sample.thread_id)
            // Only keep data from threads with more than 1 sample so we can calculate a duration
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .collect::<HashMap<_, _>>();

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
            sample.elapsed_since_start_ns > MAX_PROFILE_DURATION_NS
        })
    }

    fn remove_unreferenced_threads(&mut self) {
        if let Some(thread_metadata) = &mut self.thread_metadata {
            let thread_ids = self
                .samples
                .iter()
                .map(|sample| sample.thread_id.to_string())
                .collect::<HashSet<_>>();
            thread_metadata.retain(|thread_id, _| thread_ids.contains(thread_id));
        }
    }

    fn remove_unreferenced_queues(&mut self) {
        if let Some(queue_metadata) = &mut self.queue_metadata {
            let queue_addresses = self
                .samples
                .iter()
                .filter_map(|sample| sample.queue_address.as_ref())
                .collect::<HashSet<_>>();
            queue_metadata.retain(|queue_address, _| queue_addresses.contains(&queue_address));
        }
    }
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
struct DebugMeta {
    images: Vec<NativeDebugImage>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct OSMetadata {
    name: String,
    version: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    build_number: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RuntimeMetadata {
    name: String,
    version: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct DeviceMetadata {
    architecture: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    is_emulator: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    locale: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    manufacturer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub enum Version {
    #[default]
    Unknown,
    #[serde(rename = "1")]
    V1,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProfileMetadata {
    version: Version,

    #[serde(skip_serializing_if = "Option::is_none")]
    debug_meta: Option<DebugMeta>,

    device: DeviceMetadata,
    os: OSMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime: Option<RuntimeMetadata>,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    environment: String,
    #[serde(alias = "profile_id")]
    event_id: EventId,
    platform: String,
    timestamp: DateTime<Utc>,

    #[serde(default, skip_serializing_if = "string_is_null_or_empty")]
    release: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    dist: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    transactions: Vec<TransactionMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transaction: Option<TransactionMetadata>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    transaction_metadata: BTreeMap<String, String>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    transaction_tags: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProfilingEvent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    measurements: Option<HashMap<String, Measurement>>,
    #[serde(flatten)]
    metadata: ProfileMetadata,
    profile: SampleProfile,
}

impl ProfilingEvent {
    fn valid(&self) -> bool {
        match self.metadata.platform.as_str() {
            "cocoa" => {
                self.metadata.os.build_number.is_some()
                    && self.metadata.device.is_emulator.is_some()
                    && self.metadata.device.locale.is_some()
                    && self.metadata.device.manufacturer.is_some()
                    && self.metadata.device.model.is_some()
            }
            _ => true,
        }
    }
}

fn parse_profile(payload: &[u8]) -> Result<ProfilingEvent, ProfileError> {
    let d = &mut serde_json::Deserializer::from_slice(payload);
    let mut profile: ProfilingEvent =
        serde_path_to_error::deserialize(d).map_err(ProfileError::InvalidJson)?;

    if !profile.valid() {
        return Err(ProfileError::MissingProfileMetadata);
    }

    if profile.metadata.transaction.is_none() {
        profile.metadata.transaction = profile.metadata.transactions.drain(..).next();
    }

    let transaction = profile
        .metadata
        .transaction
        .as_ref()
        .ok_or(ProfileError::NoTransactionAssociated)?;

    if !transaction.valid() {
        return Err(ProfileError::InvalidTransactionMetadata);
    }

    // This is to be compatible with older SDKs
    if transaction.relative_end_ns > 0 {
        profile.profile.samples.retain(|sample| {
            (transaction.relative_start_ns..=transaction.relative_end_ns)
                .contains(&sample.elapsed_since_start_ns)
        });
    }

    profile.profile.normalize(
        profile.metadata.platform.as_str(),
        profile.metadata.device.architecture.as_str(),
    )?;

    Ok(profile)
}

pub fn parse_sample_profile(
    payload: &[u8],
    transaction_metadata: BTreeMap<String, String>,
    transaction_tags: BTreeMap<String, String>,
) -> Result<Vec<u8>, ProfileError> {
    let mut profile = parse_profile(payload)?;

    if let Some(transaction_name) = transaction_metadata.get("transaction") {
        if let Some(ref mut transaction) = profile.metadata.transaction {
            transaction.name = transaction_name.to_owned();
        }
    }

    // Do not replace the release if we're passing one already.
    if profile.metadata.release.is_none() {
        if let Some(release) = transaction_metadata.get("release") {
            profile.metadata.release = Some(release.to_owned());
        }
    }

    if let Some(dist) = transaction_metadata.get("dist") {
        profile.metadata.dist = dist.to_owned();
    }

    if let Some(environment) = transaction_metadata.get("environment") {
        profile.metadata.environment = environment.to_owned();
    }

    profile.metadata.transaction_metadata = transaction_metadata;
    profile.metadata.transaction_tags = transaction_tags;

    serde_json::to_vec(&profile).map_err(|_| ProfileError::CannotSerializePayload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_roundtrip() {
        let payload = include_bytes!("../tests/fixtures/profiles/sample/roundtrip.json");
        let profile = parse_profile(payload);
        assert!(profile.is_ok());
        let data = serde_json::to_vec(&profile.unwrap());
        assert!(parse_profile(&data.unwrap()[..]).is_ok());
    }

    #[test]
    fn test_expand() {
        let payload = include_bytes!("../tests/fixtures/profiles/sample/roundtrip.json");
        let profile = parse_sample_profile(payload, BTreeMap::new(), BTreeMap::new());
        assert!(profile.is_ok());
    }

    fn generate_profile() -> ProfilingEvent {
        ProfilingEvent {
            measurements: None,
            metadata: ProfileMetadata {
                debug_meta: Option::None,
                version: Version::V1,
                timestamp: Utc::now(),
                runtime: Option::None,
                device: DeviceMetadata {
                    architecture: "arm64e".to_string(),
                    is_emulator: Some(true),
                    locale: Some("en_US".to_string()),
                    manufacturer: Some("Apple".to_string()),
                    model: Some("iPhome11,3".to_string()),
                },
                os: OSMetadata {
                    build_number: Some("H3110".to_string()),
                    name: "iOS".to_string(),
                    version: "16.0".to_string(),
                },
                environment: "testing".to_string(),
                platform: "cocoa".to_string(),
                event_id: EventId::new(),
                transaction: Option::None,
                transactions: Vec::new(),
                release: Some("1.0".to_string()),
                dist: "9999".to_string(),
                transaction_metadata: BTreeMap::new(),
                transaction_tags: BTreeMap::new(),
            },
            profile: SampleProfile {
                queue_metadata: Some(HashMap::new()),
                samples: Vec::new(),
                stacks: Vec::new(),
                frames: Vec::new(),
                thread_metadata: Some(HashMap::new()),
            },
        }
    }

    #[test]
    fn test_filter_samples() {
        let mut profile = generate_profile();

        profile.profile.stacks.push(Vec::new());
        profile.profile.samples.extend(vec![
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 1,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 1,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 1,
                thread_id: 2,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 1,
                thread_id: 3,
            },
        ]);

        profile.profile.remove_single_samples_per_thread();

        assert!(profile.profile.samples.len() == 2);
    }

    #[test]
    fn test_parse_profile_with_all_samples_filtered() {
        let mut profile = generate_profile();

        profile.profile.stacks.push(Vec::new());
        profile.profile.samples.extend(vec![
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 1,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 1,
                thread_id: 2,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 1,
                thread_id: 3,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 1,
                thread_id: 4,
            },
        ]);

        let payload = serde_json::to_vec(&profile).unwrap();
        assert!(parse_profile(&payload[..]).is_err());
    }

    #[test]
    fn test_expand_with_samples_inclusive() {
        let mut profile = generate_profile();

        profile.profile.frames.push(Frame {
            ..Default::default()
        });
        profile.metadata.transaction = Some(TransactionMetadata {
            active_thread_id: 1,
            id: EventId::new(),
            name: "blah".to_string(),
            relative_cpu_end_ms: 0,
            relative_cpu_start_ms: 0,
            relative_end_ns: 30,
            relative_start_ns: 10,
            trace_id: EventId::new(),
        });
        profile.profile.stacks.push(vec![0]);
        profile.profile.samples.extend(vec![
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 10,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 20,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 30,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 40,
                thread_id: 1,
            },
        ]);

        let payload = serde_json::to_vec(&profile).unwrap();
        let profile = parse_profile(&payload[..]).unwrap();

        assert_eq!(profile.profile.samples.len(), 3);
    }

    #[test]
    fn test_expand_with_all_samples_outside_transaction() {
        let mut profile = generate_profile();

        profile.profile.frames.push(Frame {
            ..Default::default()
        });
        profile.metadata.transaction = Some(TransactionMetadata {
            active_thread_id: 1,
            id: EventId::new(),
            name: "blah".to_string(),
            relative_cpu_end_ms: 0,
            relative_cpu_start_ms: 0,
            relative_end_ns: 100,
            relative_start_ns: 50,
            trace_id: EventId::new(),
        });
        profile.profile.stacks.push(vec![0]);
        profile.profile.samples.extend(vec![
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 10,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 20,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 30,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 40,
                thread_id: 1,
            },
        ]);

        let payload = serde_json::to_vec(&profile).unwrap();
        let data = parse_sample_profile(&payload[..], BTreeMap::new(), BTreeMap::new());

        assert!(data.is_err());
    }

    #[test]
    fn test_copying_transaction() {
        let mut profile = generate_profile();
        let transaction = TransactionMetadata {
            active_thread_id: 1,
            id: EventId::new(),
            name: "blah".to_string(),
            relative_cpu_end_ms: 0,
            relative_cpu_start_ms: 0,
            relative_end_ns: 100,
            relative_start_ns: 0,
            trace_id: EventId::new(),
        };

        profile.metadata.transactions.push(transaction.clone());
        profile.profile.frames.push(Frame {
            ..Default::default()
        });
        profile.profile.stacks.push(vec![0]);
        profile.profile.samples.extend(vec![
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 10,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 20,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 30,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 40,
                thread_id: 1,
            },
        ]);

        let payload = serde_json::to_vec(&profile).unwrap();
        let profile = parse_profile(&payload[..]).unwrap();

        assert_eq!(Some(transaction), profile.metadata.transaction);
        assert!(profile.metadata.transactions.is_empty());
    }

    #[test]
    fn test_parse_with_no_transaction() {
        let profile = generate_profile();
        let payload = serde_json::to_vec(&profile).unwrap();
        assert!(parse_profile(&payload[..]).is_err());
    }

    #[test]
    fn test_profile_remove_idle_samples_at_start_and_end() {
        let mut profile = generate_profile();
        let transaction = TransactionMetadata {
            active_thread_id: 1,
            id: EventId::new(),
            name: "blah".to_string(),
            relative_cpu_end_ms: 0,
            relative_cpu_start_ms: 0,
            relative_end_ns: 100,
            relative_start_ns: 0,
            trace_id: EventId::new(),
        };

        profile.metadata.transaction = Some(transaction);
        profile.profile.frames.push(Frame {
            ..Default::default()
        });
        profile.profile.stacks = vec![vec![0], vec![]];
        profile.profile.samples = vec![
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 40,
                thread_id: 2,
            },
            Sample {
                stack_id: 1,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 50,
                thread_id: 2,
            },
            Sample {
                stack_id: 1,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 10,
                thread_id: 1,
            },
            Sample {
                stack_id: 1,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 20,
                thread_id: 1,
            },
            Sample {
                stack_id: 1,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 30,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 40,
                thread_id: 1,
            },
            Sample {
                stack_id: 1,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 50,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 60,
                thread_id: 1,
            },
            Sample {
                stack_id: 1,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 70,
                thread_id: 1,
            },
            Sample {
                stack_id: 1,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 90,
                thread_id: 1,
            },
            Sample {
                stack_id: 1,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 80,
                thread_id: 3,
            },
            Sample {
                stack_id: 1,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 90,
                thread_id: 3,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 60,
                thread_id: 2,
            },
        ];

        profile.profile.remove_idle_samples_at_the_edge();

        let mut sample_count_by_thread_id: HashMap<u64, u32> = HashMap::new();

        for sample in &profile.profile.samples {
            *sample_count_by_thread_id
                .entry(sample.thread_id)
                .or_default() += 1;
        }

        assert_eq!(sample_count_by_thread_id, HashMap::from([(1, 3), (2, 3),]));
    }

    #[test]
    fn test_profile_cleanup_metadata() {
        let mut profile = generate_profile();
        let transaction = TransactionMetadata {
            active_thread_id: 1,
            id: EventId::new(),
            name: "blah".to_string(),
            relative_cpu_end_ms: 0,
            relative_cpu_start_ms: 0,
            relative_end_ns: 100,
            relative_start_ns: 0,
            trace_id: EventId::new(),
        };

        profile.metadata.transaction = Some(transaction);
        profile.profile.frames.push(Frame {
            ..Default::default()
        });
        profile.profile.stacks = vec![vec![0]];

        let mut thread_metadata: HashMap<String, ThreadMetadata> = HashMap::new();

        thread_metadata.insert(
            "1".to_string(),
            ThreadMetadata {
                name: Some("".to_string()),
                priority: Some(1),
            },
        );
        thread_metadata.insert(
            "2".to_string(),
            ThreadMetadata {
                name: Some("".to_string()),
                priority: Some(1),
            },
        );
        thread_metadata.insert(
            "3".to_string(),
            ThreadMetadata {
                name: Some("".to_string()),
                priority: Some(1),
            },
        );
        thread_metadata.insert(
            "4".to_string(),
            ThreadMetadata {
                name: Some("".to_string()),
                priority: Some(1),
            },
        );

        let mut queue_metadata: HashMap<String, QueueMetadata> = HashMap::new();

        queue_metadata.insert(
            "0xdeadbeef".to_string(),
            QueueMetadata {
                label: "com.apple.main-thread".to_string(),
            },
        );

        queue_metadata.insert(
            "0x123456789".to_string(),
            QueueMetadata {
                label: "some-label".to_string(),
            },
        );

        profile.profile.thread_metadata = Some(thread_metadata);
        profile.profile.queue_metadata = Some(queue_metadata);
        profile.profile.samples.extend(vec![
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 10,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 20,
                thread_id: 2,
            },
        ]);

        profile.profile.remove_unreferenced_threads();
        profile.profile.remove_unreferenced_queues();

        assert_eq!(profile.profile.thread_metadata.unwrap().len(), 2);
        assert_eq!(profile.profile.queue_metadata.unwrap().len(), 1);
    }

    #[test]
    fn test_extract_transaction_tags() {
        let transaction_metadata = BTreeMap::from([(
            "transaction".to_string(),
            "some-random-transaction".to_string(),
        )]);

        let payload = include_bytes!("../tests/fixtures/profiles/sample/roundtrip.json");
        let profile_json = parse_sample_profile(payload, transaction_metadata, BTreeMap::new());
        assert!(profile_json.is_ok());

        let payload = profile_json.unwrap();
        let d = &mut serde_json::Deserializer::from_slice(&payload[..]);
        let output: ProfilingEvent = serde_path_to_error::deserialize(d)
            .map_err(ProfileError::InvalidJson)
            .unwrap();
        assert_eq!(
            output.metadata.transaction.unwrap().name,
            "some-random-transaction".to_string()
        );
    }

    #[test]
    fn test_keep_profile_under_max_duration() {
        let mut profile = generate_profile();
        profile.profile.samples.extend(vec![
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 10,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: (MAX_PROFILE_DURATION - Duration::from_secs(1)).as_nanos()
                    as u64,
                thread_id: 2,
            },
        ]);

        assert!(!profile.profile.is_above_max_duration());
    }

    #[test]
    fn test_reject_profile_over_max_duration() {
        let mut profile = generate_profile();
        profile.profile.samples.extend(vec![
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: 10,
                thread_id: 1,
            },
            Sample {
                stack_id: 0,
                queue_address: Some("0xdeadbeef".to_string()),
                elapsed_since_start_ns: (MAX_PROFILE_DURATION + Duration::from_secs(1)).as_nanos()
                    as u64,
                thread_id: 2,
            },
        ]);

        assert!(profile.profile.is_above_max_duration());
    }

    #[test]
    fn test_accept_null_or_empty_release() {
        let payload = r#"{
            "version":"1",
            "device":{
                "architecture":"arm64e",
                "is_emulator":true,
                "locale":"en_US",
                "manufacturer":"Apple",
                "model":"iPhome11,3"
            },
            "os":{
                "name":"iOS",
                "version":"16.0",
                "build_number":"H3110"
            },
            "environment":"testing",
            "event_id":"961d6b96017644db895eafd391682003",
            "platform":"cocoa",
            "release":null,
            "timestamp":"2023-11-01T15:27:15.081230Z",
            "transaction":{
                "active_thread_id": 1,
                "id":"9789498b-6970-4dda-b2a1-f9cb91d1a445",
                "name":"blah",
                "trace_id":"809ff2c0-e185-4c21-8f21-6a6fef009352"
            },
            "dist":"9999",
            "profile":{
                "samples":[
                    {
                        "stack_id":0,
                        "elapsed_since_start_ns":1,
                        "thread_id":1
                    },
                    {
                        "stack_id":0,
                        "elapsed_since_start_ns":2,
                        "thread_id":1
                    }
                ],
                "stacks":[[0]],
                "frames":[{
                    "function":"main"
                }],
                "thread_metadata":{},
                "queue_metadata":{}
            }
        }"#;
        let profile = parse_profile(payload.as_bytes());
        assert!(profile.is_ok());
        assert_eq!(profile.unwrap().metadata.release, None);
    }
}
