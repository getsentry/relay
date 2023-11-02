use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Range;

use relay_event_schema::protocol::Addr;
use serde::{Deserialize, Serialize};

use crate::error::ProfileError;
use crate::profile_metadata::ProfileMetadata;
use crate::utils::deserialize_number_from_string;
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
pub struct Profile {
    samples: Vec<Sample>,
    stacks: Vec<Vec<usize>>,
    frames: Vec<Frame>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    thread_metadata: Option<HashMap<String, ThreadMetadata>>,

    // cocoa only
    #[serde(default, skip_serializing_if = "Option::is_none")]
    queue_metadata: Option<HashMap<String, QueueMetadata>>,
}

impl Profile {
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
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SampleProfile {
    #[serde(flatten)]
    metadata: ProfileMetadata,
    profile: Profile,
}

impl SampleProfile {
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

    fn check_samples(&self) -> bool {
        for sample in &self.profile.samples {
            if self.profile.stacks.get(sample.stack_id).is_none() {
                return false;
            }
        }
        true
    }

    fn check_stacks(&self) -> bool {
        for stack in &self.profile.stacks {
            for frame_id in stack {
                if self.profile.frames.get(*frame_id).is_none() {
                    return false;
                }
            }
        }
        true
    }

    fn is_above_max_duration(&self) -> bool {
        if let Some(sample) = &self.profile.samples.last() {
            return sample.elapsed_since_start_ns > MAX_PROFILE_DURATION_NS;
        }
        false
    }

    /// Removes a sample when it's the only sample on its thread
    fn remove_single_samples_per_thread(&mut self) {
        let mut sample_count_by_thread_id: HashMap<u64, u32> = HashMap::new();

        for sample in &self.profile.samples {
            *sample_count_by_thread_id
                .entry(sample.thread_id)
                .or_default() += 1;
        }

        // Only keep data from threads with more than 1 sample so we can calculate a duration
        sample_count_by_thread_id.retain(|_, count| *count > 1);

        self.profile
            .samples
            .retain(|sample| sample_count_by_thread_id.contains_key(&sample.thread_id));
    }

    fn strip_pointer_authentication_code(&mut self) {
        self.profile.strip_pointer_authentication_code(
            &self.metadata.platform,
            &self.metadata.device.architecture,
        );
    }

    fn remove_idle_samples_at_the_edge(&mut self) {
        let mut active_ranges: HashMap<u64, Range<usize>> = HashMap::new();

        for (i, sample) in self.profile.samples.iter().enumerate() {
            let is_active = match self.profile.stacks.get(sample.stack_id) {
                Some(stack) => !stack.is_empty(),
                None => true,
            };

            if !is_active {
                continue;
            }

            if let Some(range) = active_ranges.get_mut(&sample.thread_id) {
                range.end = i + 1;
            } else {
                active_ranges.insert(sample.thread_id, i..i + 1);
            }
        }

        self.profile.samples = self
            .profile
            .samples
            .drain(..)
            .enumerate()
            .filter(|(i, sample)| {
                if let Some(range) = active_ranges.get(&sample.thread_id) {
                    range.contains(i)
                } else {
                    false
                }
            })
            .map(|(_, sample)| sample)
            .collect();
    }

    fn cleanup_thread_metadata(&mut self) {
        if let Some(thread_metadata) = &mut self.profile.thread_metadata {
            let mut thread_ids: HashSet<String> = HashSet::new();
            for sample in &self.profile.samples {
                thread_ids.insert(sample.thread_id.to_string());
            }
            thread_metadata.retain(|thread_id, _| thread_ids.contains(thread_id));
        }
    }

    fn cleanup_queue_metadata(&mut self) {
        if let Some(queue_metadata) = &mut self.profile.queue_metadata {
            let mut queue_addresses: HashSet<&String> = HashSet::new();
            for sample in &self.profile.samples {
                if let Some(queue_address) = &sample.queue_address {
                    queue_addresses.insert(queue_address);
                }
            }
            queue_metadata.retain(|queue_address, _| queue_addresses.contains(&queue_address));
        }
    }
}

fn parse_profile(payload: &[u8]) -> Result<SampleProfile, ProfileError> {
    let d = &mut serde_json::Deserializer::from_slice(payload);
    let mut profile: SampleProfile =
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

    // Clean samples before running the checks.
    profile.remove_idle_samples_at_the_edge();
    profile.remove_single_samples_per_thread();

    if profile.profile.samples.is_empty() {
        return Err(ProfileError::NotEnoughSamples);
    }

    if !profile.check_samples() {
        return Err(ProfileError::MalformedSamples);
    }

    if !profile.check_stacks() {
        return Err(ProfileError::MalformedStacks);
    }

    if profile.is_above_max_duration() {
        return Err(ProfileError::DurationIsTooLong);
    }

    profile.strip_pointer_authentication_code();
    profile.cleanup_thread_metadata();
    profile.cleanup_queue_metadata();

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

    if let Some(release) = transaction_metadata.get("release") {
        profile.metadata.release = release.to_owned();
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
    use chrono::Utc;
    use relay_event_schema::protocol::EventId;
    use std::time::Duration;

    use crate::profile_metadata::{DeviceMetadata, OSMetadata};
    use crate::profile_version::Version;
    use crate::transaction_metadata::TransactionMetadata;

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

    fn generate_profile() -> SampleProfile {
        SampleProfile {
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
                release: "1.0".to_string(),
                dist: "9999".to_string(),
                measurements: None,
                transaction_metadata: BTreeMap::new(),
                transaction_tags: BTreeMap::new(),
            },
            profile: Profile {
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

        profile.remove_single_samples_per_thread();

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

        profile.remove_idle_samples_at_the_edge();

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

        profile.cleanup_thread_metadata();
        profile.cleanup_queue_metadata();

        assert_eq!(profile.profile.thread_metadata.unwrap().len(), 2);
        assert_eq!(profile.profile.queue_metadata.unwrap().len(), 1);
    }

    #[test]
    fn test_extract_transaction_tags() {
        let transaction_metadata = BTreeMap::from([
            ("release".to_string(), "some-random-release".to_string()),
            (
                "transaction".to_string(),
                "some-random-transaction".to_string(),
            ),
        ]);

        let payload = include_bytes!("../tests/fixtures/profiles/sample/roundtrip.json");
        let profile_json = parse_sample_profile(payload, transaction_metadata, BTreeMap::new());
        assert!(profile_json.is_ok());

        let payload = profile_json.unwrap();
        let d = &mut serde_json::Deserializer::from_slice(&payload[..]);
        let output: SampleProfile = serde_path_to_error::deserialize(d)
            .map_err(ProfileError::InvalidJson)
            .unwrap();
        assert_eq!(output.metadata.release, "some-random-release".to_string());
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

        assert!(!profile.is_above_max_duration());
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

        assert!(profile.is_above_max_duration());
    }
}
