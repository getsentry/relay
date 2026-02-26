//! Perfetto trace format conversion to Sample v2.
//!
//! Handles both `PerfSample` (CPU profiling via `perf_event_open`) and
//! `StreamingProfilePacket` (in-process stack sampling) packet types.

use std::collections::BTreeMap;

use data_encoding::HEXLOWER;
use hashbrown::{HashMap, HashSet};
use prost::Message;
use relay_event_schema::protocol::{Addr, DebugId};
use relay_protocol::FiniteF64;

use crate::debug_image::DebugImage;
use crate::error::ProfileError;
use crate::sample::v2::{ProfileData, Sample};
use crate::sample::{Frame, ThreadMetadata};

mod proto;

use proto::trace_packet::Data;

/// Maximum number of raw samples we collect from a Perfetto trace before
/// bailing out. At 100 Hz across multiple threads, a 66-second chunk
/// produces at most ~6 600 samples per thread; 100 000 provides generous
/// headroom while bounding memory usage against adversarial input.
const MAX_SAMPLES: usize = 100_000;

/// See <https://perfetto.dev/docs/reference/trace-packet-proto#SequenceFlags>.
const SEQ_INCREMENTAL_STATE_CLEARED: u32 = 1;

/// Perfetto builtin clock IDs.
/// See <https://perfetto.dev/docs/concepts/clock-sync>.
const CLOCK_REALTIME: u32 = 1;
const CLOCK_BOOTTIME: u32 = 6;

fn has_incremental_state_cleared(packet: &proto::TracePacket) -> bool {
    packet
        .sequence_flags
        .is_some_and(|f| f & SEQ_INCREMENTAL_STATE_CLEARED != 0)
}

fn trusted_packet_sequence_id(packet: &proto::TracePacket) -> u32 {
    match packet.optional_trusted_packet_sequence_id {
        Some(proto::trace_packet::OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(id)) => {
            id
        }
        None => 0,
    }
}

fn extract_clock_offset(cs: &proto::ClockSnapshot) -> Option<i128> {
    let mut boottime_ns: Option<u64> = None;
    let mut realtime_ns: Option<u64> = None;

    for clock in &cs.clocks {
        match clock.clock_id {
            Some(CLOCK_BOOTTIME) => boottime_ns = clock.timestamp,
            Some(CLOCK_REALTIME) => realtime_ns = clock.timestamp,
            _ => {}
        }
    }

    match (realtime_ns, boottime_ns) {
        (Some(rt), Some(bt)) => Some(rt as i128 - bt as i128),
        _ => None,
    }
}

/// Per-sequence interned data tables, mirroring Perfetto's incremental state.
///
/// Perfetto traces use interned IDs to avoid repeating large strings and
/// structures in every packet. Each trusted packet sequence maintains its
/// own set of intern tables that can be cleared on state resets.
#[derive(Default)]
struct InternTables {
    strings: HashMap<u64, String>,
    frames: HashMap<u64, proto::Frame>,
    callstacks: HashMap<u64, proto::Callstack>,
    mappings: HashMap<u64, proto::Mapping>,
}

impl InternTables {
    fn clear(&mut self) {
        self.strings.clear();
        self.frames.clear();
        self.callstacks.clear();
        self.mappings.clear();
    }

    fn merge(&mut self, data: &proto::InternedData) {
        for s in data.function_names.iter().chain(data.mapping_paths.iter()) {
            if let Some(iid) = s.iid {
                let value = s
                    .r#str
                    .as_deref()
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .unwrap_or("")
                    .to_owned();
                self.strings.insert(iid, value);
            }
        }
        // Build IDs are raw bytes in Perfetto traces; normalize to hex for later lookup.
        for s in &data.build_ids {
            if let Some(iid) = s.iid {
                let value = match s.r#str.as_deref() {
                    Some(bytes) if !bytes.is_empty() => HEXLOWER.encode(bytes),
                    _ => String::new(),
                };
                self.strings.insert(iid, value);
            }
        }
        for f in &data.frames {
            if let Some(iid) = f.iid {
                self.frames.insert(iid, *f);
            }
        }
        for c in &data.callstacks {
            if let Some(iid) = c.iid {
                self.callstacks.insert(iid, c.clone());
            }
        }
        for m in &data.mappings {
            if let Some(iid) = m.iid {
                self.mappings.insert(iid, m.clone());
            }
        }
    }
}

/// Deduplication key for resolved stack frames.
///
/// Two Perfetto frames that resolve to the same function, module, package,
/// and instruction address are considered identical and share a single index
/// in the output frame list.
#[derive(Hash, Eq, PartialEq)]
struct FrameKey {
    function: Option<String>,
    module: Option<String>,
    package: Option<String>,
    instruction_addr: Option<u64>,
}

/// Converts a Perfetto binary trace into Sample v2 [`ProfileData`] and debug images.
pub fn convert(perfetto_bytes: &[u8]) -> Result<(ProfileData, Vec<DebugImage>), ProfileError> {
    let trace = proto::Trace::decode(perfetto_bytes).map_err(|_| ProfileError::InvalidProtobuf)?;

    let mut tables_by_seq: HashMap<u32, InternTables> = HashMap::new();
    let mut thread_meta: BTreeMap<String, ThreadMetadata> = BTreeMap::new();
    // (timestamp_ns, tid, callstack_iid, sequence_id)
    let mut raw_samples: Vec<(u64, u32, u64, u32)> = Vec::new();
    let mut clock_offset_ns: Option<i128> = None;

    for packet in &trace.packet {
        let seq_id = trusted_packet_sequence_id(packet);

        if has_incremental_state_cleared(packet) {
            tables_by_seq.entry(seq_id).or_default().clear();
        }

        if let Some(ref interned) = packet.interned_data {
            tables_by_seq.entry(seq_id).or_default().merge(interned);
        }

        match &packet.data {
            Some(Data::ClockSnapshot(cs)) => {
                if clock_offset_ns.is_none() {
                    clock_offset_ns = extract_clock_offset(cs);
                }
            }
            Some(Data::ProcessTree(pt)) => {
                for thread in &pt.threads {
                    if let Some(tid) = thread.tid {
                        let tid_str = tid.to_string();
                        thread_meta
                            .entry(tid_str)
                            .or_insert_with(|| ThreadMetadata {
                                name: thread.name.clone(),
                                priority: None,
                            });
                    }
                }
            }
            Some(Data::TrackDescriptor(td)) => {
                if let Some(ref thread) = td.thread
                    && let Some(tid) = thread.tid
                {
                    let tid_str = tid.to_string();
                    thread_meta
                        .entry(tid_str)
                        .or_insert_with(|| ThreadMetadata {
                            name: thread.thread_name.clone(),
                            priority: None,
                        });
                }
            }
            Some(Data::PerfSample(ps)) => {
                if let Some(callstack_iid) = ps.callstack_iid {
                    let ts = packet.timestamp.unwrap_or(0);
                    let tid = ps.tid.unwrap_or(0);
                    raw_samples.push((ts, tid, callstack_iid, seq_id));
                }
            }
            Some(Data::StreamingProfilePacket(spp)) => {
                let mut ts = packet.timestamp.unwrap_or(0);
                for (i, &cs_iid) in spp.callstack_iid.iter().enumerate() {
                    if i > 0
                        && let Some(&delta) = spp.timestamp_delta_us.get(i)
                    {
                        // `delta` is i64 (can be negative for out-of-order samples).
                        // Casting to u64 wraps negative values, which is correct because
                        // `wrapping_add` of a wrapped negative value subtracts as expected.
                        ts = ts.wrapping_add((delta * 1000) as u64);
                    }
                    raw_samples.push((ts, 0, cs_iid, seq_id));
                }
            }
            None => {}
        }

        if raw_samples.len() > MAX_SAMPLES {
            return Err(ProfileError::ExceedSizeLimit);
        }
    }

    if raw_samples.is_empty() {
        return Err(ProfileError::NoProfileSamplesInTrace);
    }

    let clock_offset_ns = clock_offset_ns.ok_or(ProfileError::MissingClockSnapshot)?;

    raw_samples.sort_by_key(|s| s.0);

    let empty_tables = InternTables::default();
    let mut frame_index: HashMap<FrameKey, usize> = HashMap::new();
    let mut frames: Vec<Frame> = Vec::new();
    let mut stack_index: HashMap<Vec<usize>, usize> = HashMap::new();
    let mut stacks: Vec<Vec<usize>> = Vec::new();
    let mut samples: Vec<Sample> = Vec::new();
    let mut referenced_mappings: HashSet<(u32, u64)> = HashSet::new();

    for &(ts_ns, tid, cs_iid, seq_id) in &raw_samples {
        let tables = tables_by_seq.get(&seq_id).unwrap_or(&empty_tables);

        let Some(callstack) = tables.callstacks.get(&cs_iid) else {
            continue;
        };

        let mut resolved_frame_indices: Vec<usize> = Vec::with_capacity(callstack.frame_ids.len());

        for &frame_iid in &callstack.frame_ids {
            let Some(pf) = tables.frames.get(&frame_iid) else {
                continue;
            };

            let function_name = pf
                .function_name_id
                .and_then(|id| tables.strings.get(&id))
                .cloned();

            if let Some(mid) = pf.mapping_id {
                referenced_mappings.insert((seq_id, mid));
            }

            let (key, frame) = build_frame(function_name, pf, tables);

            let idx = if let Some(&existing) = frame_index.get(&key) {
                existing
            } else {
                let idx = frames.len();
                frame_index.insert(key, idx);
                frames.push(frame);
                idx
            };

            resolved_frame_indices.push(idx);
        }

        // Perfetto stacks are root-first, Sample v2 is leaf-first.
        resolved_frame_indices.reverse();

        let stack_id = if let Some(&existing) = stack_index.get(&resolved_frame_indices) {
            existing
        } else {
            let id = stacks.len();
            stack_index.insert(resolved_frame_indices.clone(), id);
            stacks.push(resolved_frame_indices);
            id
        };

        // Compute absolute timestamp in integer nanoseconds first, then convert
        // to f64 seconds once to avoid precision loss from adding large floats.
        let abs_ns = ts_ns as i128 + clock_offset_ns;
        let ts_secs = abs_ns as f64 / 1_000_000_000.0;
        let ts_secs = (ts_secs * 1000.0).round() / 1000.0;

        if let Some(ts) = FiniteF64::new(ts_secs) {
            samples.push(Sample {
                timestamp: ts,
                stack_id,
                thread_id: tid.to_string(),
            });
        }
    }

    if samples.is_empty() {
        return Err(ProfileError::NoProfileSamplesInTrace);
    }

    // Build debug images from referenced native mappings.
    let mut debug_images: Vec<DebugImage> = Vec::new();
    let mut seen_images: HashSet<(String, u64)> = HashSet::new();

    for &(seq_id, mapping_id) in &referenced_mappings {
        let Some(tables) = tables_by_seq.get(&seq_id) else {
            continue;
        };
        let Some(mapping) = tables.mappings.get(&mapping_id) else {
            continue;
        };

        let code_file = {
            let parts: Vec<&str> = mapping
                .path_string_ids
                .iter()
                .filter_map(|id| tables.strings.get(id).map(|s| s.as_str()))
                .collect();
            if parts.is_empty() {
                continue;
            }
            parts.join("/")
        };

        if is_java_mapping(&code_file) {
            continue;
        }

        let image_addr = mapping.start.unwrap_or(0);

        if !seen_images.insert((code_file.clone(), image_addr)) {
            continue;
        }

        let debug_id = mapping
            .build_id
            .and_then(|bid| tables.strings.get(&bid))
            .and_then(|hex_str| build_id_to_debug_id(hex_str));

        let Some(debug_id) = debug_id else {
            continue;
        };

        let image_size = mapping.end.unwrap_or(0).saturating_sub(image_addr);
        let image_vmaddr = mapping.load_bias.unwrap_or(0);

        debug_images.push(DebugImage::native_image(
            code_file,
            debug_id,
            image_addr,
            image_vmaddr,
            image_size,
        ));
    }

    Ok((
        ProfileData {
            samples,
            stacks,
            frames,
            thread_metadata: thread_meta,
        },
        debug_images,
    ))
}

/// Resolves a Perfetto frame into a [`FrameKey`] and a Sample v2 [`Frame`].
///
/// Java frames (identified by mapping path) have their fully-qualified name
/// split into module and function. Native frames compute an absolute
/// instruction address from `rel_pc` and the mapping start address.
fn build_frame(
    function_name: Option<String>,
    pf: &proto::Frame,
    tables: &InternTables,
) -> (FrameKey, Frame) {
    let mapping = pf.mapping_id.and_then(|mid| tables.mappings.get(&mid));

    let mapping_path = mapping.and_then(|m| {
        let parts: Vec<&str> = m
            .path_string_ids
            .iter()
            .filter_map(|id| tables.strings.get(id).map(|s| s.as_str()))
            .collect();
        if parts.is_empty() {
            None
        } else {
            Some(parts.join("/"))
        }
    });

    let is_java = mapping_path.as_deref().is_some_and(is_java_mapping);

    if is_java {
        // For Java frames, split "com.example.MyClass.myMethod" into
        // module="com.example.MyClass" and function="myMethod".
        let (module, function) = match &function_name {
            Some(name) => match name.rsplit_once('.') {
                Some((class, method)) => (Some(class.to_owned()), Some(method.to_owned())),
                None => (None, Some(name.clone())),
            },
            None => (None, None),
        };

        let key = FrameKey {
            function: function.clone(),
            module: module.clone(),
            package: mapping_path.clone(),
            instruction_addr: None,
        };

        let frame = Frame {
            function,
            module,
            package: mapping_path,
            platform: Some("java".to_owned()),
            ..Default::default()
        };

        (key, frame)
    } else {
        let instruction_addr = match (pf.rel_pc, mapping) {
            (Some(rel_pc), Some(m)) => Some(rel_pc.wrapping_add(m.start.unwrap_or(0))),
            (Some(rel_pc), None) => Some(rel_pc),
            (None, _) => None,
        };

        let key = FrameKey {
            function: function_name.clone(),
            module: None,
            package: mapping_path.clone(),
            instruction_addr,
        };

        let frame = Frame {
            function: function_name,
            package: mapping_path,
            instruction_addr: instruction_addr.map(Addr),
            platform: Some("native".to_owned()),
            ..Default::default()
        };

        (key, frame)
    }
}

/// Returns `true` if the mapping path indicates a JVM/ART runtime mapping.
fn is_java_mapping(path: &str) -> bool {
    const JVM_EXTENSIONS: &[&str] = &[".oat", ".odex", ".vdex", ".jar", ".dex"];

    if path.contains("dalvik-jit-code-cache") {
        return true;
    }
    JVM_EXTENSIONS.iter().any(|ext| path.ends_with(ext))
}

/// Converts a hex-encoded ELF build ID string into a Sentry [`DebugId`].
///
/// The first 16 bytes of the build ID are interpreted as a little-endian UUID
/// (byte-swapping the time_low, time_mid, and time_hi_and_version fields).
/// If the build ID is shorter than 16 bytes it is zero-padded on the right.
fn build_id_to_debug_id(hex_str: &str) -> Option<DebugId> {
    let bytes = HEXLOWER.decode(hex_str.as_bytes()).ok()?;
    if bytes.is_empty() {
        return None;
    }

    let mut buf = [0u8; 16];
    let len = bytes.len().min(16);
    buf[..len].copy_from_slice(&bytes[..len]);

    // Swap from little-endian ELF byte order to UUID mixed-endian format.
    // time_low (bytes 0..4): reverse
    buf[..4].reverse();
    // time_mid (bytes 4..6): reverse
    buf[4..6].reverse();
    // time_hi_and_version (bytes 6..8): reverse
    buf[6..8].reverse();

    let uuid = uuid::Uuid::from_bytes(buf);
    uuid.to_string().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_BOOTTIME_NS: u64 = 1_000_000_000;
    const TEST_REALTIME_NS: u64 = 1_700_000_001_000_000_000;

    fn make_clock_snapshot_packet() -> proto::TracePacket {
        proto::TracePacket {
            timestamp: None,
            interned_data: None,
            sequence_flags: None,
            optional_trusted_packet_sequence_id: None,
            data: Some(Data::ClockSnapshot(proto::ClockSnapshot {
                clocks: vec![
                    proto::clock_snapshot::Clock {
                        clock_id: Some(CLOCK_BOOTTIME),
                        timestamp: Some(TEST_BOOTTIME_NS),
                    },
                    proto::clock_snapshot::Clock {
                        clock_id: Some(CLOCK_REALTIME),
                        timestamp: Some(TEST_REALTIME_NS),
                    },
                ],
                primary_trace_clock: Some(CLOCK_BOOTTIME),
            })),
        }
    }

    fn make_interned_string(iid: u64, value: &[u8]) -> proto::InternedString {
        proto::InternedString {
            iid: Some(iid),
            r#str: Some(value.to_vec()),
        }
    }

    fn make_frame(iid: u64, function_name_id: u64) -> proto::Frame {
        proto::Frame {
            iid: Some(iid),
            function_name_id: Some(function_name_id),
            mapping_id: None,
            rel_pc: None,
        }
    }

    fn make_perf_sample_packet(
        timestamp: u64,
        seq_id: u32,
        tid: u32,
        callstack_iid: u64,
    ) -> proto::TracePacket {
        proto::TracePacket {
            timestamp: Some(timestamp),
            interned_data: None,
            sequence_flags: None,
            optional_trusted_packet_sequence_id: Some(
                proto::trace_packet::OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(
                    seq_id,
                ),
            ),
            data: Some(Data::PerfSample(proto::PerfSample {
                cpu: None,
                pid: None,
                tid: Some(tid),
                callstack_iid: Some(callstack_iid),
            })),
        }
    }

    fn make_interned_data_packet(
        seq_id: u32,
        clear_state: bool,
        interned_data: proto::InternedData,
    ) -> proto::TracePacket {
        proto::TracePacket {
            timestamp: None,
            interned_data: Some(interned_data),
            sequence_flags: if clear_state {
                Some(SEQ_INCREMENTAL_STATE_CLEARED)
            } else {
                None
            },
            optional_trusted_packet_sequence_id: Some(
                proto::trace_packet::OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(
                    seq_id,
                ),
            ),
            data: None,
        }
    }

    fn build_minimal_trace() -> Vec<u8> {
        let trace = proto::Trace {
            packet: vec![
                make_clock_snapshot_packet(),
                make_interned_data_packet(
                    1,
                    true,
                    proto::InternedData {
                        function_names: vec![
                            make_interned_string(1, b"main"),
                            make_interned_string(2, b"foo"),
                        ],
                        frames: vec![
                            proto::Frame {
                                iid: Some(1),
                                function_name_id: Some(1),
                                mapping_id: None,
                                rel_pc: Some(0x1000),
                            },
                            proto::Frame {
                                iid: Some(2),
                                function_name_id: Some(2),
                                mapping_id: None,
                                rel_pc: Some(0x2000),
                            },
                        ],
                        callstacks: vec![proto::Callstack {
                            iid: Some(1),
                            frame_ids: vec![1, 2], // root-first: main -> foo
                        }],
                        ..Default::default()
                    },
                ),
                // Thread descriptor.
                proto::TracePacket {
                    timestamp: None,
                    interned_data: None,
                    sequence_flags: None,
                    optional_trusted_packet_sequence_id: Some(
                        proto::trace_packet::OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(1),
                    ),
                    data: Some(Data::TrackDescriptor(proto::TrackDescriptor {
                        uuid: None,
                        thread: Some(proto::ThreadDescriptor {
                            pid: Some(100),
                            tid: Some(42),
                            thread_name: Some("main-thread".to_owned()),
                        }),
                    })),
                },
                make_perf_sample_packet(1_000_000_000, 1, 42, 1),
                make_perf_sample_packet(1_010_000_000, 1, 42, 1),
            ],
        };
        trace.encode_to_vec()
    }

    #[test]
    fn test_convert_minimal_trace() {
        let bytes = build_minimal_trace();
        let result = convert(&bytes);
        assert!(result.is_ok(), "conversion failed: {result:?}");

        let (data, _images) = result.unwrap();

        assert_eq!(data.samples.len(), 2);
        assert_eq!(data.samples[0].thread_id, "42");
        assert_eq!(data.frames.len(), 2);

        assert_eq!(data.stacks.len(), 1);
        let stack = &data.stacks[0];
        assert_eq!(stack.len(), 2);

        // Leaf-first order: foo, then main.
        assert_eq!(data.frames[stack[0]].function.as_deref(), Some("foo"));
        assert_eq!(data.frames[stack[1]].function.as_deref(), Some("main"));

        assert!(data.thread_metadata.contains_key("42"));
        assert_eq!(
            data.thread_metadata["42"].name.as_deref(),
            Some("main-thread")
        );
    }

    #[test]
    fn test_convert_empty_trace() {
        let trace = proto::Trace { packet: vec![] };
        let bytes = trace.encode_to_vec();
        let result = convert(&bytes);
        assert!(matches!(result, Err(ProfileError::NoProfileSamplesInTrace)));
    }

    #[test]
    fn test_convert_invalid_protobuf() {
        let result = convert(b"not a valid protobuf");
        assert!(matches!(result, Err(ProfileError::InvalidProtobuf)));
    }

    #[test]
    fn test_convert_missing_clock_snapshot() {
        let trace = proto::Trace {
            packet: vec![
                make_interned_data_packet(
                    1,
                    true,
                    proto::InternedData {
                        function_names: vec![make_interned_string(1, b"func")],
                        frames: vec![make_frame(1, 1)],
                        callstacks: vec![proto::Callstack {
                            iid: Some(1),
                            frame_ids: vec![1],
                        }],
                        ..Default::default()
                    },
                ),
                make_perf_sample_packet(1_000_000_000, 1, 1, 1),
                make_perf_sample_packet(1_010_000_000, 1, 1, 1),
            ],
        };
        let bytes = trace.encode_to_vec();
        let result = convert(&bytes);
        assert!(matches!(result, Err(ProfileError::MissingClockSnapshot)));
    }

    #[test]
    fn test_streaming_profile_packet() {
        let trace = proto::Trace {
            packet: vec![
                make_clock_snapshot_packet(),
                make_interned_data_packet(
                    1,
                    true,
                    proto::InternedData {
                        function_names: vec![make_interned_string(1, b"func_a")],
                        frames: vec![make_frame(1, 1)],
                        callstacks: vec![proto::Callstack {
                            iid: Some(10),
                            frame_ids: vec![1],
                        }],
                        ..Default::default()
                    },
                ),
                proto::TracePacket {
                    timestamp: Some(2_000_000_000),
                    interned_data: None,
                    sequence_flags: None,
                    optional_trusted_packet_sequence_id: Some(
                        proto::trace_packet::OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(1),
                    ),
                    data: Some(Data::StreamingProfilePacket(
                        proto::StreamingProfilePacket {
                            callstack_iid: vec![10, 10],
                            timestamp_delta_us: vec![0, 10_000], // 0, +10ms
                        },
                    )),
                },
            ],
        };
        let bytes = trace.encode_to_vec();
        let result = convert(&bytes);
        assert!(result.is_ok(), "conversion failed: {result:?}");

        let (data, _images) = result.unwrap();
        assert_eq!(data.samples.len(), 2);
        // Timestamps are rebased using ClockSnapshot: offset = REALTIME - BOOTTIME.
        let duration = data.samples[1].timestamp.to_f64() - data.samples[0].timestamp.to_f64();
        assert!(
            (duration - 0.01).abs() < 0.001,
            "expected ~10ms delta, got {duration}"
        );
        // First sample at 2.0s boottime -> 2.0 + (REALTIME - BOOTTIME)/1e9 in Unix seconds.
        let expected_offset = (TEST_REALTIME_NS as f64 - TEST_BOOTTIME_NS as f64) / 1e9;
        let expected_ts = 2.0 + expected_offset;
        assert!((data.samples[0].timestamp.to_f64() - expected_ts).abs() < 0.001);
    }

    #[test]
    fn test_mapping_resolution() {
        let trace = proto::Trace {
            packet: vec![
                make_clock_snapshot_packet(),
                make_interned_data_packet(
                    1,
                    true,
                    proto::InternedData {
                        function_names: vec![
                            make_interned_string(1, b"my_func"),
                            make_interned_string(10, b"libfoo.so"),
                        ],
                        frames: vec![proto::Frame {
                            iid: Some(1),
                            function_name_id: Some(1),
                            mapping_id: Some(1),
                            rel_pc: Some(0x100),
                        }],
                        callstacks: vec![proto::Callstack {
                            iid: Some(1),
                            frame_ids: vec![1],
                        }],
                        mappings: vec![proto::Mapping {
                            iid: Some(1),
                            build_id: None,
                            start: Some(0x7000),
                            end: Some(0x8000),
                            load_bias: None,
                            path_string_ids: vec![10],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ),
                make_perf_sample_packet(1_000_000_000, 1, 1, 1),
                make_perf_sample_packet(1_010_000_000, 1, 1, 1),
            ],
        };
        let bytes = trace.encode_to_vec();
        let (data, images) = convert(&bytes).unwrap();

        assert_eq!(data.frames.len(), 1);
        let frame = &data.frames[0];
        assert_eq!(frame.platform.as_deref(), Some("native"));
        assert_eq!(frame.function.as_deref(), Some("my_func"));
        assert_eq!(frame.instruction_addr, Some(Addr(0x7100))); // rel_pc + start
        assert_eq!(frame.package.as_deref(), Some("libfoo.so"));
        assert!(frame.module.is_none());
        // No build_id on the mapping, so no debug images.
        assert!(images.is_empty());
    }

    #[test]
    fn test_incremental_state_reset() {
        let trace = proto::Trace {
            packet: vec![
                make_clock_snapshot_packet(),
                make_interned_data_packet(
                    1,
                    true,
                    proto::InternedData {
                        function_names: vec![make_interned_string(1, b"old_func")],
                        frames: vec![make_frame(1, 1)],
                        callstacks: vec![proto::Callstack {
                            iid: Some(1),
                            frame_ids: vec![1],
                        }],
                        ..Default::default()
                    },
                ),
                // State reset replaces everything.
                make_interned_data_packet(
                    1,
                    true,
                    proto::InternedData {
                        function_names: vec![make_interned_string(1, b"new_func")],
                        frames: vec![make_frame(1, 1)],
                        callstacks: vec![proto::Callstack {
                            iid: Some(1),
                            frame_ids: vec![1],
                        }],
                        ..Default::default()
                    },
                ),
                make_perf_sample_packet(1_000_000_000, 1, 1, 1),
                make_perf_sample_packet(1_010_000_000, 1, 1, 1),
            ],
        };
        let bytes = trace.encode_to_vec();
        let (data, _images) = convert(&bytes).unwrap();

        // After reset, "old_func" should be gone; only "new_func" remains.
        assert_eq!(data.frames.len(), 1);
        assert_eq!(data.frames[0].function.as_deref(), Some("new_func"));
    }

    #[test]
    fn test_convert_android_pftrace() {
        let bytes = include_bytes!("../../tests/fixtures/android/perfetto/android.pftrace");

        let result = convert(bytes.as_slice());
        assert!(result.is_ok(), "conversion failed: {result:?}");

        let (data, images) = result.unwrap();
        assert!(!data.samples.is_empty(), "expected samples");
        assert!(!data.frames.is_empty(), "expected frames");
        assert!(!data.stacks.is_empty(), "expected stacks");

        // All samples must reference valid stacks.
        for sample in &data.samples {
            assert!(
                sample.stack_id < data.stacks.len(),
                "sample references out-of-bounds stack_id {}",
                sample.stack_id
            );
        }

        // All stacks must reference valid frames.
        for stack in &data.stacks {
            for &frame_idx in stack {
                assert!(
                    frame_idx < data.frames.len(),
                    "stack references out-of-bounds frame index {frame_idx}",
                );
            }
        }

        let java_count = data
            .frames
            .iter()
            .filter(|f| f.platform.as_deref() == Some("java"))
            .count();
        let native_count = data
            .frames
            .iter()
            .filter(|f| f.platform.as_deref() == Some("native"))
            .count();
        assert!(java_count > 0, "expected java frames");
        assert!(native_count > 0, "expected native frames");

        assert!(
            !images.is_empty(),
            "expected debug images from native mappings"
        );
    }

    #[test]
    fn test_frame_deduplication() {
        let trace = proto::Trace {
            packet: vec![
                make_clock_snapshot_packet(),
                make_interned_data_packet(
                    1,
                    true,
                    proto::InternedData {
                        function_names: vec![make_interned_string(1, b"shared")],
                        frames: vec![proto::Frame {
                            iid: Some(1),
                            function_name_id: Some(1),
                            mapping_id: None,
                            rel_pc: Some(0x100),
                        }],
                        // Two different callstacks referencing the same frame.
                        callstacks: vec![
                            proto::Callstack {
                                iid: Some(1),
                                frame_ids: vec![1],
                            },
                            proto::Callstack {
                                iid: Some(2),
                                frame_ids: vec![1],
                            },
                        ],
                        ..Default::default()
                    },
                ),
                make_perf_sample_packet(1_000_000_000, 1, 1, 1),
                make_perf_sample_packet(1_010_000_000, 1, 1, 2),
            ],
        };
        let bytes = trace.encode_to_vec();
        let (data, _images) = convert(&bytes).unwrap();

        // Same frame referenced from two callstacks should be deduplicated.
        assert_eq!(data.frames.len(), 1);
        assert_eq!(data.stacks.len(), 1); // Same single-frame stack, also deduped.
        assert_eq!(data.samples.len(), 2);
    }

    #[test]
    fn test_is_java_mapping() {
        // JVM mappings.
        assert!(is_java_mapping("system/framework/arm64/boot-framework.oat"));
        assert!(is_java_mapping("data/app/.../oat/arm64/base.odex"));
        assert!(is_java_mapping("base.vdex"));
        assert!(is_java_mapping("system/framework/framework.jar"));
        assert!(is_java_mapping("classes.dex"));
        assert!(is_java_mapping("[anon_shmem:dalvik-jit-code-cache]"));

        // Native mappings.
        assert!(!is_java_mapping("libc.so"));
        assert!(!is_java_mapping("libhwui.so"));
        assert!(!is_java_mapping("apex/com.android.art/lib64/libart.so"));
        assert!(!is_java_mapping("app_process64"));
    }

    #[test]
    fn test_java_frame_splitting() {
        let trace = proto::Trace {
            packet: vec![
                make_clock_snapshot_packet(),
                make_interned_data_packet(
                    1,
                    true,
                    proto::InternedData {
                        function_names: vec![make_interned_string(1, b"android.view.View.draw")],
                        frames: vec![proto::Frame {
                            iid: Some(1),
                            function_name_id: Some(1),
                            mapping_id: Some(1),
                            rel_pc: Some(0x100),
                        }],
                        callstacks: vec![proto::Callstack {
                            iid: Some(1),
                            frame_ids: vec![1],
                        }],
                        mappings: vec![proto::Mapping {
                            iid: Some(1),
                            start: Some(0x1000),
                            path_string_ids: vec![10],
                            ..Default::default()
                        }],
                        mapping_paths: vec![make_interned_string(10, b"boot-framework.oat")],
                        ..Default::default()
                    },
                ),
                make_perf_sample_packet(1_000_000_000, 1, 1, 1),
                make_perf_sample_packet(1_010_000_000, 1, 1, 1),
            ],
        };
        let bytes = trace.encode_to_vec();
        let (data, _images) = convert(&bytes).unwrap();

        assert_eq!(data.frames.len(), 1);
        let frame = &data.frames[0];
        assert_eq!(frame.platform.as_deref(), Some("java"));
        assert_eq!(frame.module.as_deref(), Some("android.view.View"));
        assert_eq!(frame.function.as_deref(), Some("draw"));
        assert_eq!(frame.package.as_deref(), Some("boot-framework.oat"));
        assert!(frame.instruction_addr.is_none());
    }

    #[test]
    fn test_native_frame() {
        let trace = proto::Trace {
            packet: vec![
                make_clock_snapshot_packet(),
                make_interned_data_packet(
                    1,
                    true,
                    proto::InternedData {
                        function_names: vec![make_interned_string(1, b"__epoll_pwait")],
                        frames: vec![proto::Frame {
                            iid: Some(1),
                            function_name_id: Some(1),
                            mapping_id: Some(1),
                            rel_pc: Some(0x100),
                        }],
                        callstacks: vec![proto::Callstack {
                            iid: Some(1),
                            frame_ids: vec![1],
                        }],
                        mappings: vec![proto::Mapping {
                            iid: Some(1),
                            start: Some(0x7000),
                            path_string_ids: vec![10],
                            ..Default::default()
                        }],
                        mapping_paths: vec![make_interned_string(10, b"libc.so")],
                        ..Default::default()
                    },
                ),
                make_perf_sample_packet(1_000_000_000, 1, 1, 1),
                make_perf_sample_packet(1_010_000_000, 1, 1, 1),
            ],
        };
        let bytes = trace.encode_to_vec();
        let (data, _images) = convert(&bytes).unwrap();

        assert_eq!(data.frames.len(), 1);
        let frame = &data.frames[0];
        assert_eq!(frame.platform.as_deref(), Some("native"));
        assert_eq!(frame.function.as_deref(), Some("__epoll_pwait"));
        assert_eq!(frame.package.as_deref(), Some("libc.so"));
        assert_eq!(frame.instruction_addr, Some(Addr(0x7100))); // rel_pc + start
        assert!(frame.module.is_none());
    }

    #[test]
    fn test_build_id_to_debug_id() {
        // 20-byte ELF build ID (common for GNU build IDs).
        let debug_id = build_id_to_debug_id("b03e4a7f5e884c8da04b05fa32cc4cbd69faff51").unwrap();
        // First 16 bytes: b0 3e 4a 7f 5e 88 4c 8d a0 4b 05 fa 32 cc 4c bd
        // After LE→UUID swap:
        //   time_low  (0..4)  reversed: 7f4a3eb0
        //   time_mid  (4..6)  reversed: 885e
        //   time_hi   (6..8)  reversed: 8d4c
        //   rest      (8..16) unchanged: a04b05fa32cc4cbd
        assert_eq!(debug_id.to_string(), "7f4a3eb0-885e-8d4c-a04b-05fa32cc4cbd");
    }

    #[test]
    fn test_build_id_to_debug_id_short() {
        // Build ID shorter than 16 bytes → zero-padded.
        let debug_id = build_id_to_debug_id("aabbccdd").unwrap();
        // Bytes: aa bb cc dd 00 00 00 00 00 00 00 00 00 00 00 00
        // After swap: ddccbbaa-0000-0000-0000-000000000000
        assert_eq!(debug_id.to_string(), "ddccbbaa-0000-0000-0000-000000000000");
    }

    #[test]
    fn test_build_id_to_debug_id_empty() {
        assert!(build_id_to_debug_id("").is_none());
    }

    #[test]
    fn test_mapping_with_build_id() {
        // Raw 20-byte ELF build ID (as it appears in Perfetto traces).
        let build_id_raw: &[u8] = &[
            0xb0, 0x3e, 0x4a, 0x7f, 0x5e, 0x88, 0x4c, 0x8d, 0xa0, 0x4b, 0x05, 0xfa, 0x32, 0xcc,
            0x4c, 0xbd, 0x69, 0xfa, 0xff, 0x51,
        ];

        let trace = proto::Trace {
            packet: vec![
                make_clock_snapshot_packet(),
                make_interned_data_packet(
                    1,
                    true,
                    proto::InternedData {
                        function_names: vec![make_interned_string(1, b"native_func")],
                        frames: vec![proto::Frame {
                            iid: Some(1),
                            function_name_id: Some(1),
                            mapping_id: Some(1),
                            rel_pc: Some(0x200),
                        }],
                        callstacks: vec![proto::Callstack {
                            iid: Some(1),
                            frame_ids: vec![1],
                        }],
                        mappings: vec![proto::Mapping {
                            iid: Some(1),
                            build_id: Some(20),
                            start: Some(0x7000_0000),
                            end: Some(0x7001_0000),
                            load_bias: Some(0x1000),
                            path_string_ids: vec![10],
                            start_offset: None,
                            exact_offset: None,
                        }],
                        mapping_paths: vec![make_interned_string(10, b"libexample.so")],
                        build_ids: vec![make_interned_string(20, build_id_raw)],
                    },
                ),
                make_perf_sample_packet(1_000_000_000, 1, 1, 1),
                make_perf_sample_packet(1_010_000_000, 1, 1, 1),
            ],
        };
        let bytes = trace.encode_to_vec();
        let (data, images) = convert(&bytes).unwrap();

        assert_eq!(data.frames.len(), 1);
        assert_eq!(images.len(), 1);

        let img_json = serde_json::to_value(&images[0]).unwrap();
        assert_eq!(img_json["code_file"], "libexample.so");
        assert_eq!(img_json["debug_id"], "7f4a3eb0-885e-8d4c-a04b-05fa32cc4cbd");
        assert_eq!(img_json["image_addr"], "0x70000000");
        assert_eq!(img_json["image_vmaddr"], "0x1000");
        assert_eq!(img_json["image_size"], 0x10000);
        assert_eq!(img_json["type"], "symbolic");
    }

    #[test]
    fn test_process_tree_thread_names() {
        let trace = proto::Trace {
            packet: vec![
                make_clock_snapshot_packet(),
                // ProcessTree with thread names.
                proto::TracePacket {
                    timestamp: None,
                    interned_data: None,
                    sequence_flags: None,
                    optional_trusted_packet_sequence_id: None,
                    data: Some(proto::trace_packet::Data::ProcessTree(proto::ProcessTree {
                        threads: vec![
                            proto::process_tree::Thread {
                                tid: Some(42),
                                name: Some("main".to_owned()),
                                tgid: Some(42),
                            },
                            proto::process_tree::Thread {
                                tid: Some(43),
                                name: Some("RenderThread".to_owned()),
                                tgid: Some(42),
                            },
                        ],
                    })),
                },
                make_interned_data_packet(
                    1,
                    true,
                    proto::InternedData {
                        function_names: vec![make_interned_string(1, b"doWork")],
                        frames: vec![proto::Frame {
                            iid: Some(1),
                            function_name_id: Some(1),
                            mapping_id: None,
                            rel_pc: None,
                        }],
                        callstacks: vec![proto::Callstack {
                            iid: Some(1),
                            frame_ids: vec![1],
                        }],
                        ..Default::default()
                    },
                ),
                make_perf_sample_packet(1_000_000_000, 1, 42, 1),
                make_perf_sample_packet(1_010_000_000, 1, 43, 1),
            ],
        };
        let bytes = trace.encode_to_vec();
        let (data, _images) = convert(&bytes).unwrap();

        assert_eq!(data.thread_metadata.len(), 2);
        assert_eq!(
            data.thread_metadata
                .get("42")
                .and_then(|m| m.name.as_deref()),
            Some("main"),
        );
        assert_eq!(
            data.thread_metadata
                .get("43")
                .and_then(|m| m.name.as_deref()),
            Some("RenderThread"),
        );
    }

    #[test]
    fn test_exceeds_max_samples() {
        let mut packets = vec![
            make_clock_snapshot_packet(),
            make_interned_data_packet(
                1,
                true,
                proto::InternedData {
                    function_names: vec![make_interned_string(1, b"func")],
                    frames: vec![make_frame(1, 1)],
                    callstacks: vec![proto::Callstack {
                        iid: Some(1),
                        frame_ids: vec![1],
                    }],
                    ..Default::default()
                },
            ),
        ];
        for i in 0..=MAX_SAMPLES as u64 {
            packets.push(make_perf_sample_packet(1_000_000_000 + i * 1_000, 1, 1, 1));
        }
        let trace = proto::Trace { packet: packets };
        let bytes = trace.encode_to_vec();
        let result = convert(&bytes);
        assert!(
            matches!(result, Err(ProfileError::ExceedSizeLimit)),
            "expected ExceedSizeLimit, got {result:?}"
        );
    }

    #[test]
    fn test_negative_timestamp_delta() {
        let trace = proto::Trace {
            packet: vec![
                make_clock_snapshot_packet(),
                make_interned_data_packet(
                    1,
                    true,
                    proto::InternedData {
                        function_names: vec![make_interned_string(1, b"func_a")],
                        frames: vec![make_frame(1, 1)],
                        callstacks: vec![proto::Callstack {
                            iid: Some(10),
                            frame_ids: vec![1],
                        }],
                        ..Default::default()
                    },
                ),
                proto::TracePacket {
                    timestamp: Some(3_000_000_000),
                    interned_data: None,
                    sequence_flags: None,
                    optional_trusted_packet_sequence_id: Some(
                        proto::trace_packet::OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(1),
                    ),
                    data: Some(Data::StreamingProfilePacket(
                        proto::StreamingProfilePacket {
                            callstack_iid: vec![10, 10, 10],
                            timestamp_delta_us: vec![0, 20_000, -5_000], // 0, +20ms, -5ms
                        },
                    )),
                },
            ],
        };
        let bytes = trace.encode_to_vec();
        let (data, _images) = convert(&bytes).unwrap();

        assert_eq!(data.samples.len(), 3);
        // After sorting: sample at 3.0s, then 3.0+0.015=3.015s, then 3.0+0.020=3.020s
        let t0 = data.samples[0].timestamp.to_f64();
        let t1 = data.samples[1].timestamp.to_f64();
        let t2 = data.samples[2].timestamp.to_f64();
        assert!(
            t0 < t1 && t1 < t2,
            "expected sorted timestamps: {t0}, {t1}, {t2}"
        );
        // The gap between t1 and t2 should be ~5ms (the -5ms sample comes before the +20ms one).
        let gap = t2 - t1;
        assert!((gap - 0.005).abs() < 0.001, "expected ~5ms gap, got {gap}");
    }

    #[test]
    fn test_multi_sequence_traces() {
        let trace = proto::Trace {
            packet: vec![
                make_clock_snapshot_packet(),
                // Sequence 1: has "alpha" function.
                make_interned_data_packet(
                    1,
                    true,
                    proto::InternedData {
                        function_names: vec![make_interned_string(1, b"alpha")],
                        frames: vec![make_frame(1, 1)],
                        callstacks: vec![proto::Callstack {
                            iid: Some(1),
                            frame_ids: vec![1],
                        }],
                        ..Default::default()
                    },
                ),
                // Sequence 2: reuses iid=1 but for "beta" function.
                make_interned_data_packet(
                    2,
                    true,
                    proto::InternedData {
                        function_names: vec![make_interned_string(1, b"beta")],
                        frames: vec![make_frame(1, 1)],
                        callstacks: vec![proto::Callstack {
                            iid: Some(1),
                            frame_ids: vec![1],
                        }],
                        ..Default::default()
                    },
                ),
                make_perf_sample_packet(1_000_000_000, 1, 1, 1), // seq 1 -> alpha
                make_perf_sample_packet(1_010_000_000, 2, 2, 1), // seq 2 -> beta
            ],
        };
        let bytes = trace.encode_to_vec();
        let (data, _images) = convert(&bytes).unwrap();

        assert_eq!(data.samples.len(), 2);
        // Should have two distinct frames from the two sequences.
        assert_eq!(data.frames.len(), 2);
        let frame_names: Vec<_> = data
            .frames
            .iter()
            .map(|f| f.function.as_deref().unwrap_or(""))
            .collect();
        assert!(frame_names.contains(&"alpha"), "expected alpha frame");
        assert!(frame_names.contains(&"beta"), "expected beta frame");
    }

    #[test]
    fn test_empty_callstack() {
        let trace = proto::Trace {
            packet: vec![
                make_clock_snapshot_packet(),
                make_interned_data_packet(
                    1,
                    true,
                    proto::InternedData {
                        function_names: vec![make_interned_string(1, b"func")],
                        frames: vec![make_frame(1, 1)],
                        callstacks: vec![
                            proto::Callstack {
                                iid: Some(1),
                                frame_ids: vec![], // empty callstack
                            },
                            proto::Callstack {
                                iid: Some(2),
                                frame_ids: vec![1], // valid callstack
                            },
                        ],
                        ..Default::default()
                    },
                ),
                make_perf_sample_packet(1_000_000_000, 1, 1, 1), // empty callstack
                make_perf_sample_packet(1_010_000_000, 1, 1, 2), // valid callstack
            ],
        };
        let bytes = trace.encode_to_vec();
        let (data, _images) = convert(&bytes).unwrap();

        // Both samples are emitted, but the empty one produces a deduplicated empty stack.
        assert_eq!(data.samples.len(), 2);
        // The valid callstack should produce one frame.
        assert_eq!(data.frames.len(), 1);
        assert_eq!(data.frames[0].function.as_deref(), Some("func"));
    }
}
