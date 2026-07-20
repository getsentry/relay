//! Perfetto trace format conversion to Sample v2.
//!
//! Decodes Android Perfetto traces (`PerfSample` packets + `ClockSnapshot` +
//! interned data) into the Sample v2 profile format.

use std::collections::BTreeMap;

use bytes::Buf;
use prost::Message;
use prost::encoding::{self, WireType};
use proto::trace_packet::Data;

use relay_protocol::FiniteF64;

use crate::debug_image::DebugImage;
use crate::error::ProfileError;
use crate::perfetto::proto;
use crate::sample::ThreadMetadata;
use crate::sample::v2::{ProfileData, Sample, StackId};

mod build;
mod cache;
mod consts;
mod intern;
mod utils;

/// Converts a Perfetto binary trace into Sample v2 [`ProfileData`] and debug images.
pub fn convert(perfetto_bytes: &[u8]) -> Result<(ProfileData, Vec<DebugImage>), ProfileError> {
    let mut clock_offset: Option<FiniteF64> = None;
    let mut observed_pid: Option<u32> = None;

    let mut samples = Vec::new();

    let mut intern_db = intern::Database::default();
    let mut cache_db = cache::Database::default();
    let mut images = build::Images::default();
    let mut frames = build::Frames::default();
    let mut stacks = build::Stacks::default();

    visit_trace_packets(perfetto_bytes, |mut packet| {
        let tables = intern_db.intern(&mut packet);
        let caches = cache_db.for_packet(&packet);

        match packet.data {
            Some(Data::ClockSnapshot(cs)) if clock_offset.is_none() => {
                clock_offset = utils::extract_clock_offset(&cs).map(utils::ns_to_secs);
            }
            Some(Data::PerfSample(sample)) if let Some(callstack_iid) = sample.callstack_iid => {
                let timestamp_ns = packet.timestamp.unwrap_or(0);
                let thread_id = sample.tid.unwrap_or(0);
                if observed_pid.is_none() {
                    observed_pid = sample.pid;
                }

                let mut seq = SequenceState {
                    images: &mut images,
                    frames: &mut frames,
                    stacks: &mut stacks,
                    caches,
                    tables,
                };
                if let Some(stack_id) = seq.resolve_callstack(callstack_iid)? {
                    samples.push(Sample {
                        timestamp: utils::ns_to_secs(timestamp_ns as i128),
                        stack_id,
                        thread_id: thread_id.to_string(),
                    });
                }
            }
            _ => {}
        }

        Ok(())
    })?;

    if samples.is_empty() {
        return Err(ProfileError::NotEnoughSamples);
    }

    let clock_offset = clock_offset.ok_or(ProfileError::InvalidSampledProfile)?;

    for sample in &mut samples {
        sample.timestamp += clock_offset;
    }
    samples.sort_unstable_by_key(|s| s.timestamp);

    // On Android/Linux the main thread's tid equals the process pid.
    // Label it "main" so the UI can identify it.
    let thread_metadata = observed_pid
        .map(|pid| {
            BTreeMap::from([(
                pid.to_string(),
                ThreadMetadata {
                    name: Some("main".to_owned()),
                    priority: None,
                },
            )])
        })
        .unwrap_or_default();

    Ok((
        ProfileData {
            samples,
            stacks: stacks.into_stacks(),
            frames: frames.into_frames(),
            thread_metadata,
        },
        images.into_images(),
    ))
}

/// All state required to process a single perf data packet.
#[derive(Debug)]
struct SequenceState<'a> {
    images: &'a mut build::Images,
    frames: &'a mut build::Frames,
    stacks: &'a mut build::Stacks,
    caches: &'a mut cache::Caches,
    tables: &'a intern::Tables,
}

impl SequenceState<'_> {
    /// Resolves a call-stack id from a perf trace packet.
    ///
    /// This will incrementally collect found images, frames and the stack.
    /// Returns the stack id if the stack could be resolved.
    fn resolve_callstack(&mut self, cs_iid: u64) -> Result<Option<StackId>, ProfileError> {
        if let Some(&stack_id) = self.caches.callstacks.get(&cs_iid) {
            return Ok(Some(stack_id));
        }

        let Some(callstack) = self.tables.resolve_callstack(cs_iid) else {
            return Ok(None);
        };

        if callstack.frame_ids.len() > consts::MAX_CALLSTACK_DEPTH {
            return Err(ProfileError::ExceedSizeLimit);
        }
        let mut resolved_frames = Vec::with_capacity(callstack.frame_ids.len());

        for &frame_iid in &callstack.frame_ids {
            if let Some(&idx) = self.caches.frames.get(&frame_iid) {
                resolved_frames.push(idx);
                continue;
            }

            let Some(pf) = self.tables.resolve_frame(frame_iid) else {
                continue;
            };

            let function_name = self.tables.resolve_function_name(pf.function_name_id);
            let mapping = self.tables.resolve_mapping(pf.mapping_id);
            let path = self
                .caches
                .mapping_paths
                .resolve(pf.mapping_id, mapping, self.tables);

            if let (Some(mapping), Some(path)) = (mapping, path) {
                self.images.add(mapping, path, self.tables);
            }

            let frame_id = self.frames.add(function_name, path, pf, mapping)?;
            self.caches.frames.insert(frame_iid, frame_id);
            resolved_frames.push(frame_id);
        }

        // Perfetto stacks are root-first, Sample v2 is leaf-first.
        resolved_frames.reverse();

        let stack_id = self.stacks.add(resolved_frames);
        self.caches.callstacks.insert(cs_iid, stack_id);

        Ok(Some(stack_id))
    }
}

/// Parse trace packets, passing each decoded packet to `visit`.
fn visit_trace_packets(
    mut perfetto_bytes: &[u8],
    mut visit: impl FnMut(proto::TracePacket) -> Result<(), ProfileError>,
) -> Result<(), ProfileError> {
    let mut packet_count = 0;
    let mut sample_count = 0;

    while perfetto_bytes.has_remaining() {
        let (tag, wire_type) = encoding::decode_key(&mut perfetto_bytes)
            .map_err(|_| ProfileError::InvalidSampledProfile)?;

        match (tag, wire_type) {
            (1, WireType::LengthDelimited) => {
                packet_count += 1;
                if packet_count >= consts::MAX_TRACE_PACKETS {
                    return Err(ProfileError::ExceedSizeLimit);
                }

                let len = encoding::decode_varint(&mut perfetto_bytes)
                    .map_err(|_| ProfileError::InvalidSampledProfile)?;
                let len = usize::try_from(len).map_err(|_| ProfileError::ExceedSizeLimit)?;

                if len > consts::MAX_TRACE_PACKET_BYTES {
                    return Err(ProfileError::ExceedSizeLimit);
                }

                let data = perfetto_bytes
                    .get(..len)
                    .ok_or(ProfileError::InvalidSampledProfile)?;
                let packet = proto::TracePacket::decode(data)
                    .map_err(|_| ProfileError::InvalidSampledProfile)?;
                perfetto_bytes.advance(len);

                sample_count += matches!(packet.data, Some(Data::PerfSample(_))) as usize;
                if sample_count >= consts::MAX_SAMPLES {
                    return Err(ProfileError::ExceedSizeLimit);
                }

                visit(packet)?;
            }
            (1, _) => return Err(ProfileError::InvalidSampledProfile),
            _ => encoding::skip_field(
                wire_type,
                tag,
                &mut perfetto_bytes,
                encoding::DecodeContext::default(),
            )
            .map_err(|_| ProfileError::InvalidSampledProfile)?,
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use consts::*;

    const CLOCK_MONOTONIC: u32 = 3;
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

    fn make_perf_sample_packet_with_pid(
        timestamp: u64,
        seq_id: u32,
        pid: u32,
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
                pid: Some(pid),
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

    fn decode_trace_packets(
        perfetto_bytes: &[u8],
    ) -> Result<Vec<proto::TracePacket>, ProfileError> {
        let mut packets = Vec::new();
        visit_trace_packets(perfetto_bytes, |packet| {
            packets.push(packet);
            Ok(())
        })?;
        Ok(packets)
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
                // pid == tid so the main-thread fallback names the thread.
                make_perf_sample_packet_with_pid(1_000_000_000, 1, 42, 42, 1),
                make_perf_sample_packet_with_pid(1_010_000_000, 1, 42, 42, 1),
            ],
        };
        trace.encode_to_vec()
    }

    fn write_trace_packet_field_header(bytes: &mut Vec<u8>, len: usize) {
        encoding::encode_key(1, WireType::LengthDelimited, bytes);
        encoding::encode_varint(len as u64, bytes);
    }

    #[test]
    fn test_convert_minimal_trace() {
        let bytes = build_minimal_trace();
        let (data, _images) = convert(&bytes).unwrap();

        insta::assert_json_snapshot!(data, @r###"
        {
          "samples": [
            {
              "timestamp": 1700000001.0,
              "stack_id": 0,
              "thread_id": "42"
            },
            {
              "timestamp": 1700000001.01,
              "stack_id": 0,
              "thread_id": "42"
            }
          ],
          "stacks": [
            [
              1,
              0
            ]
          ],
          "frames": [
            {
              "function": "main",
              "instruction_addr": "0x1000",
              "platform": "native"
            },
            {
              "function": "foo",
              "instruction_addr": "0x2000",
              "platform": "native"
            }
          ],
          "thread_metadata": {
            "42": {
              "name": "main"
            }
          }
        }
        "###);
    }

    #[test]
    fn test_convert_empty_trace() {
        let trace = proto::Trace { packet: vec![] };
        let bytes = trace.encode_to_vec();
        let result = convert(&bytes);
        assert!(matches!(result, Err(ProfileError::NotEnoughSamples)));
    }

    #[test]
    fn test_convert_invalid_protobuf() {
        let result = convert(b"not a valid protobuf");
        assert!(matches!(result, Err(ProfileError::InvalidSampledProfile)));
    }

    #[test]
    fn test_decode_trace_packets_exceeds_max_packets() {
        let mut bytes = Vec::with_capacity((MAX_TRACE_PACKETS + 1) * 2);
        for _ in 0..=MAX_TRACE_PACKETS {
            write_trace_packet_field_header(&mut bytes, 0);
        }

        let result = decode_trace_packets(&bytes);
        assert!(
            matches!(result, Err(ProfileError::ExceedSizeLimit)),
            "expected ExceedSizeLimit, got {result:?}"
        );
    }

    #[test]
    fn test_decode_trace_packets_exceeds_max_packet_bytes() {
        let mut bytes = Vec::new();
        write_trace_packet_field_header(&mut bytes, MAX_TRACE_PACKET_BYTES + 1);

        let result = decode_trace_packets(&bytes);
        assert!(
            matches!(result, Err(ProfileError::ExceedSizeLimit)),
            "expected ExceedSizeLimit, got {result:?}"
        );
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
        assert!(matches!(result, Err(ProfileError::InvalidSampledProfile)));
    }

    #[test]
    fn test_convert_uses_primary_trace_clock() {
        let trace = proto::Trace {
            packet: vec![
                proto::TracePacket {
                    timestamp: None,
                    interned_data: None,
                    sequence_flags: None,
                    optional_trusted_packet_sequence_id: None,
                    data: Some(Data::ClockSnapshot(proto::ClockSnapshot {
                        clocks: vec![
                            proto::clock_snapshot::Clock {
                                clock_id: Some(CLOCK_MONOTONIC),
                                timestamp: Some(500_000_000),
                            },
                            proto::clock_snapshot::Clock {
                                clock_id: Some(CLOCK_BOOTTIME),
                                timestamp: Some(TEST_BOOTTIME_NS),
                            },
                            proto::clock_snapshot::Clock {
                                clock_id: Some(CLOCK_REALTIME),
                                timestamp: Some(TEST_REALTIME_NS),
                            },
                        ],
                        primary_trace_clock: Some(CLOCK_MONOTONIC),
                    })),
                },
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
                make_perf_sample_packet(600_000_000, 1, 1, 1),
            ],
        };
        let bytes = trace.encode_to_vec();
        let (data, _images) = convert(&bytes).unwrap();

        insta::assert_json_snapshot!(data.samples, @r###"
        [
          {
            "timestamp": 1700000001.1,
            "stack_id": 0,
            "thread_id": "1"
          }
        ]
        "###);
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
                        function_names: vec![make_interned_string(1, b"my_func")],
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
                        mapping_paths: vec![make_interned_string(10, b"libfoo.so")],
                        ..Default::default()
                    },
                ),
                make_perf_sample_packet(1_000_000_000, 1, 1, 1),
                make_perf_sample_packet(1_010_000_000, 1, 1, 1),
            ],
        };
        let bytes = trace.encode_to_vec();
        let (data, images) = convert(&bytes).unwrap();

        insta::assert_json_snapshot!(data.frames, @r###"
        [
          {
            "function": "my_func",
            "instruction_addr": "0x7100",
            "package": "libfoo.so",
            "platform": "native"
          }
        ]
        "###);
        // No build_id on the mapping, so no debug images.
        assert!(images.is_empty());
    }

    #[test]
    fn test_separate_interning_namespaces() {
        // Perfetto uses separate ID namespaces per InternedData field.
        // function_names iid=1 and mapping_paths iid=1 must NOT collide.
        let trace = proto::Trace {
            packet: vec![
                make_clock_snapshot_packet(),
                make_interned_data_packet(
                    1,
                    true,
                    proto::InternedData {
                        function_names: vec![make_interned_string(1, b"my_func")],
                        mapping_paths: vec![make_interned_string(1, b"libfoo.so")],
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
                            path_string_ids: vec![1],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ),
                make_perf_sample_packet(1_000_000_000, 1, 1, 1),
            ],
        };
        let bytes = trace.encode_to_vec();
        let (data, _images) = convert(&bytes).unwrap();

        assert_eq!(data.frames.len(), 1);
        let frame = &data.frames[0];
        // Both use iid=1 but must resolve independently.
        assert_eq!(frame.function.as_deref(), Some("my_func"));
        assert_eq!(frame.package.as_deref(), Some("libfoo.so"));
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
    fn test_incremental_state_reset_with_samples_before_and_after() {
        // Samples collected before an incremental state reset must resolve
        // against the pre-reset intern tables, not the post-reset ones.
        // This catches the two-pass bug where deferred resolution would use
        // the final (post-reset) table state for all samples.
        let trace = proto::Trace {
            packet: vec![
                make_clock_snapshot_packet(),
                // Pre-reset: iid 1 = "old_func".
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
                // Sample BEFORE reset — should resolve to "old_func".
                make_perf_sample_packet(1_000_000_000, 1, 1, 1),
                // State reset: iid 1 now = "new_func".
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
                // Sample AFTER reset — should resolve to "new_func".
                make_perf_sample_packet(1_010_000_000, 1, 1, 1),
            ],
        };
        let bytes = trace.encode_to_vec();
        let (data, _images) = convert(&bytes).unwrap();

        assert_eq!(data.samples.len(), 2);
        // Both functions must be present — the pre-reset sample must NOT
        // silently resolve to "new_func".
        assert_eq!(data.frames.len(), 2);
        let frame_names: Vec<_> = data
            .frames
            .iter()
            .map(|f| f.function.as_deref().unwrap_or(""))
            .collect();
        assert!(
            frame_names.contains(&"old_func"),
            "expected old_func from pre-reset sample, got: {frame_names:?}"
        );
        assert!(
            frame_names.contains(&"new_func"),
            "expected new_func from post-reset sample, got: {frame_names:?}"
        );
    }

    #[test]
    fn test_convert_android_pftrace() {
        let bytes = include_bytes!("../../../tests/fixtures/android/perfetto/android.pftrace");

        let result = convert(bytes.as_slice());
        assert!(result.is_ok(), "conversion failed: {result:?}");

        let (data, images) = result.unwrap();
        assert!(!data.samples.is_empty(), "expected samples");
        assert!(!data.frames.is_empty(), "expected frames");
        assert!(!data.stacks.is_empty(), "expected stacks");

        // All samples must reference valid stacks.
        for sample in &data.samples {
            assert!(sample.stack_id.0 < data.stacks.len());
        }

        // All stacks must reference valid frames.
        for stack in &data.stacks {
            for &frame_idx in stack {
                assert!(frame_idx.0 < data.frames.len());
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

        // The fixture contains samples from multiple threads.
        let thread_ids: std::collections::BTreeSet<&str> =
            data.samples.iter().map(|s| s.thread_id.as_str()).collect();
        assert!(
            thread_ids.len() > 1,
            "expected samples from multiple threads, got: {thread_ids:?}"
        );

        // The fixture has no ProcessTree/TrackDescriptor, but the main thread
        // (tid == pid) should still be labeled "main" via pid-based inference.
        assert!(
            !data.thread_metadata.is_empty(),
            "expected main thread metadata from pid inference"
        );
        // The lowest tid in PerfSample traces is typically the main thread (tid == pid).
        let main_tid = thread_ids.iter().next().unwrap();
        assert_eq!(
            data.thread_metadata
                .get(*main_tid)
                .and_then(|m| m.name.as_deref()),
            Some("main"),
            "expected main thread to be labeled via pid inference"
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

        insta::assert_json_snapshot!(data.frames, @r###"
        [
          {
            "function": "draw",
            "module": "android.view.View",
            "package": "boot-framework.oat",
            "platform": "java"
          }
        ]
        "###);
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

        insta::assert_json_snapshot!(data.frames, @r###"
        [
          {
            "function": "__epoll_pwait",
            "instruction_addr": "0x7100",
            "package": "libc.so",
            "platform": "native"
          }
        ]
        "###);
    }

    #[test]
    fn test_native_instruction_addr_overflow_does_not_wrap() {
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
                            rel_pc: Some(2),
                        }],
                        callstacks: vec![proto::Callstack {
                            iid: Some(1),
                            frame_ids: vec![1],
                        }],
                        mappings: vec![proto::Mapping {
                            iid: Some(1),
                            start: Some(u64::MAX - 1),
                            path_string_ids: vec![10],
                            ..Default::default()
                        }],
                        mapping_paths: vec![make_interned_string(10, b"liboverflow.so")],
                        ..Default::default()
                    },
                ),
                make_perf_sample_packet(1_000_000_000, 1, 1, 1),
            ],
        };
        let bytes = trace.encode_to_vec();
        let (data, _images) = convert(&bytes).unwrap();

        assert_eq!(data.frames.len(), 1);
        assert!(data.frames[0].instruction_addr.is_none());
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

        insta::assert_json_snapshot!(images[0], @r###"
        {
          "code_file": "libexample.so",
          "debug_id": "7f4a3eb0-885e-8d4c-a04b-05fa32cc4cbd",
          "type": "symbolic",
          "image_addr": "0x70000000",
          "image_vmaddr": "0x1000",
          "image_size": 65536
        }
        "###);
    }

    #[test]
    fn test_main_thread_inferred_from_pid() {
        // The main thread (tid == pid) is labeled "main" automatically;
        // worker threads carry no name source and remain unnamed.
        let trace = proto::Trace {
            packet: vec![
                make_clock_snapshot_packet(),
                make_interned_data_packet(
                    1,
                    true,
                    proto::InternedData {
                        function_names: vec![make_interned_string(1, b"doWork")],
                        frames: vec![make_frame(1, 1)],
                        callstacks: vec![proto::Callstack {
                            iid: Some(1),
                            frame_ids: vec![1],
                        }],
                        ..Default::default()
                    },
                ),
                make_perf_sample_packet_with_pid(1_000_000_000, 1, 100, 100, 1), // main thread
                make_perf_sample_packet_with_pid(1_010_000_000, 1, 100, 101, 1), // worker thread
            ],
        };
        let bytes = trace.encode_to_vec();
        let (data, _images) = convert(&bytes).unwrap();

        // Main thread (tid == pid == 100) should be labeled "main".
        assert_eq!(
            data.thread_metadata
                .get("100")
                .and_then(|m| m.name.as_deref()),
            Some("main"),
        );
        // Worker thread (tid 101) should have no metadata since no name source exists.
        assert!(!data.thread_metadata.contains_key("101"));
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
