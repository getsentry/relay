//! Perfetto trace format conversion to Sample v2.
//!
//! Decodes Android Perfetto traces (`PerfSample` packets + `ClockSnapshot` +
//! interned data) into the Sample v2 profile format.

use std::collections::BTreeMap;
use std::hash::BuildHasher;

use bytes::Buf;
use hashbrown::hash_table::Entry;
use hashbrown::{DefaultHashBuilder, HashMap, HashSet, HashTable};
use prost::Message;
use prost::encoding::{self, WireType};

use relay_event_schema::protocol::{Addr, DebugId};
use relay_protocol::FiniteF64;

use crate::debug_image::{DebugImage, ImageType};
use crate::error::ProfileError;
use crate::sample::v2::{ProfileData, Sample};
use crate::sample::{Frame, ThreadMetadata};

use crate::perfetto::proto;

use proto::trace_packet::Data;

/// Maximum number of raw samples we collect from a Perfetto trace before
/// bailing out. At 100 Hz across multiple threads, a 66-second chunk
/// produces at most ~6 600 samples per thread; 100 000 provides generous
/// headroom while bounding memory usage against adversarial input.
const MAX_SAMPLES: usize = 100_000;

/// Maximum number of top-level Perfetto trace packets we decode before
/// bailing out.
///
/// `MAX_SAMPLES` only counts useful `PerfSample` packets. Valid traces also
/// need clock snapshots, interned data updates, incremental state resets, and
/// other metadata packets. The extra 10 000 packets are allocation headroom for
/// that metadata while keeping the top-level packet vector bounded.
const MAX_TRACE_PACKETS: usize = MAX_SAMPLES + 10_000;

/// Maximum encoded size of a single top-level Perfetto trace packet.
///
/// This bounds allocations from repeated fields inside an individual packet
/// before handing the packet body to prost for decoding.
const MAX_TRACE_PACKET_BYTES: usize = 4 * 1024 * 1024;

/// Upper bound on the number of frames in a call stack.
///
/// Resolved frame indices are used as keys in a hash map so we should avoid excessive hashing.
const MAX_CALLSTACK_DEPTH: usize = 1000;

/// Upper bound on joinable path segments.
///
/// Prevents excessive string joins.
const MAX_PATH_SEGMENTS: usize = 100;

const MAX_UNIQUE_FRAMES: usize = 1_000_000;

/// See <https://perfetto.dev/docs/reference/trace-packet-proto#SequenceFlags>.
const SEQ_INCREMENTAL_STATE_CLEARED: u32 = 1;

/// Perfetto builtin real time clock ID.
///
/// See <https://perfetto.dev/docs/concepts/clock-sync>.
const CLOCK_REALTIME: u32 = 1;
/// Perfetto builtin boot time clock ID.
///
/// See <https://perfetto.dev/docs/concepts/clock-sync>.
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
    let primary_clock = cs.primary_trace_clock.unwrap_or(CLOCK_BOOTTIME);

    if primary_clock == CLOCK_REALTIME {
        return Some(0);
    }

    let mut primary_ns: Option<u64> = None;
    let mut realtime_ns: Option<u64> = None;

    for clock in &cs.clocks {
        match clock.clock_id {
            Some(CLOCK_REALTIME) => realtime_ns = clock.timestamp,
            Some(clock_id) if clock_id == primary_clock => primary_ns = clock.timestamp,
            _ => {}
        }
    }

    match (realtime_ns, primary_ns) {
        (Some(rt), Some(primary)) => Some(rt as i128 - primary as i128),
        _ => None,
    }
}

/// Parse trace packets, passing each decoded packet to `visit`.
///
/// This avoids holding all decoded packet allocations at once. The packet count
/// and per-packet byte limits still bound adversarial traces.
fn visit_trace_packets(
    mut perfetto_bytes: &[u8],
    mut visit: impl FnMut(proto::TracePacket) -> Result<(), ProfileError>,
) -> Result<(), ProfileError> {
    let mut packet_count = 0;
    while perfetto_bytes.has_remaining() {
        let (tag, wire_type) = encoding::decode_key(&mut perfetto_bytes)
            .map_err(|_| ProfileError::InvalidSampledProfile)?;

        match (tag, wire_type) {
            (1, WireType::LengthDelimited) => {
                if packet_count >= MAX_TRACE_PACKETS {
                    return Err(ProfileError::ExceedSizeLimit);
                }

                let len = encoding::decode_varint(&mut perfetto_bytes)
                    .map_err(|_| ProfileError::InvalidSampledProfile)?;
                let len = usize::try_from(len).map_err(|_| ProfileError::ExceedSizeLimit)?;

                if len > MAX_TRACE_PACKET_BYTES {
                    return Err(ProfileError::ExceedSizeLimit);
                }

                if len > perfetto_bytes.remaining() {
                    return Err(ProfileError::InvalidSampledProfile);
                }

                let packet = proto::TracePacket::decode(&perfetto_bytes[..len])
                    .map_err(|_| ProfileError::InvalidSampledProfile)?;
                perfetto_bytes.advance(len);
                packet_count += 1;
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

/// Per-sequence interned data tables, mirroring Perfetto's incremental state.
///
/// Perfetto traces use interned IDs to avoid repeating large strings and
/// structures in every packet. Each trusted packet sequence maintains its
/// own set of intern tables that can be cleared on state resets.
///
/// Per the Perfetto spec, each `InternedData` field constructs its **own**
/// interning index — IDs are scoped per field, not shared across string types.
/// See <https://android.googlesource.com/platform/external/perfetto/+/refs/heads/master/protos/perfetto/trace/interned_data/interned_data.proto>.
#[derive(Debug, Default)]
struct InternTables {
    // HashMap over BTreeMap: these tables can grow large (one entry per interned symbol).
    function_names: HashMap<u64, String>,
    mapping_paths: HashMap<u64, String>,
    build_ids: HashMap<u64, Vec<u8>>,
    frames: HashMap<u64, proto::Frame>,
    callstacks: HashMap<u64, proto::Callstack>,
    mappings: HashMap<u64, proto::Mapping>,
    mapping_path_cache: HashMap<u64, Option<String>>,
    frame_cache: HashMap<u64, usize>,
    callstack_cache: HashMap<u64, usize>,
}

impl InternTables {
    fn merge(&mut self, data: proto::InternedData) {
        self.mapping_path_cache.clear();
        self.frame_cache.clear();
        self.callstack_cache.clear();

        let proto::InternedData {
            function_names,
            mapping_paths,
            build_ids,
            frames,
            callstacks,
            mappings,
        } = data;

        self.function_names.extend(intern_strings(function_names));
        self.mapping_paths.extend(intern_strings(mapping_paths));
        self.build_ids.extend(
            build_ids
                .into_iter()
                .filter_map(|is| Some((is.iid?, is.r#str?))),
        );
        self.frames
            .extend(frames.into_iter().filter_map(|f| Some((f.iid?, f))));
        self.callstacks
            .extend(callstacks.into_iter().filter_map(|c| Some((c.iid?, c))));
        self.mappings
            .extend(mappings.into_iter().filter_map(|m| Some((m.iid?, m))));
    }
}

/// Resolves and caches the joined path of a mapping, returning a reference
/// into the cache.
fn mapping_path<'a>(
    mapping_id: u64,
    mappings: &HashMap<u64, proto::Mapping>,
    mapping_paths: &HashMap<u64, String>,
    mapping_path_cache: &'a mut HashMap<u64, Option<String>>,
) -> Option<&'a str> {
    mapping_path_cache
        .entry(mapping_id)
        .or_insert_with(|| {
            mappings
                .get(&mapping_id)
                .and_then(|mapping| resolve_mapping_path(mapping, mapping_paths))
        })
        .as_deref()
}

fn intern_strings(strings: Vec<proto::InternedString>) -> impl Iterator<Item = (u64, String)> {
    strings
        .into_iter()
        .filter_map(|is| Some((is.iid?, String::from_utf8(is.r#str?).ok()?)))
}

/// Deduplication key for resolved stack frames.
///
/// Two Perfetto frames that resolve to the same function, module, package,
/// and instruction address are considered identical and share a single index
/// in the output frame list.
#[derive(Debug, PartialEq, Eq, Hash)]
struct FrameCandidate<'a> {
    function: Option<&'a str>,
    module: Option<&'a str>,
    package: Option<&'a str>,
    instruction_addr: Option<u64>,
}

impl<'a> FrameCandidate<'a> {
    fn new(frame: &'a Frame) -> Self {
        Self {
            function: frame.function.as_deref(),
            module: frame.module.as_deref(),
            package: frame.package.as_deref(),
            instruction_addr: frame.instruction_addr.map(|addr| addr.0),
        }
    }
}

/// Mutable context for callstack resolution, collecting frames, stacks,
/// and debug images during a single conversion pass.
#[derive(Default)]
struct ResolveContext {
    /// Indexes into `frames`, keyed by the [`FrameCandidate`].
    frame_index: HashTable<usize>,
    /// Hasher used to access [`Self::frame_index`].
    hasher: DefaultHashBuilder,
    frames: Vec<Frame>,
    stack_index: HashMap<Vec<usize>, usize>,
    debug_images: Vec<DebugImage>,
    seen_images: HashSet<(String, u64)>,
}

/// Converts a Perfetto binary trace into Sample v2 [`ProfileData`] and debug images.
pub fn convert(perfetto_bytes: &[u8]) -> Result<(ProfileData, Vec<DebugImage>), ProfileError> {
    let mut tables_by_seq: HashMap<u32, InternTables> = HashMap::new();
    let mut thread_meta: BTreeMap<u32, ThreadMetadata> = BTreeMap::new();
    let mut clock_offset_ns: Option<i128> = None;
    let mut observed_pid: Option<u32> = None;

    // Samples are resolved eagerly during packet iteration (single-pass) so
    // that incremental state resets don't cause earlier samples to be resolved
    // against a post-reset intern table. We collect (ts_ns, tid, stack_id)
    // tuples and apply clock offset + sorting after the loop.
    let mut ctx = ResolveContext::default();
    let mut resolved_samples: Vec<ResolvedSample> = Vec::new();
    let mut sample_count: usize = 0;

    visit_trace_packets(perfetto_bytes, |packet| {
        let seq_id = trusted_packet_sequence_id(&packet);

        let intern_tables = tables_by_seq.entry(seq_id).or_default();

        if has_incremental_state_cleared(&packet) {
            *intern_tables = Default::default();
        }

        if let Some(interned_data) = packet.interned_data {
            intern_tables.merge(interned_data);
        }

        match packet.data {
            Some(Data::ClockSnapshot(cs)) if clock_offset_ns.is_none() => {
                clock_offset_ns = extract_clock_offset(&cs);
            }
            Some(Data::PerfSample(sample)) => {
                if let Some(callstack_iid) = sample.callstack_iid {
                    let timestamp_ns = packet.timestamp.unwrap_or(0);
                    let thread_id = sample.tid.unwrap_or(0);
                    if observed_pid.is_none() {
                        observed_pid = sample.pid;
                    }
                    sample_count += 1;
                    if sample_count > MAX_SAMPLES {
                        return Err(ProfileError::ExceedSizeLimit);
                    }

                    if let Some(stack_id) =
                        resolve_callstack(callstack_iid, intern_tables, &mut ctx)?
                    {
                        resolved_samples.push(ResolvedSample {
                            timestamp_ns,
                            thread_id,
                            stack_id,
                        });
                    }
                }
            }
            _ => {}
        }

        Ok(())
    })?;

    if resolved_samples.is_empty() {
        return Err(ProfileError::NotEnoughSamples);
    }

    // On Android/Linux the main thread's tid equals the process pid.
    // Label it "main" so the UI can identify it.
    if let Some(pid) = observed_pid {
        thread_meta.entry(pid).or_insert_with(|| ThreadMetadata {
            name: Some("main".to_owned()),
            priority: None,
        });
    }

    let clock_offset_ns = clock_offset_ns.ok_or(ProfileError::InvalidSampledProfile)?;

    resolved_samples.sort_by_key(|s| s.timestamp_ns);

    let mut samples: Vec<Sample> = Vec::with_capacity(resolved_samples.len());
    for ResolvedSample {
        timestamp_ns,
        thread_id,
        stack_id,
    } in resolved_samples
    {
        // Compute absolute timestamp in integer nanoseconds first, then convert
        // to f64 seconds once to avoid precision loss from adding large floats.
        let abs_ns = timestamp_ns as i128 + clock_offset_ns;
        let ts_secs = abs_ns as f64 / 1_000_000_000.0;
        let ts_secs = (ts_secs * 1000.0).round() / 1000.0;

        if let Some(ts) = FiniteF64::new(ts_secs) {
            samples.push(Sample {
                timestamp: ts,
                stack_id,
                thread_id: thread_id.to_string(),
            });
        }
    }

    if samples.is_empty() {
        return Err(ProfileError::NotEnoughSamples);
    }

    // Convert u32 thread keys to String for the output format.
    let thread_metadata = thread_meta
        .into_iter()
        .map(|(tid, meta)| (tid.to_string(), meta))
        .collect();

    // Stacks are stored only as `stack_index` keys during conversion;
    // materialize them into the id-ordered output vector.
    let mut stacks = vec![Vec::new(); ctx.stack_index.len()];
    for (stack, id) in ctx.stack_index {
        stacks[id] = stack;
    }

    Ok((
        ProfileData {
            samples,
            stacks,
            frames: ctx.frames,
            thread_metadata,
        },
        ctx.debug_images,
    ))
}

struct ResolvedSample {
    timestamp_ns: u64,
    thread_id: u32,
    stack_id: usize,
}

/// Resolves a callstack iid against the current intern tables, deduplicating
/// frames and stacks, and collecting debug images for native mappings.
///
/// Returns `Some(stack_id)` if the callstack was resolved, or `None` if the
/// callstack iid was not found in the tables.
fn resolve_callstack(
    cs_iid: u64,
    tables: &mut InternTables,
    ctx: &mut ResolveContext,
) -> Result<Option<usize>, ProfileError> {
    let InternTables {
        function_names,
        mapping_paths,
        build_ids,
        frames,
        callstacks,
        mappings,
        mapping_path_cache,
        frame_cache,
        callstack_cache,
    } = tables;

    if let Some(&stack_id) = callstack_cache.get(&cs_iid) {
        return Ok(Some(stack_id));
    }

    let Some(callstack) = callstacks.get(&cs_iid) else {
        return Ok(None);
    };

    if callstack.frame_ids.len() > MAX_CALLSTACK_DEPTH {
        return Err(ProfileError::ExceedSizeLimit);
    }

    let mut resolved_frame_indices: Vec<usize> = Vec::with_capacity(callstack.frame_ids.len());

    for &frame_iid in &callstack.frame_ids {
        if let Some(&idx) = frame_cache.get(&frame_iid) {
            resolved_frame_indices.push(idx);
            continue;
        }

        let Some(&pf) = frames.get(&frame_iid) else {
            continue;
        };

        let function_name = pf
            .function_name_id
            .and_then(|id| function_names.get(&id))
            .map(String::as_str);

        let mapping = pf.mapping_id.and_then(|mid| mappings.get(&mid));
        let path = pf
            .mapping_id
            .and_then(|mid| mapping_path(mid, mappings, mapping_paths, mapping_path_cache));

        if let Some(mapping) = mapping
            && let Some(image) = collect_debug_image(mapping, path, build_ids, &mut ctx.seen_images)
        {
            ctx.debug_images.push(image);
        }

        let candidate = frame_candidate(function_name, path, &pf, mapping);

        let idx = match ctx.frame_index.entry(
            ctx.hasher.hash_one(&candidate),
            |&i| candidate == FrameCandidate::new(&ctx.frames[i]),
            |&i| ctx.hasher.hash_one(FrameCandidate::new(&ctx.frames[i])),
        ) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                let next_idx = ctx.frames.len();
                if next_idx >= MAX_UNIQUE_FRAMES {
                    return Err(ProfileError::ExceedSizeLimit);
                }
                ctx.frames.push(build_frame(&candidate));
                entry.insert(next_idx);
                next_idx
            }
        };

        frame_cache.insert(frame_iid, idx);
        resolved_frame_indices.push(idx);
    }

    // Perfetto stacks are root-first, Sample v2 is leaf-first.
    resolved_frame_indices.reverse();

    let next_id = ctx.stack_index.len();
    let stack_id = *ctx
        .stack_index
        .entry(resolved_frame_indices)
        .or_insert(next_id);

    callstack_cache.insert(cs_iid, stack_id);

    Ok(Some(stack_id))
}

/// Builds a debug image from a native mapping if not already seen.
///
/// Returns `Some(DebugImage)` for new native mappings with a valid build ID,
/// or `None` if the mapping has no path, is Java-only, already seen, or lacks
/// a valid debug ID.
fn collect_debug_image(
    mapping: &proto::Mapping,
    code_file: Option<&str>,
    build_ids: &HashMap<u64, Vec<u8>>,
    seen_images: &mut HashSet<(String, u64)>,
) -> Option<DebugImage> {
    let code_file = code_file?;

    if is_java_mapping(code_file) {
        return None;
    }

    let image_addr = mapping.start;

    let debug_id = mapping
        .build_id
        .and_then(|bid| build_ids.get(&bid))
        .and_then(|bytes| build_id_to_debug_id(bytes))?;

    // Insert into dedup set only after validating we have a valid debug_id,
    // so that a mapping first seen without a build_id doesn't block a later
    // valid encounter from a different packet sequence.
    if !seen_images.insert((code_file.to_owned(), image_addr.unwrap_or(0))) {
        return None;
    }

    let image_size = mapping
        .end
        .unwrap_or(0)
        .saturating_sub(image_addr.unwrap_or(0));
    let image_vmaddr = mapping.load_bias;

    Some(DebugImage {
        code_file: Some(code_file.to_owned().into()),
        debug_id: Some(debug_id),
        image_type: ImageType::Symbolic,
        image_addr: image_addr.map(Addr),
        image_vmaddr: image_vmaddr.map(Addr),
        image_size,
        uuid: None,
    })
}

/// Resolves a Perfetto frame into a borrowed [`FrameCandidate`] dedup view.
///
/// Java frames (identified by mapping path) have their fully-qualified name
/// split into module and function. Native frames compute an absolute
/// instruction address from `rel_pc` and the mapping start address.
fn frame_candidate<'a>(
    function_name: Option<&'a str>,
    mapping_path: Option<&'a str>,
    pf: &proto::Frame,
    mapping: Option<&proto::Mapping>,
) -> FrameCandidate<'a> {
    let is_java = mapping_path.is_some_and(is_java_mapping);

    if is_java {
        // For Java frames, split "com.example.MyClass.myMethod" into
        // module="com.example.MyClass" and function="myMethod".
        let (module, function) = match function_name {
            Some(name) => match name.rsplit_once('.') {
                Some((class, method)) => (Some(class), Some(method)),
                None => (None, Some(name)),
            },
            None => (None, None),
        };

        FrameCandidate {
            function,
            module,
            package: mapping_path,
            instruction_addr: None,
        }
    } else {
        FrameCandidate {
            function: function_name,
            module: None,
            package: mapping_path,
            instruction_addr: frame_instruction_addr(pf, mapping),
        }
    }
}

/// Materializes an owned Sample v2 [`Frame`] from a borrowed [`FrameCandidate`].
///
/// The platform is derived from the package: a Java mapping path implies a
/// Java frame (Java candidates never carry an instruction address).
fn build_frame(candidate: &FrameCandidate<'_>) -> Frame {
    let is_java = candidate.package.is_some_and(is_java_mapping);

    Frame {
        function: candidate.function.map(str::to_owned),
        module: candidate.module.map(str::to_owned),
        package: candidate.package.map(str::to_owned),
        instruction_addr: candidate.instruction_addr.map(Addr),
        platform: Some(if is_java { "java" } else { "native" }.to_owned()),
        ..Default::default()
    }
}

fn frame_instruction_addr(pf: &proto::Frame, mapping: Option<&proto::Mapping>) -> Option<u64> {
    let rel_pc = pf.rel_pc?;

    match mapping.and_then(|m| m.start) {
        Some(start) => start.checked_add(rel_pc),
        None => Some(rel_pc),
    }
}

/// Joins a mapping's interned path segments with `/` into a single file path.
///
/// Returns `None` if the mapping has no resolvable path segments.
fn resolve_mapping_path(
    mapping: &proto::Mapping,
    mapping_paths: &HashMap<u64, String>,
) -> Option<String> {
    if mapping.path_string_ids.len() > MAX_PATH_SEGMENTS {
        return None;
    }

    let mut path = String::new();
    let mut has_segment = false;
    for id in &mapping.path_string_ids {
        let Some(segment) = mapping_paths.get(id) else {
            continue;
        };

        if has_segment {
            path.push('/');
        }
        path.push_str(segment);
        has_segment = true;
    }

    if path.is_empty() { None } else { Some(path) }
}

/// Returns `true` if the mapping path indicates a JVM/ART runtime mapping.
fn is_java_mapping(path: &str) -> bool {
    const JVM_EXTENSIONS: &[&str] = &[".oat", ".odex", ".vdex", ".jar", ".dex"];

    if path.contains("dalvik-jit-code-cache") {
        return true;
    }
    JVM_EXTENSIONS.iter().any(|ext| path.ends_with(ext))
}

/// Converts a raw ELF build ID into a Sentry [`DebugId`].
///
/// The first 16 bytes of the build ID are interpreted as a little-endian UUID.
/// If the build ID is shorter than 16 bytes it is zero-padded on the right.
fn build_id_to_debug_id(raw: &[u8]) -> Option<DebugId> {
    if raw.is_empty() {
        return None;
    }

    let mut buf = [0u8; 16];
    let len = raw.len().min(16);
    buf[..len].copy_from_slice(&raw[..len]);

    let uuid = uuid::Uuid::from_bytes_le(buf);
    Some(DebugId::from(uuid))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_build_id_to_debug_id() {
        // 20-byte ELF build ID (common for GNU build IDs).
        let raw = &[
            0xb0, 0x3e, 0x4a, 0x7f, 0x5e, 0x88, 0x4c, 0x8d, 0xa0, 0x4b, 0x05, 0xfa, 0x32, 0xcc,
            0x4c, 0xbd, 0x69, 0xfa, 0xff, 0x51,
        ];
        let debug_id = build_id_to_debug_id(raw).unwrap();
        // First 16 bytes interpreted as little-endian UUID:
        //   time_low  (0..4)  reversed: 7f4a3eb0
        //   time_mid  (4..6)  reversed: 885e
        //   time_hi   (6..8)  reversed: 8d4c
        //   rest      (8..16) unchanged: a04b05fa32cc4cbd
        assert_eq!(debug_id.to_string(), "7f4a3eb0-885e-8d4c-a04b-05fa32cc4cbd");
    }

    #[test]
    fn test_build_id_to_debug_id_short() {
        // Build ID shorter than 16 bytes → zero-padded.
        let debug_id = build_id_to_debug_id(&[0xaa, 0xbb, 0xcc, 0xdd]).unwrap();
        // Bytes: aa bb cc dd 00 00 00 00 00 00 00 00 00 00 00 00
        // After swap: ddccbbaa-0000-0000-0000-000000000000
        assert_eq!(debug_id.to_string(), "ddccbbaa-0000-0000-0000-000000000000");
    }

    #[test]
    fn test_build_id_to_debug_id_empty() {
        assert!(build_id_to_debug_id(&[]).is_none());
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
