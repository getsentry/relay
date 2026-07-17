/// See <https://perfetto.dev/docs/reference/trace-packet-proto#SequenceFlags>.
pub const SEQ_INCREMENTAL_STATE_CLEARED: u32 = 1;

/// Perfetto builtin real time clock ID.
///
/// See <https://perfetto.dev/docs/concepts/clock-sync>.
pub const CLOCK_REALTIME: u32 = 1;
/// Perfetto builtin boot time clock ID.
///
/// See <https://perfetto.dev/docs/concepts/clock-sync>.
pub const CLOCK_BOOTTIME: u32 = 6;

/// Estimated maximum amount of memory which can be used for storing interning tables and caches.
pub const MAX_MEMORY_WORK: usize = 15 * 1024 * 1024;

/// Estimated maximum amount of memory which can be used for the converted profile.
pub const MAX_MEMORY_RESULT: usize = 40 * 1024 * 1024;

/// Maximum amount of unique sequence ids allowed in a single profile.
pub const MAX_SEQUENCE_IDS: usize = 64;

/// Maximum number of raw samples we collect from a Perfetto trace before
/// bailing out. At 100 Hz across multiple threads, a 66-second chunk
/// produces at most ~6 600 samples per thread; 100 000 provides generous
/// headroom while bounding memory usage against adversarial input.
pub const MAX_SAMPLES: usize = 100_000;

/// Maximum number of top-level Perfetto trace packets we decode before
/// bailing out.
///
/// `MAX_SAMPLES` only counts useful `PerfSample` packets. Valid traces also
/// need clock snapshots, interned data updates, incremental state resets, and
/// other metadata packets. The extra 10 000 packets are allocation headroom for
/// that metadata while keeping the top-level packet vector bounded.
pub const MAX_TRACE_PACKETS: usize = MAX_SAMPLES + 10_000;

/// Maximum encoded size of a single top-level Perfetto trace packet.
///
/// This bounds allocations from repeated fields inside an individual packet
/// before handing the packet body to prost for decoding.
pub const MAX_TRACE_PACKET_BYTES: usize = 4 * 1024 * 1024;

/// Upper bound on the number of frames in a call stack.
pub const MAX_CALLSTACK_DEPTH: usize = 1000;

/// Upper bound on joinable path segments.
///
/// Prevents excessive string joins.
pub const MAX_PATH_SEGMENTS: usize = 100;

/// Maximum amount of unique frames in a single profile.
pub const MAX_UNIQUE_FRAMES: usize = 1_000_000;
