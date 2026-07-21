//! Proof-of-concept adversarial inputs for the Perfetto parser.
//!
//! Each test crafts a bogus-but-valid trace in memory with `prost` and runs it
//! through [`convert`] — the same entry point the ingest pipeline uses
//! (`PerfettoProfileChunk::parse` → `convert`).
//!
//! S1–S7 assert the *desired safe* outcome (the parser should reject or bound
//! the abusive input). They therefore fail right now.
//!
//! ## Scale
//!
//! Per-item sizes are pushed to their structural ceilings (a single interned
//! string up to [`consts::MAX_TRACE_PACKET_BYTES`], a stored stack up to ~1M
//! ids, `MAX_PATH_SEGMENTS` path segments). The *amplified count* is a small
//! fixed multiple — enough to blow past [`MEM_LIMIT`].
//!
//! Against the current, unmitigated parser these OOM-abort the whole test
//! process (SIGABRT); run a single scenario by name to observe it. Once a
//! mitigation rejects the abusive input early, allocation stays under the
//! ceiling, no OOM occurs, and the `is_err()` assertion passes — that is the
//! signal the fix works. See `docs/perfetto-parser-attacks.md`.

use std::alloc::System;

use cap::Cap;
use prost::Message as _;

use super::{consts, convert};
use crate::ProfileError;
use crate::perfetto::proto;
use crate::perfetto::proto::trace_packet::{Data, OptionalTrustedPacketSequenceId};

// --- per-test memory ceiling -------------------------------------------------

/// Test-only allocator (the [`cap`](https://crates.io/crates/cap) crate) that
/// enforces a hard byte ceiling, causing a genuine out-of-memory crash (SIGABRT),
/// once the limits are reached.
///
/// Starts unlimited so the test binary and unrelated tests are unaffected until
/// a test sets the limit. An OOM abort takes the whole test process down, so
/// to observe a specific scenario run it by name, e.g.
/// `cargo test -p relay-profiling s1_frame_string_clone_bomb`.
#[global_allocator]
static ALLOCATOR: Cap<System> = Cap::new(System, usize::MAX);

/// Hard per-test memory ceiling installed at the start of each test.
const MEM_LIMIT: usize = 16 * 1024 * 1024;

/// Largest single interned string that still fits inside one trace packet,
/// leaving room for protobuf message framing.
const MAX_STR: usize = consts::MAX_TRACE_PACKET_BYTES - 64;

// --- builders ----------------------------------------------------------------

fn seq(id: u32) -> Option<OptionalTrustedPacketSequenceId> {
    Some(OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(id))
}

/// A valid clock snapshot so `convert` can resolve a clock offset.
fn clock_snapshot() -> proto::TracePacket {
    proto::TracePacket {
        data: Some(Data::ClockSnapshot(proto::ClockSnapshot {
            clocks: vec![
                proto::clock_snapshot::Clock {
                    clock_id: Some(consts::CLOCK_BOOTTIME),
                    timestamp: Some(1_000_000_000),
                },
                proto::clock_snapshot::Clock {
                    clock_id: Some(consts::CLOCK_REALTIME),
                    timestamp: Some(1_700_000_000_000_000_000),
                },
            ],
            primary_trace_clock: Some(consts::CLOCK_BOOTTIME),
        })),
        ..Default::default()
    }
}

fn interned(seq_id: u32, clear: bool, data: proto::InternedData) -> proto::TracePacket {
    proto::TracePacket {
        interned_data: Some(data),
        sequence_flags: clear.then_some(consts::SEQ_INCREMENTAL_STATE_CLEARED),
        optional_trusted_packet_sequence_id: seq(seq_id),
        ..Default::default()
    }
}

fn sample(ts: u64, seq_id: u32, tid: u32, cs_iid: u64) -> proto::TracePacket {
    proto::TracePacket {
        timestamp: Some(ts),
        optional_trusted_packet_sequence_id: seq(seq_id),
        data: Some(Data::PerfSample(proto::PerfSample {
            cpu: None,
            pid: Some(tid),
            tid: Some(tid),
            callstack_iid: Some(cs_iid),
        })),
        ..Default::default()
    }
}

fn istr(iid: u64, s: &[u8]) -> proto::InternedString {
    proto::InternedString {
        iid: Some(iid),
        r#str: Some(s.to_vec()),
    }
}

fn frame(
    iid: u64,
    function_name_id: Option<u64>,
    mapping_id: Option<u64>,
    rel_pc: u64,
) -> proto::Frame {
    proto::Frame {
        iid: Some(iid),
        function_name_id,
        mapping_id,
        rel_pc: Some(rel_pc),
    }
}

/// Splits `frame_iids` into callstacks no deeper than `MAX_CALLSTACK_DEPTH`,
/// numbering the callstack iids `1..=n`.
fn chunked_callstacks(frame_iids: &[u64]) -> Vec<proto::Callstack> {
    frame_iids
        .chunks(consts::MAX_CALLSTACK_DEPTH)
        .enumerate()
        .map(|(i, ids)| proto::Callstack {
            iid: Some(i as u64 + 1),
            frame_ids: ids.to_vec(),
        })
        .collect()
}

fn encode(packets: Vec<proto::TracePacket>) -> Vec<u8> {
    proto::Trace { packet: packets }.encode_to_vec()
}

// --- S1: frame string-clone bomb ---------------------------------------------

/// One shared function-name string — maxed to a full packet (~4 MiB) — is cloned
/// once per *unique* frame, uniqueness coming for free from a distinct `rel_pc`
/// (→ distinct `instruction_addr`). The string lives once in the input, `N`
/// times in the output.
///
/// Maxed: `|S|` = `MAX_STR`. Structural ceiling: `N` up to `MAX_UNIQUE_FRAMES`
/// (1M) → ~4 TiB of clones.
///
/// SAFE OUTCOME: the parser bounds per-conversion cloned bytes and rejects.
#[test]
fn s1_frame_string_clone_bomb() {
    ALLOCATOR.set_limit(MEM_LIMIT).unwrap();
    let s_len = MAX_STR;
    let n = 16u64; // 16 × ~4 MiB clones
    let big = vec![b'x'; s_len];

    let frame_iids: Vec<u64> = (1..=n).collect();
    let frames: Vec<proto::Frame> = frame_iids
        .iter()
        .map(|&f| frame(f, Some(1), None, f)) // distinct rel_pc → distinct frame
        .collect();
    let callstacks = chunked_callstacks(&frame_iids);

    // Big string in its own packet; frames/callstacks in a second (no clear, or
    // it would wipe the string) — both packets stay under the 4 MiB limit.
    let mut packets = vec![
        clock_snapshot(),
        interned(
            1,
            true,
            proto::InternedData {
                function_names: vec![istr(1, &big)],
                ..Default::default()
            },
        ),
        interned(
            1,
            false,
            proto::InternedData {
                frames,
                callstacks: callstacks.clone(),
                ..Default::default()
            },
        ),
    ];
    for cs in &callstacks {
        packets.push(sample(1_000_000_000, 1, 1, cs.iid.unwrap()));
    }

    let result = convert(&encode(packets));
    assert!(
        result.is_err(),
        "expected the parser to bound cloned-string memory and reject; \
         it cloned a {s_len}-byte string {n} times"
    );
}

// --- S2: debug-image clone bomb ----------------------------------------------

/// One shared native library path — maxed to a full packet — is cloned into a
/// new `DebugImage` for every distinct `mapping.start` (and into every frame's
/// package). `Images` has no count cap.
///
/// Maxed: `|C|` = `MAX_STR`. Structural ceiling: `N` bounded only by the frame
/// budget → images unbounded.
///
/// SAFE OUTCOME: the parser caps the number/size of debug images and rejects.
#[test]
fn s2_debug_image_clone_bomb() {
    ALLOCATOR.set_limit(MEM_LIMIT).unwrap();
    let code_file = format!("{}.so", "x".repeat(MAX_STR - 3)).into_bytes();
    // Each distinct mapping clones C twice: the image code_file and the frame package.
    let n = 8u64; // 8 × ~4 MiB × 2
    let build_id = vec![0xABu8; 20];

    let mappings: Vec<proto::Mapping> = (1..=n)
        .map(|m| proto::Mapping {
            iid: Some(m),
            build_id: Some(20),
            start: Some(m), // distinct image_addr → distinct image
            path_string_ids: vec![10],
            ..Default::default()
        })
        .collect();
    let frames: Vec<proto::Frame> = (1..=n).map(|m| frame(m, None, Some(m), 0)).collect();
    let frame_iids: Vec<u64> = (1..=n).collect();
    let callstacks = chunked_callstacks(&frame_iids);

    let mut packets = vec![
        clock_snapshot(),
        interned(
            1,
            true,
            proto::InternedData {
                mapping_paths: vec![istr(10, &code_file)],
                build_ids: vec![istr(20, &build_id)],
                ..Default::default()
            },
        ),
        interned(
            1,
            false,
            proto::InternedData {
                mappings,
                frames,
                callstacks: callstacks.clone(),
                ..Default::default()
            },
        ),
    ];
    for cs in &callstacks {
        packets.push(sample(1_000_000_000, 1, 1, cs.iid.unwrap()));
    }

    let result = convert(&encode(packets));
    assert!(
        result.is_err(),
        "expected the parser to cap debug-image count/size and reject; \
         it built {n} images each cloning a {}-byte path",
        code_file.len()
    );
}

// --- S3: mapping-path join bomb ----------------------------------------------

/// A single mapping whose `path_string_ids` repeats one large string the maximum
/// `MAX_PATH_SEGMENTS` times, joined with "/" into one giant String.
///
/// Maxed: `MAX_PATH_SEGMENTS` segments. Structural ceiling: each segment up to
/// ~4 MiB → ~400 MiB joined per mapping.
///
/// SAFE OUTCOME: the parser bounds the joined path length (or rejects repeats).
#[test]
fn s3_mapping_path_join_bomb() {
    ALLOCATOR.set_limit(MEM_LIMIT).unwrap();
    let segments = consts::MAX_PATH_SEGMENTS;
    let p_len = 512 * 1024; // 100 × 512 KiB ≈ 50 MiB joined
    let big_path = vec![b'y'; p_len];

    let bytes = encode(vec![
        clock_snapshot(),
        interned(
            1,
            true,
            proto::InternedData {
                function_names: vec![istr(1, b"f")],
                mapping_paths: vec![istr(10, &big_path)],
                mappings: vec![proto::Mapping {
                    iid: Some(1),
                    start: Some(0x1000),
                    path_string_ids: vec![10; segments], // one id, repeated
                    ..Default::default()
                }],
                frames: vec![frame(1, Some(1), Some(1), 1)],
                callstacks: vec![proto::Callstack {
                    iid: Some(1),
                    frame_ids: vec![1],
                }],
                ..Default::default()
            },
        ),
        sample(1_000_000_000, 1, 1, 1),
    ]);

    let result = convert(&bytes);
    assert!(
        result.is_err(),
        "expected the parser to bound the joined path length and reject; \
         it built a ~{}-byte package from one repeated segment",
        segments * p_len
    );
}

// --- S4: store-time vs use-time gap ------------------------------------------

/// A callstack whose `frame_ids` vastly exceeds `MAX_CALLSTACK_DEPTH` is decoded
/// and retained in full; the depth cap only fires when a sample *references* it.
///
/// Maxed: ~1M stored ids (an ~8 MB `Vec<u64>` from a ~2 MB packet). Structural
/// ceiling: up to a full 4 MiB packet of ids.
///
/// SAFE OUTCOME: the depth limit is enforced at store time, so the oversized
/// callstack is rejected whether or not it is referenced. The unreferenced case
/// is accepted today, so the first assertion FAILS.
#[test]
fn s4_depth_limit_should_be_store_time() {
    ALLOCATOR.set_limit(MEM_LIMIT).unwrap();
    let oversized: Vec<u64> = vec![1; 1_000_000]; // ≫ MAX_CALLSTACK_DEPTH

    let build = |referenced_cs: u64| {
        encode(vec![
            clock_snapshot(),
            interned(
                1,
                true,
                proto::InternedData {
                    function_names: vec![istr(1, b"f")],
                    frames: vec![frame(1, Some(1), None, 1)],
                    callstacks: vec![
                        proto::Callstack {
                            iid: Some(1),
                            frame_ids: vec![1], // small, valid
                        },
                        proto::Callstack {
                            iid: Some(2),
                            frame_ids: oversized.clone(),
                        },
                    ],
                    ..Default::default()
                },
            ),
            sample(1_000_000_000, 1, 1, referenced_cs),
        ])
    };

    // FAILS today: the oversized callstack is stored and the trace is accepted
    // because nothing references it. A store-time check would reject it.
    assert!(
        matches!(convert(&build(1)), Err(ProfileError::ExceedSizeLimit)),
        "expected store-time rejection of the oversized callstack"
    );

    // Already enforced at use time — passes today and must keep failing closed.
    assert!(matches!(
        convert(&build(2)),
        Err(ProfileError::ExceedSizeLimit)
    ));
}

// --- S5: unbounded interned tables -------------------------------------------

/// A million distinct-iid interned entries are absorbed with no total-entry cap.
///
/// Maxed: `N` = 1M distinct entries, spread across packets each under the 4 MiB
/// limit. Structural ceiling: bounded only by the 50 MiB input cap.
///
/// SAFE OUTCOME: the parser caps total interned entries and rejects.
#[test]
fn s5_interned_tables_should_be_capped() {
    ALLOCATOR.set_limit(MEM_LIMIT).unwrap();
    const N: u64 = 1_000_000;
    const PER_PACKET: u64 = 500_000; // keep each packet under MAX_TRACE_PACKET_BYTES

    let mut packets = vec![clock_snapshot()];
    let mut emitted = 0u64;
    let mut first = true;
    while emitted < N {
        let end = (emitted + PER_PACKET).min(N);
        let function_names: Vec<proto::InternedString> =
            (emitted + 1..=end).map(|i| istr(i, b"")).collect();

        let data = if first {
            // Anchor a referenced frame/callstack so the trace is valid.
            proto::InternedData {
                function_names,
                frames: vec![frame(1, Some(1), None, 1)],
                callstacks: vec![proto::Callstack {
                    iid: Some(1),
                    frame_ids: vec![1],
                }],
                ..Default::default()
            }
        } else {
            proto::InternedData {
                function_names,
                ..Default::default()
            }
        };

        packets.push(interned(1, first, data));
        first = false;
        emitted = end;
    }
    packets.push(sample(1_000_000_000, 1, 1, 1));

    let result = convert(&encode(packets));
    assert!(
        result.is_err(),
        "expected a total interned-entry cap to reject; it absorbed {N} entries"
    );
}

// --- S6: sequence-ID fan-out --------------------------------------------------

/// Every packet uses a distinct `trusted_packet_sequence_id`, so `intern_db` and
/// `cache_db` retain one never-evicted `Tables`+`Caches` pair per sequence.
///
/// Maxed: `M` = 50k live sequence states. Structural ceiling: up to
/// `MAX_TRACE_PACKETS` (~110k) distinct sequences.
///
/// SAFE OUTCOME: the parser caps the number of distinct sequences and rejects.
#[test]
fn s6_sequence_id_fan_out_should_be_capped() {
    ALLOCATOR.set_limit(MEM_LIMIT).unwrap();
    const M: u32 = 50_000;

    let mut packets = vec![clock_snapshot()];
    for s in 1..=M {
        packets.push(interned(
            s,
            true,
            proto::InternedData {
                function_names: vec![istr(1, b"f")],
                frames: vec![frame(1, Some(1), None, 1)],
                callstacks: vec![proto::Callstack {
                    iid: Some(1),
                    frame_ids: vec![1],
                }],
                ..Default::default()
            },
        ));
    }
    // A single valid sample against the first sequence keeps the trace valid.
    packets.push(sample(1_000_000_000, 1, 1, 1));

    let result = convert(&encode(packets));
    assert!(
        result.is_err(),
        "expected a distinct-sequence cap to reject; it retained {M} sequence states"
    );
}

// --- S7: never-clear cumulative interning ------------------------------------

/// A single sequence that only clears on its first packet, then keeps adding
/// full-packet (~4 MiB) interned strings. Nothing is freed, so interned state
/// grows toward the whole input.
///
/// Maxed: ~`K × 4 MiB` accumulated, kept under the 50 MiB input cap. Structural
/// ceiling: the full 50 MiB payload retained at once.
///
/// SAFE OUTCOME: a total interned-byte budget (independent of the clear flag)
/// rejects.
#[test]
fn s7_never_clear_should_be_capped() {
    ALLOCATOR.set_limit(MEM_LIMIT).unwrap();
    const K: u64 = 8; // 8 × ~4 MiB ≈ 32 MiB, under the 50 MiB payload cap
    let chunk = vec![b'z'; MAX_STR];

    let mut packets = vec![
        clock_snapshot(),
        // First packet clears, anchors the referenced frame, and adds one chunk.
        interned(
            1,
            true,
            proto::InternedData {
                function_names: vec![istr(1, b"main"), istr(2, &chunk)],
                frames: vec![frame(1, Some(1), None, 1)],
                callstacks: vec![proto::Callstack {
                    iid: Some(1),
                    frame_ids: vec![1],
                }],
                ..Default::default()
            },
        ),
    ];
    // Subsequent packets never clear — each distinct-iid chunk just piles up.
    for i in 2..K {
        packets.push(interned(
            1,
            false,
            proto::InternedData {
                function_names: vec![istr(i + 1, &chunk)],
                ..Default::default()
            },
        ));
    }
    packets.push(sample(1_000_000_000, 1, 1, 1));

    let result = convert(&encode(packets));
    assert!(
        result.is_err(),
        "expected a cumulative interned-byte budget to reject; \
         it retained {K} un-cleared ~{MAX_STR}-byte chunks"
    );
}
