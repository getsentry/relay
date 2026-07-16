use relay_event_schema::protocol::DebugId;
use relay_protocol::FiniteF64;

use crate::perfetto::convert::consts;
use crate::perfetto::proto;
use crate::perfetto::proto::trace_packet::OptionalTrustedPacketSequenceId;

/// A Perfetto trusted sequence id extracted from a [`proto::TracePacket`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SequenceId(u32);

impl SequenceId {
    /// Creates a new [`Self`] from a [`proto::TracePacket`].
    pub fn new(packet: &proto::TracePacket) -> Self {
        let seq_id = packet
            .optional_trusted_packet_sequence_id
            .map(|OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(id)| id)
            // Default to `0` here, semantically this is equivalent to an invalid id,
            // which is good enough for our purposes.
            .unwrap_or(0);

        Self(seq_id)
    }
}

/// Returns `true` if the [`proto::TracePacket`] has its internal state cleared flag set.
pub fn has_incremental_state_cleared(packet: &proto::TracePacket) -> bool {
    packet
        .sequence_flags
        .is_some_and(|f| f & consts::SEQ_INCREMENTAL_STATE_CLEARED != 0)
}

pub fn extract_clock_offset(cs: &proto::ClockSnapshot) -> Option<i128> {
    let primary_clock = cs.primary_trace_clock.unwrap_or(consts::CLOCK_BOOTTIME);

    if primary_clock == consts::CLOCK_REALTIME {
        return Some(0);
    }

    let mut primary_ns: Option<u64> = None;
    let mut realtime_ns: Option<u64> = None;

    for clock in &cs.clocks {
        match clock.clock_id {
            Some(consts::CLOCK_REALTIME) => realtime_ns = clock.timestamp,
            Some(clock_id) if clock_id == primary_clock => primary_ns = clock.timestamp,
            _ => {}
        }
    }

    match (realtime_ns, primary_ns) {
        (Some(rt), Some(primary)) => Some(rt as i128 - primary as i128),
        _ => None,
    }
}

/// Returns `true` if the mapping path indicates a JVM/ART runtime mapping.
pub fn is_java_mapping(path: &str) -> bool {
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
pub fn build_id_to_debug_id(raw: &[u8]) -> Option<DebugId> {
    if raw.is_empty() {
        return None;
    }

    let mut buf = [0u8; 16];
    let len = raw.len().min(16);
    buf[..len].copy_from_slice(&raw[..len]);

    let uuid = uuid::Uuid::from_bytes_le(buf);
    Some(DebugId::from(uuid))
}

/// Converts nanoseconds to a float (seconds) with minimal precision loss.
pub fn ns_to_secs(ns: i128) -> FiniteF64 {
    const NANOS_PER_SEC: i128 = 1_000_000_000;
    let secs = ns.div_euclid(NANOS_PER_SEC);
    let frac = ns.rem_euclid(NANOS_PER_SEC);
    let res = FiniteF64::new(secs as f64 + (frac as f64) / 1e9);
    // This can't actually be ever infinite or NaN.
    debug_assert!(res.is_some());
    res.unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
