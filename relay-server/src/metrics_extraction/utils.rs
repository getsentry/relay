use std::hash::Hasher as _;

use hash32::{FnvHasher, Hasher as _};

/// Hashes the passed string using FNV32.
///
/// The default for metrics has been changed from fnv32 to crc32 to align the implementation with
/// the SDKs.
///
/// To not break already extracted metrics, the metric extraction still uses fnv32.
pub fn hash_fnv_32(string: &str) -> u32 {
    let mut hasher = FnvHasher::default();
    hasher.write(string.as_bytes());
    hasher.finish32()
}
