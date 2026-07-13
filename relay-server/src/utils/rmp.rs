use rmp_serde::Deserializer;
use rmp_serde::decode::{ReadReader, ReadRefReader};

/// Maximum nesting depth allowed when deserializing MessagePack payloads.
///
/// `rmp_serde` default (1024) too permissive for
/// Relay's types (e.g. `Annotated<Value>`). Each recursive deserialization frame is
/// several kilobytes, so a payload can exhaust the thread stack well before the built-in counter
/// trips.
/// This limit matches the one in `serde_json`.
pub const MAX_DEPTH: usize = 128;

/// Returns a Deserializer with a bounded recursion depth.
///
/// Caps nesting at
/// [`MAX_DEPTH`] to protect against stack overflows from maliciously nested payloads.
#[allow(clippy::disallowed_types)]
pub fn slice_deserializer<'a>(data: &'a [u8]) -> Deserializer<ReadRefReader<'a, [u8]>> {
    let mut deserializer = Deserializer::from_read_ref(data);
    deserializer.set_max_depth(MAX_DEPTH);
    deserializer
}

/// Returns a Deserializer with a bounded recursion depth.
///
/// Caps nesting at
/// [`MAX_DEPTH`] to protect against stack overflows from maliciously nested payloads.
///
/// Like `slice_deserializer`, but allows consuming multiple items from the slice.
#[allow(clippy::disallowed_types)]
pub fn stream_deserializer<'a>(data: &'a [u8]) -> Deserializer<ReadReader<&'a [u8]>> {
    let mut deserializer = Deserializer::new(data);
    deserializer.set_max_depth(MAX_DEPTH);
    deserializer
}
