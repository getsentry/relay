use rmp_serde::decode::ReadRefReader;
use serde::de::IgnoredAny;
use serde::{Deserialize, Deserializer, de};

/// Maximum nesting depth allowed when deserializing MessagePack payloads.
///
/// `rmp_serde` default (1024) too permissive for
/// Relay's types (e.g. `Annotated<Value>`). Each recursive deserialization frame is
/// several kilobytes, so a payload can exhaust the thread stack well before the built-in counter
/// trips.
/// This limit matches the one in `serde_json`.
pub const MSGPACK_MAX_DEPTH: usize = 128;

/// Returns a Deserializer with a bounded recursion depth.
///
/// Caps nesting at
/// [`MSGPACK_MAX_DEPTH`] to protect against stack overflows from maliciously nested payloads.
#[allow(clippy::disallowed_types)]
pub fn msgpack_deserializer<'a>(
    data: &'a [u8],
) -> rmp_serde::Deserializer<ReadRefReader<'a, [u8]>> {
    let mut deserializer = rmp_serde::Deserializer::from_read_ref(data);
    deserializer.set_max_depth(MSGPACK_MAX_DEPTH);
    deserializer
}

/// Deserializes only the count of a sequence ignoring all individual items.
#[derive(Clone, Copy, Debug, Default)]
pub struct SeqCount(pub usize);

impl<'de> Deserialize<'de> for SeqCount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl<'a> de::Visitor<'a> for Visitor {
            type Value = SeqCount;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a sequence")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'a>,
            {
                let mut count = 0;
                while seq.next_element::<IgnoredAny>()?.is_some() {
                    count += 1;
                }

                Ok(SeqCount(count))
            }
        }

        deserializer.deserialize_seq(Visitor)
    }
}
