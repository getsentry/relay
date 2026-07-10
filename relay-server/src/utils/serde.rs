use serde::de::IgnoredAny;
use serde::{Deserialize, Deserializer, de};

/// Maximum nesting depth allowed when deserializing MessagePack payloads.
///
/// `rmp_serde` does enforce a recursion limit, but its default (1024) is far too permissive for
/// Relay's deeply generic types (e.g. `Annotated<Value>`): each recursive deserialization frame is
/// several kilobytes, so a payload can exhaust the thread stack well before the built-in counter
/// trips. We therefore cap nesting at the same depth `serde_json` enforces by default (128), which
/// bounds the recursion long before it can overflow the stack while still accepting any realistic
/// payload. See `set_max_depth` on [`rmp_serde::Deserializer`].
pub const MSGPACK_MAX_DEPTH: usize = 128;

/// Deserializes a value from MessagePack bytes with a bounded recursion depth.
///
/// This is a drop-in replacement for [`rmp_serde::from_slice`] that caps nesting at
/// [`MSGPACK_MAX_DEPTH`] to protect against stack overflows from maliciously nested payloads.
pub fn msgpack_from_slice<'a, T>(data: &'a [u8]) -> Result<T, rmp_serde::decode::Error>
where
    T: Deserialize<'a>,
{
    let mut deserializer = rmp_serde::Deserializer::from_read_ref(data);
    deserializer.set_max_depth(MSGPACK_MAX_DEPTH);
    T::deserialize(&mut deserializer)
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
