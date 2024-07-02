use serde::de::IgnoredAny;
use serde::{de, Deserialize, Deserializer};

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
