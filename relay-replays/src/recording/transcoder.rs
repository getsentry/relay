//! Serde transcoder modeled after `serde_transcode`.

use std::cell::RefCell;
use std::fmt;

use serde::de;
use serde::ser::{self, SerializeMap, SerializeSeq};

/// A Serde transcoder.
///
/// In most cases, the `transcode` function should be used instead of this
/// type.
///
/// # Note
///
/// Unlike traditional serializable types, `Transcoder`'s `Serialize`
/// implementation is *not* idempotent, as it advances the state of its
/// internal `Deserializer`. It should only ever be serialized once.
pub struct StringTranscoder<'a, D>(RefCell<Option<D>>, &'a dyn Fn(&mut String));

impl<'de, 'a, D> StringTranscoder<'a, D>
where
    D: de::Deserializer<'de>,
{
    /// Constructs a new `Transcoder`.
    pub fn new(d: D, transform: &'a dyn Fn(&mut String)) -> StringTranscoder<D> {
        StringTranscoder(RefCell::new(Some(d)), transform)
    }
}

impl<'de, 'a, D> ser::Serialize for StringTranscoder<'a, D>
where
    D: de::Deserializer<'de>,
{
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let Self(ref d, ref f) = self;

        d.borrow_mut()
            .take()
            .expect("Transcoder may only be serialized once")
            .deserialize_any(Visitor(s, f))
            .map_err(d2s)
    }
}

struct Visitor<'a, S>(S, &'a dyn Fn(&mut String));

impl<'de, 'a, S> de::Visitor<'de> for Visitor<'a, S>
where
    S: ser::Serializer,
{
    type Value = S::Ok;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "any value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.0.serialize_bool(v).map_err(s2d)
    }

    fn visit_i8<E>(self, v: i8) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.0.serialize_i8(v).map_err(s2d)
    }

    fn visit_i16<E>(self, v: i16) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.0.serialize_i16(v).map_err(s2d)
    }

    fn visit_i32<E>(self, v: i32) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.0.serialize_i32(v).map_err(s2d)
    }

    fn visit_i64<E>(self, v: i64) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.0.serialize_i64(v).map_err(s2d)
    }

    fn visit_u8<E>(self, v: u8) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.0.serialize_u8(v).map_err(s2d)
    }

    fn visit_u16<E>(self, v: u16) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.0.serialize_u16(v).map_err(s2d)
    }

    fn visit_u32<E>(self, v: u32) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.0.serialize_u32(v).map_err(s2d)
    }

    fn visit_u64<E>(self, v: u64) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.0.serialize_u64(v).map_err(s2d)
    }

    serde::serde_if_integer128! {
        fn visit_i128<E>(self, v: i128) -> Result<S::Ok, E>
            where E: de::Error
        {
            self.0.serialize_i128(v).map_err(s2d)
        }

        fn visit_u128<E>(self, v: u128) -> Result<S::Ok, E>
            where E: de::Error
        {
            self.0.serialize_u128(v).map_err(s2d)
        }
    }

    fn visit_f32<E>(self, v: f32) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.0.serialize_f32(v).map_err(s2d)
    }

    fn visit_f64<E>(self, v: f64) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.0.serialize_f64(v).map_err(s2d)
    }

    fn visit_char<E>(self, v: char) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.0.serialize_char(v).map_err(s2d)
    }

    fn visit_str<E>(self, v: &str) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.visit_string(v.to_owned())
    }

    fn visit_string<E>(self, mut v: String) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.1(&mut v);
        self.0.serialize_str(&v).map_err(s2d)
    }

    fn visit_unit<E>(self) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.0.serialize_unit().map_err(s2d)
    }

    fn visit_none<E>(self) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.0.serialize_none().map_err(s2d)
    }

    fn visit_some<D>(self, d: D) -> Result<S::Ok, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let Self(s, f) = self;
        s.serialize_some(&StringTranscoder::new(d, f)).map_err(s2d)
    }

    fn visit_newtype_struct<D>(self, d: D) -> Result<S::Ok, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let Self(s, f) = self;

        s.serialize_newtype_struct("<unknown>", &StringTranscoder::new(d, f))
            .map_err(s2d)
    }

    fn visit_seq<V>(self, mut v: V) -> Result<S::Ok, V::Error>
    where
        V: de::SeqAccess<'de>,
    {
        let mut s = self.0.serialize_seq(v.size_hint()).map_err(s2d)?;
        while let Some(()) = v.next_element_seed(SeqSeed(&mut s, self.1))? {}
        s.end().map_err(s2d)
    }

    fn visit_map<V>(self, mut v: V) -> Result<S::Ok, V::Error>
    where
        V: de::MapAccess<'de>,
    {
        let mut s = self.0.serialize_map(v.size_hint()).map_err(s2d)?;
        while let Some(()) = v.next_key_seed(KeySeed(&mut s, self.1))? {
            v.next_value_seed(ValueSeed(&mut s, self.1))?;
        }
        s.end().map_err(s2d)
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.0.serialize_bytes(v).map_err(s2d)
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<S::Ok, E>
    where
        E: de::Error,
    {
        self.0.serialize_bytes(&v).map_err(s2d)
    }
}

struct SeqSeed<'a, S: 'a>(&'a mut S, &'a dyn Fn(&mut String));

impl<'de, 'a, S> de::DeserializeSeed<'de> for SeqSeed<'a, S>
where
    S: ser::SerializeSeq,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let Self(s, f) = self;
        s.serialize_element(&StringTranscoder::new(deserializer, f))
            .map_err(s2d)
    }
}

struct KeySeed<'a, S: 'a>(&'a mut S, &'a dyn Fn(&mut String));

impl<'de, 'a, S> de::DeserializeSeed<'de> for KeySeed<'a, S>
where
    S: ser::SerializeMap,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let Self(s, _) = self;
        // NOTE: Skip keys for transforming
        s.serialize_key(&StringTranscoder::new(deserializer, &|_| ()))
            .map_err(s2d)
    }
}

struct ValueSeed<'a, S: 'a>(&'a mut S, &'a dyn Fn(&mut String));

impl<'de, 'a, S> de::DeserializeSeed<'de> for ValueSeed<'a, S>
where
    S: ser::SerializeMap,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let Self(s, f) = self;
        s.serialize_value(&StringTranscoder::new(deserializer, f))
            .map_err(s2d)
    }
}

fn d2s<D, S>(d: D) -> S
where
    D: de::Error,
    S: ser::Error,
{
    S::custom(d.to_string())
}

fn s2d<S, D>(s: S) -> D
where
    S: ser::Error,
    D: de::Error,
{
    D::custom(s.to_string())
}
