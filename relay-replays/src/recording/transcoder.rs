//! Serde transcoder modeled after `serde_transcode`.

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;

use serde::de;
use serde::ser::{self, SerializeMap, SerializeSeq};

/// A Serde transcoder that transforms strings.
///
/// Streams all contents from the deserializer into the serializer and passes
/// strings through a mutation callback.
///
/// # Note
///
/// Unlike traditional serializable types, `StringTranscoder`'s `Serialize`
/// implementation is *not* idempotent, as it advances the state of its
/// internal `Deserializer`. It should only ever be serialized once.
pub struct StringTranscoder<D, F>(RefCell<Option<D>>, Rc<RefCell<F>>);

impl<'de, D, F> StringTranscoder<D, F>
where
    D: de::Deserializer<'de>,
    F: FnMut(&mut String),
{
    /// Constructs a new `StringTranscoder`.
    pub fn new(deserializer: D, transform: F) -> Self {
        Self::from_visitor(deserializer, Rc::new(RefCell::new(transform)))
    }

    fn from_visitor(deserializer: D, transform: Rc<RefCell<F>>) -> Self {
        Self(RefCell::new(Some(deserializer)), transform)
    }
}

impl<'de, D, F> ser::Serialize for StringTranscoder<D, F>
where
    D: de::Deserializer<'de>,
    F: FnMut(&mut String),
{
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        self.0
            .borrow_mut()
            .take()
            .expect("Transcoder may only be serialized once")
            .deserialize_any(Visitor(s, self.1.clone()))
            .map_err(d2s)
    }
}

struct Visitor<S, F>(S, Rc<RefCell<F>>);

impl<'de, S, F> de::Visitor<'de> for Visitor<S, F>
where
    S: ser::Serializer,
    F: FnMut(&mut String),
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
        (self.1.borrow_mut())(&mut v);
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
        self.0
            .serialize_some(&StringTranscoder::from_visitor(d, self.1))
            .map_err(s2d)
    }

    fn visit_newtype_struct<D>(self, d: D) -> Result<S::Ok, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        self.0
            .serialize_newtype_struct("<unknown>", &StringTranscoder::from_visitor(d, self.1))
            .map_err(s2d)
    }

    fn visit_seq<V>(self, mut v: V) -> Result<S::Ok, V::Error>
    where
        V: de::SeqAccess<'de>,
    {
        let mut s = self.0.serialize_seq(v.size_hint()).map_err(s2d)?;
        while let Some(()) = v.next_element_seed(SeqSeed(&mut s, self.1.clone()))? {}
        s.end().map_err(s2d)
    }

    fn visit_map<V>(self, mut v: V) -> Result<S::Ok, V::Error>
    where
        V: de::MapAccess<'de>,
    {
        let mut s = self.0.serialize_map(v.size_hint()).map_err(s2d)?;
        // NOTE: Intentionally no string transforms on keys.
        while let Some(()) = v.next_key_seed(KeySeed(&mut s))? {
            v.next_value_seed(ValueSeed(&mut s, self.1.clone()))?;
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

struct SeqSeed<'a, S: 'a, F>(&'a mut S, Rc<RefCell<F>>);

impl<'de, 'a, S, F> de::DeserializeSeed<'de> for SeqSeed<'a, S, F>
where
    S: ser::SerializeSeq,
    F: FnMut(&mut String),
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: de::Deserializer<'de>,
    {
        self.0
            .serialize_element(&StringTranscoder::from_visitor(deserializer, self.1))
            .map_err(s2d)
    }
}

struct KeySeed<'a, S: 'a>(&'a mut S);

impl<'de, 'a, S> de::DeserializeSeed<'de> for KeySeed<'a, S>
where
    S: ser::SerializeMap,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: de::Deserializer<'de>,
    {
        self.0
            .serialize_key(&StringTranscoder::new(deserializer, |_| ()))
            .map_err(s2d)
    }
}

struct ValueSeed<'a, S: 'a, F>(&'a mut S, Rc<RefCell<F>>);

impl<'de, 'a, S, F> de::DeserializeSeed<'de> for ValueSeed<'a, S, F>
where
    S: ser::SerializeMap,
    F: FnMut(&mut String),
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: de::Deserializer<'de>,
    {
        self.0
            .serialize_value(&StringTranscoder::from_visitor(deserializer, self.1))
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
