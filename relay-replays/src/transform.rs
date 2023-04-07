//! A transforming `serde` Deserializer`.

use std::borrow::Cow;
use std::fmt;

use serde::de;

/// A transform for deserialized values.
///
/// This transformer defines callbacks that will be called by a [`Deserializer`] during
/// deserialization to map values inline. The default for every transform callback is the identity
/// function, which will not change the value.
///
/// There is a default implementation for all functions with a matching signature, for example
/// `FnMut(&str) -> Cow<str>`.
///
/// # Strings and Bytes
///
/// When implementing a transform for strings or bytes, **always implement both** the owned and
/// borrowed version:
///
///  - `transform_str` and `transform_string` for strings
///  - `transform_bytes` and `transform_byte_buf` for bytes.
///
/// # Numbers
///
/// If the deserializer is used on a format that supports all numeric types, the default of each
/// transform function is the identity. To override this, all of `transform_i*` and `transform_u*`
/// have to be implemented.
///
/// # Example
///
/// ```ignore
/// struct StringDefault(&'static str);
///
/// impl Transform for StringDefault {
///     fn transform_str<'a>(&mut self, v: &'a str) -> Cow<'a, str> {
///         match v {
///             "" => Cow::Borrowed(self.0),
///             other => Cow::Borrowed(other),
///         }
///     }
///
///     fn transform_string(&mut self, v: String) -> Cow<'a, str> {
///         match v.as_str() {
///             "" => Cow::Borrowed(self.0),
///             _ => Cow::Owned(v),
///         }
///     }
/// }
/// ```
pub trait Transform<'de> {
    fn push_path(&mut self, _key: &'de str) {}

    fn pop_path(&mut self) {}

    fn transform_bool(&mut self, v: bool) -> bool {
        v
    }

    fn transform_i8(&mut self, v: i8) -> i8 {
        v
    }

    fn transform_i16(&mut self, v: i16) -> i16 {
        v
    }

    fn transform_i32(&mut self, v: i32) -> i32 {
        v
    }

    fn transform_i64(&mut self, v: i64) -> i64 {
        v
    }

    fn transform_u8(&mut self, v: u8) -> u8 {
        v
    }

    fn transform_u16(&mut self, v: u16) -> u16 {
        v
    }

    fn transform_u32(&mut self, v: u32) -> u32 {
        v
    }

    fn transform_u64(&mut self, v: u64) -> u64 {
        v
    }

    serde::serde_if_integer128! {
        fn transform_i128(&mut self, v: i128) -> i128 {
            v
        }

        fn transform_u128(&mut self, v: u128) -> u128 {
            v
        }
    }

    fn transform_f32(&mut self, v: f32) -> f32 {
        v
    }

    fn transform_f64(&mut self, v: f64) -> f64 {
        v
    }

    fn transform_char(&mut self, v: char) -> char {
        v
    }

    fn transform_str<'a>(&mut self, v: &'a str) -> Cow<'a, str> {
        Cow::Borrowed(v)
    }

    fn transform_string(&mut self, v: String) -> Cow<'static, str> {
        Cow::Owned(v)
    }

    fn transform_bytes<'a>(&mut self, v: &'a [u8]) -> Cow<'a, [u8]> {
        Cow::Borrowed(v)
    }

    fn transform_byte_buf(&mut self, v: Vec<u8>) -> Cow<'static, [u8]> {
        Cow::Owned(v)
    }
}

enum Mut<'a, T> {
    Owned(T),
    Borrowed(&'a mut T),
}

impl<T> Mut<'_, T> {
    fn as_mut(&mut self) -> &mut T {
        match self {
            Self::Owned(ref mut t) => t,
            Self::Borrowed(t) => t,
        }
    }
}

/// A [`Deserializer`](de::Deserializer) that maps all values through a [`Transform`].
///
/// This deserializer wraps another deserializer. Values are transformed inline during
/// deserialization and passed directly to the `Deserialize` implementation. All errors are passed
/// through without modification.
///
///  # Lifetime
///
/// The lifetime parameter is an implementation detail. [`new`](Self::new) returns a transforming
/// deserializer with static lifetime.
///
/// # Example
///
/// ```ignore
/// struct Identity;
///
/// let json = "42";
/// let json_deserializer = &mut serde_json::Deserializer::from_str(&json);
/// let deserializer = Deserializer::new(json_deserializer, Identity);
///
/// let number = deserializer.deserialize_u32(deserializer).unwrap();
/// assert_eq!(number, 42);
/// ```
pub struct Deserializer<'a, D, T>(D, Mut<'a, T>, bool);

impl<'de, 'a, D, T> Deserializer<'a, D, T>
where
    D: de::Deserializer<'de>,
    T: Transform<'de>,
{
    /// Creates a new `Deserializer`.
    pub fn new(deserializer: D, transformer: T) -> Self {
        Self(deserializer, Mut::Owned(transformer), false)
    }

    fn borrowed(deserializer: D, transformer: &'a mut T) -> Self {
        Self(deserializer, Mut::Borrowed(transformer), false)
    }

    fn key(deserializer: D, transformer: &'a mut T) -> Self {
        Self(deserializer, Mut::Borrowed(transformer), true)
    }
}

impl<'de, 'a, D, T> de::Deserializer<'de> for Deserializer<'a, D, T>
where
    D: de::Deserializer<'de>,
    T: Transform<'de>,
{
    type Error = D::Error;

    fn deserialize_any<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_any(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_bool<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_bool(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_i8<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_i8(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_i16<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_i16(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_i32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_i32(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_i64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_i64(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_u8<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_u8(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_u16<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_u16(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_u32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_u32(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_u64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_u64(Visitor(visitor, self.1.as_mut(), self.2))
    }

    serde::serde_if_integer128! {
        fn deserialize_i128<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: de::Visitor<'de>,
        {
            self.0.deserialize_i128(Visitor(visitor, self.1.as_mut(), self.2))
        }

        fn deserialize_u128<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: de::Visitor<'de>,
        {
            self.0.deserialize_u128(Visitor(visitor, self.1.as_mut(), self.2))
        }
    }

    fn deserialize_f32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_f32(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_f64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_f64(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_char<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_char(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_str<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_str(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_string<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_string(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_bytes<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_bytes(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_byte_buf<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_byte_buf(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_option<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_option(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_unit<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_unit(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_unit_struct<V>(
        mut self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_unit_struct(name, Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_newtype_struct<V>(
        mut self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_newtype_struct(name, Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_seq<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_seq(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_tuple<V>(mut self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_tuple(len, Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_tuple_struct<V>(
        mut self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_tuple_struct(name, len, Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_map<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_map(Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_struct<V>(
        mut self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_struct(name, fields, Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_enum<V>(
        mut self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_enum(name, variants, Visitor(visitor, self.1.as_mut(), self.2))
    }

    fn deserialize_identifier<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_identifier(Visitor(visitor, self.1.as_mut(), true))
    }

    fn deserialize_ignored_any<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0
            .deserialize_ignored_any(Visitor(visitor, self.1.as_mut(), self.2))
    }
}

struct Visitor<'a, V, T>(V, &'a mut T, bool);

impl<'de, 'a, V, T> de::Visitor<'de> for Visitor<'a, V, T>
where
    V: de::Visitor<'de>,
    T: Transform<'de>,
{
    type Value = V::Value;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "any value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.0.visit_bool(self.1.transform_bool(v))
    }

    fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.0.visit_i8(self.1.transform_i8(v))
    }

    fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.0.visit_i16(self.1.transform_i16(v))
    }

    fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.0.visit_i32(self.1.transform_i32(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.0.visit_i64(self.1.transform_i64(v))
    }

    fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.0.visit_u8(self.1.transform_u8(v))
    }

    fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.0.visit_u16(self.1.transform_u16(v))
    }

    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.0.visit_u32(self.1.transform_u32(v))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.0.visit_u64(self.1.transform_u64(v))
    }

    serde::serde_if_integer128! {
        fn visit_i128<E>(self, v: i128) -> Result<Self::Value, E>
            where E: de::Error
        {
            self.0.visit_i128(self.1.transform_i128(v))
        }

        fn visit_u128<E>(self, v: u128) -> Result<Self::Value, E>
            where E: de::Error
        {
            self.0.visit_u128(self.1.transform_u128(v))
        }
    }

    fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.0.visit_f32(self.1.transform_f32(v))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.0.visit_f64(self.1.transform_f64(v))
    }

    fn visit_char<E>(self, v: char) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.0.visit_char(self.1.transform_char(v))
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if self.2 {
            self.1.push_path(v);
            return self.0.visit_borrowed_str(v);
        };

        let res = match self.1.transform_str(v) {
            Cow::Borrowed(v) => self.0.visit_borrowed_str(v),
            Cow::Owned(v) => self.0.visit_string(v),
        };
        res
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if self.2 {
            return self.0.visit_str(v);
        };
        match self.1.transform_str(v) {
            Cow::Borrowed(v) => self.0.visit_str(v),
            Cow::Owned(v) => self.0.visit_string(v),
        }
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if self.2 {
            return self.0.visit_string(v);
        };
        match self.1.transform_string(v) {
            Cow::Borrowed(v) => self.0.visit_borrowed_str(v),
            Cow::Owned(v) => self.0.visit_string(v),
        }
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.0.visit_unit()
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.0.visit_none()
    }

    fn visit_some<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        self.0.visit_some(Deserializer::borrowed(d, self.1))
    }

    fn visit_newtype_struct<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        self.0
            .visit_newtype_struct(Deserializer::borrowed(d, self.1))
    }

    fn visit_seq<A>(self, v: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        self.0.visit_seq(SeqAccess(v, self.1))
    }

    fn visit_map<A>(self, v: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        self.0.visit_map(MapAccess(v, self.1))
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match self.1.transform_bytes(v) {
            Cow::Borrowed(v) => self.0.visit_bytes(v),
            Cow::Owned(v) => self.0.visit_byte_buf(v),
        }
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match self.1.transform_byte_buf(v) {
            Cow::Borrowed(v) => self.0.visit_bytes(v),
            Cow::Owned(v) => self.0.visit_byte_buf(v),
        }
    }
}

struct SeqAccess<'a, A, T>(A, &'a mut T);

impl<'de, 'a, A, T> de::SeqAccess<'de> for SeqAccess<'a, A, T>
where
    A: de::SeqAccess<'de>,
    T: Transform<'de>,
{
    type Error = A::Error;

    fn size_hint(&self) -> Option<usize> {
        self.0.size_hint()
    }

    fn next_element_seed<S>(&mut self, seed: S) -> Result<Option<S::Value>, Self::Error>
    where
        S: de::DeserializeSeed<'de>,
    {
        self.0.next_element_seed(DeserializeValueSeed(seed, self.1))
    }
}

struct MapAccess<'a, A, T>(A, &'a mut T);

impl<'de, 'a, A, T> de::MapAccess<'de> for MapAccess<'a, A, T>
where
    A: de::MapAccess<'de>,
    T: Transform<'de>,
{
    type Error = A::Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: de::DeserializeSeed<'de>,
    {
        // NOTE: No transform on keys.
        self.0.next_key_seed(DeserializeKeySeed(seed, self.1))
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        self.0.next_value_seed(DeserializeValueSeed(seed, self.1))
    }
}

struct DeserializeValueSeed<'a, D, T>(D, &'a mut T);

impl<'de, 'a, D, T> de::DeserializeSeed<'de> for DeserializeValueSeed<'a, D, T>
where
    D: de::DeserializeSeed<'de>,
    T: Transform<'de>,
{
    type Value = D::Value;

    fn deserialize<X>(self, deserializer: X) -> Result<Self::Value, X::Error>
    where
        X: serde::Deserializer<'de>,
    {
        let res = self
            .0
            .deserialize(Deserializer::borrowed(deserializer, self.1));
        self.1.pop_path();
        res
    }
}

struct DeserializeKeySeed<'a, D, T>(D, &'a mut T);

impl<'de, 'a, D, T> de::DeserializeSeed<'de> for DeserializeKeySeed<'a, D, T>
where
    D: de::DeserializeSeed<'de>,
    T: Transform<'de>,
{
    type Value = D::Value;

    fn deserialize<X>(self, deserializer: X) -> Result<Self::Value, X::Error>
    where
        X: serde::Deserializer<'de>,
    {
        self.0.deserialize(Deserializer::key(deserializer, self.1))
    }
}
