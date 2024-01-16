use std::num::FpCategory;

use serde::ser::{
    SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
    SerializeTupleStruct, SerializeTupleVariant,
};
use serde::{Serialize, Serializer};

pub fn to_json_vec<S>(value: &S) -> Result<Vec<u8>, serde_json::Error>
where
    S: Serialize,
{
    let mut serializer = serde_json::Serializer::new(Vec::with_capacity(128));
    value.serialize(Finite(&mut serializer))?;
    Ok(serializer.into_inner())
}

/// TODO: Doc the heck out of this
pub struct Finite<S>(S);

impl<T> Serialize for Finite<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(Finite(serializer))
    }
}

impl<S> Serializer for Finite<S>
where
    S: Serializer,
{
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = Finite<S::SerializeSeq>;
    type SerializeTuple = Finite<S::SerializeTuple>;
    type SerializeTupleStruct = Finite<S::SerializeTupleStruct>;
    type SerializeTupleVariant = Finite<S::SerializeTupleVariant>;
    type SerializeMap = Finite<S::SerializeMap>;
    type SerializeStruct = Finite<S::SerializeStruct>;
    type SerializeStructVariant = Finite<S::SerializeStructVariant>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_bool(v)
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_i8(v)
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_i16(v)
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_i32(v)
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_i64(v)
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_u8(v)
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_u16(v)
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_u32(v)
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_u64(v)
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        match v.classify() {
            FpCategory::Nan => self.0.serialize_none(),
            FpCategory::Infinite if v.is_sign_negative() => self.0.serialize_f32(f32::MIN),
            FpCategory::Infinite => self.0.serialize_f32(f32::MAX),
            _ => self.0.serialize_f32(v),
        }
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        match v.classify() {
            FpCategory::Nan => self.0.serialize_none(),
            FpCategory::Infinite if v.is_sign_negative() => self.0.serialize_f64(f64::MIN),
            FpCategory::Infinite => self.0.serialize_f64(f64::MAX),
            _ => self.0.serialize_f64(v),
        }
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_char(v)
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_str(v)
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_bytes(v)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_none()
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        self.0.serialize_some(value)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit_struct(name)
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit_variant(name, variant_index, variant)
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        self.0.serialize_newtype_struct(name, value)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        self.0
            .serialize_newtype_variant(name, variant_index, variant, value)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.0.serialize_seq(len).map(Finite)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.0.serialize_tuple(len).map(Finite)
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.0.serialize_tuple_struct(name, len).map(Finite)
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.0
            .serialize_tuple_variant(name, variant_index, variant, len)
            .map(Finite)
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        self.0.serialize_map(len).map(Finite)
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.0.serialize_struct(name, len).map(Finite)
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.0
            .serialize_struct_variant(name, variant_index, variant, len)
            .map(Finite)
    }
}

impl<S> SerializeSeq for Finite<S>
where
    S: SerializeSeq,
{
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        self.0.serialize_element(&Finite(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<S> SerializeTuple for Finite<S>
where
    S: SerializeTuple,
{
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.0.serialize_element(&Finite(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<S> SerializeTupleStruct for Finite<S>
where
    S: SerializeTupleStruct,
{
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.0.serialize_field(&Finite(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<S> SerializeTupleVariant for Finite<S>
where
    S: SerializeTupleVariant,
{
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.0.serialize_field(&Finite(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<S> SerializeMap for Finite<S>
where
    S: SerializeMap,
{
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.0.serialize_key(&Finite(key))
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.0.serialize_value(&Finite(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<S> SerializeStruct for Finite<S>
where
    S: SerializeStruct,
{
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.0.serialize_field(key, &Finite(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<S> SerializeStructVariant for Finite<S>
where
    S: SerializeStructVariant,
{
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.0.serialize_field(key, &Finite(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inf64() {
        let json = to_json_vec(&f64::INFINITY).unwrap();
        let deserialized = serde_json::from_slice::<f64>(&json).unwrap();
        assert_eq!(deserialized, f64::MAX);
    }

    #[test]
    fn test_inf32() {
        let json = to_json_vec(&f32::INFINITY).unwrap();
        let deserialized = serde_json::from_slice::<f32>(&json).unwrap();
        assert_eq!(deserialized, f32::MAX);
    }

    #[test]
    fn test_neg_inf64() {
        let json = to_json_vec(&f64::NEG_INFINITY).unwrap();
        let deserialized = serde_json::from_slice::<f64>(&json).unwrap();
        assert_eq!(deserialized, f64::MIN);
    }

    #[test]
    fn test_neg_inf32() {
        let json = to_json_vec(&f32::NEG_INFINITY).unwrap();
        let deserialized = serde_json::from_slice::<f32>(&json).unwrap();
        assert_eq!(deserialized, f32::MIN);
    }

    #[test]
    fn test_nan32() {
        let json = to_json_vec(&f32::NAN).unwrap();
        let deserialized = serde_json::from_slice::<Option<f32>>(&json).unwrap();
        // NB: does NOT roundtrip
        assert_eq!(deserialized, None);
    }

    #[test]
    fn test_nan64() {
        let json = to_json_vec(&f64::NAN).unwrap();
        let deserialized = serde_json::from_slice::<Option<f64>>(&json).unwrap();
        // NB: does NOT roundtrip
        assert_eq!(deserialized, None);
    }
}
