use serde::de::value::Error;
use serde::{ser, Serialize};
use smallvec::SmallVec;

use crate::types::{IntoValue, SkipSerialization};

/// Estimates the size in bytes this would be in JSON.
pub fn estimate_size<T: IntoValue>(value: Option<&T>) -> usize {
    let mut ser = SizeEstimatingSerializer::new();
    if let Some(value) = value {
        IntoValue::serialize_payload(value, &mut ser, SkipSerialization::default()).unwrap();
    }
    ser.size()
}

/// Estimates the size in bytes this would be in JSON, but does not recurse into objects or arrays.
pub fn estimate_size_flat<T: IntoValue>(value: Option<&T>) -> usize {
    let mut ser = SizeEstimatingSerializer::new();
    ser.flat = true;
    if let Some(value) = value {
        IntoValue::serialize_payload(value, &mut ser, SkipSerialization::default()).unwrap();
    }
    ser.size()
}

/// Helper serializer that efficiently determines how much space something might take.
///
/// This counts in estimated bytes.
#[derive(Default)]
struct SizeEstimatingSerializer {
    size: usize,
    item_stack: SmallVec<[bool; 16]>,
    flat: bool,
}

impl SizeEstimatingSerializer {
    /// Creates a new serializer
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the calculated size
    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    fn count_size(&mut self, incr: usize) {
        if self.flat && !self.item_stack.is_empty() {
            return;
        }

        self.size += incr;
    }

    fn push(&mut self) {
        self.item_stack.push(false);
    }

    fn pop(&mut self) {
        self.item_stack.pop();
    }

    fn count_comma_sep(&mut self) {
        if let Some(state) = self.item_stack.last_mut() {
            if !*state {
                *state = true;
            } else {
                self.count_size(1);
            }
        }
    }
}

impl<'a> ser::Serializer for &'a mut SizeEstimatingSerializer {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    #[inline]
    fn serialize_bool(self, v: bool) -> Result<(), Error> {
        self.count_size(if v { 4 } else { 5 });
        Ok(())
    }

    #[inline(always)]
    fn serialize_i8(self, v: i8) -> Result<(), Error> {
        self.serialize_i64(i64::from(v))
    }

    #[inline(always)]
    fn serialize_i16(self, v: i16) -> Result<(), Error> {
        self.serialize_i64(i64::from(v))
    }

    #[inline(always)]
    fn serialize_i32(self, v: i32) -> Result<(), Error> {
        self.serialize_i64(i64::from(v))
    }

    #[inline]
    fn serialize_i64(self, v: i64) -> Result<(), Error> {
        self.size += &v.to_string().len();
        Ok(())
    }

    #[inline(always)]
    fn serialize_u8(self, v: u8) -> Result<(), Error> {
        self.serialize_u64(u64::from(v))
    }

    #[inline(always)]
    fn serialize_u16(self, v: u16) -> Result<(), Error> {
        self.serialize_u64(u64::from(v))
    }

    #[inline(always)]
    fn serialize_u32(self, v: u32) -> Result<(), Error> {
        self.serialize_u64(u64::from(v))
    }

    #[inline]
    fn serialize_u64(self, v: u64) -> Result<(), Error> {
        self.count_size(v.to_string().len());
        Ok(())
    }

    #[inline(always)]
    fn serialize_f32(self, v: f32) -> Result<(), Error> {
        self.serialize_f64(f64::from(v))
    }

    #[inline]
    fn serialize_f64(self, v: f64) -> Result<(), Error> {
        self.count_size(v.to_string().len());
        Ok(())
    }

    #[inline(always)]
    fn serialize_char(self, _v: char) -> Result<(), Error> {
        self.count_size(1);
        Ok(())
    }

    #[inline(always)]
    fn serialize_str(self, v: &str) -> Result<(), Error> {
        self.count_size(v.len() + 2);
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<(), Error> {
        use serde::ser::SerializeSeq;
        let mut seq = self.serialize_seq(Some(v.len()))?;
        for byte in v {
            seq.serialize_element(byte)?;
        }
        seq.end()
    }

    #[inline(always)]
    fn serialize_none(self) -> Result<(), Error> {
        self.serialize_unit()
    }

    #[inline(always)]
    fn serialize_some<T>(self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    #[inline(always)]
    fn serialize_unit(self) -> Result<(), Error> {
        self.count_size(4);
        Ok(())
    }

    #[inline(always)]
    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), Error> {
        self.serialize_unit()
    }

    #[inline(always)]
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<(), Error> {
        self.serialize_str(variant)
    }

    #[inline(always)]
    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        // { x : y }
        self.count_size(3);
        variant.serialize(&mut *self)?;
        value.serialize(&mut *self)?;
        Ok(())
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        self.count_size(1);
        self.push();
        Ok(self)
    }

    #[inline(always)]
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Error> {
        self.serialize_seq(Some(len))
    }

    #[inline(always)]
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        // { x: [
        self.count_size(3);
        variant.serialize(&mut *self)?;
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Error> {
        // {
        self.count_size(1);
        self.push();
        Ok(self)
    }

    #[inline(always)]
    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Error> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        // { x: {
        self.count_size(3);
        variant.serialize(&mut *self)?;
        Ok(self)
    }
}

impl<'a> ser::SerializeSeq for &'a mut SizeEstimatingSerializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        self.count_comma_sep();
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), Error> {
        self.pop();
        self.count_size(1);
        Ok(())
    }
}

// Same thing but for tuples.
impl<'a> ser::SerializeTuple for &'a mut SizeEstimatingSerializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        self.count_comma_sep();
        value.serialize(&mut **self)
    }

    #[inline(always)]
    fn end(self) -> Result<(), Error> {
        self.count_size(1);
        Ok(())
    }
}

// Same thing but for tuple structs.
impl<'a> ser::SerializeTupleStruct for &'a mut SizeEstimatingSerializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        self.count_comma_sep();
        value.serialize(&mut **self)
    }

    #[inline(always)]
    fn end(self) -> Result<(), Error> {
        self.count_size(1);
        Ok(())
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut SizeEstimatingSerializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        self.count_comma_sep();
        value.serialize(&mut **self)
    }

    #[inline(always)]
    fn end(self) -> Result<(), Error> {
        self.count_size(2);
        Ok(())
    }
}

impl<'a> ser::SerializeMap for &'a mut SizeEstimatingSerializer {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        self.count_comma_sep();
        key.serialize(&mut **self)
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        self.count_size(1);
        value.serialize(&mut **self)
    }

    #[inline(always)]
    fn end(self) -> Result<(), Error> {
        self.pop();
        self.count_size(1);
        Ok(())
    }
}

impl<'a> ser::SerializeStruct for &'a mut SizeEstimatingSerializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        self.count_comma_sep();
        self.count_size(2);
        key.serialize(&mut **self)?;
        value.serialize(&mut **self)
    }

    #[inline(always)]
    fn end(self) -> Result<(), Error> {
        self.count_size(1);
        Ok(())
    }
}

impl<'a> ser::SerializeStructVariant for &'a mut SizeEstimatingSerializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        self.count_comma_sep();
        self.size += 2;
        key.serialize(&mut **self)?;
        value.serialize(&mut **self)
    }

    #[inline(always)]
    fn end(self) -> Result<(), Error> {
        self.count_size(2);
        Ok(())
    }
}

#[test]
fn test_estimate_size() {
    use crate::types::{Annotated, Object, Value};
    let json = r#"{"a":["Hello","World","aha","hmm",false,{"blub":42,"x":true},null]}"#;
    let value = Annotated::<Object<Value>>::from_json(json).unwrap();
    assert_eq!(estimate_size(value.value()), json.len());
}
