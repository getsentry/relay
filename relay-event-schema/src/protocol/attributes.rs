use std::borrow::{Borrow, Cow};
use std::fmt;

use enumset::EnumSet;
use relay_protocol::{
    Annotated, Empty, FromValue, IntoValue, Meta, Object, SkipSerialization, Value,
};

use crate::processor::{
    Pii, ProcessValue, ProcessingResult, ProcessingState, Processor, ValueType,
};

#[derive(Clone, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct Attribute {
    #[metastructure(flatten)]
    pub value: AttributeValue,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, trim = false)]
    pub other: Object<Value>,
}

impl Attribute {
    pub fn new(attribute_type: AttributeType, value: Value) -> Self {
        Self {
            value: AttributeValue {
                ty: Annotated::new(attribute_type),
                value: Annotated::new(value),
            },
            other: Object::new(),
        }
    }

    /// Returns the string value of this attribute.
    pub fn into_string(self) -> Option<String> {
        self.value.value.into_value()?.into_string()
    }
}

impl fmt::Debug for Attribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Attribute")
            .field("value", &self.value.value)
            .field("type", &self.value.ty)
            .field("other", &self.other)
            .finish()
    }
}

impl<T> From<T> for Attribute
where
    AttributeValue: From<T>,
{
    fn from(value: T) -> Self {
        let value = value.into();
        Self {
            value,
            other: Default::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct AttributeValue {
    #[metastructure(field = "type", required = true, trim = false, pii = "false")]
    pub ty: Annotated<AttributeType>,
    #[metastructure(required = "value_or_meta", pii = "attribute_pii_from_conventions")]
    pub value: Annotated<Value>,
}

macro_rules! impl_from {
    ($ty:ident, $aty: expr) => {
        impl From<Annotated<$ty>> for AttributeValue {
            fn from(value: Annotated<$ty>) -> Self {
                AttributeValue {
                    ty: Annotated::new($aty),
                    value: value.map_value(Into::into),
                }
            }
        }

        impl From<$ty> for AttributeValue {
            fn from(value: $ty) -> Self {
                AttributeValue::from(Annotated::new(value))
            }
        }
    };
}

impl_from!(String, AttributeType::String);
impl_from!(i64, AttributeType::Integer);
impl_from!(f64, AttributeType::Double);
impl_from!(bool, AttributeType::Boolean);

impl From<Annotated<&str>> for AttributeValue {
    fn from(value: Annotated<&str>) -> Self {
        Self {
            ty: Annotated::new(AttributeType::String),
            value: value.map_value(Into::into),
        }
    }
}

impl From<&str> for AttributeValue {
    fn from(value: &str) -> Self {
        Self::from(Annotated::new(value))
    }
}

impl From<Vec<String>> for AttributeValue {
    fn from(value: Vec<String>) -> Self {
        let values = value
            .into_iter()
            .map(|value| Annotated::new(Value::String(value)))
            .collect();

        Self {
            ty: Annotated::new(AttributeType::Array),
            value: Annotated::new(Value::Array(values)),
        }
    }
}

/// Determines the `Pii` value for an attribute (or, more exactly, the
/// attribute's `value` field) by looking it up in `relay-conventions`.
///
/// The attribute's value may be addressed by `<key>.value` (for advanced
/// scrubbing rules) or by just `<key>` (for default scrubbing rules). Therefore,
/// we iterate backwards through the processing state's segments and try to look
/// up each in the conventions.
///
/// If the attribute is not found in the conventions, this returns `Pii::True`
/// as a precaution.
pub fn attribute_pii_from_conventions(state: &ProcessingState) -> Pii {
    for key in state.keys() {
        let Some(info) = relay_conventions::attribute_info(key) else {
            continue;
        };

        return match info.apply_scrubbing {
            relay_conventions::ApplyScrubbing::Auto => Pii::True,
            relay_conventions::ApplyScrubbing::Never => Pii::False,
            relay_conventions::ApplyScrubbing::Manual => Pii::Maybe,
        };
    }

    Pii::True
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttributeType {
    /// A boolean.
    ///
    /// The respective value must be of type [`Value::Bool`].
    Boolean,
    /// An integer type.
    ///
    /// The respective value must be of type [`Value::I64`] or [`Value::U64`].
    Integer,
    /// A floating point/double type.
    ///
    /// The respective value must be of type [`Value::F64`].
    Double,
    /// A string type.
    ///
    /// The respective value must be of type [`Value::String`].
    String,
    /// A string type.
    ///
    /// The respective value must be of type [`Value::Array`].
    Array,
    /// An unknown type.
    ///
    /// Kept for forward compatibility.
    Unknown(String),
}

impl ProcessValue for AttributeType {}

impl AttributeType {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Boolean => "boolean",
            Self::Integer => "integer",
            Self::Double => "double",
            Self::String => "string",
            Self::Array => "array",
            Self::Unknown(value) => value,
        }
    }

    pub fn unknown_string() -> String {
        "unknown".to_owned()
    }
}

impl fmt::Display for AttributeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<String> for AttributeType {
    fn from(value: String) -> Self {
        match value.as_str() {
            "boolean" => Self::Boolean,
            "integer" => Self::Integer,
            "double" => Self::Double,
            "string" => Self::String,
            "array" => Self::Array,
            _ => Self::Unknown(value),
        }
    }
}

impl Empty for AttributeType {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

impl FromValue for AttributeType {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match String::from_value(value) {
            Annotated(Some(value), meta) => Annotated(Some(value.into()), meta),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl IntoValue for AttributeType {
    fn into_value(self) -> Value
    where
        Self: Sized,
    {
        Value::String(match self {
            Self::Unknown(s) => s,
            s => s.to_string(),
        })
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        serde::ser::Serialize::serialize(self.as_str(), s)
    }
}

/// Wrapper struct around a collection of attributes with some convenience methods.
#[derive(Debug, Clone, Default, PartialEq, Empty, FromValue, IntoValue)]
pub struct Attributes(pub Object<Attribute>);

impl Attributes {
    /// Creates an empty collection of attributes.
    pub fn new() -> Self {
        Self(Object::new())
    }

    /// Returns the value of the attribute with the given key.
    pub fn get_value<Q>(&self, key: &Q) -> Option<&Value>
    where
        String: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.get_annotated_value(key)?.value()
    }

    /// Returns the attribute with the given key.
    pub fn get_attribute<Q>(&self, key: &Q) -> Option<&Attribute>
    where
        String: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0.get(key)?.value()
    }

    /// Returns the attribute value as annotated.
    pub fn get_annotated_value<Q>(&self, key: &Q) -> Option<&Annotated<Value>>
    where
        String: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        Some(&self.0.get(key)?.value()?.value.value)
    }

    /// Returns the mutable attribute value as annotated.
    pub fn get_annotated_value_mut<Q>(&mut self, key: &Q) -> Option<&mut Annotated<Value>>
    where
        String: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        Some(&mut self.0.get_mut(key)?.value_mut().as_mut()?.value.value)
    }

    /// Inserts an attribute with the given value into this collection.
    pub fn insert<K: Into<String>, V: Into<AttributeValue>>(&mut self, key: K, value: V) {
        fn inner(slf: &mut Attributes, key: String, value: AttributeValue) {
            let attribute = Annotated::new(Attribute {
                value,
                other: Default::default(),
            });
            slf.0.insert(key, attribute);
        }
        let value = value.into();
        if !value.value.is_empty() {
            inner(self, key.into(), value);
        }
    }

    /// Inserts an attribute with the given value if it was not already present.
    pub fn insert_if_missing<F, V>(&mut self, key: &str, value: F)
    where
        F: FnOnce() -> V,
        V: Into<AttributeValue>,
    {
        if !self.0.contains_key(key) {
            self.insert(key.to_owned(), value());
        }
    }

    /// Checks whether this collection contains an attribute with the given key.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        String: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0.contains_key(key)
    }

    /// Removes an attribute from this collection.
    pub fn remove<Q>(&mut self, key: &Q) -> Option<Annotated<Attribute>>
    where
        String: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0.remove(key)
    }
}

impl IntoIterator for Attributes {
    type Item = (String, Annotated<Attribute>);

    type IntoIter = std::collections::btree_map::IntoIter<String, Annotated<Attribute>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl FromIterator<(String, Annotated<Attribute>)> for Attributes {
    fn from_iter<T: IntoIterator<Item = (String, Annotated<Attribute>)>>(iter: T) -> Self {
        Self(Object::from_iter(iter))
    }
}

impl<const N: usize> From<[(String, Annotated<Attribute>); N]> for Attributes {
    fn from(value: [(String, Annotated<Attribute>); N]) -> Self {
        value.into_iter().collect()
    }
}

// We need to manually implement `ProcessValue` for `Attributes`.
// Deriving it, even with `process_func`, causes both `process_value`
// and `process_child_values` to be called because it's a newtype struct.
impl ProcessValue for Attributes {
    #[inline]
    fn value_type(&self) -> EnumSet<ValueType> {
        EnumSet::only(ValueType::Object)
    }

    #[inline]
    fn process_value<P>(
        &mut self,
        meta: &mut Meta,
        processor: &mut P,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        P: Processor,
    {
        processor.process_attributes(self, meta, state)
    }

    fn process_child_values<P>(
        &mut self,
        processor: &mut P,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        P: Processor,
    {
        let enter_state = state.enter_nothing(Some(Cow::Borrowed(state.attrs())));
        self.0.process_child_values(processor, &enter_state)
    }
}

/// Calculates the canonical size of [`Attributes`].
///
/// Simple data types have a fixed size assigned to them:
///  - Boolean: 1 byte
///  - Integer: 8 byte
///  - Double: 8 Byte
///  - Strings are counted by their byte (UTF-8 encoded) representation.
///
/// Complex types like objects and arrays are counted as the sum of all contained simple data types.
///
/// The size of all attributes is the sum of all attribute values and their keys.
pub fn attributes_size(attributes: &Attributes) -> usize {
    attributes
        .0
        .iter()
        .map(|(k, v)| k.len() + attribute_size(v))
        .sum()
}

/// Calculates the size of a single attribute.
///
/// As described in [`attributes_size`], only the value of the attribute is considered for the size
/// of an attribute.
pub fn attribute_size(v: &Annotated<Attribute>) -> usize {
    v.value().map(|v| &v.value.value).map_or(0, value_size)
}

/// Recursively calculates the size of a [`Value`], using the rules described in [`attributes_size`].
pub fn value_size(v: &Annotated<Value>) -> usize {
    let Some(v) = v.value() else {
        return 0;
    };

    match v {
        Value::Bool(_) => 1,
        Value::I64(_) => 8,
        Value::U64(_) => 8,
        Value::F64(_) => 8,
        Value::String(v) => v.len(),
        Value::Array(v) => v.iter().map(value_size).sum(),
        Value::Object(v) => v.iter().map(|(k, v)| k.len() + value_size(v)).sum(),
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::{Error, Object};

    use super::*;

    #[test]
    fn test_value_size_basic() {
        assert_eq!(value_size(&Value::Bool(true).into()), 1);
        assert_eq!(value_size(&Value::Bool(false).into()), 1);
        assert_eq!(value_size(&Value::I64(0).into()), 8);
        assert_eq!(value_size(&Value::I64(i64::MIN).into()), 8);
        assert_eq!(value_size(&Value::I64(i64::MAX).into()), 8);
        assert_eq!(value_size(&Value::U64(0).into()), 8);
        assert_eq!(value_size(&Value::U64(u64::MIN).into()), 8);
        assert_eq!(value_size(&Value::U64(u64::MAX).into()), 8);
        assert_eq!(value_size(&Value::F64(123.42).into()), 8);
        assert_eq!(value_size(&Value::F64(f64::MAX).into()), 8);
        assert_eq!(value_size(&Value::F64(f64::MIN).into()), 8);
        assert_eq!(value_size(&Value::F64(f64::NAN).into()), 8);
        assert_eq!(value_size(&Value::F64(f64::NEG_INFINITY).into()), 8);
        assert_eq!(value_size(&Value::String("foobar".to_owned()).into()), 6);
        assert_eq!(value_size(&Value::String("ඞ".to_owned()).into()), 3);
        assert_eq!(value_size(&Value::String("".to_owned()).into()), 0);
    }

    #[test]
    fn test_value_size_array() {
        let array = Value::Array(vec![
            Annotated::empty(),
            Annotated::new(Value::Bool(true)),
            Annotated::new(Value::Bool(false)),
            Annotated::from_error(Error::invalid("oops"), Some(Value::U64(0))),
            Annotated::new(Value::String("42".to_owned())),
            Annotated::new(Value::Array(vec![])),
            Annotated::new(Value::Array(vec![
                Annotated::new(Value::Array(vec![Annotated::new(Value::Bool(false))])),
                Annotated::new(Value::Object(Object::from([
                    ("ඞ".to_owned(), Annotated::new(Value::I64(3))),
                    ("empty_key".to_owned(), Annotated::empty()),
                ]))),
                Annotated::empty(),
            ])),
        ]);

        assert_eq!(value_size(&array.into()), 25);
    }

    #[test]
    fn test_value_size_object() {
        let obj = Value::Object(Object::from([
            ("".to_owned(), Annotated::new(Value::Bool(false))),
            ("1".to_owned(), Annotated::new(Value::Bool(false))),
            ("ඞ".to_owned(), Annotated::empty()),
            (
                "key".to_owned(),
                Annotated::new(Value::Object(Object::from([
                    (
                        "foo".to_owned(),
                        Annotated::new(Value::Array(vec![
                            Annotated::new(Value::I64(21)),
                            Annotated::new(Value::F64(42.0)),
                        ])),
                    ),
                    ("bar".to_owned(), Annotated::empty()),
                ]))),
            ),
        ]));

        assert_eq!(value_size(&obj.into()), 31);
    }

    macro_rules! assert_calculated_size_of {
        ($expected:expr, $json:expr) => {{
            let json = $json;

            let attrs = Annotated::<Attributes>::from_json(json)
                .unwrap()
                .into_value()
                .unwrap();

            let size = attributes_size(&attrs);
            assert_eq!(size, $expected, "attrs: {json}");
        }};
    }

    #[test]
    fn test_attributes_size_full() {
        assert_calculated_size_of!(
            82,
            r#"{
            "k1": {
                "value": "string value",
                "type": "string"
            },
            "k2": {
                "value": 18446744073709551615,
                "type": "integer"
            },
            "k3": {
                "value": 42.01234567891234567899,
                "type": "double"
            },
            "k4": {
                "value": false,
                "type": "boolean"
            },
            "k5": {
                "value": {
                    "nested": {
                        "array": [1.0, 2, -12, "7 bytes", false]
                    }
                },
                "type": "not yet supported"
            }
        }"#
        );
    }
}
