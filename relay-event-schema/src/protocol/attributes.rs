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
    #[metastructure(additional_properties)]
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

    /// Stores `meta` information in the attribute value, unless it is [`None`].
    pub fn annotated_from_value(value: Annotated<Value>) -> Annotated<Self> {
        let Annotated(value, meta) = value;
        let Some(value) = value else {
            return Annotated(None, meta);
        };
        Annotated::new(Attribute {
            value: AttributeValue {
                ty: Annotated::new(AttributeType::derive_from_value(&value)),
                value: Annotated(Some(value), meta),
            },
            other: Default::default(),
        })
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

#[derive(Debug, Clone, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct AttributeValue {
    #[metastructure(field = "type", required = true, trim = false, pii = "false")]
    pub ty: Annotated<AttributeType>,
    #[metastructure(required = true, pii = "attribute_pii_from_conventions")]
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

        return match info.pii {
            relay_conventions::Pii::True => Pii::True,
            relay_conventions::Pii::False => Pii::False,
            relay_conventions::Pii::Maybe => Pii::Maybe,
        };
    }

    Pii::True
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttributeType {
    Boolean,
    Integer,
    Double,
    String,
    Array,
    Object,
    Unknown(String),
}

impl ProcessValue for AttributeType {}

impl AttributeType {
    pub fn derive_from_value(value: &Value) -> Self {
        match value {
            Value::Bool(_) => Self::Boolean,
            Value::I64(_) => Self::Integer,
            Value::U64(_) => Self::Integer,
            Value::F64(_) => Self::Double,
            Value::String(_) => Self::String,
            Value::Array(_) => Self::Array,
            Value::Object(_) => Self::Object,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Boolean => "boolean",
            Self::Integer => "integer",
            Self::Double => "double",
            Self::String => "string",
            Self::Array => "array",
            Self::Object => "object",
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
            "object" => Self::Object,
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

    /// Inserts an attribute with the given value into this collection.
    pub fn insert<K: Into<String>, V: Into<AttributeValue>>(&mut self, key: K, value: V) {
        fn inner(slf: &mut Attributes, key: String, value: AttributeValue) {
            let attribute = Annotated::new(Attribute {
                value,
                other: Default::default(),
            });
            slf.insert_raw(key, attribute);
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

    /// Inserts an annotated attribute into this collection.
    pub fn insert_raw(&mut self, key: String, attribute: Annotated<Attribute>) {
        self.0.insert(key, attribute);
    }

    /// Checks whether this collection contains an attribute with the given key.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        String: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0.contains_key(key)
    }

    /// Iterates over this collection's attribute keys and values.
    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, String, Annotated<Attribute>> {
        self.0.iter()
    }

    /// Iterates mutably over this collection's attribute keys and values.
    pub fn iter_mut(
        &mut self,
    ) -> std::collections::btree_map::IterMut<'_, String, Annotated<Attribute>> {
        self.0.iter_mut()
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
