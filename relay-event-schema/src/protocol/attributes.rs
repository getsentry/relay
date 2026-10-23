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

    pub fn get_value_with_deprecations(&self, key: &str) -> Option<&Value> {
        self.get_attribute_with_deprecations(key)?
            .value
            .value
            .value()
    }

    /// Returns the attribute with the given key.
    pub fn get_attribute<Q>(&self, key: &Q) -> Option<&Attribute>
    where
        String: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0.get(key)?.value()
    }

    pub fn get_attribute_with_deprecations(&self, key: &str) -> Option<&Attribute> {
        relay_conventions::attribute_names(key).find_map(|name| {
            self.get_attribute(name.as_ref())
                .filter(|attribute| attribute.value.value.value().is_some())
        })
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

#[cfg(test)]
mod deprecation_tests {
    use super::*;
    use crate::protocol::SpanData;

    fn assert_lookup(pairs: &[(&str, &str)], key: &str, expected: Option<&str>) {
        let attributes = Attributes::from_iter(
            pairs
                .iter()
                .map(|(key, value)| ((*key).to_owned(), Annotated::new(Attribute::from(*value)))),
        );
        let data = SpanData::from(Object::from_iter(pairs.iter().map(|(key, value)| {
            (
                (*key).to_owned(),
                Annotated::new(Value::String((*value).to_owned())),
            )
        })));
        let original_attributes = attributes.clone();
        let original_data = data.clone();
        assert_eq!(
            attributes
                .get_value_with_deprecations(key)
                .and_then(Value::as_str),
            expected
        );
        assert_eq!(
            data.get_value_with_deprecations(key)
                .and_then(Value::as_str),
            expected
        );
        assert_eq!(
            attributes
                .get_attribute_with_deprecations(key)
                .and_then(|attribute| attribute.value.value.value())
                .and_then(Value::as_str),
            expected
        );
        assert_eq!(attributes, original_attributes);
        assert_eq!(data, original_data);
    }

    #[test]
    fn lookup_prefers_current_then_alphabetical_predecessors() {
        let pairs = [
            ("http.request.method", "current"),
            ("http.method", "first"),
            ("http.request_method", "second"),
            ("method", "third"),
        ];
        for offset in 0..pairs.len() {
            for (key, _) in pairs {
                assert_lookup(&pairs[offset..], key, Some(pairs[offset].1));
            }
        }
        assert_lookup(&[], "http.request.method", None);
    }

    #[test]
    fn lookup_includes_compatible_no_status_deprecations() {
        assert_lookup(&[("route", "/users/:id")], "http.route", Some("/users/:id"));
    }

    #[test]
    fn lookup_excludes_transforms_and_incompatible_links() {
        for (old, current) in [
            ("gen_ai.request.messages", "gen_ai.input.messages"),
            ("gen_ai.response.text", "gen_ai.output.messages"),
            ("hardwareConcurrency", "device.processor_count"),
            ("db.params", "db.query.parameter.id"),
            ("db.sql.bindings", "db.query.parameter.id"),
            ("query.id", "url.query"),
        ] {
            assert_lookup(&[(old, "old")], current, None);
            assert_lookup(&[(current, "current")], old, None);
            assert_lookup(&[(old, "old")], old, Some("old"));
        }
    }

    #[test]
    fn lookup_handles_dynamic_and_unknown_names() {
        assert_lookup(
            &[("url.path.params.id", "123")],
            "url.path.parameter.id",
            Some("123"),
        );
        assert_lookup(
            &[("custom.attribute", "custom")],
            "custom.attribute",
            Some("custom"),
        );
        assert_lookup(&[], "custom.attribute", None);
    }

    #[test]
    fn lookup_does_not_skip_present_empty_values() {
        assert_lookup(
            &[("http.request.method", ""), ("http.method", "POST")],
            "method",
            Some(""),
        );
    }

    #[test]
    fn lookup_skips_missing_attributes_and_values() {
        let mut attributes = Attributes::from([
            ("http.request.method".to_owned(), Annotated::empty()),
            (
                "http.method".to_owned(),
                Annotated::new(Attribute::from(Annotated::<String>::empty())),
            ),
            ("method".to_owned(), Annotated::new(Attribute::from("POST"))),
        ]);
        let data = SpanData::from(Object::from([
            ("http.request.method".to_owned(), Annotated::empty()),
            ("http.method".to_owned(), Annotated::empty()),
            (
                "method".to_owned(),
                Annotated::new(Value::String("POST".to_owned())),
            ),
        ]));
        assert_eq!(
            attributes
                .get_value_with_deprecations("http.request.method")
                .and_then(Value::as_str),
            Some("POST")
        );
        assert_eq!(
            data.get_value_with_deprecations("http.request.method")
                .and_then(Value::as_str),
            Some("POST")
        );
        assert_eq!(
            attributes.get_attribute_with_deprecations("http.request.method"),
            attributes.get_attribute("method"),
        );
        attributes.0.insert(
            "http.request.method".to_owned(),
            Annotated::new(Attribute::from(false)),
        );
        assert_eq!(
            attributes.get_value_with_deprecations("method"),
            Some(&Value::Bool(false))
        );
    }
}
