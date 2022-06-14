use std::fmt;

use failure::Fail;

#[cfg(feature = "jsonschema")]
use schemars::gen::SchemaGenerator;
#[cfg(feature = "jsonschema")]
use schemars::schema::Schema;

use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::types::{Empty, Error, FromValue, IntoValue, Map, Meta, SkipSerialization, Value};

/// Represents a tree of meta objects.
#[derive(Default, Debug, Serialize)]
pub struct MetaTree {
    /// The node's meta data
    #[serde(rename = "", skip_serializing_if = "Meta::is_empty")]
    pub meta: Meta,

    /// References to the children.
    #[serde(flatten)]
    pub children: MetaMap,
}

impl MetaTree {
    fn from_value(value: Annotated<Value>) -> Self {
        match value {
            Annotated(Some(Value::Object(mut map)), _) => MetaTree {
                meta: map
                    .remove("")
                    .and_then(|value| {
                        // this is not the fastest operation because we cannot currently
                        // deserialize stright from a `Value`.  However since we expect
                        // to handle very few meta data initially this is okay enough
                        let value: serde_json::Value = value.into();
                        serde_json::from_value(value).ok()
                    })
                    .unwrap_or_default(),
                children: map
                    .into_iter()
                    .map(|(k, v)| (k, MetaTree::from_value(v)))
                    .collect(),
            },
            _ => MetaTree::default(),
        }
    }

    /// Checks if the tree has any errors.
    pub fn has_errors(&self) -> bool {
        self.meta.has_errors() || self.children.values().any(MetaTree::has_errors)
    }

    /// Checks if the tree is empty.
    pub fn is_empty(&self) -> bool {
        self.meta.is_empty() && self.children.values().all(MetaTree::is_empty)
    }
}

/// Meta for children.
pub type MetaMap = Map<String, MetaTree>;

pub type ProcessingResult = Result<(), ProcessingAction>;

/// Used to indicate how to handle an annotated value in a callback.
#[must_use = "This `ProcessingAction` must be handled by `Annotated::apply`"]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Fail)]
pub enum ProcessingAction {
    /// Discards the value entirely.
    #[fail(display = "value should be hard-deleted (unreachable, should not surface as error!)")]
    DeleteValueHard,

    /// Discards the value and moves it into meta's `original_value`.
    #[fail(display = "value should be hard-deleted (unreachable, should not surface as error!)")]
    DeleteValueSoft,

    /// The event is invalid (needs to bubble up)
    #[fail(display = "invalid transaction event: {}", _0)]
    InvalidTransaction(&'static str),
}

/// Wrapper for data fields with optional meta data.
#[derive(Clone, PartialEq)]
pub struct Annotated<T>(pub Option<T>, pub Meta);

/// An utility to serialize annotated objects with payload.
pub struct SerializableAnnotated<'a, T>(pub &'a Annotated<T>);

impl<'a, T: IntoValue> Serialize for SerializableAnnotated<'a, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize_with_meta(serializer)
    }
}

impl<T: fmt::Debug> fmt::Debug for Annotated<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Annotated(Some(ref value), ref meta) => {
                if meta.is_empty() {
                    fmt::Debug::fmt(value, f)
                } else {
                    f.debug_tuple("Annotated").field(value).field(meta).finish()
                }
            }
            Annotated(None, ref meta) => {
                if meta.is_empty() {
                    f.pad("~")
                } else {
                    fmt::Debug::fmt(meta, f)
                }
            }
        }
    }
}

impl<T> Annotated<T> {
    /// Creates a new annotated value without meta data.
    #[inline]
    pub fn new(value: T) -> Self {
        Annotated(Some(value), Meta::default())
    }

    /// Creates an empty annotated value without meta data.
    #[inline]
    pub fn empty() -> Self {
        Annotated(None, Meta::default())
    }

    // Creates an empty annotated value with error attached.
    pub fn from_error<E>(err: E, value: Option<Value>) -> Self
    where
        E: Into<Error>,
    {
        Annotated(None, {
            let mut meta = Meta::from_error(err);
            meta.set_original_value(value);
            meta
        })
    }

    #[inline]
    pub fn value(&self) -> Option<&T> {
        self.0.as_ref()
    }

    #[inline]
    pub fn value_mut(&mut self) -> &mut Option<T> {
        &mut self.0
    }

    #[inline]
    pub fn set_value(&mut self, value: Option<T>) {
        self.0 = value;
    }

    #[inline]
    pub fn meta(&self) -> &Meta {
        &self.1
    }

    #[inline]
    pub fn meta_mut(&mut self) -> &mut Meta {
        &mut self.1
    }

    #[inline]
    pub fn into_value(self) -> Option<T> {
        self.0
    }

    pub fn and_then<F, U, R>(self, f: F) -> Annotated<U>
    where
        F: FnOnce(T) -> R,
        R: Into<Annotated<U>>,
    {
        if let Some(value) = self.0 {
            let Annotated(value, meta) = f(value).into();
            Annotated(value, self.1.merge(meta))
        } else {
            Annotated(None, self.1)
        }
    }

    /// Maps an `Annotated<T>` to an `Annotated<U>` and keeps the original meta data.
    pub fn map_value<U, F>(self, f: F) -> Annotated<U>
    where
        F: FnOnce(T) -> U,
    {
        Annotated(self.0.map(f), self.1)
    }

    /// Inserts a value computed from `f` into the value if it is `None`, then returns a mutable
    /// reference to the contained value.
    pub fn get_or_insert_with<F>(&mut self, f: F) -> &mut T
    where
        F: FnOnce() -> T,
    {
        self.value_mut().get_or_insert_with(f)
    }
}

impl<T> Annotated<T>
where
    T: AsRef<str>,
{
    /// Returns a reference to the string value if set.
    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        self.value().map(AsRef::as_ref)
    }
}

impl Annotated<Value> {
    /// Returns a reference to the string value if this value is a string and it is set.
    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        self.value().and_then(Value::as_str)
    }
}

impl<T> Annotated<T>
where
    T: Empty,
{
    /// Returns whether this value should be skipped during serialization.
    ///
    /// An `Annotated<T>` is always serialized if it has meta data. Otherwise, serialization
    /// depends on the behavior. For `SkipSerialization::Empty`, the `Empty` trait is used to
    /// determine emptiness of the contained value and defaults to `false` for no value.
    pub fn skip_serialization(&self, behavior: SkipSerialization) -> bool {
        if !self.meta().is_empty() {
            return false;
        }

        match behavior {
            SkipSerialization::Never => false,
            SkipSerialization::Null(_) => self.value().is_none(),
            SkipSerialization::Empty(false) => self.value().map_or(true, Empty::is_empty),
            SkipSerialization::Empty(true) => self.value().map_or(true, Empty::is_deep_empty),
        }
    }
}

impl<T> Annotated<T>
where
    T: FromValue,
{
    /// Deserializes an annotated from a deserializer
    pub fn deserialize_with_meta<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        Ok(FromValue::from_value(
            match Option::<Value>::deserialize(deserializer)? {
                Some(Value::Object(mut map)) => {
                    let meta_tree = map
                        .remove("_meta")
                        .map(MetaTree::from_value)
                        .unwrap_or_default();

                    let mut value: Annotated<Value> = Annotated::new(Value::Object(map));
                    value.attach_meta_tree(meta_tree);
                    value
                }
                other => Annotated::from(other),
            },
        ))
    }

    /// Deserializes an annotated from a JSON string.
    pub fn from_json(s: &str) -> Result<Self, serde_json::Error> {
        Self::deserialize_with_meta(&mut serde_json::Deserializer::from_str(s))
    }

    /// Deserializes an annotated from JSON bytes.
    pub fn from_json_bytes(b: &[u8]) -> Result<Self, serde_json::Error> {
        Self::deserialize_with_meta(&mut serde_json::Deserializer::from_slice(b))
    }
}

impl<T> Annotated<T>
where
    T: IntoValue,
{
    /// Serializes an annotated value into a serializer.
    pub fn serialize_with_meta<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map_ser = serializer.serialize_map(None)?;
        let meta_tree = IntoValue::extract_meta_tree(self);

        if let Some(value) = self.value() {
            // NOTE: This is a hack and known to be instable use of serde.
            use serde::__private::ser::FlatMapSerializer;
            IntoValue::serialize_payload(
                value,
                FlatMapSerializer(&mut map_ser),
                SkipSerialization::default(),
            )?;
        }

        if !meta_tree.is_empty() {
            map_ser.serialize_key("_meta")?;
            map_ser.serialize_value(&meta_tree)?;
        }

        map_ser.end()
    }

    /// Serializes an annotated value into a JSON string.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        let mut ser = serde_json::Serializer::new(Vec::with_capacity(128));
        self.serialize_with_meta(&mut ser)?;
        Ok(unsafe { String::from_utf8_unchecked(ser.into_inner()) })
    }

    /// Serializes an annotated value into a pretty JSON string.
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        let mut ser = serde_json::Serializer::pretty(Vec::with_capacity(128));
        self.serialize_with_meta(&mut ser)?;
        Ok(unsafe { String::from_utf8_unchecked(ser.into_inner()) })
    }

    /// Serializes an annotated value into a JSON string.
    pub fn payload_to_json(&self) -> Result<String, serde_json::Error> {
        let mut ser = serde_json::Serializer::new(Vec::with_capacity(128));

        match self.value() {
            Some(value) => {
                IntoValue::serialize_payload(value, &mut ser, SkipSerialization::default())?
            }
            None => ser.serialize_unit()?,
        }

        Ok(unsafe { String::from_utf8_unchecked(ser.into_inner()) })
    }

    /// Serializes an annotated value into a pretty JSON string.
    pub fn payload_to_json_pretty(&self) -> Result<String, serde_json::Error> {
        let mut ser = serde_json::Serializer::pretty(Vec::with_capacity(128));

        match self.value() {
            Some(value) => {
                IntoValue::serialize_payload(value, &mut ser, SkipSerialization::default())?
            }
            None => ser.serialize_unit()?,
        }

        Ok(unsafe { String::from_utf8_unchecked(ser.into_inner()) })
    }

    /// Modifies this value based on the action returned by `f`.
    #[inline]
    pub fn apply<F, R>(&mut self, f: F) -> ProcessingResult
    where
        F: FnOnce(&mut T, &mut Meta) -> R,
        R: Into<ProcessingResult>,
    {
        let result = match (self.0.as_mut(), &mut self.1) {
            (Some(value), meta) => f(value, meta).into(),
            (None, _) => Ok(()),
        };

        match result {
            Ok(()) => (),
            Err(ProcessingAction::DeleteValueHard) => self.0 = None,
            Err(ProcessingAction::DeleteValueSoft) => {
                self.1.set_original_value(self.0.take());
            }
            x @ Err(ProcessingAction::InvalidTransaction(_)) => return x,
        }

        Ok(())
    }
}

impl Annotated<Value> {
    fn attach_meta_tree(&mut self, mut meta_tree: MetaTree) {
        match self.value_mut() {
            Some(Value::Array(items)) => {
                for (idx, item) in items.iter_mut().enumerate() {
                    if let Some(meta_tree) = meta_tree.children.remove(&idx.to_string()) {
                        item.attach_meta_tree(meta_tree);
                    }
                }
            }
            Some(Value::Object(items)) => {
                for (key, value) in items.iter_mut() {
                    if let Some(meta_tree) = meta_tree.children.remove(key) {
                        value.attach_meta_tree(meta_tree);
                    }
                }
            }
            _ => {}
        }

        *self.meta_mut() = meta_tree.meta;
    }
}

impl<T> From<T> for Annotated<T> {
    fn from(t: T) -> Self {
        Annotated::new(t)
    }
}

impl<T> From<Option<T>> for Annotated<T> {
    fn from(option: Option<T>) -> Self {
        Annotated(option, Meta::default())
    }
}

impl<T> Default for Annotated<T> {
    fn default() -> Annotated<T> {
        Annotated::empty()
    }
}

// This hack is needed to make our custom derive for JsonSchema simpler. However, Serialize should
// not be implemented on Annotated as one should usually use ToValue directly, or
// SerializableAnnotated explicitly if really needed (eg: tests)
#[cfg(feature = "jsonschema")]
impl<T> schemars::JsonSchema for Annotated<T>
where
    T: schemars::JsonSchema,
{
    fn schema_name() -> String {
        format!("Annotated_{}", T::schema_name())
    }

    fn json_schema(gen: &mut SchemaGenerator) -> Schema {
        Option::<T>::json_schema(gen)
    }

    fn is_referenceable() -> bool {
        false
    }
}

#[test]
fn test_annotated_deserialize_with_meta() {
    use crate::types::ErrorKind;

    #[derive(Debug, Empty, FromValue, IntoValue)]
    struct Foo {
        id: Annotated<u64>,
        #[metastructure(field = "type")]
        ty: Annotated<String>,
    }

    let annotated_value = Annotated::<Foo>::from_json(
        r#"
        {
            "id": "blaflasel",
            "type": "testing",
            "_meta": {
                "id": {
                    "": {
                        "err": ["unknown_error"]
                    }
                },
                "type": {
                    "": {
                        "err": ["invalid_data"]
                    }
                }
            }
        }
    "#,
    )
    .unwrap();

    assert_eq!(annotated_value.value().unwrap().id.value(), None);
    assert_eq!(
        annotated_value
            .value()
            .unwrap()
            .id
            .meta()
            .iter_errors()
            .collect::<Vec<&Error>>(),
        vec![
            &Error::new(ErrorKind::Unknown("unknown_error".to_string())),
            &Error::expected("an unsigned integer")
        ],
    );
    assert_eq!(
        annotated_value.value().unwrap().ty.as_str(),
        Some("testing")
    );
    assert_eq!(
        annotated_value
            .value()
            .unwrap()
            .ty
            .meta()
            .iter_errors()
            .collect::<Vec<&Error>>(),
        vec![&Error::new(ErrorKind::InvalidData)],
    );

    let json = annotated_value.to_json_pretty().unwrap();
    assert_eq_str!(
        json,
        r#"{
  "id": null,
  "type": "testing",
  "_meta": {
    "id": {
      "": {
        "err": [
          "unknown_error",
          [
            "invalid_data",
            {
              "reason": "expected an unsigned integer"
            }
          ]
        ],
        "val": "blaflasel"
      }
    },
    "type": {
      "": {
        "err": [
          "invalid_data"
        ]
      }
    }
  }
}"#
    );
}
