use std::collections::BTreeMap;

use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_derive::Serialize;
use serde_json;

use crate::types::{FromValue, Meta, Object, ToValue, Value};

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
                    }).unwrap_or_default(),
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
        if !self.meta.has_errors() {
            return true;
        }
        for (_, value) in self.children.iter() {
            if !value.has_errors() {
                return true;
            }
        }
        false
    }

    /// Checks if the tree is empty.
    pub fn is_empty(&self) -> bool {
        if !self.meta.is_empty() {
            return false;
        }
        for (_, value) in self.children.iter() {
            if !value.is_empty() {
                return false;
            }
        }
        true
    }
}

/// Meta for children.
pub type MetaMap = BTreeMap<String, MetaTree>;

/// Used to indicate how to handle an annotated value in a callback.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ValueAction {
    /// Keeps the value as is.
    Keep,

    /// Discards the value entirely.
    DeleteHard,

    /// Discards the value and moves it into meta's `original_value`.
    DeleteSoft,
}

impl ValueAction {
    /// Returns the result of `f` if the current action is `ValueAction::Keep`.
    #[inline]
    pub fn and_then<F>(self, mut f: F) -> Self
    where
        F: FnMut() -> Self,
    {
        match self {
            ValueAction::Keep => f(),
            ValueAction::DeleteHard | ValueAction::DeleteSoft => self,
        }
    }
}

impl Default for ValueAction {
    #[inline]
    fn default() -> Self {
        ValueAction::Keep
    }
}

impl From<()> for ValueAction {
    fn from(_: ()) -> Self {
        ValueAction::Keep
    }
}

impl From<bool> for ValueAction {
    fn from(b: bool) -> Self {
        if b {
            ValueAction::Keep
        } else {
            ValueAction::DeleteHard
        }
    }
}

/// Wrapper for data fields with optional meta data.
#[derive(Debug, PartialEq, Clone)]
pub struct Annotated<T>(pub Option<T>, pub Meta);

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

    /// From an error
    pub fn from_error<S: Into<String>>(err: S, value: Option<Value>) -> Self {
        Annotated(None, {
            let mut meta = Meta::default();
            meta.add_error(err);
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
        self.0.get_or_insert_with(f)
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

impl<'de, T: FromValue> Annotated<T> {
    /// Deserializes an annotated from a deserializer
    pub fn deserialize_with_meta<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(FromValue::from_value(
            match Value::deserialize(deserializer)? {
                Value::Object(mut map) => {
                    let meta_tree = map
                        .remove("_meta")
                        .map(MetaTree::from_value)
                        .unwrap_or_default();

                    let mut value: Annotated<Value> = Annotated::new(Value::Object(map));
                    value.attach_meta_tree(meta_tree);
                    value
                }
                value => Annotated::new(value),
            },
        ))
    }

    /// Deserializes an annotated from a JSON string.
    pub fn from_json(s: &'de str) -> Result<Self, serde_json::Error> {
        Self::deserialize_with_meta(&mut serde_json::Deserializer::from_str(s))
    }

    /// Deserializes an annotated from JSON bytes.
    pub fn from_json_bytes(b: &'de [u8]) -> Result<Self, serde_json::Error> {
        Self::deserialize_with_meta(&mut serde_json::Deserializer::from_slice(b))
    }
}

impl<T: ToValue> Annotated<T> {
    /// Serializes an annotated value into a serializer.
    pub fn serialize_with_meta<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map_ser = serializer.serialize_map(None)?;
        let meta_tree = ToValue::extract_meta_tree(self);

        if let Some(ref value) = self.0 {
            use serde::private::ser::FlatMapSerializer;
            ToValue::serialize_payload(value, FlatMapSerializer(&mut map_ser))?;
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

        match self.0 {
            Some(ref value) => ToValue::serialize_payload(value, &mut ser)?,
            None => ser.serialize_unit()?,
        }

        Ok(unsafe { String::from_utf8_unchecked(ser.into_inner()) })
    }

    /// Serializes an annotated value into a pretty JSON string.
    pub fn payload_to_json_pretty(&self) -> Result<String, serde_json::Error> {
        let mut ser = serde_json::Serializer::pretty(Vec::with_capacity(128));

        match self.0 {
            Some(ref value) => ToValue::serialize_payload(value, &mut ser)?,
            None => ser.serialize_unit()?,
        }

        Ok(unsafe { String::from_utf8_unchecked(ser.into_inner()) })
    }

    /// Checks if this value can be skipped upon serialization.
    pub fn skip_serialization(&self) -> bool {
        if !self.1.is_empty() {
            return false;
        }

        if let Some(ref value) = self.0 {
            value.skip_serialization()
        } else {
            true
        }
    }

    /// Modifies this value based on the action returned by `f`.
    #[inline]
    pub fn apply<F, R>(&mut self, f: F)
    where
        F: FnOnce(&mut T, &mut Meta) -> R,
        R: Into<ValueAction>,
    {
        let result = match (self.0.as_mut(), &mut self.1) {
            (Some(value), meta) => f(value, meta).into(),
            (None, _) => Default::default(),
        };

        match result {
            ValueAction::DeleteHard => self.0 = None,
            ValueAction::Keep => (),
            ValueAction::DeleteSoft => {
                self.1
                    .set_original_value(self.0.take().map(ToValue::to_value));
            }
        }
    }
}

impl Annotated<Value> {
    pub fn into_object(self) -> Annotated<Object<Value>> {
        match self {
            Annotated(Some(Value::Object(object)), meta) => Annotated(Some(object), meta),
            Annotated(Some(_), mut meta) => {
                meta.add_error("expected object");
                Annotated(None, meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
        }
    }

    fn attach_meta_tree(&mut self, mut meta_tree: MetaTree) {
        match self.0 {
            Some(Value::Array(ref mut items)) => {
                for (idx, item) in items.iter_mut().enumerate() {
                    if let Some(meta_tree) = meta_tree.children.remove(&idx.to_string()) {
                        item.attach_meta_tree(meta_tree);
                    }
                }
            }
            Some(Value::Object(ref mut items)) => {
                for (key, value) in items.iter_mut() {
                    if let Some(meta_tree) = meta_tree.children.remove(key) {
                        value.attach_meta_tree(meta_tree);
                    }
                }
            }
            _ => {}
        }
        self.1 = meta_tree.meta;
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

impl<T: Serialize> Serialize for Annotated<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Serialize::serialize(&self.0, serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Annotated<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Annotated::new(Deserialize::deserialize(deserializer)?))
    }
}

#[test]
fn test_annotated_deserialize_with_meta() {
    #[derive(ToValue, FromValue, Debug)]
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
                        "err": ["invalid id"]
                    }
                },
                "type": {
                    "": {
                        "err": ["invalid type"]
                    }
                }
            }
        }
    "#,
    ).unwrap();

    assert_eq!(annotated_value.0.as_ref().unwrap().id.0, None);
    assert_eq!(
        annotated_value
            .0
            .as_ref()
            .unwrap()
            .id
            .1
            .iter_errors()
            .collect::<Vec<_>>(),
        vec!["invalid id", "expected an unsigned integer"]
    );
    assert_eq!(
        annotated_value.0.as_ref().unwrap().ty.0,
        Some("testing".into())
    );
    assert_eq!(
        annotated_value
            .0
            .as_ref()
            .unwrap()
            .ty
            .1
            .iter_errors()
            .collect::<Vec<_>>(),
        vec!["invalid type"]
    );

    let json = annotated_value.to_json().unwrap();
    assert_eq_str!(json, r#"{"id":null,"type":"testing","_meta":{"id":{"":{"err":["invalid id","expected an unsigned integer"],"val":"blaflasel"}},"type":{"":{"err":["invalid type"]}}}}"#);
}
