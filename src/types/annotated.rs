use std::collections::BTreeMap;

use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_derive::Serialize;
use serde_json;

use crate::processor::{
    join_chunks, split_chunks, Chunk, FromValue, MaxChars, ProcessValue, ProcessingState,
    Processor, SizeEstimatingSerializer, ToValue,
};
use crate::types::{Meta, Object, RemarkType, Value};

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
    fn from_json_value(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Object(mut map) => MetaTree {
                meta: map
                    .remove("")
                    .and_then(|value| serde_json::from_value(value).ok())
                    .unwrap_or_default(),
                children: map
                    .into_iter()
                    .map(|(k, v)| (k, MetaTree::from_json_value(v)))
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

/// Wrapper for data fields with optional meta data.
#[derive(Debug, PartialEq, Clone)]
pub struct Annotated<T>(pub Option<T>, pub Meta);

pub trait IntoAnnotated<T> {
    fn into_annotated(self) -> Annotated<T>;
}

impl<T> IntoAnnotated<T> for Annotated<T> {
    fn into_annotated(self) -> Annotated<T> {
        self
    }
}

impl<T> IntoAnnotated<T> for T {
    fn into_annotated(self) -> Annotated<T> {
        Annotated::new(self)
    }
}

impl<T> IntoAnnotated<T> for Option<T> {
    fn into_annotated(self) -> Annotated<T> {
        Annotated(self, Meta::default())
    }
}

impl<T, E> IntoAnnotated<T> for Result<T, E>
where
    E: Into<String>,
{
    fn into_annotated(self) -> Annotated<T> {
        match self {
            Ok(value) => Annotated::new(value),
            Err(err) => Annotated::from_error(err, None),
        }
    }
}

impl<T> IntoAnnotated<T> for (T, Meta) {
    fn into_annotated(self) -> Annotated<T> {
        Annotated(Some(self.0), self.1)
    }
}

impl<T> Annotated<T> {
    /// Creates a new annotated value without meta data.
    pub fn new(value: T) -> Annotated<T> {
        Annotated(Some(value), Meta::default())
    }

    /// Creates an empty annotated value without meta data.
    pub fn empty() -> Annotated<T> {
        Annotated(None, Meta::default())
    }

    /// From an error
    pub fn from_error<S: Into<String>>(err: S, value: Option<Value>) -> Annotated<T> {
        let mut rv = Annotated::empty();
        rv.1.add_error(err, value);
        rv
    }

    /// Attaches a value required error if the value is missing.
    pub fn require_value(&mut self) {
        if self.0.is_none() && !self.1.has_errors() {
            self.1.add_error("value required", None);
        }
    }

    pub fn value(&self) -> Option<&T> {
        self.0.as_ref()
    }

    pub fn is_valid(&self) -> bool {
        !self.1.has_errors()
    }

    pub fn is_present(&self) -> bool {
        self.0.is_some()
    }

    pub fn modify<F>(&mut self, f: F)
    where
        F: FnOnce(Self) -> Self,
    {
        *self = f(std::mem::replace(self, Annotated::empty()));
    }

    pub fn and_then<F, U, R>(self, f: F) -> Annotated<U>
    where
        F: FnOnce(T) -> R,
        R: IntoAnnotated<U>,
    {
        if let Some(value) = self.0 {
            let Annotated(value, meta) = f(value).into_annotated();
            Annotated(value, self.1.merge(meta))
        } else {
            Annotated(None, self.1)
        }
    }

    pub fn or_else<F, R>(self, f: F) -> Self
    where
        F: FnOnce() -> R,
        R: IntoAnnotated<T>,
    {
        if self.0.is_none() {
            let Annotated(value, meta) = f().into_annotated();
            Annotated(value, self.1.merge(meta))
        } else {
            self
        }
    }

    pub fn filter_map<P, F, R>(self, predicate: P, f: F) -> Self
    where
        P: FnOnce(&Annotated<T>) -> bool,
        F: FnOnce(T) -> R,
        R: IntoAnnotated<T>,
    {
        if predicate(&self) {
            self.and_then(f)
        } else {
            self
        }
    }

    /// Maps an `Annotated<T>` to an `Annotated<U>` and keeps the original meta data.
    pub fn map_value<U, F>(self, f: F) -> Annotated<U>
    where
        F: FnOnce(T) -> U,
    {
        Annotated(self.0.map(f), self.1)
    }

    /// Replaces the value option of an `Annotated<T>` with the result of `f` if the value is not
    /// `None` and returns a new `Annotated<U>`.
    pub fn and_then_value<U, F>(self, f: F) -> Annotated<U>
    where
        F: FnOnce(T) -> Option<U>,
    {
        Annotated(self.0.and_then(f), self.1)
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

impl<T: ProcessValue> Annotated<T> {
    /// Estimates the size in bytes this would be in JSON.
    pub fn process<P: Processor>(self, processor: &mut P) -> Annotated<T> {
        ProcessValue::process_value(self, processor, ProcessingState::root())
    }
}

impl<T: ToValue> Annotated<T> {
    /// Estimates the size in bytes this would be in JSON.
    pub fn estimate_size(&self) -> usize {
        let mut ser = SizeEstimatingSerializer::new();
        if let Some(ref value) = self.0 {
            ToValue::serialize_payload(value, &mut ser).unwrap();
        }
        ser.size()
    }
}

impl<'de, T: FromValue> Annotated<T> {
    /// Deserializes an annotated from a deserializer
    pub fn deserialize_with_meta<D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Annotated<T>, D::Error> {
        let value = match serde_json::Value::deserialize(deserializer)? {
            serde_json::Value::Object(mut map) => {
                let meta_tree = map
                    .remove("_meta")
                    .map(MetaTree::from_json_value)
                    .unwrap_or_default();

                let mut value: Annotated<Value> = serde_json::Value::Object(map).into();
                value.attach_meta_tree(meta_tree);
                value
            }
            value => value.into(),
        };

        Ok(FromValue::from_value(value))
    }

    /// Deserializes an annotated from a JSON string.
    pub fn from_json(s: &'de str) -> Result<Annotated<T>, serde_json::Error> {
        Self::deserialize_with_meta(&mut serde_json::Deserializer::from_str(s))
    }

    /// Deserializes an annotated from JSON bytes.
    pub fn from_json_bytes(b: &'de [u8]) -> Result<Annotated<T>, serde_json::Error> {
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
        self.0.is_none() && self.1.is_empty()
    }
}

impl Annotated<Value> {
    pub fn into_object(self) -> Annotated<Object<Value>> {
        match self {
            Annotated(Some(Value::Object(object)), meta) => Annotated(Some(object), meta),
            Annotated(Some(_), mut meta) => {
                meta.add_error("expected object", None);
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

impl Annotated<String> {
    pub fn map_value_chunked<F>(self, f: F) -> Annotated<String>
    where
        F: FnOnce(Vec<Chunk>) -> Vec<Chunk>,
    {
        let Annotated(old_value, mut meta) = self;
        let new_value = old_value.map(|value| {
            let old_chunks = split_chunks(&value, meta.iter_remarks());
            let new_chunks = f(old_chunks);
            let (new_value, remarks) = join_chunks(new_chunks);
            *meta.remarks_mut() = remarks.into_iter().collect();
            if new_value != value {
                meta.set_original_length(Some(value.chars().count() as u32));
            }
            new_value
        });
        Annotated(new_value, meta)
    }

    pub fn trim_string(self, max_chars: MaxChars) -> Annotated<String> {
        let limit = max_chars.limit();
        let allowance_limit = limit + max_chars.allowance();

        if self.0.is_none() || self.0.as_ref().unwrap().chars().count() < allowance_limit {
            return self;
        }

        // otherwise we trim down to max chars
        self.map_value_chunked(|chunks| {
            let mut length = 0;
            let mut rv = vec![];

            for chunk in chunks {
                let chunk_chars = chunk.chars();

                // if the entire chunk fits, just put it in
                if length + chunk_chars < limit {
                    rv.push(chunk);
                    length += chunk_chars;
                    continue;
                }

                match chunk {
                    // if there is enough space for this chunk and the 3 character
                    // ellipsis marker we can push the remaining chunk
                    Chunk::Redaction { .. } => {
                        if length + chunk_chars + 3 < allowance_limit {
                            rv.push(chunk);
                        }
                    }

                    // if this is a text chunk, we can put the remaining characters in.
                    Chunk::Text { text } => {
                        let mut remaining = String::new();
                        for c in text.chars() {
                            if length < limit - 3 {
                                remaining.push(c);
                            } else {
                                break;
                            }
                            length += 1;
                        }
                        rv.push(Chunk::Text { text: remaining });
                    }
                }

                rv.push(Chunk::Redaction {
                    text: "...".to_string(),
                    rule_id: "!limit".to_string(),
                    ty: RemarkType::Substituted,
                });
                break;
            }

            rv
        })
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

/// Utility trait to find out if an object is empty.
pub trait IsEmpty {
    /// A generic check if the object is considered empty.
    fn generic_is_empty(&self) -> bool;
}

impl<T> IsEmpty for Vec<T> {
    fn generic_is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl IsEmpty for String {
    fn generic_is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<T: IsEmpty> Annotated<T> {
    /// Attaches a value required error if the value is null or an empty string
    pub fn require_nonempty_value(&mut self) {
        if self.1.has_errors() {
            return;
        }

        if self
            .0
            .as_ref()
            .map(|x| x.generic_is_empty())
            .unwrap_or(true)
        {
            self.0 = None;
            self.1.add_error("non-empty value required", None);
        }
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

#[test]
fn test_estimate_size() {
    let json = r#"{"a":["Hello","World","aha","hmm",false,{"blub":42,"x":true},null]}"#;
    let value = Annotated::<Object<Value>>::from_json(json).unwrap();
    assert_eq!(value.estimate_size(), json.len());
}
