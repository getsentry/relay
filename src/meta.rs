//! Event meta data.

use std::collections::BTreeMap;
use std::fmt;

use serde::de::{self, Deserialize, Deserializer, IgnoredAny};
use serde::private::ser::FlatMapSerializer;
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use serde_derive::{Deserialize, Serialize};
use serde_json;
use smallvec::SmallVec;

#[cfg(test)]
use general_derive::{FromValue, ToValue};

use crate::chunks;
use crate::processor::{CapSize, FromValue, ProcessValue, ProcessingState, Processor, ToValue};
use crate::types::{Array, Object};

pub use serde_json::Error;

/// The start (inclusive) and end (exclusive) indices of a `Remark`.
pub type Range = (usize, usize);

/// Gives an indication about the type of remark.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemarkType {
    /// The remark just annotates a value but the value did not change.
    #[serde(rename = "a")]
    Annotated,
    /// The original value was removed entirely.
    #[serde(rename = "x")]
    Removed,
    /// The original value was substituted by a replacement value.
    #[serde(rename = "s")]
    Substituted,
    /// The original value was masked.
    #[serde(rename = "m")]
    Masked,
    /// The original value was replaced through pseudonymization.
    #[serde(rename = "p")]
    Pseudonymized,
    /// The original value was encrypted (not implemented yet).
    #[serde(rename = "e")]
    Encrypted,
}

/// Information on a modified section in a string.
#[derive(Clone, Debug, PartialEq)]
pub struct Remark {
    ty: RemarkType,
    rule_id: String,
    range: Option<Range>,
}

impl Remark {
    /// Creates a new remark.
    pub fn new<S: Into<String>>(ty: RemarkType, rule_id: S) -> Self {
        Remark {
            rule_id: rule_id.into(),
            ty,
            range: None,
        }
    }

    /// Creates a new text remark with range indices.
    pub fn with_range<S: Into<String>>(ty: RemarkType, rule_id: S, range: Range) -> Self {
        Remark {
            rule_id: rule_id.into(),
            ty,
            range: Some(range),
        }
    }

    /// The note of this remark.
    pub fn rule_id(&self) -> &str {
        &self.rule_id
    }

    /// The range of this remark.
    pub fn range(&self) -> Option<&Range> {
        self.range.as_ref()
    }

    /// The length of this range.
    pub fn len(&self) -> Option<usize> {
        self.range.map(|r| r.1 - r.0)
    }

    /// Indicates if the remark refers to an empty range
    pub fn is_empty(&self) -> bool {
        self.len().map_or(false, |l| l == 0)
    }

    /// Returns the type.
    pub fn ty(&self) -> RemarkType {
        self.ty
    }
}

struct RemarkVisitor;

impl<'de> de::Visitor<'de> for RemarkVisitor {
    type Value = Remark;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a meta remark")
    }

    fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let rule_id = seq
            .next_element()?
            .ok_or_else(|| de::Error::custom("missing required rule-id"))?;
        let ty = seq
            .next_element()?
            .ok_or_else(|| de::Error::custom("missing required remark-type"))?;
        let start = seq.next_element()?;
        let end = seq.next_element()?;

        // Drain the sequence
        while let Some(IgnoredAny) = seq.next_element()? {}

        let range = match (start, end) {
            (Some(start), Some(end)) => Some((start, end)),
            _ => None,
        };

        Ok(Remark { ty, rule_id, range })
    }
}

impl<'de> Deserialize<'de> for Remark {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_seq(RemarkVisitor)
    }
}

impl Serialize for Remark {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(None)?;
        seq.serialize_element(self.rule_id())?;
        seq.serialize_element(&self.ty())?;
        if let Some(range) = self.range() {
            seq.serialize_element(&range.0)?;
            seq.serialize_element(&range.1)?;
        }
        seq.end()
    }
}

/// Meta information for a data field in the event payload.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Meta {
    /// Remarks detailling modifications of this field.
    #[serde(
        default,
        skip_serializing_if = "SmallVec::is_empty",
        rename = "rem"
    )]
    remarks: SmallVec<[Remark; 3]>,

    /// Errors that happened during deserialization or processing.
    #[serde(
        default,
        skip_serializing_if = "SmallVec::is_empty",
        rename = "err"
    )]
    errors: SmallVec<[String; 3]>,

    /// The original length of modified text fields or collections.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "len"
    )]
    original_length: Option<u32>,

    /// In some cases the original value might be sent along.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "val"
    )]
    original_value: Option<Value>,
}

impl PartialEq for Meta {
    fn eq(&self, other: &Self) -> bool {
        self.remarks == other.remarks
            && self.errors == other.errors
            && self.original_length == other.original_length
    }
}

impl Meta {
    /// The original length of this field, if applicable.
    pub fn original_length(&self) -> Option<usize> {
        self.original_length.map(|x| x as usize)
    }

    /// Updates the original length of this annotation.
    pub fn set_original_length(&mut self, original_length: Option<u32>) {
        self.original_length = original_length;
    }

    /// Iterates all remarks on this field.
    pub fn iter_remarks(&self) -> impl Iterator<Item = &Remark> {
        self.remarks.iter()
    }

    /// Indicates whether this field has remarks.
    pub fn has_remarks(&self) -> bool {
        !self.remarks.is_empty()
    }

    /// Iterates errors on this field.
    pub fn iter_errors(&self) -> impl Iterator<Item = &str> {
        self.errors.iter().map(|x| x.as_str())
    }

    /// Mutable reference to errors of this field.
    pub fn add_error<S: Into<String>>(&mut self, err: S, value: Option<Value>) {
        self.errors.push(err.into());
        if let Some(value) = value {
            self.original_value = Some(value);
        }
    }

    /// Adds an unexpected value error.
    pub fn add_unexpected_value_error(&mut self, expectation: &str, value: Value) {
        self.add_error(format!("expected {}", expectation), Some(value));
    }

    /// Take out the original value.
    pub fn take_original_value(&mut self) -> Option<Value> {
        self.original_value.take()
    }

    /// Indicates whether this field has errors.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Indicates whether this field has meta data attached.
    pub fn is_empty(&self) -> bool {
        self.original_length.is_none() && self.remarks.is_empty() && self.errors.is_empty()
    }

    /// Merges this meta with another one.
    pub fn merge(mut self, other: Meta) -> Meta {
        self.remarks.extend(other.remarks.into_iter());
        self.errors.extend(other.errors.into_iter());
        if self.original_length.is_none() {
            self.original_length = other.original_length;
        }
        if self.original_value.is_none() {
            self.original_value = other.original_value;
        }
        self
    }
}

impl<T> Default for Annotated<T> {
    fn default() -> Annotated<T> {
        Annotated::empty()
    }
}

impl Default for Meta {
    fn default() -> Meta {
        Meta {
            remarks: SmallVec::new(),
            errors: SmallVec::new(),
            original_length: None,
            original_value: None,
        }
    }
}

/// Wrapper for data fields with optional meta data.
#[derive(Debug, PartialEq, Clone)]
pub struct Annotated<T>(pub Option<T>, pub Meta);

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
    pub fn require_value(mut self) -> Annotated<T> {
        if self.0.is_none() && !self.1.has_errors() {
            self.1.add_error("value required", None);
        }
        self
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
    /// Processes an annotated value with a processor.
    pub fn process<P: Processor>(self, processor: &P) -> Self {
        ProcessValue::process_value(self, processor, ProcessingState::root())
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

impl<'de, T: FromValue> Annotated<T> {
    /// Deserializes an annotated from a deserializer
    pub fn deserialize_with_meta<D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Annotated<T>, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        Ok(FromValue::from_value(inline_meta_in_json(value)))
    }

    /// Deserializes an annotated from a JSON string.
    pub fn from_json(s: &'de str) -> Result<Annotated<T>, Error> {
        Self::deserialize_with_meta(&mut serde_json::Deserializer::from_str(s))
    }

    /// Deserializes an annotated from JSON bytes.
    pub fn from_json_bytes(b: &'de [u8]) -> Result<Annotated<T>, Error> {
        Self::deserialize_with_meta(&mut serde_json::Deserializer::from_slice(b))
    }
}

impl<T: ToValue> Annotated<T> {
    /// Serializes an annotated value into a serializer.
    pub fn serialize_with_meta<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map_ser = serializer.serialize_map(None)?;
        let meta_tree = ToValue::extract_meta_tree(self);
        if let Some(ref value) = self.0 {
            ToValue::serialize_payload(value, FlatMapSerializer(&mut map_ser))?;
        }
        if !meta_tree.is_empty() {
            map_ser.serialize_key("_meta")?;
            map_ser.serialize_value(&meta_tree)?;
        }
        map_ser.end()
    }

    /// Serializes an annotated value into a JSON string.
    pub fn to_json(&self) -> Result<String, Error> {
        let mut ser = serde_json::Serializer::new(Vec::with_capacity(128));
        self.serialize_with_meta(&mut ser)?;
        Ok(unsafe { String::from_utf8_unchecked(ser.into_inner()) })
    }

    /// Serializes an annotated value into a pretty JSON string.
    pub fn to_json_pretty(&self) -> Result<String, Error> {
        let mut ser = serde_json::Serializer::pretty(Vec::with_capacity(128));
        self.serialize_with_meta(&mut ser)?;
        Ok(unsafe { String::from_utf8_unchecked(ser.into_inner()) })
    }

    /// Serializes an annotated value into a JSON string.
    pub fn payload_to_json(&self) -> Result<String, Error> {
        let mut ser = serde_json::Serializer::new(Vec::with_capacity(128));
        if let Some(ref value) = self.0 {
            ToValue::serialize_payload(value, &mut ser)?;
        } else {
            ser.serialize_unit()?;
        }
        Ok(unsafe { String::from_utf8_unchecked(ser.into_inner()) })
    }

    /// Serializes an annotated value into a pretty JSON string.
    pub fn payload_to_json_pretty(&self) -> Result<String, Error> {
        let mut ser = serde_json::Serializer::pretty(Vec::with_capacity(128));
        if let Some(ref value) = self.0 {
            ToValue::serialize_payload(value, &mut ser)?;
        } else {
            ser.serialize_unit()?;
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
}

impl Annotated<String> {
    pub fn map_value_chunked<F>(self, f: F) -> Annotated<String>
    where
        F: FnOnce(Vec<chunks::Chunk>) -> Vec<chunks::Chunk>,
    {
        let Annotated(old_value, mut meta) = self;
        let new_value = old_value.map(|value| {
            let old_chunks = chunks::split(&value, meta.remarks.iter());
            let new_chunks = f(old_chunks);
            let (new_value, remarks) = chunks::join(new_chunks);
            meta.remarks = remarks.into_iter().collect();
            if new_value != value {
                meta.original_length = Some(value.chars().count() as u32);
            }
            new_value
        });
        Annotated(new_value, meta)
    }

    pub fn trim_string(self, cap_size: CapSize) -> Annotated<String> {
        let limit = cap_size.max_chars();
        let grace_limit = limit + cap_size.grace_chars();

        if self.0.is_none() || self.0.as_ref().unwrap().chars().count() < grace_limit {
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
                    chunks::Chunk::Redaction { .. } => {
                        if length + chunk_chars + 3 < grace_limit {
                            rv.push(chunk);
                        }
                    }

                    // if this is a text chunk, we can put the remaining characters in.
                    chunks::Chunk::Text { text } => {
                        let mut remaining = String::new();
                        for c in text.chars() {
                            if length < limit - 3 {
                                remaining.push(c);
                            } else {
                                break;
                            }
                            length += 1;
                        }
                        rv.push(chunks::Chunk::Text { text: remaining });
                    }
                }

                rv.push(chunks::Chunk::Redaction {
                    text: "...".to_string(),
                    rule_id: "!len".to_string(),
                    ty: RemarkType::Substituted,
                });
                break;
            }

            rv
        })
    }
}

/// Represents a boxed value.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Array(Array<Value>),
    Object(Object<Value>),
}

pub struct ValueDescription<'a>(&'a Value);

impl<'a> fmt::Display for ValueDescription<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self.0 {
            Value::Null => f.pad("null"),
            Value::Bool(true) => f.pad("true"),
            Value::Bool(false) => f.pad("false"),
            Value::I64(val) => write!(f, "integer {}", val),
            Value::U64(val) => write!(f, "integer {}", val),
            Value::F64(val) => write!(f, "float {}", val),
            Value::String(ref val) => f.pad(val),
            Value::Array(_) => f.pad("an array"),
            Value::Object(_) => f.pad("an object"),
        }
    }
}

impl Value {
    /// Returns a formattable that gives a helper description of the value.
    pub fn describe(&self) -> ValueDescription {
        ValueDescription(self)
    }

    /// Returns the string if this value is a string, otherwise `None`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(string) => Some(string.as_str()),
            _ => None,
        }
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            Value::Null => serializer.serialize_unit(),
            Value::Bool(val) => serializer.serialize_bool(val),
            Value::I64(val) => serializer.serialize_i64(val),
            Value::U64(val) => serializer.serialize_u64(val),
            Value::F64(val) => serializer.serialize_f64(val),
            Value::String(ref val) => serializer.serialize_str(val),
            Value::Array(ref items) => {
                let mut seq_ser = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    match item {
                        Annotated(Some(val), _) => seq_ser.serialize_element(val)?,
                        Annotated(None, _) => seq_ser.serialize_element(&())?,
                    }
                }
                seq_ser.end()
            }
            Value::Object(ref items) => {
                let mut map_ser = serializer.serialize_map(Some(items.len()))?;
                for (key, value) in items {
                    map_ser.serialize_key(key)?;
                    match value {
                        Annotated(Some(val), _) => map_ser.serialize_value(val)?,
                        Annotated(None, _) => map_ser.serialize_value(&())?,
                    }
                }
                map_ser.end()
            }
        }
    }
}

impl From<serde_json::Value> for Value {
    fn from(value: serde_json::Value) -> Value {
        match value {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(value) => Value::Bool(value),
            serde_json::Value::Number(num) => {
                if let Some(val) = num.as_i64() {
                    Value::I64(val)
                } else if let Some(val) = num.as_u64() {
                    Value::U64(val)
                } else if let Some(val) = num.as_f64() {
                    Value::F64(val)
                } else {
                    Value::Null
                }
            }
            serde_json::Value::String(val) => Value::String(val),
            serde_json::Value::Array(items) => {
                Value::Array(items.into_iter().map(From::from).collect())
            }
            serde_json::Value::Object(items) => {
                Value::Object(items.into_iter().map(|(k, v)| (k, From::from(v))).collect())
            }
        }
    }
}

impl From<serde_json::Value> for Annotated<Value> {
    fn from(value: serde_json::Value) -> Annotated<Value> {
        let value: Value = value.into();
        match value {
            Value::Null => Annotated(None, Meta::default()),
            other => Annotated(Some(other), Meta::default()),
        }
    }
}

impl From<Value> for serde_json::Value {
    fn from(value: Value) -> serde_json::Value {
        match value {
            Value::Null => serde_json::Value::Null,
            Value::Bool(value) => serde_json::Value::Bool(value),
            Value::I64(value) => serde_json::Value::Number(value.into()),
            Value::U64(value) => serde_json::Value::Number(value.into()),
            Value::F64(value) => serde_json::Number::from_f64(value)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::String(val) => serde_json::Value::String(val),
            Value::Array(items) => {
                serde_json::Value::Array(items.into_iter().map(From::from).collect())
            }
            Value::Object(items) => serde_json::Value::Object(
                items.into_iter().map(|(k, v)| (k, From::from(v))).collect(),
            ),
        }
    }
}

impl From<Annotated<Value>> for serde_json::Value {
    fn from(value: Annotated<Value>) -> serde_json::Value {
        value.0.map(From::from).unwrap_or(serde_json::Value::Null)
    }
}

/// Meta for children.
pub type MetaMap = BTreeMap<String, MetaTree>;

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

fn meta_tree_from_value(value: serde_json::Value) -> MetaTree {
    if let serde_json::Value::Object(mut map) = value {
        let meta = map
            .remove("")
            .and_then(|value| serde_json::from_value(value).ok());
        let children = map
            .into_iter()
            .map(|(k, v)| (k, meta_tree_from_value(v)))
            .collect();
        MetaTree {
            meta: meta.unwrap_or_else(Meta::default),
            children,
        }
    } else {
        MetaTree::default()
    }
}

fn attach_meta(value: &mut Annotated<Value>, mut meta_tree: MetaTree) {
    match value.0 {
        Some(Value::Array(ref mut items)) => {
            for (idx, item) in items.iter_mut().enumerate() {
                if let Some(meta_tree) = meta_tree.children.remove(&idx.to_string()) {
                    attach_meta(item, meta_tree);
                }
            }
        }
        Some(Value::Object(ref mut items)) => {
            for (key, value) in items.iter_mut() {
                if let Some(meta_tree) = meta_tree.children.remove(key) {
                    attach_meta(value, meta_tree);
                }
            }
        }
        _ => {}
    }
    value.1 = meta_tree.meta;
}

fn inline_meta_in_json(value: serde_json::Value) -> Annotated<Value> {
    match value {
        serde_json::Value::Object(mut map) => {
            let meta_tree = map
                .remove("_meta")
                .map(meta_tree_from_value)
                .unwrap_or_default();
            let mut value = serde_json::Value::Object(map).into();
            attach_meta(&mut value, meta_tree);
            value
        }
        value => value.into(),
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
fn test_string_trimming() {
    let value = Annotated::new("This is my long string I want to have trimmed down!".to_string());
    let new_value = value.trim_string(CapSize::Hard(20));
    assert_eq_dbg!(
        new_value,
        Annotated(
            Some("This is my long s...".into()),
            Meta {
                remarks: vec![Remark {
                    ty: RemarkType::Substituted,
                    rule_id: "!len".to_string(),
                    range: Some((17, 20)),
                }].into(),
                original_length: Some(51),
                ..Default::default()
            }
        )
    );
}
