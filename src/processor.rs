//! Provides support for processing structures.
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use uuid::Uuid;

use meta::{Annotated, MetaMap, MetaTree, Value};
use protocol::{Event, Exception, Frame, Stacktrace};

#[derive(Debug, Clone)]
enum PathItem<'a> {
    StaticKey(&'a str),
    DynamicKey(String),
    Index(usize),
}

/// The maximum size of a field.
#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum CapSize {
    EnumLike,
    Summary,
    Message,
    Payload,
    Symbol,
    Path,
    ShortPath,
}

impl CapSize {
    pub fn max_input_length(&self) -> usize {
        match *self {
            CapSize::EnumLike => 128,
            CapSize::Summary => 1024,
            CapSize::Message => 8196,
            CapSize::Payload => 20_000,
            CapSize::Symbol => 256,
            CapSize::Path => 256,
            CapSize::ShortPath => 128,
        }
    }

    pub fn grace_size(&self) -> usize {
        match *self {
            CapSize::EnumLike => 10,
            CapSize::Summary => 100,
            CapSize::Message => 200,
            CapSize::Payload => 1000,
            CapSize::Symbol => 20,
            CapSize::Path => 40,
            CapSize::ShortPath => 20,
        }
    }

    pub fn max_field_length(&self) -> usize {
        self.max_input_length() + self.grace_size()
    }
}

/// The type of PII contained on a field.
#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum PiiKind {
    Freeform,
    Ip,
    Id,
    Username,
    Hostname,
    Sensitive,
    Name,
    Email,
    Location,
    Databag,
}

/// Meta information about a field.
#[derive(Debug, Clone)]
pub struct FieldAttrs {
    /// Optionally the name of the field.
    pub name: Option<&'static str>,
    /// If the field is required.
    pub required: bool,
    /// The maximum size of the field.
    pub cap_size: Option<CapSize>,
    /// The type of PII on the field.
    pub pii_kind: Option<PiiKind>,
}

const DEFAULT_FIELD_ATTRS: FieldAttrs = FieldAttrs {
    name: None,
    required: false,
    cap_size: None,
    pii_kind: None,
};

impl Default for FieldAttrs {
    fn default() -> FieldAttrs {
        DEFAULT_FIELD_ATTRS.clone()
    }
}

/// Processing state passed downwards during processing.
#[derive(Debug, Clone)]
pub struct ProcessingState<'a> {
    parent: Option<&'a ProcessingState<'a>>,
    path: Option<PathItem<'a>>,
    attrs: Option<Cow<'static, FieldAttrs>>,
}

/// Represents the path in a structure
#[derive(Debug)]
pub struct Path<'a>(&'a ProcessingState<'a>);

impl<'a> Path<'a> {
    /// Returns the current key if there is one
    #[inline(always)]
    pub fn key(&self) -> Option<&str> {
        self.0.path.as_ref().and_then(|value| match *value {
            PathItem::StaticKey(s) => Some(s),
            PathItem::DynamicKey(ref s) => Some(s.as_str()),
            PathItem::Index(_) => None,
        })
    }

    /// Returns the current index if there is one
    #[inline(always)]
    pub fn index(&self) -> Option<usize> {
        self.0.path.as_ref().and_then(|value| match *value {
            PathItem::StaticKey(_) => None,
            PathItem::DynamicKey(_) => None,
            PathItem::Index(idx) => Some(idx),
        })
    }

    /// Returns a path iterator.
    pub fn iter(&'a self) -> impl Iterator<Item = &'a PathItem<'a>> {
        let mut items = vec![];
        let mut ptr = Some(self.0);
        while let Some(p) = ptr {
            if let Some(ref path) = p.path {
                items.push(path);
            }
            ptr = p.parent;
        }
        items.reverse();
        items.into_iter()
    }
}

impl<'a> fmt::Display for PathItem<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PathItem::StaticKey(s) => f.pad(s),
            PathItem::DynamicKey(ref s) => f.pad(s.as_str()),
            PathItem::Index(val) => write!(f, "{}", val),
        }
    }
}

impl<'a> fmt::Display for Path<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let path = self.0.path();
        for (idx, item) in path.iter().enumerate() {
            if idx > 0 {
                write!(f, ".")?;
            }
            write!(f, "{}", item)?;
        }
        Ok(())
    }
}

impl<'a> ProcessingState<'a> {
    /// Derives a processing state by entering a static key.
    #[inline(always)]
    pub fn enter_static(
        &'a self,
        key: &'static str,
        attrs: Option<Cow<'static, FieldAttrs>>,
    ) -> ProcessingState<'a> {
        ProcessingState {
            parent: Some(self),
            path: Some(PathItem::StaticKey(key)),
            attrs: attrs,
        }
    }

    /// Derives a processing state by entering a borrowed key.
    #[inline(always)]
    pub fn enter_borrowed(
        &'a self,
        key: &'a str,
        attrs: Option<Cow<'static, FieldAttrs>>,
    ) -> ProcessingState<'a> {
        ProcessingState {
            parent: Some(self),
            path: Some(PathItem::StaticKey(key)),
            attrs: attrs,
        }
    }

    /// Derives a processing state by entering an index.
    #[inline(always)]
    pub fn enter_index(
        &'a self,
        idx: usize,
        attrs: Option<Cow<'static, FieldAttrs>>,
    ) -> ProcessingState<'a> {
        ProcessingState {
            parent: Some(self),
            path: Some(PathItem::Index(idx)),
            attrs: attrs,
        }
    }

    /// Returns the path in the processing state.
    #[inline(always)]
    pub fn path(&'a self) -> Path<'a> {
        Path(&self)
    }

    /// Returns the field attributes.
    #[inline(always)]
    pub fn attrs(&self) -> &FieldAttrs {
        match self.attrs {
            Some(ref cow) => &cow,
            None => &DEFAULT_FIELD_ATTRS,
        }
    }
}

/// A trait for processing the protocol.
pub trait Processor {
    #[inline(always)]
    fn process_event(&self, event: Annotated<Event>, state: ProcessingState) -> Annotated<Event> {
        let _state = state;
        event
    }
    #[inline(always)]
    fn process_exception(
        &self,
        exception: Annotated<Exception>,
        state: ProcessingState,
    ) -> Annotated<Exception> {
        let _state = state;
        exception
    }
    #[inline(always)]
    fn process_stacktrace(
        &self,
        stacktrace: Annotated<Stacktrace>,
        state: ProcessingState,
    ) -> Annotated<Stacktrace> {
        let _state = state;
        stacktrace
    }
    #[inline(always)]
    fn process_frame(&self, frame: Annotated<Frame>, state: ProcessingState) -> Annotated<Frame> {
        let _state = state;
        frame
    }
}

/// Implemented for all meta structures.
pub trait FromValue {
    /// Creates a meta structure from an annotated boxed value.
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized;
}

/// Implemented for all meta structures.
pub trait ToValue {
    /// Boxes the meta structure back into a value.
    fn to_value(value: Annotated<Self>) -> Annotated<Value>
    where
        Self: Sized;

    /// Extracts children meta map out of a value.
    #[inline(always)]
    fn extract_child_meta(&self) -> MetaMap
    where
        Self: Sized,
    {
        Default::default()
    }

    /// Efficiently serializes the payload directly.
    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer;

    /// Extracts the meta tree out of annotated value.
    ///
    /// This should not be overridden by implementators, instead `extract_child_meta`
    /// should be provided instead.
    #[inline(always)]
    fn extract_meta_tree(value: &Annotated<Self>) -> MetaTree
    where
        Self: Sized,
    {
        MetaTree {
            meta: value.1.clone(),
            children: match value.0 {
                Some(ref value) => ToValue::extract_child_meta(value),
                None => Default::default(),
            },
        }
    }
}

/// Similar to `ToValue` but for keys only.
pub trait ToKey: Clone {
    /// Converts a value to a key.
    fn to_key(key: Self) -> String
    where
        Self: Sized;
}

/// Similar to `FromValue` but for keys only.
pub trait FromKey {
    /// Converts a key string into an instance of the object.
    fn from_key(key: String) -> Self
    where
        Self: Sized;
}

impl ToKey for String {
    #[inline(always)]
    fn to_key(key: String) -> String {
        key
    }
}

impl FromKey for String {
    #[inline(always)]
    fn from_key(key: String) -> String {
        key
    }
}

pub trait ProcessValue {
    /// Executes a processor on the tree.
    #[inline(always)]
    fn process_value<P: Processor>(
        value: Annotated<Self>,
        processor: &P,
        state: ProcessingState,
    ) -> Annotated<Self>
    where
        Self: Sized,
    {
        let _processor = processor;
        let _state = state;
        value
    }
}

// This needs to be public because the derive crate emits it
#[doc(hidden)]
pub struct SerializePayload<'a, T: 'a>(pub &'a Annotated<T>);

impl<'a, T: ToValue> Serialize for SerializePayload<'a, T> {
    #[inline(always)]
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self.0 {
            Annotated(Some(ref value), _) => ToValue::serialize_payload(value, s),
            Annotated(None, _) => s.serialize_unit(),
        }
    }
}

macro_rules! primitive_meta_structure {
    ($type:ident, $meta_type:ident, $expectation:expr) => {
        impl FromValue for $type {
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::$meta_type(value)), meta) => Annotated(Some(value), meta),
                    Annotated(Some(Value::Null), meta) => Annotated(None, meta),
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(Some(value), mut meta) => {
                        meta.add_error(format!(
                            "expected {}, got {}",
                            $expectation,
                            value.describe()
                        ));
                        Annotated(None, meta)
                    }
                }
            }
        }

        primitive_to_value!($type, $meta_type);

        impl ProcessValue for $type {}
    };
}

primitive_meta_structure!(String, String, "a string");
primitive_meta_structure!(bool, Bool, "a boolean");
numeric_meta_structure!(u64, U64, "an unsigned integer");
numeric_meta_structure!(i64, I64, "a signed integer");
numeric_meta_structure!(f64, F64, "a floating point value");
primitive_meta_structure_through_string!(Uuid, "a uuid");

impl<T: FromValue> FromValue for Vec<Annotated<T>> {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Array(items)), meta) => Annotated(
                Some(items.into_iter().map(FromValue::from_value).collect()),
                meta,
            ),
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(_, mut meta) => {
                meta.add_error("expected array".to_string());
                Annotated(None, meta)
            }
        }
    }
}

impl<T: ToValue> ToValue for Vec<Annotated<T>> {
    #[inline(always)]
    fn to_value(value: Annotated<Self>) -> Annotated<Value> {
        match value {
            Annotated(Some(value), meta) => Annotated(
                Some(Value::Array(
                    value.into_iter().map(ToValue::to_value).collect(),
                )),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
    #[inline(always)]
    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        let mut seq_ser = s.serialize_seq(Some(self.len()))?;
        for item in self {
            seq_ser.serialize_element(&SerializePayload(item))?;
        }
        seq_ser.end()
    }
    fn extract_child_meta(&self) -> MetaMap
    where
        Self: Sized,
    {
        let mut children = MetaMap::new();
        for (idx, item) in self.iter().enumerate() {
            let tree = ToValue::extract_meta_tree(item);
            if !tree.is_empty() {
                children.insert(idx.to_string(), tree);
            }
        }
        children
    }
}

impl<T: ProcessValue> ProcessValue for Vec<Annotated<T>> {
    fn process_value<P: Processor>(
        value: Annotated<Self>,
        processor: &P,
        state: ProcessingState,
    ) -> Annotated<Self> {
        match value {
            Annotated(Some(value), meta) => Annotated(
                Some(
                    value
                        .into_iter()
                        .enumerate()
                        .map(|(idx, v)| {
                            let inner_state = state.enter_index(idx, None);
                            ProcessValue::process_value(v, processor, inner_state)
                        }).collect(),
                ),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl<K: FromKey + Ord, T: FromValue> FromValue for BTreeMap<K, Annotated<T>> {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Object(items)), meta) => Annotated(
                Some(
                    items
                        .into_iter()
                        .map(|(k, v)| (FromKey::from_key(k), FromValue::from_value(v)))
                        .collect(),
                ),
                meta,
            ),
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(_, mut meta) => {
                meta.add_error("expected object".to_string());
                Annotated(None, meta)
            }
        }
    }
}

impl<K: ToKey, T: ToValue> ToValue for BTreeMap<K, Annotated<T>> {
    fn to_value(value: Annotated<Self>) -> Annotated<Value> {
        match value {
            Annotated(Some(value), meta) => Annotated(
                Some(Value::Object(
                    value
                        .into_iter()
                        .map(|(k, v)| (ToKey::to_key(k), ToValue::to_value(v)))
                        .collect(),
                )),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }

    #[inline(always)]
    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        let mut map_ser = s.serialize_map(Some(self.len()))?;
        for (key, value) in self {
            if !value.skip_serialization() {
                map_ser.serialize_key(&ToKey::to_key(key.clone()))?;
                map_ser.serialize_value(&SerializePayload(value))?;
            }
        }
        map_ser.end()
    }

    fn extract_child_meta(&self) -> BTreeMap<String, MetaTree>
    where
        Self: Sized,
    {
        let mut children = MetaMap::new();
        for (key, value) in self.iter() {
            let tree = ToValue::extract_meta_tree(value);
            if !tree.is_empty() {
                children.insert(ToKey::to_key(key.clone()), tree);
            }
        }
        children
    }
}

impl<K: ToKey + Ord, T: ProcessValue> ProcessValue for BTreeMap<K, Annotated<T>> {
    fn process_value<P: Processor>(
        value: Annotated<Self>,
        processor: &P,
        state: ProcessingState,
    ) -> Annotated<Self> {
        match value {
            Annotated(Some(value), meta) => Annotated(
                Some(
                    value
                        .into_iter()
                        .map(|(k, v)| {
                            let v = {
                                let k = ToKey::to_key(k.clone());
                                let inner_state = state.enter_borrowed(&k, None);
                                ProcessValue::process_value(v, processor, inner_state)
                            };
                            (k, v)
                        }).collect(),
                ),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl FromValue for Value {
    #[inline(always)]
    fn from_value(value: Annotated<Value>) -> Annotated<Value> {
        value
    }
}

impl ToValue for Value {
    #[inline(always)]
    fn to_value(value: Annotated<Value>) -> Annotated<Value> {
        value
    }

    #[inline(always)]
    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        Serialize::serialize(self, s)
    }

    fn extract_child_meta(&self) -> BTreeMap<String, MetaTree>
    where
        Self: Sized,
    {
        let mut children = MetaMap::new();
        match *self {
            Value::Object(ref items) => {
                for (key, value) in items.iter() {
                    let tree = ToValue::extract_meta_tree(value);
                    if !tree.is_empty() {
                        children.insert(key.to_string(), tree);
                    }
                }
            }
            Value::Array(ref items) => {
                for (idx, item) in items.iter().enumerate() {
                    let tree = ToValue::extract_meta_tree(item);
                    if !tree.is_empty() {
                        children.insert(idx.to_string(), tree);
                    }
                }
            }
            _ => {}
        }
        children
    }
}

impl ProcessValue for Value {
    fn process_value<P: Processor>(
        value: Annotated<Self>,
        processor: &P,
        state: ProcessingState,
    ) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Object(items)), meta) => Annotated(
                Some(Value::Object(
                    items
                        .into_iter()
                        .map(|(k, v)| {
                            let v = {
                                let inner_state = state.enter_borrowed(k.as_str(), None);
                                ProcessValue::process_value(v, processor, inner_state)
                            };
                            (k, v)
                        }).collect(),
                )),
                meta,
            ),
            Annotated(Some(Value::Array(items)), meta) => Annotated(
                Some(Value::Array(
                    items
                        .into_iter()
                        .enumerate()
                        .map(|(idx, v)| {
                            let inner_state = state.enter_index(idx, None);
                            ProcessValue::process_value(v, processor, inner_state)
                        }).collect(),
                )),
                meta,
            ),
            other => other,
        }
    }
}

fn datetime_to_timestamp(dt: DateTime<Utc>) -> f64 {
    let micros = f64::from(dt.timestamp_subsec_micros()) / 1_000_000f64;
    dt.timestamp() as f64 + micros
}

impl FromValue for DateTime<Utc> {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => {
                let parsed = match value.parse::<NaiveDateTime>() {
                    Ok(dt) => Ok(DateTime::from_utc(dt, Utc)),
                    Err(_) => value.parse(),
                };
                match parsed {
                    Ok(value) => Annotated(Some(value), meta),
                    Err(err) => {
                        meta.add_error(err.to_string());
                        Annotated(None, meta)
                    }
                }
            }
            Annotated(Some(Value::U64(ts)), meta) => {
                Annotated(Some(Utc.timestamp_opt(ts as i64, 0).unwrap()), meta)
            }
            Annotated(Some(Value::I64(ts)), meta) => {
                Annotated(Some(Utc.timestamp_opt(ts, 0).unwrap()), meta)
            }
            Annotated(Some(Value::F64(ts)), meta) => {
                let secs = ts as i64;
                let micros = (ts.fract() * 1_000_000f64) as u32;
                Annotated(Some(Utc.timestamp_opt(secs, micros * 1000).unwrap()), meta)
            }
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(_, mut meta) => {
                meta.add_error("expected timestamp");
                Annotated(None, meta)
            }
        }
    }
}

impl ToValue for DateTime<Utc> {
    fn to_value(value: Annotated<Self>) -> Annotated<Value> {
        match value {
            Annotated(Some(value), meta) => {
                Annotated(Some(Value::F64(datetime_to_timestamp(value))), meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
        }
    }

    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        Serialize::serialize(&datetime_to_timestamp(*self), s)
    }
}

impl ProcessValue for DateTime<Utc> {}

impl<T: FromValue> FromValue for Box<T> {
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized,
    {
        let annotated: Annotated<T> = FromValue::from_value(value);
        Annotated(annotated.0.map(Box::new), annotated.1)
    }
}

impl<T: ToValue + Clone> ToValue for Box<T> {
    fn to_value(value: Annotated<Self>) -> Annotated<Value>
    where
        Self: Sized,
    {
        ToValue::to_value(Annotated(value.0.map(|x| *x), value.1))
    }

    #[inline(always)]
    fn extract_child_meta(&self) -> MetaMap
    where
        Self: Sized,
    {
        ToValue::extract_child_meta(&**self)
    }

    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        ToValue::serialize_payload(&**self, s)
    }

    #[inline(always)]
    fn extract_meta_tree(value: &Annotated<Self>) -> MetaTree
    where
        Self: Sized,
    {
        // TODO: Unnecessary clone
        let value: Annotated<T> =
            Annotated(value.0.as_ref().map(|x| (**x).clone()), value.1.clone());
        ToValue::extract_meta_tree(&value)
    }
}

impl<T: ProcessValue> ProcessValue for Box<T> {
    /// Executes a processor on the tree.
    #[inline(always)]
    fn process_value<P: Processor>(
        value: Annotated<Self>,
        processor: &P,
        state: ProcessingState,
    ) -> Annotated<Self>
    where
        Self: Sized,
    {
        let value: Annotated<T> = Annotated(value.0.map(|x| *x), value.1);
        let rv = ProcessValue::process_value(value, processor, state);
        Annotated(rv.0.map(Box::new), rv.1)
    }
}
