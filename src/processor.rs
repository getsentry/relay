//! Provides support for processing structures.
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;

use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use uuid::Uuid;

use meta::{Annotated, MetaTree, Value};
use protocol::{Event, Exception, Frame, Stacktrace};

#[derive(Debug, Clone)]
enum PathItem<'a> {
    StaticKey(&'a str),
    DynamicKey(String),
    Index(usize),
}

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

#[derive(Debug, Clone)]
pub struct FieldAttrs {
    pub name: Option<&'static str>,
    pub required: bool,
    pub cap_size: Option<CapSize>,
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
pub trait MetaStructure {
    /// Creates a meta structure from an annotated boxed value.
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized;

    /// Boxes the meta structure back into a value.
    fn to_value(value: Annotated<Self>) -> Annotated<Value>
    where
        Self: Sized;

    /// Extracts the meta tree out of annotated value.
    #[inline(always)]
    fn extract_meta_tree(value: &Annotated<Self>) -> MetaTree
    where
        Self: Sized,
    {
        MetaTree {
            meta: value.1.clone(),
            children: Default::default(),
        }
    }

    /// Efficiently serializes the payload directly.
    fn serialize_payload<S>(value: &Annotated<Self>, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer;

    /// Executes a processor on the tree.
    #[inline(always)]
    fn process<P: Processor>(
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
pub struct SerializeMetaStructurePayload<'a, T: 'a>(pub &'a Annotated<T>);

impl<'a, T: MetaStructure> Serialize for SerializeMetaStructurePayload<'a, T> {
    #[inline(always)]
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        MetaStructure::serialize_payload(self.0, s)
    }
}

macro_rules! primitive_meta_structure {
    ($type:ident, $meta_type:ident, $expectation:expr) => {
        impl MetaStructure for $type {
            #[inline(always)]
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::$meta_type(value)), meta) => Annotated(Some(value), meta),
                    Annotated(Some(Value::Null), meta) => Annotated(None, meta),
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(_, mut meta) => {
                        meta.add_error(format!("expected {}", $expectation));
                        Annotated(None, meta)
                    }
                }
            }
            #[inline(always)]
            fn to_value(value: Annotated<Self>) -> Annotated<Value> {
                match value {
                    Annotated(Some(value), meta) => Annotated(Some(Value::$meta_type(value)), meta),
                    Annotated(None, meta) => Annotated(None, meta),
                }
            }
            #[inline(always)]
            fn serialize_payload<S>(value: &Annotated<Self>, s: S) -> Result<S::Ok, S::Error>
            where
                Self: Sized,
                S: Serializer,
            {
                let &Annotated(ref value, _) = value;
                Serialize::serialize(value, s)
            }
        }
    };
}

macro_rules! numeric_meta_structure {
    ($type:ident, $meta_type:ident, $expectation:expr) => {
        impl MetaStructure for $type {
            #[inline(always)]
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::U64(value)), meta) => {
                        Annotated(Some(value as $type), meta)
                    }
                    Annotated(Some(Value::I64(value)), meta) => {
                        Annotated(Some(value as $type), meta)
                    }
                    Annotated(Some(Value::F64(value)), meta) => {
                        Annotated(Some(value as $type), meta)
                    }
                    Annotated(Some(Value::Null), meta) => Annotated(None, meta),
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(_, mut meta) => {
                        meta.add_error(format!("expected {}", $expectation));
                        Annotated(None, meta)
                    }
                }
            }
            #[inline(always)]
            fn to_value(value: Annotated<Self>) -> Annotated<Value> {
                match value {
                    Annotated(Some(value), meta) => Annotated(Some(Value::$meta_type(value)), meta),
                    Annotated(None, meta) => Annotated(None, meta),
                }
            }
            #[inline(always)]
            fn serialize_payload<S>(value: &Annotated<Self>, s: S) -> Result<S::Ok, S::Error>
            where
                Self: Sized,
                S: Serializer,
            {
                let &Annotated(ref value, _) = value;
                Serialize::serialize(value, s)
            }
        }
    };
}

macro_rules! primitive_meta_structure_through_string {
    ($type:ident, $expectation:expr) => {
        impl MetaStructure for $type {
            #[inline(always)]
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                        Ok(value) => Annotated(Some(value), meta),
                        Err(err) => {
                            meta.add_error(err.to_string());
                            Annotated(None, meta)
                        }
                    },
                    Annotated(Some(Value::Null), meta) => Annotated(None, meta),
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(_, mut meta) => {
                        meta.add_error(format!("expected {}", $expectation));
                        Annotated(None, meta)
                    }
                }
            }
            #[inline(always)]
            fn to_value(value: Annotated<Self>) -> Annotated<Value> {
                match value {
                    Annotated(Some(value), meta) => {
                        Annotated(Some(Value::String(value.to_string())), meta)
                    }
                    Annotated(None, meta) => Annotated(None, meta),
                }
            }
            #[inline(always)]
            fn serialize_payload<S>(value: &Annotated<Self>, s: S) -> Result<S::Ok, S::Error>
            where
                Self: Sized,
                S: Serializer,
            {
                let &Annotated(ref value, _) = value;
                Serialize::serialize(value, s)
            }
        }
    };
}

primitive_meta_structure!(String, String, "a string");
primitive_meta_structure!(bool, Bool, "a boolean");
numeric_meta_structure!(u64, U64, "an unsigned integer");
numeric_meta_structure!(i64, I64, "a signed integer");
numeric_meta_structure!(f64, F64, "a floating point value");
primitive_meta_structure_through_string!(Uuid, "a uuid");

impl<T: MetaStructure> MetaStructure for Vec<Annotated<T>> {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Array(items)), meta) => Annotated(
                Some(items.into_iter().map(MetaStructure::from_value).collect()),
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
    #[inline(always)]
    fn to_value(value: Annotated<Self>) -> Annotated<Value> {
        match value {
            Annotated(Some(value), meta) => Annotated(
                Some(Value::Array(
                    value.into_iter().map(MetaStructure::to_value).collect(),
                )),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
    #[inline(always)]
    fn serialize_payload<S>(value: &Annotated<Self>, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        match value {
            &Annotated(Some(ref value), _) => {
                let mut seq_ser = s.serialize_seq(Some(value.len()))?;
                for item in value {
                    seq_ser.serialize_element(&SerializeMetaStructurePayload(item))?;
                }
                seq_ser.end()
            }
            &Annotated(None, _) => s.serialize_unit(),
        }
    }
    fn extract_meta_tree(value: &Annotated<Self>) -> MetaTree
    where
        Self: Sized,
    {
        let mut meta_tree = MetaTree {
            meta: value.1.clone(),
            children: Default::default(),
        };
        if let &Annotated(Some(ref items), _) = value {
            for (idx, item) in items.iter().enumerate() {
                let tree = MetaStructure::extract_meta_tree(item);
                if !tree.is_empty() {
                    meta_tree.children.insert(idx.to_string(), tree);
                }
            }
        }
        meta_tree
    }
    fn process<P: Processor>(
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
                            MetaStructure::process(v, processor, inner_state)
                        }).collect(),
                ),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl<T: MetaStructure> MetaStructure for BTreeMap<String, Annotated<T>> {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Object(items)), meta) => Annotated(
                Some(
                    items
                        .into_iter()
                        .map(|(k, v)| (k, MetaStructure::from_value(v)))
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
    #[inline(always)]
    fn to_value(value: Annotated<Self>) -> Annotated<Value> {
        match value {
            Annotated(Some(value), meta) => Annotated(
                Some(Value::Object(
                    value
                        .into_iter()
                        .map(|(k, v)| (k, MetaStructure::to_value(v)))
                        .collect(),
                )),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }

    #[inline(always)]
    fn serialize_payload<S>(value: &Annotated<Self>, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        match value {
            &Annotated(Some(ref items), _) => {
                let mut map_ser = s.serialize_map(Some(items.len()))?;
                for (key, value) in items {
                    if !value.skip_serialization() {
                        map_ser.serialize_key(key)?;
                        map_ser.serialize_value(&SerializeMetaStructurePayload(value))?;
                    }
                }
                map_ser.end()
            }
            &Annotated(None, _) => s.serialize_unit(),
        }
    }

    fn extract_meta_tree(value: &Annotated<Self>) -> MetaTree
    where
        Self: Sized,
    {
        let mut meta_tree = MetaTree {
            meta: value.1.clone(),
            children: Default::default(),
        };
        if let &Annotated(Some(ref items), _) = value {
            for (key, value) in items.iter() {
                let tree = MetaStructure::extract_meta_tree(value);
                if !tree.is_empty() {
                    meta_tree.children.insert(key.to_string(), tree);
                }
            }
        }
        meta_tree
    }

    fn process<P: Processor>(
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
                                let inner_state = state.enter_borrowed(k.as_str(), None);
                                MetaStructure::process(v, processor, inner_state)
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

impl MetaStructure for Value {
    #[inline(always)]
    fn from_value(value: Annotated<Value>) -> Annotated<Value> {
        value
    }

    #[inline(always)]
    fn to_value(value: Annotated<Value>) -> Annotated<Value> {
        value
    }

    #[inline(always)]
    fn serialize_payload<S>(value: &Annotated<Self>, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        let &Annotated(ref value, _) = value;
        Serialize::serialize(value, s)
    }

    // TODO: implement extract_meta_tree and process

    #[inline(always)]
    fn process<P: Processor>(
        value: Annotated<Self>,
        processor: &P,
        state: ProcessingState,
    ) -> Annotated<Self> {
        let _processor = processor;
        let _state = state;
        value
    }
}
