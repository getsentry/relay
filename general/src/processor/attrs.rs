use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

use failure::Fail;
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::processor::ProcessValue;
use crate::types::Annotated;

/// Error for unknown value types.
#[derive(Debug, Fail)]
#[fail(display = "unknown value type")]
pub struct UnknownValueTypeError;

/// The (simplified) type of a value.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum ValueType {
    String,
    Number,
    Boolean,
    DateTime,
    Array,
    Object,
    Event,
    Exception,
    Stacktrace,
    Frame,
    Request,
    User,
    LogEntry,
    Thread,
    Breadcrumb,
}

impl ValueType {
    pub fn for_field<T: ProcessValue>(field: &Annotated<T>) -> Option<Self> {
        field.value().and_then(ProcessValue::value_type)
    }

    pub fn name(self) -> &'static str {
        match self {
            ValueType::String => "string",
            ValueType::Number => "number",
            ValueType::Boolean => "boolean",
            ValueType::DateTime => "datetime",
            ValueType::Array => "array",
            ValueType::Object => "object",
            ValueType::Event => "event",
            ValueType::Exception => "exception",
            ValueType::Stacktrace => "stacktrace",
            ValueType::Frame => "frame",
            ValueType::Request => "request",
            ValueType::User => "user",
            ValueType::LogEntry => "logentry",
            ValueType::Thread => "thread",
            ValueType::Breadcrumb => "breadcrumb",
        }
    }
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl FromStr for ValueType {
    type Err = UnknownValueTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "string" | "freeform" => ValueType::String,
            "number" => ValueType::Number,
            "bool" | "boolean" => ValueType::Boolean,
            "datetime" => ValueType::DateTime,
            "array" | "list" => ValueType::Array,
            "object" | "databag" => ValueType::Object,
            "event" => ValueType::Event,
            "exception" => ValueType::Exception,
            "stacktrace" => ValueType::Stacktrace,
            "frame" => ValueType::Frame,
            "request" => ValueType::Request,
            "user" => ValueType::User,
            "logentry" => ValueType::LogEntry,
            "thread" => ValueType::Thread,
            "breadcrumb" => ValueType::Breadcrumb,
            _ => return Err(UnknownValueTypeError),
        })
    }
}

/// The maximum length of a field.
#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum MaxChars {
    EnumLike,
    Summary,
    Message,
    Symbol,
    Path,
    ShortPath,
    Email,
    Culprit,
    TagKey,
    TagValue,
    Hard(usize),
    Soft(usize),
}

impl MaxChars {
    /// The cap in number of unicode characters.
    pub fn limit(self) -> usize {
        match self {
            MaxChars::EnumLike => 128,
            MaxChars::Summary => 1024,
            MaxChars::Message => 8192,
            MaxChars::Symbol => 256,
            MaxChars::Path => 256,
            MaxChars::ShortPath => 128,
            // these are from constants.py
            MaxChars::Email => 75,
            MaxChars::Culprit => 200,
            MaxChars::TagKey => 32,
            MaxChars::TagValue => 200,
            MaxChars::Soft(len) | MaxChars::Hard(len) => len,
        }
    }

    /// The number of extra characters permitted.
    pub fn allowance(self) -> usize {
        match self {
            MaxChars::EnumLike => 0,
            MaxChars::Summary => 100,
            MaxChars::Message => 200,
            MaxChars::Symbol => 20,
            MaxChars::Path => 40,
            MaxChars::ShortPath => 20,
            MaxChars::Email => 0,
            MaxChars::Culprit => 0,
            MaxChars::TagKey => 0,
            MaxChars::TagValue => 0,
            MaxChars::Soft(_) => 10,
            MaxChars::Hard(_) => 0,
        }
    }
}

/// The maximum size of a databag.
#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum BagSize {
    Small,
    Medium,
    Large,
}

impl BagSize {
    /// Maximum depth of the structure.
    pub fn max_depth(self) -> usize {
        match self {
            BagSize::Small => 3,
            BagSize::Medium => 3,
            BagSize::Large => 5,
        }
    }

    /// Maximum estimated JSON bytes.
    pub fn max_size(self) -> usize {
        match self {
            BagSize::Small => 1024,
            BagSize::Medium => 2048,
            BagSize::Large => 8192,
        }
    }
}

/// The type of PII contained on a field.
#[derive(Debug, Clone, Copy, PartialEq, Hash, PartialOrd, Ord, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PiiKind {
    Text,
    Value,
    Container,
}

/// Meta information about a field.
#[derive(Debug, Clone)]
pub struct FieldAttrs {
    /// Optionally the name of the field.
    pub name: Option<&'static str>,
    /// If the field is required.
    pub required: bool,
    /// If the field should be non-empty.
    pub nonempty: bool,
    /// A regex to validate the (string) value against.
    pub match_regex: Option<Regex>,
    /// The maximum char length of this field.
    pub max_chars: Option<MaxChars>,
    /// The maximum bag size of this field.
    pub bag_size: Option<BagSize>,
    /// The type of PII on the field.
    pub pii: bool,
}

lazy_static::lazy_static! {
    static ref DEFAULT_FIELD_ATTRS: FieldAttrs = FieldAttrs {
        name: None,
        required: false,
        nonempty: false,
        match_regex: None,
        max_chars: None,
        bag_size: None,
        pii: false,
    };
}

lazy_static::lazy_static! {
    static ref PII_FIELD_ATTRS: FieldAttrs = FieldAttrs {
        name: None,
        required: false,
        nonempty: false,
        match_regex: None,
        max_chars: None,
        bag_size: None,
        pii: true,
    };
}

impl FieldAttrs {
    /// Like default but with pii turned on.
    pub fn default_pii() -> &'static FieldAttrs {
        &*PII_FIELD_ATTRS
    }
}

impl Default for FieldAttrs {
    fn default() -> FieldAttrs {
        DEFAULT_FIELD_ATTRS.clone()
    }
}

#[derive(Debug, Clone)]
enum PathItem<'a> {
    StaticKey(&'a str),
    DynamicKey(String),
    Index(usize),
}

impl<'a> fmt::Display for PathItem<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            PathItem::StaticKey(s) => f.pad(s),
            PathItem::DynamicKey(ref s) => f.pad(s.as_str()),
            PathItem::Index(val) => write!(f, "{}", val),
        }
    }
}

/// Processing state passed downwards during processing.
#[derive(Debug, Clone)]
pub struct ProcessingState<'a> {
    parent: Option<&'a ProcessingState<'a>>,
    path: Option<PathItem<'a>>,
    attrs: Option<Cow<'a, FieldAttrs>>,
    value_type: Option<ValueType>,
    depth: usize,
}

static ROOT_STATE: ProcessingState = ProcessingState {
    parent: None,
    path: None,
    attrs: None,
    value_type: None,
    depth: 0,
};

impl<'a> ProcessingState<'a> {
    /// Returns the root processing state.
    pub fn root() -> &'static ProcessingState<'static> {
        &ROOT_STATE
    }

    /// Derives a processing state by entering a static key.
    pub fn enter_static(
        &'a self,
        key: &'static str,
        attrs: Option<Cow<'static, FieldAttrs>>,
        value_type: Option<ValueType>,
    ) -> Self {
        ProcessingState {
            parent: Some(self),
            path: Some(PathItem::StaticKey(key)),
            attrs,
            value_type,
            depth: self.depth + 1,
        }
    }

    /// Derives a processing state by entering a borrowed key.
    pub fn enter_borrowed(
        &'a self,
        key: &'a str,
        attrs: Option<Cow<'a, FieldAttrs>>,
        value_type: Option<ValueType>,
    ) -> Self {
        ProcessingState {
            parent: Some(self),
            path: Some(PathItem::StaticKey(key)),
            attrs,
            value_type,
            depth: self.depth + 1,
        }
    }

    /// Derives a processing state by entering an index.
    pub fn enter_index(
        &'a self,
        idx: usize,
        attrs: Option<Cow<'a, FieldAttrs>>,
        value_type: Option<ValueType>,
    ) -> Self {
        ProcessingState {
            parent: Some(self),
            path: Some(PathItem::Index(idx)),
            attrs,
            value_type,
            depth: self.depth + 1,
        }
    }

    /// Returns the path in the processing state.
    pub fn path(&'a self) -> Path<'a> {
        Path(&self)
    }

    pub fn value_type(&self) -> Option<ValueType> {
        self.value_type
    }

    /// Returns the field attributes.
    pub fn attrs(&self) -> &FieldAttrs {
        match self.attrs {
            Some(ref cow) => &cow,
            None => &DEFAULT_FIELD_ATTRS,
        }
    }

    /// Derives the attrs for recursion.
    pub fn inner_attrs(&self) -> Option<Cow<'_, FieldAttrs>> {
        if self.attrs().pii {
            Some(Cow::Borrowed(FieldAttrs::default_pii()))
        } else {
            None
        }
    }

    /// Iterates through this state and all its ancestors up the hierarchy.
    pub fn iter(&'a self) -> ProcessingStateIter<'a> {
        ProcessingStateIter {
            state: Some(self),
            size: self.depth,
        }
    }
}

pub struct ProcessingStateIter<'a> {
    state: Option<&'a ProcessingState<'a>>,
    size: usize,
}

impl<'a> Iterator for ProcessingStateIter<'a> {
    type Item = &'a ProcessingState<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.state?;
        self.state = current.parent;
        Some(current)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size))
    }
}

impl<'a> ExactSizeIterator for ProcessingStateIter<'a> {}

impl<'a> IntoIterator for &'a ProcessingState<'a> {
    type Item = &'a ProcessingState<'a>;
    type IntoIter = ProcessingStateIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> Default for ProcessingState<'a> {
    fn default() -> Self {
        ProcessingState::root().clone()
    }
}

/// Represents the path in a structure
#[derive(Debug)]
pub struct Path<'a>(&'a ProcessingState<'a>);

impl<'a> Path<'a> {
    /// Returns the current key if there is one
    #[inline]
    pub fn key(&self) -> Option<&str> {
        self.0.path.as_ref().and_then(|value| match *value {
            PathItem::StaticKey(s) => Some(s),
            PathItem::DynamicKey(ref s) => Some(s.as_str()),
            PathItem::Index(_) => None,
        })
    }

    /// Returns the current index if there is one
    #[inline]
    pub fn index(&self) -> Option<usize> {
        self.0.path.as_ref().and_then(|value| match *value {
            PathItem::StaticKey(_) => None,
            PathItem::DynamicKey(_) => None,
            PathItem::Index(idx) => Some(idx),
        })
    }

    /// Returns a path iterator.
    pub fn iter(&'a self) -> impl Iterator<Item = &'a PathItem<'a>> {
        let mut items = Vec::with_capacity(self.0.depth);

        for state in self.0.iter() {
            if let Some(ref path) = state.path {
                items.push(path)
            }
        }

        items.into_iter().rev()
    }
}

impl<'a> fmt::Display for Path<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
