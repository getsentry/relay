use std::borrow::Cow;
use std::fmt;
use std::ops::{Deref, RangeInclusive};

use enumset::{EnumSet, EnumSetType};
use relay_protocol::Annotated;

use crate::processor::ProcessValue;

/// Error for unknown value types.
#[derive(Debug)]
pub struct UnknownValueTypeError;

impl fmt::Display for UnknownValueTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown value type")
    }
}

impl std::error::Error for UnknownValueTypeError {}

/// The (simplified) type of a value.
#[derive(Debug, Ord, PartialOrd, EnumSetType)]
pub enum ValueType {
    // Basic types
    String,
    Binary,
    Number,
    Boolean,
    DateTime,
    Array,
    Object,

    // Roots
    Event,
    Attachments,
    Replay,

    // Protocol types
    Exception,
    Stacktrace,
    Frame,
    Request,
    User,
    LogEntry,
    Message,
    Thread,
    Breadcrumb,
    Span,
    ClientSdkInfo,

    // Attachments and Contents
    Minidump,
    HeapMemory,
    StackMemory,
}

impl ValueType {
    pub fn for_field<T: ProcessValue>(field: &Annotated<T>) -> EnumSet<Self> {
        field
            .value()
            .map(ProcessValue::value_type)
            .unwrap_or_else(EnumSet::empty)
    }
}

relay_common::derive_fromstr_and_display!(ValueType, UnknownValueTypeError, {
    ValueType::String => "string",
    ValueType::Binary => "binary",
    ValueType::Number => "number",
    ValueType::Boolean => "boolean" | "bool",
    ValueType::DateTime => "datetime",
    ValueType::Array => "array" | "list",
    ValueType::Object => "object",
    ValueType::Event => "event",
    ValueType::Attachments => "attachments",
    ValueType::Replay => "replay",
    ValueType::Exception => "error" | "exception",
    ValueType::Stacktrace => "stack" | "stacktrace",
    ValueType::Frame => "frame",
    ValueType::Request => "http" | "request",
    ValueType::User => "user",
    ValueType::LogEntry => "logentry",
    ValueType::Message => "message",
    ValueType::Thread => "thread",
    ValueType::Breadcrumb => "breadcrumb",
    ValueType::Span => "span",
    ValueType::ClientSdkInfo => "sdk",
    ValueType::Minidump => "minidump",
    ValueType::HeapMemory => "heap_memory",
    ValueType::StackMemory => "stack_memory",
});

/// The maximum length of a field.
#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum MaxChars {
    Hash,
    EnumLike,
    Summary,
    Message,
    Symbol,
    Path,
    ShortPath,
    Logger,
    Email,
    Culprit,
    TagKey,
    TagValue,
    Environment,
    Distribution,
    Hard(usize),
    Soft(usize),
}

impl MaxChars {
    /// The cap in number of unicode characters.
    pub fn limit(self) -> usize {
        match self {
            MaxChars::Hash => 128,
            MaxChars::EnumLike => 128,
            MaxChars::Summary => 1024,
            MaxChars::Message => 8192,
            MaxChars::Symbol => 256,
            MaxChars::Path => 256,
            MaxChars::ShortPath => 128,
            // These limits below were initially derived from Sentry's original implementation in
            // Python. Before changing any of these limits, check if the values are inserted into
            // the database by the Sentry processing pipeline, and if there are limits on the
            // database column.
            MaxChars::Logger => 64,       // src/sentry/constants.py:???
            MaxChars::Email => 75,        // src/sentry/constants.py:MAX_EMAIL_FIELD_LENGTH
            MaxChars::Culprit => 200,     // src/sentry/constants.py:MAX_CULPRIT_LENGTH
            MaxChars::TagKey => 200,      // src/sentry/constants.py:MAX_TAG_KEY_LENGTH
            MaxChars::TagValue => 200,    // src/sentry/constants.py:MAX_TAG_VALUE_LENGTH
            MaxChars::Environment => 64,  // src/sentry/constants.py:ENVIRONMENT_NAME_MAX_LENGTH
            MaxChars::Distribution => 64, // src/sentry/models/distribution.py
            MaxChars::Soft(len) | MaxChars::Hard(len) => len,
        }
    }

    /// The number of extra characters permitted.
    pub fn allowance(self) -> usize {
        match self {
            MaxChars::Hash => 0,
            MaxChars::EnumLike => 0,
            MaxChars::Summary => 100,
            MaxChars::Message => 200,
            MaxChars::Symbol => 20,
            MaxChars::Path => 40,
            MaxChars::ShortPath => 20,
            MaxChars::Logger => 0,
            MaxChars::Email => 0,
            MaxChars::Culprit => 0,
            MaxChars::TagKey => 0,
            MaxChars::TagValue => 0,
            MaxChars::Environment => 0,
            MaxChars::Distribution => 0,
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
    Larger,
    Massive,
}

impl BagSize {
    /// Maximum depth of the structure.
    pub fn max_depth(self) -> usize {
        match self {
            BagSize::Small => 3,
            BagSize::Medium => 5,
            BagSize::Large => 7,
            BagSize::Larger => 7,
            BagSize::Massive => 7,
        }
    }

    /// Maximum estimated JSON bytes.
    pub fn max_size(self) -> usize {
        match self {
            BagSize::Small => 1024,
            BagSize::Medium => 2048,
            BagSize::Large => 8192,
            BagSize::Larger => 16384,
            BagSize::Massive => 262_144,
        }
    }
}

/// Whether an attribute should be PII-strippable/should be subject to datascrubbers
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum Pii {
    /// The field will be stripped by default
    True,
    /// The field cannot be stripped at all
    False,
    /// The field will only be stripped when addressed with a specific path selector, but generic
    /// selectors such as `$string` do not apply.
    Maybe,
}

/// Meta information about a field.
#[derive(Debug, Clone, Copy)]
pub struct FieldAttrs {
    /// Optionally the name of the field.
    pub name: Option<&'static str>,
    /// If the field is required.
    pub required: bool,
    /// If the field should be non-empty.
    pub nonempty: bool,
    /// Whether to trim whitespace from this string.
    pub trim_whitespace: bool,
    /// A set of allowed or denied character ranges for this string.
    pub characters: Option<CharacterSet>,
    /// The maximum char length of this field.
    pub max_chars: Option<MaxChars>,
    /// The maximum bag size of this field.
    pub bag_size: Option<BagSize>,
    /// The type of PII on the field.
    pub pii: Pii,
    /// Whether additional properties should be retained during normalization.
    pub retain: bool,
}

/// A set of characters allowed or denied for a (string) field.
///
/// Note that this field is generated in the derive, it can't be constructed easily in tests.
#[derive(Clone, Copy)]
pub struct CharacterSet {
    /// Generated in derive for performance. Can be left out when set is created manually.
    pub char_is_valid: fn(char) -> bool,
    /// A set of ranges that are allowed/denied within the character set
    pub ranges: &'static [RangeInclusive<char>],
    /// Whether the character set is inverted
    pub is_negative: bool,
}

impl fmt::Debug for CharacterSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CharacterSet")
            .field("ranges", &self.ranges)
            .field("is_negative", &self.is_negative)
            .finish()
    }
}

impl FieldAttrs {
    /// Creates default `FieldAttrs`.
    pub const fn new() -> Self {
        FieldAttrs {
            name: None,
            required: false,
            nonempty: false,
            trim_whitespace: false,
            characters: None,
            max_chars: None,
            bag_size: None,
            pii: Pii::False,
            retain: false,
        }
    }

    /// Sets whether a value in this field is required.
    pub const fn required(mut self, required: bool) -> Self {
        self.required = required;
        self
    }

    /// Sets whether this field can have an empty value.
    ///
    /// This is distinct from `required`. An empty string (`""`) passes the "required" check but not the
    /// "nonempty" one.
    pub const fn nonempty(mut self, nonempty: bool) -> Self {
        self.nonempty = nonempty;
        self
    }

    /// Sets whether whitespace should be trimmed before validation.
    pub const fn trim_whitespace(mut self, trim_whitespace: bool) -> Self {
        self.trim_whitespace = trim_whitespace;
        self
    }

    /// Sets whether this field contains PII.
    pub const fn pii(mut self, pii: Pii) -> Self {
        self.pii = pii;
        self
    }

    /// Sets the maximum number of characters allowed in the field.
    pub const fn max_chars(mut self, max_chars: MaxChars) -> Self {
        self.max_chars = Some(max_chars);
        self
    }

    /// Sets the maximum size of all children in this field.
    ///
    /// This applies particularly to objects and arrays, but also to structures.
    pub const fn bag_size(mut self, bag_size: BagSize) -> Self {
        self.bag_size = Some(bag_size);
        self
    }

    /// Sets whether additional properties should be retained during normalization.
    pub const fn retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }
}

static DEFAULT_FIELD_ATTRS: FieldAttrs = FieldAttrs::new();
static PII_TRUE_FIELD_ATTRS: FieldAttrs = FieldAttrs::new().pii(Pii::True);
static PII_MAYBE_FIELD_ATTRS: FieldAttrs = FieldAttrs::new().pii(Pii::Maybe);

impl Default for FieldAttrs {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Eq, Ord, PartialOrd)]
enum PathItem<'a> {
    StaticKey(&'a str),
    OwnedKey(String),
    Index(usize),
}

impl<'a> PartialEq for PathItem<'a> {
    fn eq(&self, other: &PathItem<'a>) -> bool {
        self.key() == other.key() && self.index() == other.index()
    }
}

impl<'a> PathItem<'a> {
    /// Returns the key if there is one
    #[inline]
    pub fn key(&self) -> Option<&str> {
        match self {
            PathItem::StaticKey(s) => Some(s),
            PathItem::OwnedKey(s) => Some(s.as_str()),
            PathItem::Index(_) => None,
        }
    }

    /// Returns the index if there is one
    #[inline]
    pub fn index(&self) -> Option<usize> {
        match self {
            PathItem::StaticKey(_) | PathItem::OwnedKey(_) => None,
            PathItem::Index(idx) => Some(*idx),
        }
    }
}

impl<'a> fmt::Display for PathItem<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PathItem::StaticKey(s) => f.pad(s),
            PathItem::OwnedKey(s) => f.pad(s.as_str()),
            PathItem::Index(val) => write!(f, "{val}"),
        }
    }
}

/// Like [`std::borrow::Cow`], but with a boxed value.
///
/// This is useful for types that contain themselves, where otherwise the layout of the type
/// cannot be computed, for example
///
/// ```rust,ignore
/// struct Foo<'a>(Cow<'a, Foo<'a>>); // will not compile
/// struct Bar<'a>(BoxCow<'a, Bar<'a>>); // will compile
/// ```
#[derive(Debug, Clone)]
enum BoxCow<'a, T> {
    Borrowed(&'a T),
    Owned(Box<T>),
}

impl<T> Deref for BoxCow<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            BoxCow::Borrowed(inner) => inner,
            BoxCow::Owned(inner) => inner.deref(),
        }
    }
}

/// An event's processing state.
///
/// The processing state describes an item in an event which is being processed, an example
/// of processing might be scrubbing the event for PII.  The processing state itself
/// describes the current item and it's parent, which allows you to follow all the items up
/// to the root item.  You can think of processing an event as a visitor pattern visiting
/// all items in the event and the processing state is a stack describing the currently
/// visited item and all it's parents.
#[derive(Debug, Clone)]
pub struct ProcessingState<'a> {
    // In event scrubbing, every state holds a reference to its parent.
    // In Replay scrubbing, we do not call `process_*` recursively,
    // but instead hold a single `ProcessingState` that represents the current item.
    // This item owns its parent (plus ancestors) exclusively, which is why we use `BoxCow` here
    // rather than `Rc` / `Arc`.
    parent: Option<BoxCow<'a, ProcessingState<'a>>>,
    path_item: Option<PathItem<'a>>,
    attrs: Option<Cow<'a, FieldAttrs>>,
    value_type: EnumSet<ValueType>,
    depth: usize,
}

static ROOT_STATE: ProcessingState = ProcessingState {
    parent: None,
    path_item: None,
    attrs: None,
    value_type: enumset::enum_set!(),
    depth: 0,
};

impl<'a> ProcessingState<'a> {
    /// Returns the root processing state.
    pub fn root() -> &'static ProcessingState<'static> {
        &ROOT_STATE
    }

    /// Creates a new root state.
    pub fn new_root(
        attrs: Option<Cow<'static, FieldAttrs>>,
        value_type: impl IntoIterator<Item = ValueType>,
    ) -> ProcessingState<'static> {
        ProcessingState {
            parent: None,
            path_item: None,
            attrs,
            value_type: value_type.into_iter().collect(),
            depth: 0,
        }
    }

    /// Derives a processing state by entering a static key.
    pub fn enter_static(
        &'a self,
        key: &'static str,
        attrs: Option<Cow<'static, FieldAttrs>>,
        value_type: impl IntoIterator<Item = ValueType>,
    ) -> Self {
        ProcessingState {
            parent: Some(BoxCow::Borrowed(self)),
            path_item: Some(PathItem::StaticKey(key)),
            attrs,
            value_type: value_type.into_iter().collect(),
            depth: self.depth + 1,
        }
    }

    /// Derives a processing state by entering a borrowed key.
    pub fn enter_borrowed(
        &'a self,
        key: &'a str,
        attrs: Option<Cow<'a, FieldAttrs>>,
        value_type: impl IntoIterator<Item = ValueType>,
    ) -> Self {
        ProcessingState {
            parent: Some(BoxCow::Borrowed(self)),
            path_item: Some(PathItem::StaticKey(key)),
            attrs,
            value_type: value_type.into_iter().collect(),
            depth: self.depth + 1,
        }
    }

    /// Derives a processing state by entering an owned key.
    ///
    /// The new (child) state takes ownership of the current (parent) state.
    pub fn enter_owned(
        self,
        key: String,
        attrs: Option<Cow<'a, FieldAttrs>>,
        value_type: impl IntoIterator<Item = ValueType>,
    ) -> Self {
        let depth = self.depth + 1;
        ProcessingState {
            parent: Some(BoxCow::Owned(self.into())),
            path_item: Some(PathItem::OwnedKey(key)),
            attrs,
            value_type: value_type.into_iter().collect(),
            depth,
        }
    }

    /// Derives a processing state by entering an index.
    pub fn enter_index(
        &'a self,
        idx: usize,
        attrs: Option<Cow<'a, FieldAttrs>>,
        value_type: impl IntoIterator<Item = ValueType>,
    ) -> Self {
        ProcessingState {
            parent: Some(BoxCow::Borrowed(self)),
            path_item: Some(PathItem::Index(idx)),
            attrs,
            value_type: value_type.into_iter().collect(),
            depth: self.depth + 1,
        }
    }

    /// Derives a processing state without adding a path segment. Useful in newtype structs.
    pub fn enter_nothing(&'a self, attrs: Option<Cow<'a, FieldAttrs>>) -> Self {
        ProcessingState {
            attrs,
            path_item: None,
            parent: Some(BoxCow::Borrowed(self)),
            ..self.clone()
        }
    }

    /// Returns the path in the processing state.
    pub fn path(&'a self) -> Path<'a> {
        Path(self)
    }

    pub fn value_type(&self) -> EnumSet<ValueType> {
        self.value_type
    }

    /// Returns the field attributes.
    pub fn attrs(&self) -> &FieldAttrs {
        match self.attrs {
            Some(ref cow) => cow,
            None => &DEFAULT_FIELD_ATTRS,
        }
    }

    /// Derives the attrs for recursion.
    pub fn inner_attrs(&self) -> Option<Cow<'_, FieldAttrs>> {
        match self.attrs().pii {
            Pii::True => Some(Cow::Borrowed(&PII_TRUE_FIELD_ATTRS)),
            Pii::False => None,
            Pii::Maybe => Some(Cow::Borrowed(&PII_MAYBE_FIELD_ATTRS)),
        }
    }

    /// Iterates through this state and all its ancestors up the hierarchy.
    ///
    /// This starts at the top of the stack of processing states and ends at the root.  Thus
    /// the first item returned is the currently visited leaf of the event structure.
    pub fn iter(&'a self) -> ProcessingStateIter<'a> {
        ProcessingStateIter {
            state: Some(self),
            size: self.depth,
        }
    }

    /// Returns the contained parent state.
    ///
    /// - Returns `Ok(None)` if the current state is the root.
    /// - Returns `Err(self)` if the current state does not own the parent state.
    pub fn try_into_parent(self) -> Result<Option<Self>, Self> {
        match self.parent {
            Some(BoxCow::Borrowed(_)) => Err(self),
            Some(BoxCow::Owned(parent)) => Ok(Some(*parent)),
            None => Ok(None),
        }
    }

    /// Return the depth (~ indentation level) of the currently processed value.
    pub fn depth(&'a self) -> usize {
        self.depth
    }

    /// Return whether the depth changed between parent and self.
    ///
    /// This is `false` when we entered a newtype struct.
    pub fn entered_anything(&'a self) -> bool {
        if let Some(parent) = &self.parent {
            parent.depth() != self.depth()
        } else {
            true
        }
    }

    /// Returns the last path item if there is one. Skips over "dummy" path segments that exist
    /// because of newtypes.
    #[inline]
    fn path_item(&self) -> Option<&PathItem<'_>> {
        for state in self.iter() {
            if let Some(ref path_item) = state.path_item {
                return Some(path_item);
            }
        }
        None
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
        self.state = current.parent.as_deref();
        Some(current)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size))
    }
}

impl<'a> ExactSizeIterator for ProcessingStateIter<'a> {}

impl<'a> Default for ProcessingState<'a> {
    fn default() -> Self {
        ProcessingState::root().clone()
    }
}

/// Represents the [`ProcessingState`] as a path.
///
/// This is a view of a [`ProcessingState`] which treats the stack of states as a path.
#[derive(Debug)]
pub struct Path<'a>(&'a ProcessingState<'a>);

impl<'a> Path<'a> {
    /// Returns the current key if there is one
    #[inline]
    pub fn key(&self) -> Option<&str> {
        PathItem::key(self.0.path_item()?)
    }

    /// Returns the current index if there is one
    #[inline]
    pub fn index(&self) -> Option<usize> {
        PathItem::index(self.0.path_item()?)
    }

    /// Return the depth (~ indentation level) of the currently processed value.
    pub fn depth(&self) -> usize {
        self.0.depth()
    }

    /// Returns the field attributes of the current path item.
    pub fn attrs(&self) -> &FieldAttrs {
        self.0.attrs()
    }

    /// Iterates through the states in this path.
    pub fn iter(&self) -> ProcessingStateIter<'_> {
        self.0.iter()
    }
}

impl<'a> fmt::Display for Path<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut items = Vec::with_capacity(self.0.depth);
        for state in self.0.iter() {
            if let Some(ref path_item) = state.path_item {
                items.push(path_item)
            }
        }

        for (idx, item) in items.into_iter().rev().enumerate() {
            if idx > 0 {
                write!(f, ".")?;
            }
            write!(f, "{item}")?;
        }
        Ok(())
    }
}
