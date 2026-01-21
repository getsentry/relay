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
    OurLog,
    TraceMetric,
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
    ValueType::OurLog => "log",
    ValueType::TraceMetric => "trace_metric",

    ValueType::Span => "span",
    ValueType::ClientSdkInfo => "sdk",
    ValueType::Minidump => "minidump",
    ValueType::HeapMemory => "heap_memory",
    ValueType::StackMemory => "stack_memory",
});

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

/// A static or dynamic `Pii` value.
#[derive(Debug, Clone, Copy)]
pub enum PiiMode {
    /// A static value.
    Static(Pii),
    /// A dynamic value, computed based on a `ProcessingState`.
    Dynamic(fn(&ProcessingState) -> Pii),
}

/// A static or dynamic Option<`usize`> value.
///
/// Used for the fields `max_chars` and `max_bytes`.
#[derive(Debug, Clone, Copy)]
pub enum SizeMode {
    Static(Option<usize>),
    Dynamic(fn(&ProcessingState) -> Option<usize>),
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
    pub max_chars: SizeMode,
    /// The extra char length allowance on top of max_chars.
    pub max_chars_allowance: usize,
    /// The maximum depth of this field.
    pub max_depth: Option<usize>,
    /// The maximum number of bytes of this field.
    pub max_bytes: SizeMode,
    /// The type of PII on the field.
    pub pii: PiiMode,
    /// Whether additional properties should be retained during normalization.
    pub retain: bool,
    /// Whether the trimming processor is allowed to shorten or drop this field.
    pub trim: bool,
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
            max_chars: SizeMode::Static(None),
            max_chars_allowance: 0,
            max_depth: None,
            max_bytes: SizeMode::Static(None),
            pii: PiiMode::Static(Pii::False),
            retain: false,
            trim: true,
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
        self.pii = PiiMode::Static(pii);
        self
    }

    /// Sets whether this field contains PII dynamically based on the current state.
    pub const fn pii_dynamic(mut self, pii: fn(&ProcessingState) -> Pii) -> Self {
        self.pii = PiiMode::Dynamic(pii);
        self
    }

    /// Sets the maximum number of characters allowed in the field.
    pub const fn max_chars(mut self, max_chars: usize) -> Self {
        self.max_chars = SizeMode::Static(Some(max_chars));
        self
    }

    /// Sets the maximum number of characters allowed in the field dynamically based on the current state.
    pub const fn max_chars_dynamic(
        mut self,
        max_chars: fn(&ProcessingState) -> Option<usize>,
    ) -> Self {
        self.max_chars = SizeMode::Dynamic(max_chars);
        self
    }

    /// Sets the maximum number of bytes allowed in the field.
    pub const fn max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = SizeMode::Static(Some(max_bytes));
        self
    }

    /// Sets the maximum number of bytes allowed in the field dynamically based on the current state.
    pub const fn max_bytes_dynamic(
        mut self,
        max_bytes: fn(&ProcessingState) -> Option<usize>,
    ) -> Self {
        self.max_bytes = SizeMode::Dynamic(max_bytes);
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

impl PathItem<'_> {
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

impl fmt::Display for PathItem<'_> {
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
        match self.pii() {
            Pii::True => Some(Cow::Borrowed(&PII_TRUE_FIELD_ATTRS)),
            Pii::False => None,
            Pii::Maybe => Some(Cow::Borrowed(&PII_MAYBE_FIELD_ATTRS)),
        }
    }

    /// Returns the PII status for this state.
    ///
    /// If the state's `FieldAttrs` contain a fixed PII status,
    /// it is returned. If they contain a dynamic PII status (a function),
    /// it is applied to this state and the output returned.
    pub fn pii(&self) -> Pii {
        match self.attrs().pii {
            PiiMode::Static(pii) => pii,
            PiiMode::Dynamic(pii_fn) => pii_fn(self),
        }
    }

    /// Returns the max bytes for this state.
    ///
    /// If the state's `FieldAttrs` contain a fixed `max_bytes` value,
    /// it is returned. If they contain a dynamic `max_bytes` value (a function),
    /// it is applied to this state and the output returned.
    pub fn max_bytes(&self) -> Option<usize> {
        match self.attrs().max_bytes {
            SizeMode::Static(n) => n,
            SizeMode::Dynamic(max_bytes_fn) => max_bytes_fn(self),
        }
    }

    /// Returns the max chars for this state.
    ///
    /// If the state's `FieldAttrs` contain a fixed `max_chars` value,
    /// it is returned. If they contain a dynamic `max_chars` value (a function),
    /// it is applied to this state and the output returned.
    pub fn max_chars(&self) -> Option<usize> {
        match self.attrs().max_chars {
            SizeMode::Static(n) => n,
            SizeMode::Dynamic(max_chars_fn) => max_chars_fn(self),
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
    #[expect(
        clippy::result_large_err,
        reason = "this method returns `self` in the error case"
    )]
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

    /// Returns an iterator over the "keys" in this state,
    /// in order from right to left (or innermost state to outermost).
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.iter()
            .filter_map(|state| state.path_item.as_ref())
            .flat_map(|item| item.key())
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

impl ExactSizeIterator for ProcessingStateIter<'_> {}

impl Default for ProcessingState<'_> {
    fn default() -> Self {
        ProcessingState::root().clone()
    }
}

/// Represents the [`ProcessingState`] as a path.
///
/// This is a view of a [`ProcessingState`] which treats the stack of states as a path.
#[derive(Debug)]
pub struct Path<'a>(&'a ProcessingState<'a>);

impl Path<'_> {
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

    /// Returns the PII status for this path.
    pub fn pii(&self) -> Pii {
        self.0.pii()
    }

    /// Iterates through the states in this path.
    pub fn iter(&self) -> ProcessingStateIter<'_> {
        self.0.iter()
    }
}

impl fmt::Display for Path<'_> {
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

#[cfg(test)]
mod tests {

    use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, SerializableAnnotated};

    use crate::processor::attrs::ROOT_STATE;
    use crate::processor::{Pii, ProcessValue, ProcessingState, Processor, process_value};

    fn pii_from_item_name(state: &ProcessingState) -> Pii {
        match state.path_item().and_then(|p| p.key()) {
            Some("true_item") => Pii::True,
            Some("false_item") => Pii::False,
            _ => Pii::Maybe,
        }
    }

    fn max_chars_from_item_name(state: &ProcessingState) -> Option<usize> {
        match state.path_item().and_then(|p| p.key()) {
            Some("short_item") => Some(10),
            Some("long_item") => Some(20),
            _ => None,
        }
    }

    #[derive(Debug, Clone, Empty, IntoValue, FromValue, ProcessValue)]
    #[metastructure(pii = "pii_from_item_name")]
    struct TestValue(#[metastructure(max_chars = "max_chars_from_item_name")] String);

    struct TestPiiProcessor;

    impl Processor for TestPiiProcessor {
        fn process_string(
            &mut self,
            value: &mut String,
            _meta: &mut relay_protocol::Meta,
            state: &ProcessingState<'_>,
        ) -> crate::processor::ProcessingResult where {
            match state.pii() {
                Pii::True => *value = "true".to_owned(),
                Pii::False => *value = "false".to_owned(),
                Pii::Maybe => *value = "maybe".to_owned(),
            }
            Ok(())
        }
    }

    struct TestTrimmingProcessor;

    impl Processor for TestTrimmingProcessor {
        fn process_string(
            &mut self,
            value: &mut String,
            _meta: &mut relay_protocol::Meta,
            state: &ProcessingState<'_>,
        ) -> crate::processor::ProcessingResult where {
            if let Some(n) = state.max_chars() {
                value.truncate(n);
            }
            Ok(())
        }
    }

    #[test]
    fn test_dynamic_pii() {
        let mut object: Annotated<Object<TestValue>> = Annotated::from_json(
            r#"
        {
          "false_item": "replace me",
          "other_item": "replace me",
          "true_item": "replace me"
        }
        "#,
        )
        .unwrap();

        process_value(&mut object, &mut TestPiiProcessor, &ROOT_STATE).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&object), @r###"
        {
          "false_item": "false",
          "other_item": "maybe",
          "true_item": "true"
        }
        "###);
    }

    #[test]
    fn test_dynamic_max_chars() {
        let mut object: Annotated<Object<TestValue>> = Annotated::from_json(
            r#"
        {
          "short_item": "Should be shortened to 10",
          "long_item": "Should be shortened to 20",
          "other_item": "Should not be shortened at all"
        }
        "#,
        )
        .unwrap();

        process_value(&mut object, &mut TestTrimmingProcessor, &ROOT_STATE).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&object), @r###"
        {
          "long_item": "Should be shortened ",
          "other_item": "Should not be shortened at all",
          "short_item": "Should be "
        }
        "###);
    }
}
