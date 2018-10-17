//! Event meta data.

use std::borrow;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::iter::FromIterator;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::de::{self, Deserialize, Deserializer, IgnoredAny};
use serde::ser::{Serialize, SerializeSeq, Serializer};
use serde_json;

/// Internal synchronization for meta data serialization.
thread_local!(static SERIALIZE_META: AtomicBool = AtomicBool::new(false));

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
        skip_serializing_if = "Vec::is_empty",
        rename = "rem"
    )]
    pub remarks: Vec<Remark>,

    /// Errors that happened during deserialization or processing.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        rename = "err"
    )]
    pub errors: Vec<String>,

    /// The original length of modified text fields or collections.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "len"
    )]
    pub original_length: Option<u32>,
}

impl PartialEq for Meta {
    fn eq(&self, other: &Self) -> bool {
        self.remarks == other.remarks
            && self.errors == other.errors
            && self.original_length == other.original_length
    }
}

impl Meta {
    /// Creates a new meta data object from an error message.
    pub fn from_error<S: Into<String>>(message: S) -> Self {
        Meta {
            remarks: Vec::new(),
            errors: vec![message.into()],
            original_length: None,
        }
    }

    /// The original length of this field, if applicable.
    pub fn original_length(&self) -> Option<usize> {
        self.original_length.map(|x| x as usize)
    }

    /// Updates the original length of this annotation.
    pub fn set_original_length(&mut self, original_length: Option<u32>) {
        self.original_length = original_length;
    }

    /// Iterates all remarks on this field.
    pub fn remarks(&self) -> impl Iterator<Item = &Remark> {
        self.remarks.iter()
    }

    /// Mutable reference to remarks of this field.
    pub fn remarks_mut(&mut self) -> &mut Vec<Remark> {
        &mut self.remarks
    }

    /// Indicates whether this field has remarks.
    pub fn has_remarks(&self) -> bool {
        !self.remarks.is_empty()
    }

    /// Iterates errors on this field.
    pub fn errors(&self) -> impl Iterator<Item = &str> {
        self.errors.iter().map(|x| x.as_str())
    }

    /// Mutable reference to errors of this field.
    pub fn errors_mut(&mut self) -> &mut Vec<String> {
        &mut self.errors
    }

    /// Indicates whether this field has errors.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Indicates that a null value is permitted for this field.
    pub fn null_is_valid(&self) -> bool {
        self.has_errors() || self.has_remarks()
    }

    /// Indicates whether this field has meta data attached.
    pub fn is_empty(&self) -> bool {
        self.original_length.is_none() && self.remarks.is_empty() && self.errors.is_empty()
    }
}

impl Default for Meta {
    fn default() -> Meta {
        Meta {
            remarks: Vec::new(),
            errors: Vec::new(),
            original_length: None,
        }
    }
}

/// Wrapper for data fields with optional meta data.
#[derive(Debug, PartialEq, Clone)]
pub struct Annotated<T>(pub Option<T>, pub Meta);

impl<T: Default> Default for Annotated<T> {
    fn default() -> Self {
        Annotated(Some(T::default()), Default::default())
    }
}

impl<T> Annotated<T> {
    pub fn require_value(self) -> Annotated<T> {
        // TODO: write error if needed to meta
        self
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    String(String),
    Array(Vec<Annotated<Value>>),
    Object(BTreeMap<String, Annotated<Value>>),
}
