use std::fmt;

use serde::de::{self, Deserialize, Deserializer, IgnoredAny};
use serde::ser::{Serialize, SerializeSeq, Serializer};
use serde_derive::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::types::Value;

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
    pub ty: RemarkType,
    pub rule_id: String,
    pub range: Option<Range>,
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

impl<'de> Deserialize<'de> for Remark {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
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

impl Meta {
    /// From an error
    pub fn from_error<S: Into<String>>(err: S, value: Option<Value>) -> Self {
        let mut rv = Self::default();
        rv.add_error(err, value);
        rv
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
    pub fn iter_remarks(&self) -> impl Iterator<Item = &Remark> {
        self.remarks.iter()
    }

    /// Indicates whether this field has remarks.
    pub fn has_remarks(&self) -> bool {
        !self.remarks.is_empty()
    }

    /// Returns a mutable reference to the remarks of this field.
    pub fn remarks_mut(&mut self) -> &mut SmallVec<[Remark; 3]> {
        &mut self.remarks
    }

    /// Adds a remark.
    pub fn add_remark(&mut self, remark: Remark) {
        self.remarks.push(remark);
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
    pub fn merge(mut self, other: Self) -> Self {
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

impl Default for Meta {
    fn default() -> Self {
        Meta {
            remarks: SmallVec::new(),
            errors: SmallVec::new(),
            original_length: None,
            original_value: None,
        }
    }
}

impl PartialEq for Meta {
    fn eq(&self, other: &Self) -> bool {
        self.remarks == other.remarks
            && self.errors == other.errors
            && self.original_length == other.original_length
    }
}
