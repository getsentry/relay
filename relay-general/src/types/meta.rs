use std::fmt;
use std::str::FromStr;

use serde::{de, ser::SerializeSeq, Deserialize, Deserializer, Serialize, Serializer};
use smallvec::SmallVec;

use crate::processor::estimate_size;
use crate::types::{IntoValue, Map, Value};

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
#[derive(Clone, Debug, PartialEq, Eq)]
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

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
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
                while let Some(de::IgnoredAny) = seq.next_element()? {}

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

/// The kind of an `Error`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// The data does not fit the schema or is semantically incorrect.
    InvalidData,

    /// The structure is missing a required attribute.
    MissingAttribute,

    /// The attribute is not allowed in this structure.
    InvalidAttribute,

    /// This value was too long and removed entirely.
    ValueTooLong,

    /// Clock-drift of the SDK has been corrected in all timestamps.
    ClockDrift,

    /// The timestamp is too old.
    PastTimestamp,

    /// The timestamp lies in the future, likely due to clock drift.
    FutureTimestamp,

    /// Any other unknown error for forward compatibility.
    Unknown(String),
}

impl ErrorKind {
    /// Parses an error kind from a `&str` or `String`.
    fn parse<S>(string: S) -> Self
    where
        S: AsRef<str> + Into<String>,
    {
        match string.as_ref() {
            "invalid_data" => ErrorKind::InvalidData,
            "missing_attribute" => ErrorKind::MissingAttribute,
            "invalid_attribute" => ErrorKind::InvalidAttribute,
            "value_too_long" => ErrorKind::ValueTooLong,
            "past_timestamp" => ErrorKind::PastTimestamp,
            "future_timestamp" => ErrorKind::FutureTimestamp,
            _ => ErrorKind::Unknown(string.into()),
        }
    }

    /// Returns the string representation of this error kind.
    pub fn as_str(&self) -> &str {
        match self {
            ErrorKind::InvalidData => "invalid_data",
            ErrorKind::MissingAttribute => "missing_attribute",
            ErrorKind::InvalidAttribute => "invalid_attribute",
            ErrorKind::ValueTooLong => "value_too_long",
            ErrorKind::PastTimestamp => "past_timestamp",
            ErrorKind::FutureTimestamp => "future_timestamp",
            ErrorKind::ClockDrift => "clock_drift",
            ErrorKind::Unknown(error) => error,
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<String> for ErrorKind {
    fn from(string: String) -> Self {
        ErrorKind::parse(string)
    }
}

impl<'a> From<&'a str> for ErrorKind {
    fn from(string: &'a str) -> Self {
        ErrorKind::parse(string)
    }
}

impl FromStr for ErrorKind {
    type Err = ();

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(ErrorKind::from(string))
    }
}

impl<'de> Deserialize<'de> for ErrorKind {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ErrorKindVisitor;

        impl<'de> de::Visitor<'de> for ErrorKindVisitor {
            type Value = ErrorKind;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "an error kind")
            }

            fn visit_str<E>(self, string: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(ErrorKind::from(string))
            }

            fn visit_string<E>(self, string: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(ErrorKind::from(string))
            }
        }

        deserializer.deserialize_str(ErrorKindVisitor)
    }
}

impl Serialize for ErrorKind {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// An error with an enumerable kind and optional data.
#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    kind: ErrorKind,
    data: Map<String, Value>,
}

impl Error {
    /// Creates a new error with the given data.
    #[inline]
    fn with_data(kind: ErrorKind, data: Map<String, Value>) -> Self {
        Error { kind, data }
    }

    /// Creates a new error without data.
    #[inline]
    pub fn new(kind: ErrorKind) -> Self {
        Error::with_data(kind, Map::default())
    }

    /// Creates a new error and allows instant modification in a function.
    #[inline]
    pub fn with<F>(kind: ErrorKind, f: F) -> Self
    where
        F: FnOnce(&mut Self),
    {
        let mut error = Error::new(kind);
        f(&mut error);
        error
    }

    /// Creates an invalid data error with a plain text reason.
    pub fn invalid<S>(reason: S) -> Self
    where
        S: std::fmt::Display,
    {
        Error::with(ErrorKind::InvalidData, |error| {
            error.insert("reason", reason.to_string());
        })
    }

    /// Creates an error that describes an invalid value.
    pub fn expected(expectation: &str) -> Self {
        // Does not use `Error::invalid` to avoid the string copy.
        Error::with(ErrorKind::InvalidData, |error| {
            error.insert("reason", format!("expected {}", expectation));
        })
    }

    /// Creates an error that describes an expected non-empty value.
    pub fn nonempty() -> Self {
        // TODO: Replace `invalid_data` this with an explicity error constant for empty values
        Error::invalid("expected a non-empty value")
    }

    /// Returns the kind of this error.
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    /// Returns an iterator over the data of this error.
    pub fn data(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.data.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Inserts a new key into the data bag of this error.
    pub fn insert<K, V>(&mut self, key: K, value: V) -> Option<Value>
    where
        K: Into<String>,
        V: Into<Value>,
    {
        self.data.insert(key.into(), value.into())
    }

    /// Retrieves a key from the data bag of this error.
    pub fn get<K>(&self, key: K) -> Option<&Value>
    where
        K: AsRef<str>,
    {
        self.data.get(key.as_ref())
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Error::new(kind)
    }
}

impl<'de> Deserialize<'de> for Error {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ErrorVisitor;

        impl<'de> de::Visitor<'de> for ErrorVisitor {
            type Value = Error;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a meta remark")
            }

            fn visit_str<E>(self, string: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Error::new(ErrorKind::from(string)))
            }

            fn visit_string<E>(self, string: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Error::new(ErrorKind::from(string)))
            }

            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let kind = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::custom("missing error kind"))?;
                let data = seq.next_element()?.unwrap_or_default();

                // Drain the sequence
                while let Some(de::IgnoredAny) = seq.next_element()? {}

                Ok(Error::with_data(kind, data))
            }
        }

        deserializer.deserialize_any(ErrorVisitor)
    }
}

impl Serialize for Error {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.data.is_empty() {
            return self.kind.serialize(serializer);
        }

        let mut seq = serializer.serialize_seq(None)?;
        seq.serialize_element(&self.kind)?;
        seq.serialize_element(&self.data)?;
        seq.end()
    }
}

/// Meta information for a data field in the event payload.
#[derive(Clone, Deserialize, Serialize)]
pub struct MetaInner {
    /// Remarks detailling modifications of this field.
    #[serde(default, skip_serializing_if = "SmallVec::is_empty", rename = "rem")]
    remarks: SmallVec<[Remark; 3]>,

    /// Errors that happened during normalization or processing.
    #[serde(default, skip_serializing_if = "SmallVec::is_empty", rename = "err")]
    errors: SmallVec<[Error; 3]>,

    /// The original length of modified text fields or collections.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "len")]
    original_length: Option<u32>,

    /// In some cases the original value might be sent along.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "val")]
    original_value: Option<Value>,
}

impl MetaInner {
    pub fn is_empty(&self) -> bool {
        self.original_length.is_none()
            && self.remarks.is_empty()
            && self.errors.is_empty()
            && self.original_value.is_none()
    }
}

/// Meta information for a data field in the event payload.
#[derive(Clone, Default, Serialize)]
pub struct Meta(Option<Box<MetaInner>>);

impl fmt::Debug for Meta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Meta")
            .field("remarks", &self.remarks())
            .field("errors", &self.errors())
            .field("original_length", &self.original_length())
            .field("original_value", &self.original_value())
            .finish()
    }
}

impl<'de> Deserialize<'de> for Meta {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Meta, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(match <Option<MetaInner>>::deserialize(deserializer)? {
            Some(value) => {
                if value.is_empty() {
                    Meta(None)
                } else {
                    Meta(Some(Box::new(value)))
                }
            }
            None => Meta(None),
        })
    }
}

impl Meta {
    /// From an error
    pub fn from_error<E: Into<Error>>(err: E) -> Self {
        let mut meta = Self::default();
        meta.add_error(err);
        meta
    }

    fn upsert(&mut self) -> &mut MetaInner {
        self.0.get_or_insert_with(|| Box::new(MetaInner::default()))
    }

    /// The original length of this field, if applicable.
    pub fn original_length(&self) -> Option<usize> {
        self.0
            .as_ref()
            .and_then(|x| x.original_length.map(|x| x as usize))
    }

    /// Updates the original length of this annotation.
    pub fn set_original_length(&mut self, original_length: Option<usize>) {
        let inner = self.upsert();
        if inner.original_length.is_none() {
            inner.original_length = original_length.map(|x| x as u32);
        }
    }

    fn remarks(&self) -> &[Remark] {
        match self.0 {
            Some(ref inner) => &inner.remarks[..],
            None => &[][..],
        }
    }

    /// Iterates all remarks on this field.
    pub fn iter_remarks(&self) -> impl Iterator<Item = &Remark> {
        self.remarks().iter()
    }

    /// Indicates whether this field has remarks.
    pub fn has_remarks(&self) -> bool {
        self.0.as_ref().map_or(false, |x| x.remarks.is_empty())
    }

    /// Clears all remarks
    pub fn clear_remarks(&mut self) {
        if let Some(ref mut inner) = self.0 {
            inner.remarks.clear();
        }
    }

    /// Adds a remark.
    pub fn add_remark(&mut self, remark: Remark) {
        self.upsert().remarks.push(remark);
    }

    fn errors(&self) -> &[Error] {
        match self.0 {
            Some(ref inner) => &inner.errors[..],
            None => &[][..],
        }
    }

    /// Iterates errors on this field.
    pub fn iter_errors(&self) -> impl Iterator<Item = &Error> {
        self.errors().iter()
    }

    /// Mutable reference to errors of this field.
    pub fn add_error<E: Into<Error>>(&mut self, err: E) {
        let errors = &mut self.upsert().errors;
        let err = err.into();
        if errors.contains(&err) {
            return;
        }
        errors.push(err);
    }

    /// Returns a reference to the original value, if any.
    pub fn original_value(&self) -> Option<&Value> {
        self.0.as_ref().and_then(|x| x.original_value.as_ref())
    }

    /// Sets the original value.
    pub fn set_original_value<T>(&mut self, original_value: Option<T>)
    where
        T: IntoValue,
    {
        // XXX: Since metadata is currently not subject to trimming, only allow really small values
        // in original_value for now.
        if estimate_size(original_value.as_ref()) < 500 {
            self.upsert().original_value = original_value.map(IntoValue::into_value);
        }
    }

    /// Take out the original value.
    pub fn take_original_value(&mut self) -> Option<Value> {
        self.0.as_mut().and_then(|x| x.original_value.take())
    }

    /// Indicates whether this field has errors.
    pub fn has_errors(&self) -> bool {
        self.0.as_ref().map_or(false, |x| !x.errors.is_empty())
    }

    /// Indicates whether this field has meta data attached.
    pub fn is_empty(&self) -> bool {
        self.0.as_ref().map_or(true, |x| x.is_empty())
    }

    /// Merges this meta with another one.
    pub fn merge(mut self, other: Self) -> Self {
        if let Some(other_inner) = other.0 {
            let other_inner = *other_inner;
            let inner = self.upsert();
            inner.remarks.extend(other_inner.remarks.into_iter());
            inner.errors.extend(other_inner.errors.into_iter());
            if inner.original_length.is_none() {
                inner.original_length = other_inner.original_length;
            }
            if inner.original_value.is_none() {
                inner.original_value = other_inner.original_value;
            }
        }
        self
    }
}

impl Default for MetaInner {
    fn default() -> Self {
        MetaInner {
            remarks: SmallVec::new(),
            errors: SmallVec::new(),
            original_length: None,
            original_value: None,
        }
    }
}

impl PartialEq for MetaInner {
    fn eq(&self, other: &Self) -> bool {
        self.remarks == other.remarks
            && self.errors == other.errors
            && self.original_length == other.original_length
            && self.original_value == other.original_value
    }
}

impl PartialEq for Meta {
    fn eq(&self, other: &Self) -> bool {
        if self.is_empty() && other.is_empty() {
            true
        } else {
            match (self.0.as_ref(), other.0.as_ref()) {
                (Some(a), Some(b)) => a == b,
                _ => false,
            }
        }
    }
}
