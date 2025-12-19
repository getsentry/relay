use std::fmt;
use std::ops::Deref;

use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, SkipSerialization, Value};
use serde::Serializer;

use crate::processor::ProcessValue;
use crate::protocol::{Attributes, Timestamp, TraceId};

use uuid::Uuid;

/// Metadata for a trace attachment.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct TraceAttachmentMeta {
    /// The ID of the trace that the attachment belongs to.
    #[metastructure(required = true, nonempty = true, trim = false)]
    pub trace_id: Annotated<TraceId>,

    /// Unique identifier for this attachment.
    #[metastructure(required = true, nonempty = true, trim = false)]
    pub attachment_id: Annotated<AttachmentId>,

    /// Timestamp when the attachment was created.
    #[metastructure(required = true, trim = false)]
    pub timestamp: Annotated<Timestamp>,

    /// Original filename of the attachment.
    #[metastructure(pii = "true", max_chars = 256, max_chars_allowance = 40, trim = false)]
    pub filename: Annotated<String>,

    /// Content type of the attachment body.
    #[metastructure(required = true, max_chars = 128, trim = false)]
    pub content_type: Annotated<String>,

    /// Arbitrary attributes on a trace attachment.
    #[metastructure(pii = "maybe")]
    pub attributes: Annotated<Attributes>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, pii = "maybe")]
    pub other: Object<Value>,
}

#[derive(Clone, Default, PartialEq, Empty, FromValue, ProcessValue)]
pub struct AttachmentId(Uuid);

impl AttachmentId {
    /// Creates a new attachment ID. Only used in tests.
    pub fn random() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Deref for AttachmentId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for AttachmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.as_simple())
    }
}

impl fmt::Debug for AttachmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AttachmentId(\"{}\")", self.0.as_simple())
    }
}

impl IntoValue for AttachmentId {
    fn into_value(self) -> Value
    where
        Self: Sized,
    {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        s.collect_str(self)
    }
}
