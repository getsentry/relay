use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;
use crate::protocol::{Attributes, Timestamp};

use uuid::Uuid;

/// Metadata for a span attachment.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct AttachmentV2Meta {
    /// Unique identifier for this attachment.
    #[metastructure(required = true, nonempty = true, trim = false)]
    pub attachment_id: Annotated<Uuid>,

    /// Timestamp when the attachment was created.
    #[metastructure(required = true)]
    pub timestamp: Annotated<Timestamp>,

    /// Original filename of the attachment.
    #[metastructure(pii = "true", max_chars = 256, max_chars_allowance = 40, trim = false)]
    pub filename: Annotated<String>,

    /// Content type of the attachment body.
    #[metastructure(required = true, max_chars = 128, trim = false)]
    pub content_type: Annotated<String>,

    /// Arbitrary attributes on a span attachment.
    #[metastructure(pii = "maybe", trim = false)]
    pub attributes: Annotated<Attributes>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, pii = "maybe")]
    pub other: Object<Value>,
}
