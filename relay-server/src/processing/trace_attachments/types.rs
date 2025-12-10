use bytes::Bytes;
use relay_event_schema::protocol::TraceAttachmentMeta;
use relay_protocol::Annotated;
use relay_quotas::DataCategory;

use crate::envelope::ParentId;
use crate::managed::{Counted, Quantities};

/// A validated and parsed span attachment.
#[derive(Debug)]
pub struct ExpandedAttachment {
    /// The ID of the log / span / metric that owns the attachment.
    pub parent_id: Option<ParentId>,

    /// The parsed metadata from the attachment.
    pub meta: Annotated<TraceAttachmentMeta>,

    /// The raw attachment body.
    pub body: Bytes,
}

impl Counted for ExpandedAttachment {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![
            (DataCategory::Attachment, self.body.len()),
            (DataCategory::AttachmentItem, 1)
        ]
    }
}
