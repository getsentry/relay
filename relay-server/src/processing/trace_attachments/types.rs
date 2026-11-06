use bytes::Bytes;
use relay_event_schema::protocol::TraceAttachmentMeta;
use relay_protocol::Annotated;
use relay_quotas::DataCategory;

use crate::envelope::ParentId;
use crate::managed::{Counted, Quantities};

/// A validated and parsed trace attachment.
#[derive(Debug)]
pub struct ExpandedAttachment {
    /// The ID of the log / span / metric that owns the attachment.
    pub parent_id: Option<ParentId>,

    /// The parsed metadata from the attachment.
    pub meta: Annotated<TraceAttachmentMeta>,

    /// The raw attachment body.
    pub body: Bytes,
}

impl ExpandedAttachment {
    /// Attachment-byte quantity, including the minimum of one byte used by serialized items.
    pub fn attachment_quantity(&self) -> usize {
        self.body.len().max(1)
    }
}

impl Counted for ExpandedAttachment {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![
            (DataCategory::Attachment, self.attachment_quantity()),
            (DataCategory::AttachmentItem, 1)
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{ContentType, Item, ItemType};
    use crate::managed::Managed;
    use crate::processing::trace_attachments::{forward, process};

    #[test]
    fn test_attachment_quantities_across_representations() {
        for (body, bytes) in [("", 1), ("x", 1), ("payload", 7)] {
            let meta = r#"{"trace_id":"0123456789abcdef0123456789abcdef","attachment_id":"00000000-0000-7000-8000-000000000001","timestamp":1700000000.0,"filename":"test.txt","content_type":"text/plain"}"#;
            let mut item = Item::new(ItemType::Attachment);
            item.set_payload(ContentType::TraceAttachment, format!("{meta}{body}"));
            item.set_meta_length(meta.len() as u32);
            let expected: Quantities = smallvec::smallvec![
                (DataCategory::Attachment, bytes),
                (DataCategory::AttachmentItem, 1)
            ];
            assert_eq!(item.quantities(), expected);
            let (item, mut handle) = Managed::for_test(item).build();
            let expanded = item.map(|item, _| process::parse_and_validate(&item).unwrap());
            assert_eq!(expanded.body.as_ref(), body.as_bytes());
            assert_eq!(expanded.quantities(), expected);

            let item =
                expanded.map(|attachment, _| forward::attachment_to_item(attachment).unwrap());
            assert_eq!(item.quantities(), expected);

            #[cfg(feature = "processing")]
            {
                let expanded = item.map(|item, _| process::parse_and_validate(&item).unwrap());
                let stored = crate::processing::trace_attachments::store::convert(
                    expanded,
                    crate::processing::Retention {
                        standard: 90,
                        downsampled: 30,
                    },
                    None,
                )
                .unwrap();
                assert_eq!(stored.quantities(), expected);
                assert_eq!(stored.body.as_ref(), body.as_bytes());
                stored.accept(|_| ());
            }
            #[cfg(not(feature = "processing"))]
            item.accept(|_| ());
            handle.assert_no_outcomes();
        }
    }
}
