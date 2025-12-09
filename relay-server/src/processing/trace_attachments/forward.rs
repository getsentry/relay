use crate::Envelope;
use crate::envelope::{ContentType, Item, ItemType, Items};
use crate::managed::{Managed, ManagedResult, Rejected};
use crate::processing::trace_attachments::types::ExpandedAttachment;
use crate::processing::trace_attachments::{ExpandedAttachments, store};
use crate::processing::{Forward, ForwardContext, StoreHandle};
use crate::services::outcome::{DiscardReason, Outcome};

impl ExpandedAttachments {
    fn serialize_envelope(self) -> Result<Box<Envelope>, serde_json::Error> {
        let Self {
            headers,
            server_sample_rate: _,
            attachments: item,
        } = self;
        let mut items = Items::new();
        for attachment in item {
            items.push(attachment_to_item(attachment)?);
        }
        Ok(Envelope::from_parts(headers, items))
    }
}

impl Forward for Managed<ExpandedAttachments> {
    fn serialize_envelope(
        self,
        _ctx: ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        self.try_map(|this, _| {
            this.serialize_envelope()
                .map_err(drop)
                .with_outcome(Outcome::Invalid(DiscardReason::Internal))
        })
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: StoreHandle<'_>,
        ctx: ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let retention = ctx.retention(|r| r.trace_attachment.as_ref());
        let server_sample_rate = self.server_sample_rate;
        for attachment in self.split(|work| work.attachments) {
            match store::convert(attachment, retention, server_sample_rate) {
                Ok(message) => s.upload(message),
                Err(_) => {} // already rejected
            }
        }
        Ok(())
    }
}

/// Converts an expanded trace attachment to an envelope item.
pub fn attachment_to_item(attachment: ExpandedAttachment) -> Result<Item, serde_json::Error> {
    let ExpandedAttachment {
        parent_id,
        meta,
        body,
    } = attachment;

    let meta_json = meta.to_json()?;
    let meta_bytes = meta_json.into_bytes();
    let meta_length = meta_bytes.len();

    let mut payload = bytes::BytesMut::with_capacity(meta_length + body.len());
    payload.extend_from_slice(&meta_bytes);
    payload.extend_from_slice(&body);

    let mut item = Item::new(ItemType::Attachment);
    item.set_payload(ContentType::AttachmentV2, payload.freeze());
    item.set_meta_length(meta_length as u32);
    item.set_parent_id(parent_id);

    Ok(item)
}
