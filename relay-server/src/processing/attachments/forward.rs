use crate::Envelope;
use crate::managed::{Managed, Rejected};
use crate::processing::attachments::AttachmentsOutput;
use crate::processing::{self, Forward};

impl Forward for AttachmentsOutput {
    fn serialize_envelope(
        self,
        _: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        let Self(attachments) = self;
        Ok(attachments.map(|attachments, _| {
            Envelope::from_parts(attachments.headers, attachments.attachments)
        }))
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: processing::StoreHandle<'_>,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        use crate::processing::attachments::Error;

        let Self(attachments) = self;

        let Some(event_id) = attachments.headers.event_id() else {
            return Err(attachments.reject_err(Error::NoEventId).map(drop));
        };

        let use_objectstore = {
            let options = &ctx.global_config.options;
            crate::utils::sample(options.objectstore_attachments_sample_rate).is_keep()
        };

        for attachment in attachments.split(|attachment| attachment.attachments) {
            let store_attachment = attachment.map(|attachment, _| {
                use crate::services::store::StoreAttachment;
                let quantities = attachment.quantities();
                StoreAttachment {
                    event_id,
                    attachment,
                    quantities,
                    retention: ctx.event_retention().standard,
                }
            });
            if use_objectstore {
                s.send_to_objectstore(store_attachment);
            } else {
                s.send_to_store(store_attachment);
            }
        }

        Ok(())
    }
}
