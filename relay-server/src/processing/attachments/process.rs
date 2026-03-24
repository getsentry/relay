use crate::managed::{Managed, Rejected};
use crate::processing::attachments::{Error, SerializedAttachments};
use crate::processing::{self, utils};

/// Runs PiiProcessors on the attachments.
pub fn scrub(
    attachments: &mut Managed<SerializedAttachments>,
    ctx: processing::Context<'_>,
) -> Result<(), Rejected<Error>> {
    attachments.try_modify(|attachments, records| {
        utils::attachments::scrub(
            attachments.attachments.iter_mut(),
            ctx.project_info,
            Some(records),
        );
        Ok::<_, Error>(())
    })
}

/// Validates the attachments and drop any invalid ones.
///
/// An attachment might be a placeholder, in which case its signature needs to be verified.
#[cfg(feature = "processing")]
pub fn validate_attachments(
    attachments: &mut Managed<SerializedAttachments>,
    ctx: processing::Context<'_>,
) {
    if !ctx.is_processing() {
        return;
    }

    attachments.modify(|attachments, records| {
        attachments.attachments.retain_mut(|attachment| {
            match utils::attachments::validate(attachment, ctx.config) {
                Ok(()) => true,
                Err(err) => {
                    records.reject_err(err, &*attachment);
                    false
                }
            }
        });
    });
}
