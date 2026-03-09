use crate::managed::{Managed, Rejected};
use crate::processing::attachments::{Error, SerializedAttachments};
use crate::processing::{self, utils};

/// Runs PiiProcessors on the attachments.
pub fn scrub(
    attachments: &mut Managed<SerializedAttachments>,
    ctx: processing::Context<'_>,
) -> Result<(), Rejected<Error>> {
    attachments.try_modify(|attachments, _| {
        utils::attachments::scrub(attachments.attachments.iter_mut(), ctx.project_info);
        Ok::<_, Error>(())
    })
}
