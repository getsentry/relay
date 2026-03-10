use relay_quotas::DataCategory;

use crate::managed::{Managed, Rejected};
use crate::processing::attachments::{Error, SerializedAttachments};
use crate::processing::{self, utils};

/// Runs PiiProcessors on the attachments.
pub fn scrub(
    attachments: &mut Managed<SerializedAttachments>,
    ctx: processing::Context<'_>,
) -> Result<(), Rejected<Error>> {
    attachments.try_modify(|attachments, records| {
        // This is needed since scrubbing a view hierarchy might change its length and thus also the
        // attachment quantity.
        records.lenient(DataCategory::Attachment);
        utils::attachments::scrub(attachments.attachments.iter_mut(), ctx.project_info);
        Ok::<_, Error>(())
    })
}
