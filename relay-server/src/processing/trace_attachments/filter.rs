use relay_dynamic_config::Feature;

use crate::managed::{Counted, Managed, Rejected};
use crate::processing::Context;
use crate::processing::trace_attachments::Error;

/// Reject data if the feature flag is disabled.
pub fn feature_flag<T: Counted>(
    work: Managed<T>,
    ctx: Context<'_>,
) -> Result<Managed<T>, Rejected<Error>> {
    let feature = Feature::TraceAttachmentProcessing;
    match ctx.should_filter(feature) {
        true => Err(work.reject_err(Error::FeatureDisabled(feature))),
        false => Ok(work),
    }
}
