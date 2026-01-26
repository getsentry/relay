use relay_dynamic_config::Feature;

use crate::managed::{Managed, Rejected};
use crate::processing::Context;
use crate::processing::replays::{Error, SerializedReplays};

/// Reject data if the feature is disabled.
pub fn feature_flag(
    replays: Managed<SerializedReplays>,
    ctx: Context<'_>,
) -> Result<Managed<SerializedReplays>, Rejected<Error>> {
    match ctx.should_filter(Feature::SessionReplay)
        || (ctx
            .project_info
            .has_feature(Feature::SessionReplayVideoDisabled)
            && !replays.videos.is_empty())
    {
        true => Err(replays.reject_err(Error::FilterFeatureFlag)),
        false => Ok(replays),
    }
}
