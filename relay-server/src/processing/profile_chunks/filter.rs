use relay_dynamic_config::Feature;

use crate::processing::Context;
use crate::processing::profile_chunks::{Error, Result};

/// Checks whether the profile ingestion feature flag is enabled for the current project.
pub fn feature_flag(ctx: Context<'_>) -> Result<()> {
    match ctx.should_filter(Feature::ContinuousProfiling) {
        true => Err(Error::FilterFeatureFlag),
        false => Ok(()),
    }
}
