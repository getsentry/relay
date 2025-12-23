use relay_dynamic_config::Feature;

use crate::processing::Context;
use crate::processing::profile_chunks::{Error, Result};

/// Checks whether the profile ingestion feature flag is enabled for the current project.
pub fn feature_flag(ctx: Context<'_>) -> Result<()> {
    let feature = match ctx
        .project_info
        .has_feature(Feature::ContinuousProfilingBetaIngest)
    {
        // Legacy feature.
        true => Feature::ContinuousProfilingBeta,
        // The post release ingestion feature.
        false => Feature::ContinuousProfiling,
    };

    match ctx.should_filter(feature) {
        true => Err(Error::FilterFeatureFlag),
        false => Ok(()),
    }
}
