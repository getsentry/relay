use relay_dynamic_config::Feature;

use crate::processing::Context;
use crate::processing::profiles::Error;

/// Filters profiles sent for a project which does not allow profile ingestion.
pub fn feature_flag(ctx: Context<'_>) -> Result<(), Error> {
    match ctx.should_filter(Feature::Profiling) {
        true => Err(Error::FeatureDisabled),
        false => Ok(()),
    }
}
