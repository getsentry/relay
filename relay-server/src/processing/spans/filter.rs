use relay_dynamic_config::Feature;

use crate::processing::Context;
use crate::processing::spans::{Error, Result};

/// Filters logs sent for a project which does not allow logs ingestion.
pub fn feature_flag(ctx: Context<'_>) -> Result<()> {
    match ctx.should_filter(Feature::StandaloneSpanIngestion) {
        true => Err(Error::FilterFeatureFlag),
        false => Ok(()),
    }
}
