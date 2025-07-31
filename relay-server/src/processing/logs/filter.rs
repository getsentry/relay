use relay_dynamic_config::Feature;

use crate::processing::Context;
use crate::processing::logs::{Error, Result};

pub fn feature_flag(ctx: Context<'_>) -> Result<()> {
    match ctx.should_filter(Feature::OurLogsIngestion) {
        true => Err(Error::FilterFeatureFlag),
        false => Ok(()),
    }
}
