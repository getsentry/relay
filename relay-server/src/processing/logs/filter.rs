use relay_dynamic_config::Feature;

use crate::processing::Context;
use crate::processing::logs::{Error, Result};
use crate::utils::{PickResult, sample};

pub fn feature_flag(ctx: Context<'_>) -> Result<()> {
    match ctx.should_filter(Feature::OurLogsIngestion) {
        true => Err(Error::FilterFeatureFlag),
        false => Ok(()),
    }
}

pub fn sampled(ctx: Context<'_>) -> Result<()> {
    let sample_rate = ctx.global_config.options.ourlogs_ingestion_sample_rate;

    match sample_rate.map(sample).unwrap_or_default() {
        PickResult::Discard => Err(Error::FilterSampling),
        PickResult::Keep => Ok(()),
    }
}
