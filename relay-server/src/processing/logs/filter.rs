use relay_dynamic_config::Feature;

use crate::processing::Context;
use crate::services::processor::ProcessingError;
use crate::utils::sample;

pub fn feature_flag(ctx: Context<'_>) -> Result<(), ProcessingError> {
    match ctx.should_filter(Feature::OurLogsIngestion) {
        // TODO: make it possible to filter with a proper error here.
        // TODO: we need to differentiate between silent and not silent
        true => Err(ProcessingError::NoEventPayload),
        false => Ok(()),
    }
}

pub fn sampled(ctx: Context<'_>) -> Result<(), ProcessingError> {
    let sample_rate = ctx.global_config.options.ourlogs_ingestion_sample_rate;

    match sample_rate.map_or(true, sample) {
        // TODO: make it possible to filter with a proper error here.
        // TODO: we need to differentiate between silent and not silent
        true => Err(ProcessingError::NoEventPayload),
        false => Ok(()),
    }
}
