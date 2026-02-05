use std::time::Duration;

use relay_dynamic_config::{RetentionConfig, RetentionsConfig};
use relay_event_normalization::eap;

use crate::envelope::EnvelopeHeaders;
use crate::processing::Context;
use crate::services::processor::MINIMUM_CLOCK_DRIFT;

/// Utility to create an [`eap::time::Config`].
pub fn time_config<F>(headers: &EnvelopeHeaders, f: F, ctx: Context<'_>) -> eap::time::Config
where
    F: FnOnce(&RetentionsConfig) -> Option<&RetentionConfig>,
{
    eap::time::Config {
        received_at: headers.meta().received_at(),
        sent_at: headers.sent_at(),
        max_in_past: Some(retention_days_to_duration(
            ctx.to_forward().retention(f).standard,
        )),
        max_in_future: ctx
            .config
            .max_secs_in_future()
            .try_into()
            .ok()
            .map(Duration::from_secs),
        minimum_clock_drift: MINIMUM_CLOCK_DRIFT,
    }
}

fn retention_days_to_duration(days: u16) -> Duration {
    const DAYS_TO_SECONDS: u64 = 24 * 60 * 60;
    Duration::from_secs(days as u64 * DAYS_TO_SECONDS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_days_to_duration() {
        assert_eq!(retention_days_to_duration(1), Duration::from_secs(86400));
    }
}
