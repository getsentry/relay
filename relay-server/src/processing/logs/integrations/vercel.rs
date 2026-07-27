use relay_event_schema::protocol::OurLog;
use relay_ourlogs::VercelLog;

use crate::integrations::VercelLogDrainFormat;
use crate::processing::logs::{Error, Result};
use crate::services::outcome::DiscardReason;

/// Expands Vercel logs into the [`OurLog`] format.
pub fn expand2<'a>(
    format: VercelLogDrainFormat,
    payload: &'a [u8],
) -> Result<Box<dyn Iterator<Item = OurLog> + 'a>> {
    match format {
        VercelLogDrainFormat::Json => Ok(Box::new(
            serde_json::from_slice::<Vec<VercelLog>>(payload)
                .map_err(|e| {
                    relay_log::debug!(
                        error = &e as &dyn std::error::Error,
                        "Failed to parse logs data as JSON"
                    );
                    Error::Invalid(DiscardReason::InvalidJson)
                })?
                .into_iter()
                .map(relay_ourlogs::vercel_log_to_sentry_log),
        )),
        VercelLogDrainFormat::NdJson => Ok(Box::new(
            payload
                .split(|&b| b == b'\n')
                .filter(|l| !l.is_empty())
                .flat_map(serde_json::from_slice::<VercelLog>)
                .map(relay_ourlogs::vercel_log_to_sentry_log),
        )),
    }
}
