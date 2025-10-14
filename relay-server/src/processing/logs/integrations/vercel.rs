use relay_event_schema::protocol::OurLog;
use relay_ourlogs::VercelLog;

use crate::integrations::VercelLogDrainFormat;
use crate::processing::logs::{Error, Result};
use crate::services::outcome::DiscardReason;

/// Expands Vercel logs into the [`OurLog`] format.
pub fn expand<F>(format: VercelLogDrainFormat, payload: &[u8], mut produce: F) -> Result<()>
where
    F: FnMut(OurLog),
{
    let mut count: i32 = 0;

    match format {
        VercelLogDrainFormat::Json => {
            let logs = serde_json::from_slice::<Vec<VercelLog>>(payload).map_err(|e| {
                relay_log::debug!(
                    error = &e as &dyn std::error::Error,
                    "Failed to parse logs data as JSON"
                );
                Error::Invalid(DiscardReason::InvalidJson)
            })?;

            for log in logs {
                count += 1;
                let ourlog = relay_ourlogs::vercel_log_to_sentry_log(log);
                produce(ourlog)
            }
        }
        VercelLogDrainFormat::NdJson => {
            for line in payload.split(|&b| b == b'\n') {
                if line.is_empty() {
                    continue;
                }

                if let Ok(log) = serde_json::from_slice::<VercelLog>(line) {
                    count += 1;
                    let ourlog = relay_ourlogs::vercel_log_to_sentry_log(log);
                    produce(ourlog);
                }
            }
        }
    }

    if count == 0 {
        relay_log::debug!("Failed to parse any logs from vercel log drain payload");
        return Err(Error::Invalid(DiscardReason::InvalidJson));
    }

    Ok(())
}
