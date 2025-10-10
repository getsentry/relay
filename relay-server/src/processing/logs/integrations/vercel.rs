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
    let logs = parse_logs_data(format, payload)?;

    for log in logs {
        let ourlog = relay_ourlogs::vercel_log_to_sentry_log(log);
        produce(ourlog)
    }

    Ok(())
}

fn parse_logs_data(format: VercelLogDrainFormat, payload: &[u8]) -> Result<Vec<VercelLog>> {
    match format {
        VercelLogDrainFormat::Json => {
            serde_json::from_slice::<Vec<VercelLog>>(payload).map_err(|e| {
                relay_log::debug!(
                    error = &e as &dyn std::error::Error,
                    "Failed to parse logs data as JSON"
                );
                Error::Invalid(DiscardReason::InvalidJson)
            })
        }
        VercelLogDrainFormat::NDJson => {
            let payload_str = std::str::from_utf8(payload).map_err(|e| {
                relay_log::debug!(
                    error = &e as &dyn std::error::Error,
                    "Failed to parse logs data as UTF-8"
                );
                Error::Invalid(DiscardReason::InvalidJson)
            })?;

            let logs: Vec<VercelLog> = payload_str
                .lines()
                .filter_map(|line| {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        return None;
                    }
                    serde_json::from_str::<VercelLog>(trimmed)
                        .map_err(|e| {
                            relay_log::debug!(
                                error = &e as &dyn std::error::Error,
                                line = trimmed,
                                "Failed to parse NDJSON line"
                            );
                        })
                        .ok()
                })
                .collect();

            if logs.is_empty() {
                relay_log::debug!("Failed to parse any logs from vercel log drain payload");
                return Err(Error::Invalid(DiscardReason::InvalidJson));
            }

            Ok(logs)
        }
    }
}
