use relay_event_schema::protocol::OurLog;
use relay_ourlogs::VercelLog;

use crate::integrations::VercelFormat;
use crate::processing::logs::{Error, Result};
use crate::services::outcome::DiscardReason;

/// Expands Vercel logs into the [`OurLog`] format.
pub fn expand<F>(format: VercelFormat, payload: &[u8], mut produce: F) -> Result<()>
where
    F: FnMut(OurLog),
{
    let logs = parse_vercel_logs(format, payload)?;

    for vercel_log in logs {
        let our_log = relay_ourlogs::vercel_log_to_sentry_log(vercel_log);
        produce(our_log);
    }

    Ok(())
}

fn parse_vercel_logs(format: VercelFormat, payload: &[u8]) -> Result<Vec<VercelLog>, Error> {
    match format {
        VercelFormat::Json => {
            // Try to parse as JSON array first
            if let Ok(logs) = serde_json::from_slice::<Vec<VercelLog>>(payload) {
                return Ok(logs);
            }

            // If that fails, try to parse as NDJSON (newline-delimited JSON)
            let payload_str = std::str::from_utf8(payload).map_err(|e| {
                relay_log::debug!(
                    error = &e as &dyn std::error::Error,
                    "Failed to parse Vercel logs as UTF-8"
                );
                Error::Invalid(DiscardReason::InvalidJson)
            })?;

            let mut logs = Vec::new();
            for line in payload_str.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                let log: VercelLog = serde_json::from_str(line).map_err(|e| {
                    relay_log::debug!(
                        error = &e as &dyn std::error::Error,
                        "Failed to parse Vercel log line as JSON"
                    );
                    Error::Invalid(DiscardReason::InvalidJson)
                })?;
                logs.push(log);
            }

            Ok(logs)
        }
    }
}
