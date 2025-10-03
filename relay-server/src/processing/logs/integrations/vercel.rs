use relay_event_schema::protocol::OurLog;
use relay_ourlogs::VercelLog;

use crate::processing::logs::{Error, Result};
use crate::services::outcome::DiscardReason;

/// Expands Vercel logs into the [`OurLog`] format.
pub fn expand<F>(payload: &[u8], mut produce: F) -> Result<()>
where
    F: FnMut(OurLog),
{
    let logs = parse_logs_data(payload)?;

    for log in logs {
        let ourlog = relay_ourlogs::vercel_log_to_sentry_log(log);
        produce(ourlog)
    }

    Ok(())
}

fn parse_logs_data(payload: &[u8]) -> Result<Vec<VercelLog>> {
    // Try parsing as JSON array first
    if let Ok(logs) = serde_json::from_slice::<Vec<VercelLog>>(payload) {
        return Ok(logs);
    }

    // Fall back to NDJSON parsing
    let payload_str = std::str::from_utf8(payload).map_err(|e| {
        relay_log::debug!(
            error = &e as &dyn std::error::Error,
            "Failed to parse logs data as UTF-8"
        );
        Error::Invalid(DiscardReason::InvalidJson)
    })?;

    let logs: Vec<VercelLog> = payload_str
        .lines()
        .filter_map(|line| serde_json::from_str::<VercelLog>(line.trim()).ok())
        .collect();

    if logs.is_empty() {
        relay_log::debug!("Failed to parse any logs from vercel log drain payload");
    }

    Ok(logs)
}
