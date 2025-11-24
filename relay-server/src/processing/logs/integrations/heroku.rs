use relay_event_schema::protocol::OurLog;

use crate::integrations::LogsIntegration;
use crate::processing::logs::{Error, Result};
use crate::services::outcome::DiscardReason;

/// Expands Heroku Logplex logs into the [`OurLog`] format.
pub fn expand<F>(_integration: &LogsIntegration, payload: &[u8], mut produce: F) -> Result<()>
where
    F: FnMut(OurLog),
{
    let mut count: i32 = 0;

    // Parse the payload as a UTF-8 string
    let payload_str = std::str::from_utf8(payload).map_err(|e| {
        relay_log::debug!(
            error = &e as &dyn std::error::Error,
            "Failed to parse Heroku log drain payload as UTF-8"
        );
        Error::Invalid(DiscardReason::InvalidJson)
    })?;

    // Process each line individually
    for line in payload_str.lines() {
        if line.is_empty() {
            continue;
        }

        // Parse the logplex-formatted message
        match relay_ourlogs::parse_logplex(line) {
            Ok(parsed_message) => {
                count += 1;
                let ourlog = relay_ourlogs::logplex_message_to_sentry_log(parsed_message);
                produce(ourlog);
            }
            Err(_) => {
                relay_log::debug!("Failed to parse logplex message");
                // Continue processing other lines even if one fails
                continue;
            }
        }
    }

    if count == 0 {
        relay_log::debug!("Failed to parse any logs from Heroku log drain payload");
        return Err(Error::Invalid(DiscardReason::InvalidJson));
    }

    Ok(())
}
