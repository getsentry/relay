//! Heroku Logplex log expansion.

use relay_event_schema::protocol::OurLog;
use relay_ourlogs::HerokuHeader;

use crate::envelope::Item;
use crate::processing::logs::{Error, Result};
use crate::services::outcome::DiscardReason;

/// Expands Heroku Logplex logs into the [`OurLog`] format.
///
/// # Arguments
///
/// * `item` - The envelope item containing the Logplex payload and headers
/// * `produce` - A callback function that receives each parsed log entry
pub fn expand<F>(item: &Item, mut produce: F) -> Result<()>
where
    F: FnMut(OurLog),
{
    let frame_id = item
        .get_header(HerokuHeader::FrameId.as_str())
        .and_then(|v| v.as_str());

    let drain_token = item
        .get_header(HerokuHeader::DrainToken.as_str())
        .and_then(|v| v.as_str());

    let user_agent = item
        .get_header(HerokuHeader::UserAgent.as_str())
        .and_then(|v| v.as_str());

    let payload = item.payload();

    let payload_str = std::str::from_utf8(&payload).map_err(|e| {
        relay_log::debug!(
            error = &e as &dyn std::error::Error,
            "Failed to parse Logplex payload as UTF-8"
        );
        Error::Invalid(DiscardReason::InvalidLog)
    })?;

    let mut count: u32 = 0;

    // Logplex sends multiple syslog messages, each on its own line
    // Each line is prefixed with a length: "<length> <syslog_message>"
    for line in payload_str.lines() {
        if line.is_empty() {
            continue;
        }

        match relay_ourlogs::parse_logplex(line) {
            Ok(msg) => {
                count += 1;
                let ourlog = relay_ourlogs::logplex_message_to_sentry_log(
                    msg,
                    frame_id,
                    drain_token,
                    user_agent,
                );
                produce(ourlog);
            }
            Err(e) => {
                relay_log::debug!(
                    error = %e,
                    "Failed to parse Logplex message, skipping"
                );
            }
        }
    }

    if count == 0 {
        relay_log::debug!("Failed to parse any logs from Heroku Logplex payload");
        return Err(Error::Invalid(DiscardReason::InvalidLog));
    }

    Ok(())
}
