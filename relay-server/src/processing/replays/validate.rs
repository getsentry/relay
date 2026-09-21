use relay_event_normalization::replay;

use crate::processing::replays::{Error, ExpandedReplay, Result};
use crate::statsd::RelayCounters;
use crate::utils::client_name_tag;

/// Checks the structural validity of a replay, rejects it if invalid.
pub fn validate(replay: &ExpandedReplay) -> Result<()> {
    let has_event_id = replay.headers.event_id().is_some();

    // Temporary counter to figure out which SDKs are sending replays without event IDs.
    relay_statsd::metric!(
        counter(RelayCounters::Replay) += 1,
        sdk = client_name_tag(replay.headers.meta().client_name()),
        has_event_id = has_event_id.to_string(),
    );

    if !has_event_id {
        return Err(Error::NoEventId);
    }

    let Some(event) = replay.payload.event() else {
        return Ok(());
    };
    let event = event.value().ok_or(Error::NoEventContent)?;
    replay::validate(event).map_err(Error::from)
}
