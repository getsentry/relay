use relay_event_normalization::replay;

use crate::processing::replays::{Error, ExpandedReplay, Result};

/// Checks the structural validity of a replay, rejects it if invalid.
pub fn validate(replay: &ExpandedReplay) -> Result<()> {
    let Some(event) = replay.payload.event() else {
        return Ok(());
    };
    let event = event.value().ok_or(Error::NoEventContent)?;
    replay::validate(event).map_err(Error::from)
}
