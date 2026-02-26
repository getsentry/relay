use relay_event_normalization::replay;

use crate::managed::{Managed, Rejected};
use crate::processing::replays::{Error, ExpandedReplay};

/// Checks the structural validity of a replay, rejects it if invalid.
pub fn validate(replay: &mut Managed<ExpandedReplay>) -> Result<(), Rejected<Error>> {
    replay.try_modify(|replay, _| {
        let Some(event) = replay.payload.event_mut() else {
            return Ok(());
        };
        let event = event.value().ok_or(Error::NoEventContent)?;
        replay::validate(event).map_err(Error::from)
    })
}
