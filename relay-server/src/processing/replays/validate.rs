use relay_event_normalization::replay;

use crate::managed::Managed;
use crate::processing::replays::{Error, ExpandedReplays};

/// Checks the structural validity of replays, rejecting invalid ones.
pub fn validate(replays: &mut Managed<ExpandedReplays>) {
    replays.retain(
        |replays| &mut replays.replays,
        |replay, _| {
            let Some(event) = replay.event_mut() else {
                return Ok(());
            };
            let event = event.value().ok_or(Error::NoEventContent)?;
            replay::validate(event).map_err(Error::from)
        },
    )
}
