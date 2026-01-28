use relay_event_normalization::replay;

use crate::managed::Managed;
use crate::processing::replays::{Error, ExpandedReplays};

/// Checks the structural validity of replays, rejecting invalid ones.
pub fn validate(replays: &mut Managed<ExpandedReplays>) {
    replays.retain(
        |replays| &mut replays.replays,
        |replay, _| {
            let event = replay.get_event().value().ok_or(Error::NoEventContent)?;
            replay::validate(event).map_err(|e| Error::InvalidPayload(e.to_string()))
        },
    )
}
