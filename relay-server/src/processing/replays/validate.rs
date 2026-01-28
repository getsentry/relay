use relay_event_normalization::replay;

use crate::managed::Managed;
use crate::processing::replays::{Error, ExpandedReplays};

pub fn validate(replays: &mut Managed<ExpandedReplays>) {
    replays.retain(
        |replays| &mut replays.replays,
        |replay, _| match replay.get_event().value() {
            Some(event) => {
                // FIXME: Update the error once the underlying function is moved.
                replay::validate(event).map_err(|e| Error::InvalidPayload(e.to_string()))
            }
            None => Err(Error::NoEventContent),
        },
    )
}
