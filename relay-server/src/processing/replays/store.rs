use bytes::Bytes;
use relay_event_schema::protocol::{EventId, Replay};

use relay_protocol::Annotated;

use crate::managed::Counted;
use crate::processing::replays::{Error, ExpandedReplay};
use crate::services::store::StoreReplay;

/// Reserved bytes for message metadata.
const MESSAGE_METADATA_OVERHEAD: usize = 2000;

/// Context parameters for [`convert`].
#[derive(Debug, Clone, Copy)]
pub struct Context {
    /// The event ID.
    pub event_id: EventId,
    /// Item retention in days.
    pub retention: u16,
    /// Maximum allowed size for a replay recording Kafka message.
    pub max_replay_message_size: usize,
}

/// Converts an [`ExpandedReplay`] into a storable [`StoreReplay`].
///
/// Fails if the event can not be serialized or the created message is too large for the consumer.
pub fn convert(replay: ExpandedReplay, ctx: &Context) -> Result<StoreReplay, Error> {
    let quantities = replay.quantities();
    let (recording, event, video) = match replay {
        ExpandedReplay::StandaloneRecording { recording } => (recording, None, None),
        ExpandedReplay::WebReplay { event, recording } => (recording, Some(event), None),
        ExpandedReplay::NativeReplay {
            event,
            recording,
            video,
        } => (recording, Some(event), Some(video)),
    };
    let event = event.map(serialize_event).transpose()?;

    // Size of the consumer message. We can be reasonably sure this won't overflow because
    // of the request size validation provided by Nginx and Relay.
    let mut payload_size = MESSAGE_METADATA_OVERHEAD;
    payload_size += event.as_ref().map_or(0, |b| b.len());
    payload_size += video.as_ref().map_or(0, |b| b.len());
    payload_size += recording.len();

    if payload_size >= ctx.max_replay_message_size {
        relay_log::debug!("replay_recording over maximum size.");
        return Err(Error::TooLarge);
    }

    Ok(StoreReplay {
        event_id: ctx.event_id,
        retention_days: ctx.retention,
        recording,
        event,
        video,
        quantities,
    })
}

fn serialize_event(replay: Annotated<Replay>) -> Result<Bytes, Error> {
    replay
        .to_json()
        .map_err(|_| Error::FailedToSerializeReplay)
        .map(|json| json.into_bytes().into())
}
