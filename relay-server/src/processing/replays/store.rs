use bytes::Bytes;
use relay_event_schema::protocol::{EventId, Replay};

use relay_protocol::Annotated;

use crate::processing::replays::{Error, ExpandedReplay};
use crate::services::store::{StoreReplay, StoreReplayEvent, StoreReplayRecording};

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
    /// Whether Relay should skip publishing the replay event to Snuba.
    pub snuba_publish_disabled: bool,
}

/// Converts an [`ExpandedReplay`] into a storable [`StoreReplay`].
///
/// Fails if the event can not be serialized or the created message is too large for the consumer.
pub fn convert(replay: ExpandedReplay, ctx: &Context) -> Result<StoreReplay, Error> {
    match replay {
        ExpandedReplay::StandaloneEvent { event } => {
            let event = serialize_event(event)?;
            let store_event = convert_event(ctx, event);
            Ok(StoreReplay::Event(store_event))
        }
        ExpandedReplay::StandaloneRecording { recording } => {
            let store_replay = convert_recording(ctx, recording, None, None)?;
            Ok(StoreReplay::Recording(store_replay))
        }
        ExpandedReplay::WebReplay { event, recording } => {
            let event = serialize_event(event)?;
            let store_event = convert_event(ctx, event.clone());
            let store_replay = convert_recording(ctx, recording, Some(event), None)?;
            Ok(StoreReplay::WebReplay(store_event, store_replay))
        }
        ExpandedReplay::NativeReplay {
            event,
            recording,
            video,
        } => {
            let event = serialize_event(event)?;
            let store_event = convert_event(ctx, event.clone());
            let store_replay = convert_recording(ctx, recording, Some(event), Some(video))?;
            Ok(StoreReplay::NativeReplay(store_event, store_replay))
        }
    }
}

fn serialize_event(replay: Annotated<Replay>) -> Result<Bytes, Error> {
    replay
        .to_json()
        .map_err(|_| Error::FailedToSerializeReplay)
        .map(|json| json.into_bytes().into())
}

fn convert_event(ctx: &Context, payload: Bytes) -> StoreReplayEvent {
    StoreReplayEvent {
        event_id: ctx.event_id,
        retention_days: ctx.retention,
        payload,
        relay_snuba_publish_disabled: ctx.snuba_publish_disabled,
    }
}

fn convert_recording(
    ctx: &Context,
    payload: Bytes,
    replay_event: Option<Bytes>,
    replay_video: Option<Bytes>,
) -> Result<StoreReplayRecording, Error> {
    // Size of the consumer message. We can be reasonably sure this won't overflow because
    // of the request size validation provided by Nginx and Relay.
    let mut payload_size = MESSAGE_METADATA_OVERHEAD;
    payload_size += replay_event.as_ref().map_or(0, |b| b.len());
    payload_size += replay_video.as_ref().map_or(0, |b| b.len());
    payload_size += payload.len();

    if payload_size >= ctx.max_replay_message_size {
        relay_log::debug!("replay_recording over maximum size.");
        return Err(Error::TooLarge);
    }

    Ok(StoreReplayRecording {
        event_id: ctx.event_id,
        retention_days: ctx.retention,
        payload,
        replay_event,
        replay_video,
        relay_snuba_publish_disabled: ctx.snuba_publish_disabled,
    })
}
