use bytes::Bytes;
use smallvec::{SmallVec, smallvec};

use crate::Envelope;
use crate::envelope::{ContentType, Item, ItemType, Items};
use crate::managed::{Managed, Rejected};
#[cfg(feature = "processing")]
use crate::processing::replays::store;
use crate::processing::replays::{
    Error, ExpandedReplay, ExpandedReplays, ReplayVideoEvent, ReplaysOutput,
};
use crate::processing::{self, Forward};

/// Errors that can occur when serializing an expanded replay into envelope items.
#[derive(Debug, thiserror::Error)]
enum SerializeReplayError {
    /// Failed to serialize the replay event to JSON.
    #[error("json serialization failed")]
    Json(#[from] serde_json::Error),
    /// Failed to serialize the replay video event to MessagePack.
    #[error("msgpack serialization failed")]
    MsgPack(#[from] rmp_serde::encode::Error),
}

impl Forward for ReplaysOutput {
    fn serialize_envelope(
        self,
        _ctx: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        Ok(self.0.map(|replays, records| {
            let ExpandedReplays { headers, replays } = replays;
            let mut items = Items::new();

            for replay in replays {
                match serialize_replay(&replay) {
                    Ok(replay_items) => items.extend(replay_items),
                    Err(error) => {
                        relay_log::error!(
                            error = &error as &dyn std::error::Error,
                            event_id = ?headers.event_id(),
                            "failed to serialize replay"
                        );
                        records.reject_err(Error::FailedToSerializeReplay, replay);
                    }
                }
            }

            Envelope::from_parts(headers, items)
        }))
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: processing::StoreHandle<'_>,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let Self(replays) = self;

        let event_id = replays
            .headers
            .event_id()
            .ok_or_else(|| replays.reject_err(Error::NoEventId).map(|_| ()))?;

        let ctx = store::Context {
            event_id,
            retention: ctx.event_retention().standard,
            max_replay_message_size: ctx.config.max_replay_message_size(),
        };

        for replay in replays.split(|replay| replay.replays) {
            if let Ok(replay) = replay.try_map(|replay, _| store::convert(replay, &ctx)) {
                s.send_to_store(replay);
            }
        }
        Ok(())
    }
}

fn create_replay_event_item(payload: Bytes) -> Item {
    let mut item = Item::new(ItemType::ReplayEvent);
    item.set_payload(ContentType::Json, payload);
    item
}

fn create_replay_recording_item(payload: Bytes) -> Item {
    let mut item = Item::new(ItemType::ReplayRecording);
    item.set_payload(ContentType::OctetStream, payload);
    item
}

fn create_replay_video_item(payload: Bytes) -> Item {
    let mut item = Item::new(ItemType::ReplayVideo);
    item.set_payload(ContentType::OctetStream, payload);
    item
}

fn serialize_replay(replay: &ExpandedReplay) -> Result<SmallVec<[Item; 2]>, SerializeReplayError> {
    match replay {
        ExpandedReplay::WebReplay { event, recording } => {
            let event_bytes: Bytes = event.to_json()?.into_bytes().into();

            Ok(smallvec![
                create_replay_event_item(event_bytes),
                create_replay_recording_item(recording.clone()),
            ])
        }
        ExpandedReplay::NativeReplay {
            event,
            recording,
            video,
        } => {
            let event_bytes: Bytes = event.to_json()?.into_bytes().into();

            let video_event = ReplayVideoEvent {
                replay_event: event_bytes,
                replay_recording: recording.clone(),
                replay_video: video.clone(),
            };

            let payload = rmp_serde::to_vec_named(&video_event)?;
            Ok(smallvec![create_replay_video_item(payload.into())])
        }
        ExpandedReplay::StandaloneRecording { recording } => {
            Ok(smallvec![create_replay_recording_item(recording.clone()),])
        }
    }
}
