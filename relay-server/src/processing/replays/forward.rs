use bytes::Bytes;
use smallvec::smallvec;

use crate::Envelope;
use crate::envelope::{ContentType, Item, ItemType, Items};
use crate::managed::{Managed, ManagedResult, Rejected};
#[cfg(feature = "processing")]
use crate::processing::replays::store;
use crate::processing::replays::{ExpandedReplay, ReplayPayload, ReplayVideoEvent, ReplaysOutput};
use crate::processing::{self, Forward};
use crate::services::outcome::{DiscardReason, Outcome};

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
        self.0.try_map(|replay, _| {
            let ExpandedReplay { headers, payload } = replay;

            serialize_replay(&payload)
                .map_err(|error| {
                    relay_log::error!(
                        error = &error as &dyn std::error::Error,
                        event_id = ?headers.event_id(),
                        "failed to serialize replay"
                    );
                })
                .with_outcome(Outcome::Invalid(DiscardReason::Internal))
                .map(|items| Envelope::from_parts(headers, items))
        })
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: processing::StoreHandle<'_>,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let Self(replay) = self;

        let event_id = replay.headers.event_id().ok_or_else(|| {
            replay
                .reject_err(crate::processing::replays::Error::NoEventId)
                .map(drop)
        })?;

        let ctx = store::Context {
            event_id,
            retention: ctx.event_retention().standard,
            max_replay_message_size: ctx.config.max_replay_message_size(),
        };

        if let Ok(replay) = replay.try_map(|replay, _| store::convert(replay.payload, &ctx)) {
            s.store(replay);
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

fn serialize_replay(replay: &ReplayPayload) -> Result<Items, SerializeReplayError> {
    match replay {
        ReplayPayload::WebReplay { event, recording } => {
            let event_bytes: Bytes = event.to_json()?.into_bytes().into();

            Ok(smallvec![
                create_replay_event_item(event_bytes),
                create_replay_recording_item(recording.clone())
            ])
        }
        ReplayPayload::NativeReplay {
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
        ReplayPayload::StandaloneRecording { recording } => {
            Ok(smallvec![create_replay_recording_item(recording.clone())])
        }
    }
}
