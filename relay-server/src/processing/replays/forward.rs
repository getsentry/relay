use bytes::Bytes;

use crate::Envelope;
use crate::envelope::{ContentType, Item, ItemType, Items};
use crate::managed::{Managed, ManagedEnvelope, Rejected};
use crate::processing::replays::{Error, ExpandedReplay, ExpandedReplays, ReplaysOutput};
use crate::processing::{self, Forward};
use crate::services::processor::replay::ReplayVideoEvent;

// FIXME: Simplify this function
impl Forward for ReplaysOutput {
    fn serialize_envelope(
        self,
        _ctx: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        Ok(self.0.map(|s, r| {
            let ExpandedReplays { headers, replays } = s;
            let mut items = Items::new();
            let event_id = headers.event_id();

            for replay in replays {
                match replay {
                    ExpandedReplay::WebReplay {
                        event_header,
                        recording_header,
                        event,
                        recording,
                    } => {
                        // Parse the replay again.
                        match event.to_json() {
                            Ok(json) => {
                                let event: Bytes = json.into_bytes().into();

                                // items.push(Item::from_parts(event_header, event));
                                let mut item = Item::new(ItemType::ReplayEvent);
                                item.set_payload(ContentType::Json, event);
                                items.push(item);

                                items.push(Item::from_parts(recording_header, recording));
                            }
                            Err(error) => {
                                relay_log::error!(
                                    error = &error as &dyn std::error::Error,
                                    event_id = ?event_id,
                                    "failed to serialize replay"
                                );

                                // FIXME: Not a super fan of this.
                                r.reject_err(
                                    Error::FailedToSerializeReplayEvent,
                                    ExpandedReplay::WebReplay {
                                        event_header,
                                        recording_header,
                                        event,
                                        recording,
                                    },
                                );
                            }
                        }
                    }
                    ExpandedReplay::NativeReplay {
                        video_header,
                        event,
                        recording,
                        video,
                    } => {
                        // FIXME: Kind of duplicate logic
                        let event_bytes: Bytes = match event.to_json() {
                            Ok(json) => json.into_bytes().into(),
                            Err(error) => {
                                relay_log::error!(
                                    error = &error as &dyn std::error::Error,
                                    event_id = ?event_id,
                                    "failed to serialize replay"
                                );

                                r.reject_err(
                                    Error::FailedToSerializeReplayEvent,
                                    ExpandedReplay::NativeReplay {
                                        video_header,
                                        event,
                                        recording,
                                        video,
                                    },
                                );
                                continue;
                            }
                        };

                        let video_event = ReplayVideoEvent {
                            replay_event: event_bytes,
                            replay_recording: recording,
                            replay_video: video,
                        };
                        match rmp_serde::to_vec_named(&video_event) {
                            Ok(payload) => {
                                items.push(Item::from_parts(video_header, payload.into()));
                            }
                            Err(_) => {
                                // FIXME: Come up with a better error here.
                                // Err(ProcessingError::InvalidReplay(DiscardReason::InvalidReplayVideoEvent,))
                                r.reject_err(Error::FailedToSerializeReplayEvent, video_event);
                            }
                        }
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
        // FIXME: Already construct the kafka message here?

        let envelope = self.serialize_envelope(ctx)?;
        let envelope = ManagedEnvelope::from(envelope).into_processed();

        s.store(crate::services::store::StoreEnvelope { envelope });

        Ok(())
    }
}
