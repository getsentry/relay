use relay_event_schema::protocol::Replay;
use relay_protocol::Annotated;

use crate::envelope::Item;
use crate::managed::Managed;
use crate::processing::replays::{Error, ExpandedReplay, ExpandedReplays, SerializedReplays};
use crate::services::processor::replay::ReplayVideoEvent;

fn expand_video(item: &Item) -> Result<ExpandedReplay, Error> {
    let (headers, payload) = item.destruct();

    let ReplayVideoEvent {
        replay_event,
        replay_recording,
        replay_video,
    } = rmp_serde::from_slice(&payload).map_err(|_| Error::InvalidReplayVideoEvent)?;

    // FIXME: Come up with better approach
    let event = Annotated::<Replay>::from_json_bytes(&replay_event)
        .map_err(|e| Error::CouldNotParse(e.to_string()))?;

    Ok(ExpandedReplay::NativeReplay {
        video_header: headers,
        event,
        recording: replay_recording,
        video: replay_video,
    })
}

pub fn expand(replays: Managed<SerializedReplays>) -> Managed<ExpandedReplays> {
    replays.map(|replays, records| {
        let SerializedReplays {
            headers,
            events,
            recordings,
            videos,
        } = replays;
        let mut replays = Vec::new();

        // There should be at most one event and recording and if there is one there needs to be
        // one of the other
        match (events.as_slice(), recordings.as_slice()) {
            ([], []) => (),
            ([event], [recording]) => {
                let (event_header, event_bytes) = event.destruct();
                let (recording_header, recording_bytes) = recording.destruct();

                match Annotated::<Replay>::from_json_bytes(&event_bytes) {
                    Ok(event) => {
                        replays.push(ExpandedReplay::WebReplay {
                            event_header,
                            recording_header,
                            event,
                            recording: recording_bytes,
                        });
                    }
                    Err(err) => {
                        // FIXME: Find something better here.
                        let err = Error::CouldNotParse(err.to_string());
                        records.reject_err(err.clone(), event);
                        records.reject_err(err, recording);
                    }
                }
            }
            (a, b) => {
                // TODO: Improve the error message and log some debugging.
                a.iter().for_each(|e| {
                    records.reject_err(Error::InvalidItemCount, e);
                });
                b.iter().for_each(|e| {
                    records.reject_err(Error::InvalidItemCount, e);
                });
            }
        }

        for video in &videos {
            match expand_video(video) {
                Ok(replay) => replays.push(replay),
                Err(err) => drop(records.reject_err(err, video)),
            }
        }

        ExpandedReplays { headers, replays }
    })
}
