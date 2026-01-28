use relay_dynamic_config::Feature;
use relay_event_normalization::{GeoIpLookup, RawUserAgentInfo, replay};
use relay_event_schema::processor::{self, ProcessingState};
use relay_event_schema::protocol::Replay;
use relay_pii::PiiProcessor;
use relay_protocol::Annotated;
use relay_replays::recording::RecordingScrubber;
use relay_statsd::metric;

use crate::envelope::Item;
use crate::managed::Managed;
use crate::processing::Context;
use crate::processing::replays::{
    Error, ExpandedReplay, ExpandedReplays, ReplayVideoEvent, SerializedReplays,
};
use crate::statsd::RelayTimers;

fn expand_video(item: &Item) -> Result<ExpandedReplay, Error> {
    let (headers, payload) = item.destruct();

    let ReplayVideoEvent {
        replay_event,
        replay_recording,
        replay_video,
    } = rmp_serde::from_slice(&payload).map_err(|_| Error::InvalidReplayVideoEvent)?;

    if replay_video.is_empty() {
        return Err(Error::InvalidReplayVideoEvent);
    }

    let event = Annotated::<Replay>::from_json_bytes(&replay_event)
        .map_err(|e| Error::CouldNotParseEvent(e.to_string()))?;

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
                        records.reject_err(Error::CouldNotParseEvent(err.to_string()), event);
                        records.reject_err(Error::CouldNotParseEvent(err.to_string()), recording);
                    }
                }
            }
            (a, b) => {
                a.iter().for_each(|e| {
                    records.reject_err(Error::InvalidItemCount, e);
                });
                b.iter().for_each(|e| {
                    records.reject_err(Error::InvalidItemCount, e);
                });
            }
        }

        // From the SDKs it seems like there will only be onr video per envelope but that is not
        // clearly specified anywhere.
        for video in &videos {
            match expand_video(video) {
                Ok(replay) => replays.push(replay),
                Err(err) => drop(records.reject_err(err, video)),
            }
        }

        ExpandedReplays { headers, replays }
    })
}

pub fn normalize(replays: &mut Managed<ExpandedReplays>, geoip_lookup: &GeoIpLookup) {
    // Q: We now construct our config here on demand, is that ok, or do we want our own config?
    let meta = replays.headers.meta();
    let client_addr = meta.client_addr();
    let user_agent = RawUserAgentInfo {
        user_agent: meta.user_agent().map(String::from),
        client_hints: meta.client_hints().to_owned(),
    };

    replays.modify(|replay, _| {
        for replay in replay.replays.iter_mut() {
            replay::normalize(
                replay.get_event(),
                client_addr,
                &user_agent.as_deref(), // FIXME: Get rid of this.
                geoip_lookup,
            )
        }
    })
}

pub fn scrub_event(event: &mut Annotated<Replay>, ctx: Context<'_>) -> Result<(), Error> {
    if let Some(ref config) = ctx.project_info.config.pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        processor::process_value(event, &mut processor, ProcessingState::root())
            .map_err(|e| Error::CouldNotScrub(e.to_string()))?;
    }

    let pii_config = ctx
        .project_info
        .config
        .datascrubbing_settings
        .pii_config()
        .map_err(|e| Error::CouldNotScrub(e.to_string()))?;

    if let Some(config) = pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        processor::process_value(event, &mut processor, ProcessingState::root())
            .map_err(|e| Error::CouldNotScrub(e.to_string()))?;
    };
    Ok(())
}

pub fn scrub(replays: &mut Managed<ExpandedReplays>, ctx: Context<'_>) {
    replays.retain(
        |replays| &mut replays.replays,
        |replay, _| scrub_event(replay.get_event(), ctx),
    );
}

pub fn scrub_recording(replays: &mut Managed<ExpandedReplays>, ctx: Context<'_>) {
    if !ctx
        .project_info
        .has_feature(Feature::SessionReplayRecordingScrubbing)
    {
        return;
    }

    let event_id = replays.headers.event_id();

    replays.retain(
        |replays| &mut replays.replays,
        |replay, _| {
            let datascrubbing_config = ctx
                .project_info
                .config
                .datascrubbing_settings
                .pii_config()
                .map_err(|e| Error::PiiConfigError(e.clone()))?
                .as_ref();

            let mut scrubber = RecordingScrubber::new(
                ctx.config.max_replay_uncompressed_size(),
                ctx.project_info.config.pii_config.as_ref(),
                datascrubbing_config,
            );

            if scrubber.is_empty() {
                return Ok::<(), Error>(());
            }

            let payload = replay.get_recording();
            *payload = metric!(timer(RelayTimers::ReplayRecordingProcessing), {
                scrubber.process_recording(payload)
            })
            .map(Into::into)
            .map_err(|error| {
                relay_log::debug!(
                    error = &error as &dyn std::error::Error,
                    event_id = ?event_id,
                    project_id = ctx.project_info.project_id.map(|v| v.value()),
                    organization_id = ctx.project_info.organization_id.map(|o| o.value()),
                    "invalid replay recording"
                );
                Error::InvalidReplayRecordingEvent
            })?;
            Ok::<(), Error>(())
        },
    );
}
