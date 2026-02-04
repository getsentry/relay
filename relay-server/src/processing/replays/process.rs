use smallvec::SmallVec;

use relay_dynamic_config::Feature;
use relay_event_normalization::{GeoIpLookup, RawUserAgentInfo, replay};
use relay_event_schema::processor::{self, ProcessingState};
use relay_event_schema::protocol::Replay;
use relay_pii::PiiProcessor;
use relay_protocol::Annotated;
use relay_replays::recording::RecordingScrubber;
use relay_statsd::metric;

use crate::envelope::Item;
use crate::managed::{Managed, Rejected};
use crate::processing::Context;
use crate::processing::replays::{
    Error, ExpandedReplay, ExpandedReplays, ReplayVideoEvent, SerializedReplays,
};
use crate::statsd::RelayTimers;

fn expand_video(item: &Item) -> Result<ExpandedReplay, Error> {
    let ReplayVideoEvent {
        replay_event: event,
        replay_recording: recording,
        replay_video: video,
    } = rmp_serde::from_slice(&item.payload()).map_err(|_| Error::InvalidReplayVideoEvent)?;

    if video.is_empty() {
        return Err(Error::InvalidReplayVideoEvent);
    }

    let event = Annotated::<Replay>::from_json_bytes(&event)?;

    Ok(ExpandedReplay::NativeReplay {
        event,
        recording,
        video,
    })
}

/// Parses all serialized replays into their [`ExpandedReplays`] representation.
///
/// Does not enforce `replay_event` and `replay_recording` to be send together in the same envelope
/// since some SDKs don't do it and enforcing this would break them.
pub fn expand(replays: Managed<SerializedReplays>) -> Managed<ExpandedReplays> {
    replays.map(|replays, records| {
        let SerializedReplays {
            headers,
            events,
            recordings,
            videos,
        } = replays;
        let mut replays = Vec::new();

        match (events.as_slice(), recordings.as_slice()) {
            // Valid case (no 'web replays') if the envelope contains some replay_videos ('native replays')
            ([], []) => (),
            ([event], [recording]) => {
                let event_bytes = event.payload();
                let recording_bytes = recording.payload();

                match Annotated::<Replay>::from_json_bytes(&event_bytes) {
                    Ok(event) => {
                        replays.push(ExpandedReplay::WebReplay {
                            event,
                            recording: recording_bytes,
                        });
                    }
                    Err(err) => {
                        records.reject_err(Error::from(err), SmallVec::from([event, recording]));
                    }
                }
            }
            // Handle SDKs that send standalone `replay_event` and `replay_recording`.
            ([event], []) => match Annotated::<Replay>::from_json_bytes(&event.payload()) {
                Ok(event) => replays.push(ExpandedReplay::StandaloneEvent { event }),
                Err(err) => drop(records.reject_err(Error::from(err), event)),
            },
            ([], [recording]) => {
                replays.push(ExpandedReplay::StandaloneRecording {
                    recording: recording.payload(),
                });
            }
            (events, recordings) => {
                relay_log::error!(
                    sdk = headers.meta().client().unwrap_or("unknown"),
                    event_count = events.len(),
                    recording_count = recordings.len(),
                    "unexpected replay item count"
                );

                for event in events {
                    records.reject_err(Error::InvalidItemCount, event);
                }
                for recording in recordings {
                    records.reject_err(Error::InvalidItemCount, recording);
                }
            }
        }

        // From the SDKs it seems like there will only be one video per envelope but that is not
        // clearly specified anywhere. Also it seems like their will not be native and web replays
        // in the same envelope.
        // Currently the logic still allows for multiple videos per envelope as well as 'native' and
        // 'web' replays in the same envelope, in the future we could be more strict on this.
        for video in &videos {
            match expand_video(video) {
                Ok(replay) => replays.push(replay),
                Err(err) => drop(records.reject_err(err, video)),
            }
        }

        if replays.len() > 1 {
            relay_log::error!(
                sdk = headers.meta().client().unwrap_or("unknown"),
                event_count = events.len(),
                recording_count = recordings.len(),
                video_count = videos.len(),
                "multiple replay items in same envelope"
            );
        }

        ExpandedReplays { headers, replays }
    })
}

/// Normalizes individual replay events.
pub fn normalize(replays: &mut Managed<ExpandedReplays>, geoip_lookup: &GeoIpLookup) {
    let meta = replays.headers.meta();
    let client_addr = meta.client_addr();
    let user_agent = RawUserAgentInfo {
        user_agent: meta.user_agent().map(String::from),
        client_hints: meta.client_hints().to_owned(),
    };

    replays.modify(|replay, _| {
        for replay in replay.replays.iter_mut() {
            if let Some(event) = replay.get_event() {
                replay::normalize(event, client_addr, &user_agent.as_deref(), geoip_lookup)
            }
        }
    })
}

fn scrub_event(event: &mut Annotated<Replay>, ctx: Context<'_>) -> Result<(), Error> {
    if let Some(ref config) = ctx.project_info.config.pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        processor::process_value(event, &mut processor, ProcessingState::root())?;
    }

    let pii_config = ctx
        .project_info
        .config
        .datascrubbing_settings
        .pii_config()
        .map_err(|e| Error::PiiConfig(e.clone()))?;

    if let Some(config) = pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        processor::process_value(event, &mut processor, ProcessingState::root())?;
    };
    Ok(())
}

fn scrub_recordings(
    replays: &mut Managed<ExpandedReplays>,
    ctx: Context<'_>,
) -> Result<(), Rejected<Error>> {
    let event_id = replays.headers.event_id();
    let pii_config = match ctx.project_info.config.datascrubbing_settings.pii_config() {
        Ok(config) => config.as_ref(),
        Err(e) => return Err(replays.reject_err(Error::PiiConfig(e.clone()))),
    };

    replays.retain(
        |replays| &mut replays.replays,
        |replay, _| {
            let Some(payload) = replay.get_recording() else {
                return Ok(());
            };

            // Has some internal state so don't move out of the retain.
            let mut scrubber = RecordingScrubber::new(
                ctx.config.max_replay_uncompressed_size(),
                ctx.project_info.config.pii_config.as_ref(),
                pii_config,
            );

            if scrubber.is_empty() {
                return Ok::<(), Error>(());
            }

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
    Ok(())
}

/// Applies PII scrubbing to individual replay events and recordings.
///
/// Will reject the entire envelope if loading the pii config fails for replay_recordings.
pub fn scrub(
    replays: &mut Managed<ExpandedReplays>,
    ctx: Context<'_>,
) -> Result<(), Rejected<Error>> {
    replays.retain(
        |replays| &mut replays.replays,
        |replay, _| {
            if let Some(event) = replay.get_event() {
                scrub_event(event, ctx)
                    .inspect_err(|err| relay_log::debug!("failed to scrub pii from replay: {err}"))
            } else {
                Ok(())
            }
        },
    );

    if ctx
        .project_info
        .has_feature(Feature::SessionReplayRecordingScrubbing)
    {
        scrub_recordings(replays, ctx)
    } else {
        Ok(())
    }
}
