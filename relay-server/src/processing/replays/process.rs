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
    Error, ExpandedReplay, ReplayPayload, ReplayVideoEvent, SerializedReplays,
};
use crate::statsd::RelayTimers;

fn expand_video(item: &Item) -> Result<ReplayPayload, Error> {
    let ReplayVideoEvent {
        replay_event: event,
        replay_recording: recording,
        replay_video: video,
    } = rmp_serde::from_slice(&item.payload()).map_err(|_| Error::InvalidReplayVideoEvent)?;

    if video.is_empty() {
        return Err(Error::InvalidReplayVideoEvent);
    }

    let event = Annotated::<Replay>::from_json_bytes(&event)?;

    Ok(ReplayPayload::NativeReplay {
        event,
        recording,
        video,
    })
}

/// Parses a serialized replay into its [`ExpandedReplay`] representation.
///
/// Enforces that only one 'replay' (Web, Native) is sent per envelope. But does not enforce that
/// `replay_event` and `replay_recording` are sent together in the same envelope since some SDKs
/// don't do it and enforcing this would break them.
pub fn expand(
    replays: Managed<SerializedReplays>,
) -> Result<Managed<ExpandedReplay>, Rejected<Error>> {
    replays.try_map(|replays, _| {
        let SerializedReplays {
            headers,
            events,
            recordings,
            videos,
        } = replays;

        match (events.as_slice(), recordings.as_slice(), videos.as_slice()) {
            ([event], [recording], []) => {
                let event_bytes = event.payload();
                let event = Annotated::<Replay>::from_json_bytes(&event_bytes)?;

                Ok(ExpandedReplay {
                    headers,
                    payload: ReplayPayload::WebReplay {
                        event,
                        recording: recording.payload(),
                    },
                })
            }
            ([_], [], []) => {
                // Since standalone events previously already got dropped in the store we can drop
                // them here without breaking any SDKs.
                Err(Error::EventWithoutRecording)
            }
            ([], [recording], []) => Ok(ExpandedReplay {
                headers,
                payload: ReplayPayload::StandaloneRecording {
                    recording: recording.payload(),
                },
            }),
            ([], [], [video]) => {
                expand_video(video).map(|payload| ExpandedReplay { headers, payload })
            }
            (events, recordings, videos) => {
                relay_log::error!(
                    sdk = headers.meta().client().unwrap_or("unknown"),
                    event_count = events.len(),
                    recording_count = recordings.len(),
                    video_count = videos.len(),
                    "unexpected replay item count"
                );

                Err(Error::InvalidItemCount)
            }
        }
    })
}

/// Normalizes a replay event.
pub fn normalize(replay: &mut Managed<ExpandedReplay>, geoip_lookup: &GeoIpLookup) {
    let meta = replay.headers.meta();
    let client_addr = meta.client_addr();
    let user_agent = RawUserAgentInfo {
        user_agent: meta.user_agent().map(String::from),
        client_hints: meta.client_hints().to_owned(),
    };

    replay.modify(|replay, _| {
        if let Some(event) = replay.payload.event_mut() {
            replay::normalize(event, client_addr, &user_agent.as_deref(), geoip_lookup)
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
    }
    Ok(())
}

fn scrub_recordings(
    replay: &mut Managed<ExpandedReplay>,
    ctx: Context<'_>,
) -> Result<(), Rejected<Error>> {
    let event_id = replay.headers.event_id();
    let pii_config = match ctx.project_info.config.datascrubbing_settings.pii_config() {
        Ok(config) => config.as_ref(),
        Err(e) => return Err(replay.reject_err(Error::PiiConfig(e.clone()))),
    };

    let mut scrubber = RecordingScrubber::new(
        ctx.config.max_replay_uncompressed_size(),
        ctx.project_info.config.pii_config.as_ref(),
        pii_config,
    );

    if scrubber.is_empty() {
        return Ok(());
    }

    replay.try_modify(|replay, _| {
        let payload = replay.payload.recording_mut();
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
    })
}

/// Applies PII scrubbing to a replay event and recording.
///
/// Will reject if the scrubbing fails.
pub fn scrub(
    replay: &mut Managed<ExpandedReplay>,
    ctx: Context<'_>,
) -> Result<(), Rejected<Error>> {
    replay.try_modify(|replay, _| {
        if let Some(event) = replay.payload.event_mut() {
            scrub_event(event, ctx)
                .inspect_err(|err| relay_log::debug!("failed to scrub pii from replay: {err}"))
        } else {
            Ok(())
        }
    })?;

    if ctx
        .project_info
        .has_feature(Feature::SessionReplayRecordingScrubbing)
    {
        scrub_recordings(replay, ctx)
    } else {
        Ok(())
    }
}
