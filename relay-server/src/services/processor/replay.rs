//! Replay related processor code.
use std::error::Error;
use std::net::IpAddr;

use bytes::Bytes;
use relay_base_schema::project::ProjectId;
use relay_dynamic_config::{Feature, GlobalConfig, ProjectConfig};
use relay_event_normalization::replay::{self, ReplayError};
use relay_event_normalization::RawUserAgentInfo;
use relay_event_schema::processor::{self, ProcessingState};
use relay_event_schema::protocol::{EventId, Replay};
use relay_pii::PiiProcessor;
use relay_protocol::Annotated;
use relay_replays::recording::RecordingScrubber;
use relay_statsd::metric;
use serde::{Deserialize, Serialize};

use crate::envelope::{ContentType, ItemType};
use crate::services::outcome::DiscardReason;
use crate::services::processor::{ProcessEnvelopeState, ProcessingError, ReplayGroup};
use crate::statsd::RelayTimers;

/// Removes replays if the feature flag is not enabled.
pub fn process(
    state: &mut ProcessEnvelopeState<ReplayGroup>,
    global_config: &GlobalConfig,
) -> Result<(), ProcessingError> {
    let project_state = &state.project_state;
    let replays_disabled = state.should_filter(Feature::SessionReplay);
    let scrubbing_enabled = project_state.has_feature(Feature::SessionReplayRecordingScrubbing);
    let replay_video_disabled = project_state.has_feature(Feature::SessionReplayVideoDisabled);
    let project_id = project_state.project_id;
    let organization_id = project_state.organization_id;

    let meta = state.envelope().meta().clone();
    let client_addr = meta.client_addr();
    let event_id = state.envelope().event_id();

    let limit = state.config.max_replay_uncompressed_size();
    let project_config = project_state.config();
    let datascrubbing_config = project_config
        .datascrubbing_settings
        .pii_config()
        .map_err(|e| ProcessingError::PiiConfigError(e.clone()))?
        .as_ref();
    let mut scrubber = RecordingScrubber::new(
        limit,
        project_config.pii_config.as_ref(),
        datascrubbing_config,
    );

    let user_agent = &RawUserAgentInfo {
        user_agent: meta.user_agent(),
        client_hints: meta.client_hints().as_deref(),
    };
    let combined_envelope_items =
        project_state.has_feature(Feature::SessionReplayCombinedEnvelopeItems);

    // If the replay feature is not enabled drop the items silenty.
    if replays_disabled {
        state.managed_envelope.drop_items_silently();
        return Ok(());
    }

    // If the replay video feature is not enabled check the envelope items for a
    // replay video event.
    if replay_video_disabled && count_replay_video_events(state) > 0 {
        state.managed_envelope.drop_items_silently();
        return Ok(());
    }

    for item in state.managed_envelope.envelope_mut().items_mut() {
        // Set the combined payload header to the value of the combined feature.
        item.set_replay_combined_payload(combined_envelope_items);

        match item.ty() {
            ItemType::ReplayEvent => {
                let replay_event = handle_replay_event_item(
                    item.payload(),
                    &event_id,
                    project_config,
                    global_config,
                    client_addr,
                    user_agent,
                    project_id,
                    organization_id,
                )?;
                item.set_payload(ContentType::Json, replay_event);
            }
            ItemType::ReplayRecording => {
                let replay_recording = handle_replay_recording_item(
                    item.payload(),
                    &event_id,
                    scrubbing_enabled,
                    &mut scrubber,
                )?;
                item.set_payload(ContentType::OctetStream, replay_recording);
            }
            ItemType::ReplayVideo => {
                let replay_video = handle_replay_video_item(
                    item.payload(),
                    &event_id,
                    project_config,
                    global_config,
                    client_addr,
                    user_agent,
                    scrubbing_enabled,
                    &mut scrubber,
                    project_id,
                    organization_id,
                )?;
                item.set_payload(ContentType::OctetStream, replay_video);
            }
            _ => {}
        }
    }

    Ok(())
}

// Replay Event Processing.

#[allow(clippy::too_many_arguments)]
fn handle_replay_event_item(
    payload: Bytes,
    event_id: &Option<EventId>,
    config: &ProjectConfig,
    global_config: &GlobalConfig,
    client_ip: Option<IpAddr>,
    user_agent: &RawUserAgentInfo<&str>,
    project_id: Option<ProjectId>,
    organization_id: Option<u64>,
) -> Result<Bytes, ProcessingError> {
    let filter_settings = &config.filter_settings;

    match process_replay_event(&payload, config, client_ip, user_agent) {
        Ok(replay) => {
            if let Some(replay_type) = replay.value() {
                relay_filter::should_filter(
                    replay_type,
                    client_ip,
                    filter_settings,
                    global_config.filters(),
                )
                .map_err(ProcessingError::ReplayFiltered)?;
            }

            match replay.to_json() {
                Ok(json) => Ok(json.into_bytes().into()),
                Err(error) => {
                    relay_log::error!(
                        error = &error as &dyn Error,
                        ?event_id,
                        "failed to serialize replay"
                    );
                    Ok(payload)
                }
            }
        }
        Err(error) => {
            relay_log::warn!(
                error = &error as &dyn Error,
                ?event_id,
                project_id = project_id.map(|v| v.value()),
                organization_id = organization_id,
                "invalid replay event"
            );
            Err(match error {
                ReplayError::NoContent => {
                    ProcessingError::InvalidReplay(DiscardReason::InvalidReplayEventNoPayload)
                }
                ReplayError::CouldNotScrub(_) => {
                    ProcessingError::InvalidReplay(DiscardReason::InvalidReplayEventPii)
                }
                ReplayError::CouldNotParse(_) => {
                    ProcessingError::InvalidReplay(DiscardReason::InvalidReplayEvent)
                }
                ReplayError::InvalidPayload(_) => {
                    ProcessingError::InvalidReplay(DiscardReason::InvalidReplayEvent)
                }
                ReplayError::TooLong => {
                    ProcessingError::InvalidReplay(DiscardReason::ReplayExceededSegmentLimit)
                }
            })
        }
    }
}

/// Validates, normalizes, and scrubs PII from a replay event.
fn process_replay_event(
    payload: &[u8],
    config: &ProjectConfig,
    client_ip: Option<IpAddr>,
    user_agent: &RawUserAgentInfo<&str>,
) -> Result<Annotated<Replay>, ReplayError> {
    let mut replay =
        Annotated::<Replay>::from_json_bytes(payload).map_err(ReplayError::CouldNotParse)?;

    let Some(replay_value) = replay.value_mut() else {
        return Err(ReplayError::NoContent);
    };

    replay::validate(replay_value)?;
    replay::normalize(&mut replay, client_ip, user_agent);

    if let Some(ref config) = config.pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        processor::process_value(&mut replay, &mut processor, ProcessingState::root())
            .map_err(|e| ReplayError::CouldNotScrub(e.to_string()))?;
    }

    let pii_config = config
        .datascrubbing_settings
        .pii_config()
        .map_err(|e| ReplayError::CouldNotScrub(e.to_string()))?;
    if let Some(config) = pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        processor::process_value(&mut replay, &mut processor, ProcessingState::root())
            .map_err(|e| ReplayError::CouldNotScrub(e.to_string()))?;
    }

    Ok(replay)
}

// Replay Recording Processing

fn handle_replay_recording_item(
    payload: Bytes,
    event_id: &Option<EventId>,
    scrubbing_enabled: bool,
    scrubber: &mut RecordingScrubber,
) -> Result<Bytes, ProcessingError> {
    // XXX: Processing is there just for data scrubbing. Skip the entire expensive
    // processing step if we do not need to scrub.
    if !scrubbing_enabled || scrubber.is_empty() {
        return Ok(payload);
    }

    // Limit expansion of recordings to the max replay size. The payload is
    // decompressed temporarily and then immediately re-compressed. However, to
    // limit memory pressure, we use the replay limit as a good overall limit for
    // allocations.
    let parsed_recording = metric!(timer(RelayTimers::ReplayRecordingProcessing), {
        scrubber.process_recording(&payload)
    });

    match parsed_recording {
        Ok(recording) => Ok(recording.into()),
        Err(e) => {
            relay_log::warn!("replay-recording-event: {e} {event_id:?}");
            Err(ProcessingError::InvalidReplay(
                DiscardReason::InvalidReplayRecordingEvent,
            ))
        }
    }
}

// Replay Video Processing

#[derive(Debug, Deserialize, Serialize)]
struct ReplayVideoEvent {
    replay_event: Bytes,
    replay_recording: Bytes,
    replay_video: Bytes,
}

#[allow(clippy::too_many_arguments)]
fn handle_replay_video_item(
    payload: Bytes,
    event_id: &Option<EventId>,
    config: &ProjectConfig,
    global_config: &GlobalConfig,
    client_ip: Option<IpAddr>,
    user_agent: &RawUserAgentInfo<&str>,
    scrubbing_enabled: bool,
    scrubber: &mut RecordingScrubber,
    project_id: Option<ProjectId>,
    organization_id: Option<u64>,
) -> Result<Bytes, ProcessingError> {
    let ReplayVideoEvent {
        replay_event,
        replay_recording,
        replay_video,
    } = match rmp_serde::from_slice(&payload) {
        Ok(result) => result,
        Err(e) => {
            relay_log::warn!("replay-video-event: {e} {event_id:?}");
            return Err(ProcessingError::InvalidReplay(
                DiscardReason::InvalidReplayVideoEvent,
            ));
        }
    };

    // Process as a replay-event envelope item.
    let replay_event = handle_replay_event_item(
        replay_event,
        event_id,
        config,
        global_config,
        client_ip,
        user_agent,
        project_id,
        organization_id,
    )?;

    // Process as a replay-recording envelope item.
    let replay_recording =
        handle_replay_recording_item(replay_recording, event_id, scrubbing_enabled, scrubber)?;

    // Verify the replay-video payload is not empty.
    if replay_video.is_empty() {
        return Err(ProcessingError::InvalidReplay(
            DiscardReason::InvalidReplayVideoEvent,
        ));
    }

    match rmp_serde::to_vec_named(&ReplayVideoEvent {
        replay_event,
        replay_recording,
        replay_video,
    }) {
        Ok(payload) => Ok(payload.into()),
        Err(_) => Err(ProcessingError::InvalidReplay(
            DiscardReason::InvalidReplayVideoEvent,
        )),
    }
}

// Pre-processors

fn count_replay_video_events(state: &ProcessEnvelopeState<ReplayGroup>) -> usize {
    state
        .managed_envelope
        .envelope()
        .items()
        .filter(|item| item.ty() == &ItemType::ReplayVideo)
        .count()
}
