//! Replay related processor code.
use std::error::Error;
use std::net::IpAddr;

use bytes::Bytes;
use relay_base_schema::organization::OrganizationId;
use relay_base_schema::project::ProjectId;
use relay_dynamic_config::{Feature, GlobalConfig, ProjectConfig};
use relay_event_normalization::replay::{self, ReplayError};
use relay_event_normalization::{GeoIpLookup, RawUserAgentInfo};
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
use crate::statsd::{RelayCounters, RelayTimers};

/// Removes replays if the feature flag is not enabled.
pub fn process(
    state: &mut ProcessEnvelopeState<ReplayGroup>,
    global_config: &GlobalConfig,
    geoip_lookup: Option<&GeoIpLookup>,
) -> Result<(), ProcessingError> {
    // If the replay feature is not enabled drop the items silenty.
    if state.should_filter(Feature::SessionReplay) {
        state.managed_envelope.drop_items_silently();
        return Ok(());
    }

    // If the replay video feature is not enabled check the envelope items for a
    // replay video event.
    if state
        .project_info
        .has_feature(Feature::SessionReplayVideoDisabled)
        && count_replay_video_events(state) > 0
    {
        state.managed_envelope.drop_items_silently();
        return Ok(());
    }

    let rpc = {
        let meta = state.envelope().meta();

        ReplayProcessingConfig {
            config: &state.project_info.config,
            global_config,
            geoip_lookup,
            event_id: state.envelope().event_id(),
            project_id: state.project_info.project_id,
            organization_id: state.project_info.organization_id,
            client_addr: meta.client_addr(),
            user_agent: RawUserAgentInfo {
                user_agent: meta.user_agent().map(|s| s.to_owned()),
                client_hints: meta.client_hints().clone(),
            },
        }
    };

    let mut scrubber = if state
        .project_info
        .has_feature(Feature::SessionReplayRecordingScrubbing)
    {
        let datascrubbing_config = rpc
            .config
            .datascrubbing_settings
            .pii_config()
            .map_err(|e| ProcessingError::PiiConfigError(e.clone()))?
            .as_ref();

        Some(RecordingScrubber::new(
            state.config.max_replay_uncompressed_size(),
            rpc.config.pii_config.as_ref(),
            datascrubbing_config,
        ))
    } else {
        None
    };

    for item in state.managed_envelope.envelope_mut().items_mut() {
        if state
            .project_info
            .has_feature(Feature::SessionReplayCombinedEnvelopeItems)
        {
            item.set_replay_combined_payload(true);
        }

        match item.ty() {
            ItemType::ReplayEvent => {
                let replay_event = handle_replay_event_item(item.payload(), &rpc)?;
                item.set_payload(ContentType::Json, replay_event);
            }
            ItemType::ReplayRecording => {
                let replay_recording =
                    handle_replay_recording_item(item.payload(), scrubber.as_mut(), &rpc)?;
                item.set_payload(ContentType::OctetStream, replay_recording);
            }
            ItemType::ReplayVideo => {
                let replay_video =
                    handle_replay_video_item(item.payload(), scrubber.as_mut(), &rpc)?;
                item.set_payload(ContentType::OctetStream, replay_video);
            }
            _ => {}
        }
    }

    Ok(())
}

#[derive(Debug)]
struct ReplayProcessingConfig<'a> {
    pub config: &'a ProjectConfig,
    pub global_config: &'a GlobalConfig,
    pub geoip_lookup: Option<&'a GeoIpLookup>,
    pub event_id: Option<EventId>,
    pub project_id: Option<ProjectId>,
    pub organization_id: Option<OrganizationId>,
    pub client_addr: Option<IpAddr>,
    pub user_agent: RawUserAgentInfo<String>,
}

// Replay Event Processing.

fn handle_replay_event_item(
    payload: Bytes,
    config: &ReplayProcessingConfig<'_>,
) -> Result<Bytes, ProcessingError> {
    match process_replay_event(&payload, config) {
        Ok(replay) => {
            if let Some(replay_type) = replay.value() {
                relay_filter::should_filter(
                    replay_type,
                    config.client_addr,
                    &config.config.filter_settings,
                    config.global_config.filters(),
                )
                .map_err(ProcessingError::ReplayFiltered)?;

                // Log segments that exceed the hour limit so we can diagnose errant SDKs
                // or exotic customer implementations.
                if let Some(segment_id) = replay_type.segment_id.value() {
                    if *segment_id > 720 {
                        metric!(counter(RelayCounters::ReplayExceededSegmentLimit) += 1);

                        relay_log::debug!(
                            event_id = ?config.event_id,
                            project_id = config.project_id.map(|v| v.value()),
                            organization_id = config.organization_id.map(|o| o.value()),
                            segment_id = segment_id,
                            "replay segment-exceeded-limit"
                        );
                    }
                }
            }

            match replay.to_json() {
                Ok(json) => Ok(json.into_bytes().into()),
                Err(error) => {
                    relay_log::error!(
                        error = &error as &dyn Error,
                        event_id = ?config.event_id,
                        "failed to serialize replay"
                    );
                    Ok(payload)
                }
            }
        }
        Err(error) => {
            relay_log::warn!(
                error = &error as &dyn Error,
                event_id = ?config.event_id,
                project_id = config.project_id.map(|v| v.value()),
                organization_id = config.organization_id.map(|o| o.value()),
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
                ReplayError::DateInTheFuture => {
                    ProcessingError::InvalidReplay(DiscardReason::DateInTheFuture)
                }
            })
        }
    }
}

/// Validates, normalizes, and scrubs PII from a replay event.
fn process_replay_event(
    payload: &[u8],
    config: &ReplayProcessingConfig<'_>,
) -> Result<Annotated<Replay>, ReplayError> {
    let mut replay =
        Annotated::<Replay>::from_json_bytes(payload).map_err(ReplayError::CouldNotParse)?;

    let Some(replay_value) = replay.value_mut() else {
        return Err(ReplayError::NoContent);
    };

    replay::validate(replay_value)?;
    replay::normalize(
        &mut replay,
        config.client_addr,
        config.user_agent.as_deref(),
        config.geoip_lookup,
    );

    if let Some(ref config) = config.config.pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        processor::process_value(&mut replay, &mut processor, ProcessingState::root())
            .map_err(|e| ReplayError::CouldNotScrub(e.to_string()))?;
    }

    let pii_config = config
        .config
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
    scrubber: Option<&mut RecordingScrubber>,
    config: &ReplayProcessingConfig<'_>,
) -> Result<Bytes, ProcessingError> {
    // XXX: Processing is there just for data scrubbing. Skip the entire expensive
    // processing step if we do not need to scrub.
    let Some(scrubber) = scrubber else {
        return Ok(payload);
    };
    if scrubber.is_empty() {
        return Ok(payload);
    }

    // Limit expansion of recordings to the max replay size. The payload is
    // decompressed temporarily and then immediately re-compressed. However, to
    // limit memory pressure, we use the replay limit as a good overall limit for
    // allocations.
    metric!(timer(RelayTimers::ReplayRecordingProcessing), {
        scrubber.process_recording(&payload)
    })
    .map(Into::into)
    .map_err(|error| {
        relay_log::error!(
            error = &error as &dyn Error,
            event_id = ?config.event_id,
            project_id = config.project_id.map(|v| v.value()),
            organization_id = config.organization_id.map(|o| o.value()),
            "invalid replay recording"
        );
        ProcessingError::InvalidReplay(DiscardReason::InvalidReplayRecordingEvent)
    })
}

// Replay Video Processing

#[derive(Debug, Deserialize, Serialize)]
struct ReplayVideoEvent {
    replay_event: Bytes,
    replay_recording: Bytes,
    replay_video: Bytes,
}

fn handle_replay_video_item(
    payload: Bytes,
    scrubber: Option<&mut RecordingScrubber>,
    config: &ReplayProcessingConfig<'_>,
) -> Result<Bytes, ProcessingError> {
    let ReplayVideoEvent {
        replay_event,
        replay_recording,
        replay_video,
    } = rmp_serde::from_slice(&payload)
        .map_err(|_| ProcessingError::InvalidReplay(DiscardReason::InvalidReplayVideoEvent))?;

    // Process as a replay-event envelope item.
    let replay_event = handle_replay_event_item(replay_event, config)?;

    // Process as a replay-recording envelope item.
    let replay_recording = handle_replay_recording_item(replay_recording, scrubber, config)?;

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
