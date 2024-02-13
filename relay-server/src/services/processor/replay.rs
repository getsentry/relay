//! Replay related processor code.

use std::error::Error;
use std::net::IpAddr;

use bytes::Bytes;
use relay_config::Config;
use relay_dynamic_config::{Feature, ProjectConfig};
use relay_event_normalization::replay::{self, ReplayError};
use relay_event_normalization::RawUserAgentInfo;
use relay_event_schema::processor::{self, ProcessingState};
use relay_event_schema::protocol::Replay;
use relay_pii::PiiProcessor;
use relay_protocol::Annotated;
use relay_replays::recording::RecordingScrubber;
use relay_replays::video::validate_replay_video;
use relay_statsd::metric;

use crate::envelope::{ContentType, ItemType};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::{ProcessEnvelopeState, ProcessingError, ReplayGroup};
use crate::statsd::RelayTimers;
use crate::utils::ItemAction;

/// Removes replays if the feature flag is not enabled.
pub fn process(
    state: &mut ProcessEnvelopeState<ReplayGroup>,
    config: &Config,
) -> Result<(), ProcessingError> {
    let project_state = &state.project_state;
    let replays_enabled = project_state.has_feature(Feature::SessionReplay);
    let scrubbing_enabled = project_state.has_feature(Feature::SessionReplayRecordingScrubbing);

    let meta = state.envelope().meta().clone();
    let client_addr = meta.client_addr();
    let event_id = state.envelope().event_id();

    let limit = config.max_replay_uncompressed_size();
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

    // If any envelope item is dropped the whole request should be abandoned.
    let mut item_dropped = false;

    state.managed_envelope.retain_items(|item| {
        if !replays_enabled || item_dropped {
            return ItemAction::DropSilently;
        }

        match item.ty() {
            ItemType::ReplayEvent => {
                if combined_envelope_items {
                    item.set_replay_combined_payload(true);
                }

                match process_replay_event(&item.payload(), project_config, client_addr, user_agent)
                {
                    Ok(replay) => match replay.to_json() {
                        Ok(json) => {
                            item.set_payload(ContentType::Json, json);
                            ItemAction::Keep
                        }
                        Err(error) => {
                            relay_log::error!(
                                error = &error as &dyn Error,
                                "failed to serialize replay"
                            );
                            ItemAction::Keep
                        }
                    },
                    Err(error) => {
                        item_dropped = true;
                        relay_log::warn!(error = &error as &dyn Error, "invalid replay event");
                        ItemAction::Drop(Outcome::Invalid(match error {
                            ReplayError::NoContent => DiscardReason::InvalidReplayEventNoPayload,
                            ReplayError::CouldNotScrub(_) => DiscardReason::InvalidReplayEventPii,
                            ReplayError::CouldNotParse(_) => DiscardReason::InvalidReplayEvent,
                            ReplayError::InvalidPayload(_) => DiscardReason::InvalidReplayEvent,
                        }))
                    }
                }
            }
            ItemType::ReplayRecording => {
                if combined_envelope_items {
                    item.set_replay_combined_payload(true);
                }

                // XXX: Processing is there just for data scrubbing. Skip the entire expensive
                // processing step if we do not need to scrub.
                if !scrubbing_enabled || scrubber.is_empty() {
                    return ItemAction::Keep;
                }

                // Limit expansion of recordings to the max replay size. The payload is
                // decompressed temporarily and then immediately re-compressed. However, to
                // limit memory pressure, we use the replay limit as a good overall limit for
                // allocations.
                let parsed_recording = metric!(timer(RelayTimers::ReplayRecordingProcessing), {
                    scrubber.process_recording(&item.payload())
                });

                match parsed_recording {
                    Ok(recording) => {
                        item.set_payload(ContentType::OctetStream, recording);
                        ItemAction::Keep
                    }
                    Err(e) => {
                        item_dropped = true;
                        relay_log::warn!("replay-recording-event: {e} {event_id:?}");
                        ItemAction::Drop(Outcome::Invalid(
                            DiscardReason::InvalidReplayRecordingEvent,
                        ))
                    }
                }
            }
            ItemType::ReplayVideo => match validate_replay_video(&item.payload()) {
                Ok(()) => ItemAction::Keep,
                Err(e) => {
                    item_dropped = true;
                    relay_log::warn!("could not parse video headers: {e} {event_id:?}");
                    ItemAction::Drop(Outcome::Invalid(DiscardReason::InvalidReplayVideoEvent))
                }
            },
            _ => ItemAction::Keep,
        }
    });

    // If an envelope-item was dropped return an error result to drop the entire envelope.
    if item_dropped {
        return Err(ProcessingError::PartiallyDroppedReplayEnvelope);
    }

    Ok(())
}

/// Validates, normalizes, and scrubs PII from a replay event.
fn process_replay_event(
    payload: &Bytes,
    config: &ProjectConfig,
    client_ip: Option<IpAddr>,
    user_agent: &RawUserAgentInfo<&str>,
) -> Result<Annotated<Replay>, ReplayError> {
    let mut replay =
        Annotated::<Replay>::from_json_bytes(payload).map_err(ReplayError::CouldNotParse)?;

    if let Some(replay_value) = replay.value_mut() {
        replay::validate(replay_value)?;
        replay::normalize(replay_value, client_ip, user_agent);
    } else {
        return Err(ReplayError::NoContent);
    }

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
