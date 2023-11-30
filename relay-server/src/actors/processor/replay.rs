//! Replay related processor code.

use std::error::Error;
use std::net::IpAddr;
use std::sync::Arc;

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
use relay_statsd::metric;

use crate::actors::outcome::{DiscardReason, Outcome};
use crate::actors::processor::{ProcessEnvelopeState, ProcessingError};
use crate::envelope::{ContentType, ItemType};
use crate::statsd::RelayTimers;
use crate::utils::ItemAction;

/// Removes replays if the feature flag is not enabled.
pub fn process(
    state: &mut ProcessEnvelopeState,
    config: Arc<Config>,
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

    state.managed_envelope.retain_items(|item| match item.ty() {
        ItemType::ReplayEvent => {
            if !replays_enabled {
                return ItemAction::DropSilently;
            }

            match process_replay_event(&item.payload(), project_config, client_addr, user_agent) {
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
            if !replays_enabled {
                return ItemAction::DropSilently;
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
                    relay_log::warn!("replay-recording-event: {e} {event_id:?}");
                    ItemAction::Drop(Outcome::Invalid(DiscardReason::InvalidReplayRecordingEvent))
                }
            }
        }
        _ => ItemAction::Keep,
    });

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

    if replay.value().is_none() {
        return Err(ReplayError::NoContent);
    }

    // XXX(iker): should `validate` receive `Annotated<Replay>` instead?
    if let Some(replay_value) = replay.value_mut() {
        replay::validate(replay_value)?;
    }
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
