//! Replay related processor code.

use std::collections::BTreeMap;
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
use relay_statsd::metric;

use crate::envelope::{ContentType, Item, ItemType};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::{ProcessEnvelopeState, ProcessingError};
use crate::statsd::RelayTimers;
use crate::utils::ItemAction;

/// Removes replays if the feature flag is not enabled.
pub fn process(state: &mut ProcessEnvelopeState, config: &Config) -> Result<(), ProcessingError> {
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

    process_replays_combine_items(state)?;

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

fn process_replays_combine_items(state: &mut ProcessEnvelopeState) -> Result<(), ProcessingError> {
    let project_state = &state.project_state;
    let combined_envelope_items =
        project_state.has_feature(Feature::SessionReplayCombinedEnvelopeItems);

    if combined_envelope_items {
        // If this flag is enabled, combine both items into a single item,
        // and remove the original items.
        // The combined Item's payload is a MsgPack map with the keys
        // "replay_event" and "replay_recording".
        // The values are the original payloads of the items.
        let envelope = &mut state.envelope_mut();
        if let Some(replay_event_item) =
            envelope.take_item_by(|item| item.ty() == &ItemType::ReplayEvent)
        {
            if let Some(replay_recording_item) =
                envelope.take_item_by(|item| item.ty() == &ItemType::ReplayRecording)
            {
                let mut data = Vec::new();
                let mut combined_item_payload = BTreeMap::new();

                combined_item_payload.insert("replay_event", replay_event_item.payload().to_vec());
                combined_item_payload
                    .insert("replay_recording", replay_recording_item.payload().to_vec());

                if let Err(e) = rmp_serde::encode::write(&mut data, &combined_item_payload) {
                    relay_log::error!(
                        "failed to serialize combined replay event and recording: {}",
                        e
                    );
                    // TODO: what to do here? Drop + emit outcome?
                }

                let mut combined_item = Item::new(ItemType::CombinedReplayEventAndRecording);

                combined_item.set_payload(ContentType::MsgPack, data);
                envelope.add_item(combined_item);
            } else {
                envelope.add_item(replay_event_item)
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::envelope::{ContentType, Envelope, Item, ItemType};
    use crate::extractors::RequestMeta;
    use crate::services::processor::ProcessEnvelope;
    use crate::services::project::ProjectState;
    use crate::services::{outcome_aggregator, test_store};
    use crate::testutils::create_test_processor;
    use crate::utils::ManagedEnvelope;
    use relay_dynamic_config::Feature;
    use relay_event_schema::protocol::EventId;
    use relay_sampling::evaluation::ReservoirCounters;
    use relay_system::Addr;
    use std::sync::Arc;

    #[tokio::test]
    #[cfg(feature = "processing")]
    async fn test_replays_combined_payload() {
        let processor = create_test_processor(Default::default());
        let event_id = EventId::new();

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);

        envelope.add_item({
            let mut item = Item::new(ItemType::ReplayRecording);
            item.set_payload(ContentType::OctetStream, r###"{"foo": "bar"}"###);
            item
        });

        envelope.add_item({
            let mut item = Item::new(ItemType::ReplayEvent);
            item.set_payload(ContentType::Json, r#"{
                "type": "replay_event",
                "replay_id": "d2132d31b39445f1938d7e21b6bf0ec4",
                "replay_type": "session",
                "event_id": "d2132d31b39445f1938d7e21b6bf0ec4",
                "segment_id": 0,
                "timestamp": 1597977777.6189718,
                "replay_start_timestamp": 1597976392.6542819,
                "urls": ["sentry.io"],
                "error_ids": ["1", "2"],
                "trace_ids": ["3", "4"],
                "dist": "1.12",
                "platform": "javascript",
                "environment": "production",
                "release": 42,
                "tags": {
                    "transaction": "/organizations/:orgId/performance/:eventSlug/"
                },
                "sdk": {
                    "name": "name",
                    "version": "veresion"
                },
                "user": {
                    "id": "123",
                    "username": "user",
                    "email": "user@site.com",
                    "ip_address": "192.168.11.12"
                },
                "request": {
                    "url": null,
                    "headers": {
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15"
                    }
                },
                "contexts": {
                    "trace": {
                        "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
                        "span_id": "FA90FDEAD5F74052",
                        "type": "trace"
                    },
                    "replay": {
                        "error_sample_rate": 0.125,
                        "session_sample_rate": 0.5
                    }
                }
            }"#);
            item
        });

        let mut project_state = ProjectState::allowed();
        project_state
            .config
            .features
            .0
            .insert(Feature::SessionReplay);
        project_state
            .config
            .features
            .0
            .insert(Feature::SessionReplayCombinedEnvelopeItems);

        let mut envelopes = crate::services::processor::ProcessingGroup::split_envelope(*envelope);

        let (group, envelope) = envelopes.pop().unwrap();

        let message = ProcessEnvelope {
            envelope: ManagedEnvelope::standalone(envelope, Addr::dummy(), Addr::dummy(), group),
            project_state: Arc::new(project_state),
            sampling_project_state: None,
            reservoir_counters: ReservoirCounters::default(),
        };

        let envelope_response = processor.process(message).unwrap();
        let ctx = envelope_response.envelope.unwrap();
        let new_envelope = ctx.envelope();

        assert_eq!(new_envelope.len(), 1);

        assert_eq!(
            new_envelope.items().next().unwrap().ty(),
            &ItemType::CombinedReplayEventAndRecording
        );
    }
}
