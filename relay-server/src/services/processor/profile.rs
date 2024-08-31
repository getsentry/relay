//! Profiles related processor code.

use relay_dynamic_config::Feature;

use relay_base_schema::events::EventType;
use relay_config::Config;
use relay_event_schema::protocol::{Contexts, Event, ProfileContext};
use relay_profiling::{ProfileError, ProfileId};
use relay_protocol::Annotated;

use crate::envelope::{ContentType, Item, ItemType};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::{ProcessEnvelopeState, TransactionGroup};
use crate::utils::ItemAction;

/// Filters out invalid and duplicate profiles.
///
/// Returns the profile id of the single remaining profile, if there is one.
pub fn filter<G>(state: &mut ProcessEnvelopeState<G>) -> Option<ProfileId> {
    let profiling_disabled = state.should_filter(Feature::Profiling);
    let has_transaction = state.event_type() == Some(EventType::Transaction);
    let keep_unsampled_profiles = true;

    let mut profile_id = None;
    state.managed_envelope.retain_items(|item| match item.ty() {
        // First profile found in the envelope, we'll keep it if metadata are valid.
        ItemType::Profile if profile_id.is_none() => {
            if profiling_disabled {
                return ItemAction::DropSilently;
            }

            // Drop profile without a transaction in the same envelope,
            // except if unsampled profiles are allowed for this project.
            let profile_allowed = has_transaction || (keep_unsampled_profiles && !item.sampled());
            if !profile_allowed {
                return ItemAction::DropSilently;
            }

            match relay_profiling::parse_metadata(&item.payload(), state.project_id) {
                Ok(id) => {
                    profile_id = Some(id);
                    ItemAction::Keep
                }
                Err(err) => ItemAction::Drop(Outcome::Invalid(DiscardReason::Profiling(
                    relay_profiling::discard_reason(err),
                ))),
            }
        }
        // We found another profile, we'll drop it.
        ItemType::Profile => ItemAction::Drop(Outcome::Invalid(DiscardReason::Profiling(
            relay_profiling::discard_reason(ProfileError::TooManyProfiles),
        ))),
        _ => ItemAction::Keep,
    });

    profile_id
}

/// Transfers the profile ID from the profile item to the transaction item.
///
/// The profile id may be `None` when the envelope does not contain a profile,
/// in that case the profile context is removed.
/// Some SDKs send transactions with profile ids but omit the profile in the envelope.
pub fn transfer_id(
    state: &mut ProcessEnvelopeState<TransactionGroup>,
    profile_id: Option<ProfileId>,
) {
    let Some(event) = state.event.value_mut() else {
        return;
    };

    match profile_id {
        Some(profile_id) => {
            let contexts = event.contexts.get_or_insert_with(Contexts::new);
            contexts.add(ProfileContext {
                profile_id: Annotated::new(profile_id),
                ..ProfileContext::default()
            });
        }
        None => {
            if let Some(contexts) = event.contexts.value_mut() {
                if let Some(profile_context) = contexts.get_mut::<ProfileContext>() {
                    profile_context.profile_id = Annotated::empty();
                }
            }
        }
    }
}

/// Processes profiles and set the profile ID in the profile context on the transaction if successful.
pub fn process(state: &mut ProcessEnvelopeState<TransactionGroup>) -> Option<ProfileId> {
    let profiling_enabled = state.project_state.has_feature(Feature::Profiling);
    let mut profile_id = None;

    state.managed_envelope.retain_items(|item| match item.ty() {
        ItemType::Profile => {
            if !profiling_enabled {
                return ItemAction::DropSilently;
            }

            // There should always be an event/transaction available at this stage.
            // It is required to expand the profile. If it's missing, drop the item.
            let Some(event) = state.event.value() else {
                return ItemAction::DropSilently;
            };

            match expand_profile(item, event, &state.config) {
                Ok(id) => {
                    profile_id = Some(id);
                    ItemAction::Keep
                }
                Err(outcome) => ItemAction::Drop(outcome),
            }
        }
        _ => ItemAction::Keep,
    });

    profile_id
}

/// Transfers transaction metadata to profile and check its size.
fn expand_profile(item: &mut Item, event: &Event, config: &Config) -> Result<ProfileId, Outcome> {
    match relay_profiling::expand_profile(&item.payload(), event) {
        Ok((id, payload)) => {
            if payload.len() <= config.max_profile_size() {
                item.set_payload(ContentType::Json, payload);
                Ok(id)
            } else {
                Err(Outcome::Invalid(DiscardReason::Profiling(
                    relay_profiling::discard_reason(relay_profiling::ProfileError::ExceedSizeLimit),
                )))
            }
        }
        Err(err) => Err(Outcome::Invalid(DiscardReason::Profiling(
            relay_profiling::discard_reason(err),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    #[cfg(feature = "processing")]
    use insta::assert_debug_snapshot;
    #[cfg(not(feature = "processing"))]
    use relay_dynamic_config::Feature;
    use relay_event_schema::protocol::EventId;
    use relay_sampling::evaluation::ReservoirCounters;
    use relay_system::Addr;

    use crate::envelope::Envelope;
    use crate::extractors::RequestMeta;
    use crate::services::processor::{ProcessEnvelope, ProcessingGroup};
    use crate::services::project::ProjectInfo;
    use crate::testutils::create_test_processor;
    use crate::utils::ManagedEnvelope;

    use super::*;

    #[cfg(feature = "processing")]
    #[tokio::test]
    async fn test_profile_id_transfered() {
        // Setup
        let config = Config::from_json_value(serde_json::json!({
            "processing": {
                "enabled": true,
                "kafka_config": []
            }
        }))
        .unwrap();
        let processor = create_test_processor(config);
        let event_id = EventId::new();
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);

        // Add a valid transaction item.
        envelope.add_item({
            let mut item = Item::new(ItemType::Transaction);

            item.set_payload(
                ContentType::Json,
                r#"{
                    "event_id": "9b73438f70e044ecbd006b7fd15b7373",
                    "type": "transaction",
                    "transaction": "/foo/",
                    "timestamp": 946684810.0,
                    "start_timestamp": 946684800.0,
                    "contexts": {
                        "trace": {
                        "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                        "span_id": "fa90fdead5f74053",
                        "op": "http.server",
                        "type": "trace"
                        }
                    },
                    "transaction_info": {
                        "source": "url"
                    }
                }"#,
            );
            item
        });

        // Add a profile to the same envelope.
        envelope.add_item({
            let mut item = Item::new(ItemType::Profile);
            item.set_payload(
                ContentType::Json,
                r#"{
                    "profile_id": "012d836b15bb49d7bbf99e64295d995b",
                    "version": "1",
                    "platform": "android",
                    "os": {"name": "foo", "version": "bar"},
                    "device": {"architecture": "zap"},
                    "timestamp": "2023-10-10 00:00:00Z",
                    "profile": {
                        "samples":[
                            {
                                "stack_id":0,
                                "elapsed_since_start_ns":1,
                                "thread_id":1
                            },
                            {
                                "stack_id":0,
                                "elapsed_since_start_ns":2,
                                "thread_id":1
                            }
                        ],
                        "stacks":[[0]],
                        "frames":[{
                            "function":"main"
                        }]
                    },
                    "transactions": [
                        {
                            "id": "9b73438f70e044ecbd006b7fd15b7373",
                            "name": "/foo/",
                            "trace_id": "4c79f60c11214eb38604f4ae0781bfb2"
                        }
                    ]
                }"#,
            );
            item
        });

        let mut project_state = ProjectInfo::default();
        project_state.config.features.0.insert(Feature::Profiling);

        let mut envelopes = ProcessingGroup::split_envelope(*envelope);
        assert_eq!(envelopes.len(), 1);

        let (group, envelope) = envelopes.pop().unwrap();
        let envelope = ManagedEnvelope::new(envelope, Addr::dummy(), Addr::dummy(), group);

        let message = ProcessEnvelope {
            envelope,
            project_info: Arc::new(project_state),
            sampling_project_info: None,
            reservoir_counters: ReservoirCounters::default(),
        };

        let envelope_response = processor.process(message).unwrap();
        let ctx = envelope_response.envelope.unwrap();
        let new_envelope = ctx.envelope();

        // Get the re-serialized context.
        let item = new_envelope
            .get_item_by(|item| item.ty() == &ItemType::Transaction)
            .unwrap();
        let transaction = Annotated::<Event>::from_json_bytes(&item.payload()).unwrap();
        let context = transaction
            .value()
            .unwrap()
            .context::<ProfileContext>()
            .unwrap();

        assert_debug_snapshot!(context, @r###"
        ProfileContext {
            profile_id: EventId(
                012d836b-15bb-49d7-bbf9-9e64295d995b,
            ),
            profiler_id: ~,
        }
        "###);
    }

    #[cfg(feature = "processing")]
    #[tokio::test]
    async fn test_invalid_profile_id_not_transfered() {
        // Setup
        let config = Config::from_json_value(serde_json::json!({
            "processing": {
                "enabled": true,
                "kafka_config": []
            }
        }))
        .unwrap();
        let processor = create_test_processor(config);
        let event_id = EventId::new();
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);

        // Add a valid transaction item.
        envelope.add_item({
            let mut item = Item::new(ItemType::Transaction);

            item.set_payload(
                ContentType::Json,
                r#"{
                    "event_id": "9b73438f70e044ecbd006b7fd15b7373",
                    "type": "transaction",
                    "transaction": "/foo/",
                    "timestamp": 946684810.0,
                    "start_timestamp": 946684800.0,
                    "contexts": {
                        "trace": {
                        "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                        "span_id": "fa90fdead5f74053",
                        "op": "http.server",
                        "type": "trace"
                        }
                    },
                    "transaction_info": {
                        "source": "url"
                    }
                }"#,
            );
            item
        });

        // Add a profile to the same envelope.
        envelope.add_item({
            let mut item = Item::new(ItemType::Profile);
            item.set_payload(
                ContentType::Json,
                r#"{
                    "profile_id": "012d836b15bb49d7bbf99e64295d995b",
                    "version": "1",
                    "platform": "android",
                    "os": {"name": "foo", "version": "bar"},
                    "device": {"architecture": "zap"},
                    "timestamp": "2023-10-10 00:00:00Z",
                    "profile": {
                        "samples":[
                            {
                                "stack_id":0,
                                "elapsed_since_start_ns":1,
                                "thread_id":1
                            },
                            {
                                "stack_id":1,
                                "elapsed_since_start_ns":2,
                                "thread_id":1
                            }
                        ],
                        "stacks":[[0],[]],
                        "frames":[{
                            "function":"main"
                        }]
                    },
                    "transactions": [
                        {
                            "id": "9b73438f70e044ecbd006b7fd15b7373",
                            "name": "/foo/",
                            "trace_id": "4c79f60c11214eb38604f4ae0781bfb2"
                        }
                    ]
                }"#,
            );
            item
        });

        let mut project_info = ProjectInfo::default();
        project_info.config.features.0.insert(Feature::Profiling);

        let mut envelopes = ProcessingGroup::split_envelope(*envelope);
        assert_eq!(envelopes.len(), 1);

        let (group, envelope) = envelopes.pop().unwrap();
        let envelope = ManagedEnvelope::new(envelope, Addr::dummy(), Addr::dummy(), group);

        let message = ProcessEnvelope {
            envelope,
            project_info: Arc::new(project_info),
            sampling_project_info: None,
            reservoir_counters: ReservoirCounters::default(),
        };

        let envelope_response = processor.process(message).unwrap();
        let ctx = envelope_response.envelope.unwrap();
        let new_envelope = ctx.envelope();

        // Get the re-serialized context.
        let item = new_envelope
            .get_item_by(|item| item.ty() == &ItemType::Transaction)
            .unwrap();
        let transaction = Annotated::<Event>::from_json_bytes(&item.payload()).unwrap();
        let context = transaction
            .value()
            .unwrap()
            .context::<ProfileContext>()
            .unwrap();

        assert_debug_snapshot!(context, @r###"
        ProfileContext {
            profile_id: ~,
            profiler_id: ~,
        }
        "###);
    }

    #[tokio::test]
    async fn filter_standalone_profile() {
        relay_log::init_test!();

        // Setup
        let processor = create_test_processor(Default::default());
        let event_id = EventId::new();
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);

        // Add a profile to the same envelope.
        envelope.add_item({
            let mut item = Item::new(ItemType::Profile);
            item.set_payload(
                ContentType::Json,
                r#"{
                    "profile_id": "012d836b15bb49d7bbf99e64295d995b",
                    "version": "1",
                    "platform": "android",
                    "os": {"name": "foo", "version": "bar"},
                    "device": {"architecture": "zap"},
                    "timestamp": "2023-10-10 00:00:00Z"
                }"#,
            );
            item
        });

        let mut project_state = ProjectInfo::default();
        project_state.config.features.0.insert(Feature::Profiling);

        let mut envelopes = ProcessingGroup::split_envelope(*envelope);
        assert_eq!(envelopes.len(), 1);

        let (group, envelope) = envelopes.pop().unwrap();
        let envelope = ManagedEnvelope::new(envelope.clone(), Addr::dummy(), Addr::dummy(), group);

        let message = ProcessEnvelope {
            envelope,
            project_info: Arc::new(project_state),
            sampling_project_info: None,
            reservoir_counters: ReservoirCounters::default(),
        };

        let envelope_response = processor.process(message).unwrap();
        assert!(envelope_response.envelope.is_none());
    }

    #[cfg(feature = "processing")]
    #[tokio::test]
    async fn test_profile_id_removed_profiler_id_kept() {
        // Setup
        let config = Config::from_json_value(serde_json::json!({
            "processing": {
                "enabled": true,
                "kafka_config": []
            }
        }))
        .unwrap();
        let processor = create_test_processor(config);
        let event_id = EventId::new();
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);

        // Add a valid transaction item.
        envelope.add_item({
            let mut item = Item::new(ItemType::Transaction);

            item.set_payload(
                ContentType::Json,
                r#"{
                "type": "transaction",
                "transaction": "/foo/",
                "timestamp": 946684810.0,
                "start_timestamp": 946684800.0,
                "contexts": {
                    "trace": {
                        "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                        "span_id": "fa90fdead5f74053",
                        "op": "http.server",
                        "type": "trace"
                    },
                    "profile": {
                        "profile_id": "4c79f60c11214eb38604f4ae0781bfb2",
                        "profiler_id": "4c79f60c11214eb38604f4ae0781bfb2",
                        "type": "profile"
                    }
                },
                "transaction_info": {
                    "source": "url"
                }
            }"#,
            );
            item
        });

        let mut project_state = ProjectInfo::default();
        project_state.config.features.0.insert(Feature::Profiling);

        let mut envelopes = ProcessingGroup::split_envelope(*envelope);
        assert_eq!(envelopes.len(), 1);

        let (group, envelope) = envelopes.pop().unwrap();
        let envelope = ManagedEnvelope::new(envelope, Addr::dummy(), Addr::dummy(), group);

        let message = ProcessEnvelope {
            envelope,
            project_info: Arc::new(project_state),
            sampling_project_info: None,
            reservoir_counters: ReservoirCounters::default(),
        };

        let envelope_response = processor.process(message).unwrap();
        let ctx = envelope_response.envelope.unwrap();
        let new_envelope = ctx.envelope();

        // Get the re-serialized context.
        let item = new_envelope
            .get_item_by(|item| item.ty() == &ItemType::Transaction)
            .unwrap();
        let transaction = Annotated::<Event>::from_json_bytes(&item.payload()).unwrap();
        let context = transaction
            .value()
            .unwrap()
            .context::<ProfileContext>()
            .unwrap();

        assert_debug_snapshot!(context, @r###"
        ProfileContext {
            profile_id: ~,
            profiler_id: EventId(
                4c79f60c-1121-4eb3-8604-f4ae0781bfb2,
            ),
        }
        "###);
    }
}
