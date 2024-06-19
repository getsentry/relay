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
    let profiling_enabled = state.project_state.has_feature(Feature::Profiling);
    let has_transaction = state.event_type() == Some(EventType::Transaction);
    let keep_unsampled_profiles = state
        .project_state
        .has_feature(Feature::IngestUnsampledProfiles);

    let mut profile_id = None;
    state.managed_envelope.retain_items(|item| match item.ty() {
        // First profile found in the envelope, we'll keep it if metadata are valid.
        ItemType::Profile if profile_id.is_none() => {
            if !profiling_enabled {
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
                profiler_id: Annotated::empty(),
            });
        }
        None => {
            if let Some(contexts) = event.contexts.value_mut() {
                if let Some(profile_context) = contexts.get::<ProfileContext>() {
                    profile_context.profile_id = Annotated::empty();
                }
            }
        }
    }
}

/// Processes profiles and set the profile ID in the profile context on the transaction if successful.
pub fn process(state: &mut ProcessEnvelopeState<TransactionGroup>, config: &Config) {
    let profiling_enabled = state.project_state.has_feature(Feature::Profiling);
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

            expand_profile(item, event, config)
        }
        _ => ItemAction::Keep,
    });
}

/// Transfers transaction metadata to profile and check its size.
fn expand_profile(item: &mut Item, event: &Event, config: &Config) -> ItemAction {
    match relay_profiling::expand_profile(&item.payload(), event) {
        Ok((_id, payload)) => {
            if payload.len() <= config.max_profile_size() {
                item.set_payload(ContentType::Json, payload);
                ItemAction::Keep
            } else {
                ItemAction::Drop(Outcome::Invalid(DiscardReason::Profiling(
                    relay_profiling::discard_reason(relay_profiling::ProfileError::ExceedSizeLimit),
                )))
            }
        }
        Err(err) => ItemAction::Drop(Outcome::Invalid(DiscardReason::Profiling(
            relay_profiling::discard_reason(err),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use insta::assert_debug_snapshot;
    #[cfg(not(feature = "processing"))]
    use relay_dynamic_config::Feature;
    use relay_event_schema::protocol::EventId;
    use relay_sampling::evaluation::ReservoirCounters;
    use relay_system::Addr;

    use crate::envelope::Envelope;
    use crate::extractors::RequestMeta;
    use crate::services::processor::{ProcessEnvelope, ProcessingGroup};
    use crate::services::project::ProjectState;
    use crate::testutils::create_test_processor;
    use crate::utils::ManagedEnvelope;

    use super::*;

    #[tokio::test]
    async fn test_profile_id_transfered() {
        // Setup
        let processor = create_test_processor(Default::default());
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
                r#"
            {
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
            }
            "#,
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
                    "timestamp": "2023-10-10 00:00:00Z"
                }"#,
            );
            item
        });

        let mut project_state = ProjectState::allowed();
        project_state.config.features.0.insert(Feature::Profiling);

        let mut envelopes = ProcessingGroup::split_envelope(*envelope);
        assert_eq!(envelopes.len(), 1);

        let (group, envelope) = envelopes.pop().unwrap();
        let envelope = ManagedEnvelope::standalone(envelope, Addr::dummy(), Addr::dummy(), group);

        let message = ProcessEnvelope {
            envelope,
            project_state: Arc::new(project_state),
            sampling_project_state: None,
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

        let mut project_state = ProjectState::allowed();
        project_state.config.features.0.insert(Feature::Profiling);

        let mut envelopes = ProcessingGroup::split_envelope(*envelope);
        assert_eq!(envelopes.len(), 1);

        let (group, envelope) = envelopes.pop().unwrap();
        let envelope =
            ManagedEnvelope::standalone(envelope.clone(), Addr::dummy(), Addr::dummy(), group);

        let message = ProcessEnvelope {
            envelope,
            project_state: Arc::new(project_state),
            sampling_project_state: None,
            reservoir_counters: ReservoirCounters::default(),
        };

        let envelope_response = processor.process(message).unwrap();
        assert!(envelope_response.envelope.is_none());
    }
}
