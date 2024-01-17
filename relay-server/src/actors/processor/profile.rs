//! Profiles related processor code.

#[cfg(feature = "processing")]
use {crate::envelope::ContentType, relay_config::Config, relay_dynamic_config::Feature};

use relay_base_schema::events::EventType;
use relay_event_schema::protocol::{Contexts, ProfileContext};
use relay_profiling::ProfileError;
use relay_protocol::Annotated;

use crate::actors::outcome::{DiscardReason, Outcome};
use crate::actors::processor::ProcessEnvelopeState;
use crate::envelope::ItemType;
use crate::utils::ItemAction;

/// Removes profiles from the envelope if they can not be parsed.
pub fn filter(state: &mut ProcessEnvelopeState) {
    let transaction_count: usize = state
        .managed_envelope
        .envelope()
        .items()
        .filter(|item| item.ty() == &ItemType::Transaction)
        .count();
    let mut profile_id = None;
    state.managed_envelope.retain_items(|item| match item.ty() {
        // Drop profile without a transaction in the same envelope.
        ItemType::Profile if transaction_count == 0 => ItemAction::DropSilently,
        // First profile found in the envelope, we'll keep it if metadata are valid.
        ItemType::Profile if profile_id.is_none() => {
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
    state.profile_id = profile_id;
}

/// Transfers the profile ID from the profile item to the transaction item.
///
/// If profile processing happens at a later stage, we remove the context again.
pub fn transfer_id(state: &mut ProcessEnvelopeState) {
    if let Some(event) = state.event.value_mut() {
        if event.ty.value() == Some(&EventType::Transaction) {
            let contexts = event.contexts.get_or_insert_with(Contexts::new);
            // If we found a profile, add its ID to the profile context on the transaction.
            if let Some(profile_id) = state.profile_id {
                contexts.add(ProfileContext {
                    profile_id: Annotated::new(profile_id),
                });
            }
        }
    }
}

/// Processes profiles and set the profile ID in the profile context on the transaction if successful.
#[cfg(feature = "processing")]
pub fn process(state: &mut ProcessEnvelopeState, config: &Config) {
    let profiling_enabled = state.project_state.has_feature(Feature::Profiling);
    let mut found_profile_id = None;
    state.managed_envelope.retain_items(|item| match item.ty() {
        ItemType::Profile => {
            if !profiling_enabled {
                return ItemAction::DropSilently;
            }
            // If we don't have an event at this stage, we need to drop the profile.
            let Some(event) = state.event.value() else {
                return ItemAction::DropSilently;
            };
            match relay_profiling::expand_profile(&item.payload(), event) {
                Ok((profile_id, payload)) => {
                    if payload.len() <= config.max_profile_size() {
                        found_profile_id = Some(profile_id);
                        item.set_payload(ContentType::Json, payload);
                        ItemAction::Keep
                    } else {
                        ItemAction::Drop(Outcome::Invalid(DiscardReason::Profiling(
                            relay_profiling::discard_reason(
                                relay_profiling::ProfileError::ExceedSizeLimit,
                            ),
                        )))
                    }
                }
                Err(err) => ItemAction::Drop(Outcome::Invalid(DiscardReason::Profiling(
                    relay_profiling::discard_reason(err),
                ))),
            }
        }
        _ => ItemAction::Keep,
    });
    if found_profile_id.is_none() {
        // Remove profile context from event.
        if let Some(event) = state.event.value_mut() {
            if event.ty.value() == Some(&EventType::Transaction) {
                if let Some(contexts) = event.contexts.value_mut() {
                    contexts.remove::<ProfileContext>();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use insta::assert_debug_snapshot;
    use relay_event_schema::protocol::{Event, EventId};
    use relay_sampling::evaluation::ReservoirCounters;
    use relay_system::Addr;

    use crate::actors::processor::{Feature, ProcessEnvelope};
    use crate::actors::project::ProjectState;
    use crate::envelope::{ContentType, Envelope, Item};
    use crate::extractors::RequestMeta;
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

        let message = ProcessEnvelope {
            envelope: ManagedEnvelope::standalone(envelope, Addr::dummy(), Addr::dummy()),
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
}
