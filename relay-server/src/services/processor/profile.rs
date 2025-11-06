//! Profiles related processor code.
use relay_dynamic_config::{Feature, GlobalConfig};
use relay_quotas::DataCategory;
use std::net::IpAddr;

use relay_base_schema::events::EventType;
use relay_base_schema::project::ProjectId;
use relay_config::Config;
use relay_event_schema::protocol::{Contexts, Event, ProfileContext};
use relay_filter::ProjectFiltersConfig;
use relay_profiling::{ProfileError, ProfileId, ProfileType};
use relay_protocol::{Annotated, Empty};
#[cfg(feature = "processing")]
use relay_protocol::{Getter, Remark, RemarkType};

use crate::envelope::{ContentType, Item, ItemType};
use crate::managed::{ItemAction, ManagedEnvelope, TypedEnvelope};
use crate::processing::Context;
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::{TransactionGroup, event_type, should_filter};
use crate::services::projects::project::ProjectInfo;

/// Filters out invalid and duplicate profiles.
///
/// Returns the profile id of the single remaining profile, if there is one.
pub fn filter<Group>(
    managed_envelope: &mut TypedEnvelope<Group>,
    event: &Annotated<Event>,
    config: &Config,
    project_id: ProjectId,
    project_info: &ProjectInfo,
) -> Option<ProfileId> {
    let profiling_disabled = should_filter(config, project_info, Feature::Profiling);
    let has_transaction = event_type(event) == Some(EventType::Transaction);
    let keep_unsampled_profiles = true;

    let mut profile_id = None;
    managed_envelope.retain_items(|item| match item.ty() {
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

            match relay_profiling::parse_metadata(&item.payload(), project_id) {
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
pub fn transfer_id(event: &mut Annotated<Event>, profile_id: Option<ProfileId>) {
    let Some(event) = event.value_mut() else {
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
            if let Some(contexts) = event.contexts.value_mut()
                && let Some(profile_context) = contexts.get_mut::<ProfileContext>()
            {
                profile_context.profile_id = Annotated::empty();
            }
        }
    }
}

/// Removes the profile context from the transaction item if there is an active rate limit.
///
/// With continuous profiling profile chunks are ingested separately to transactions,
/// in the case where these profiles are rate limited the link on the associated transaction(s)
/// should also be removed.
///
/// See also: <https://github.com/getsentry/relay/issues/5071>.
pub fn remove_context_if_rate_limited(
    event: &mut Annotated<Event>,
    envelope: &ManagedEnvelope,
    ctx: Context<'_>,
) {
    let Some(event) = event.value_mut() else {
        return;
    };

    // There is always only either a transaction profile or a continuous profile, never both.
    //
    // If the `profiler_id` is set on the context, it is for a continuous profile, the case we want
    // to handle here.
    // If it is empty -> do nothing.
    let profile_ctx = event.context::<ProfileContext>();
    if profile_ctx.is_none_or(|pctx| pctx.profiler_id.is_empty() || !pctx.profile_id.is_empty()) {
        return;
    }

    // Continuous profiling has two separate categories based on the platform, infer the correct
    // category to check for rate limits.
    let scoping = envelope.scoping();
    let categories = match event.platform.as_str().map(ProfileType::from_platform) {
        Some(ProfileType::Ui) => &[
            scoping.item(DataCategory::ProfileChunkUi),
            scoping.item(DataCategory::ProfileDurationUi),
        ],
        Some(ProfileType::Backend) => &[
            scoping.item(DataCategory::ProfileChunk),
            scoping.item(DataCategory::ProfileDuration),
        ],
        _ => return,
    };

    // This is a 'best effort' approach, which is why it is enough to check against cached rate
    // limits here.
    let is_limited = ctx
        .rate_limits
        .is_any_limited_with_quotas(ctx.project_info.get_quotas(), categories);

    if is_limited && let Some(contexts) = event.contexts.value_mut() {
        let _ = contexts.remove::<ProfileContext>();
    }
}

/// Strip out the profiler_id from the transaction's profile context if the transaction lasts less than 20ms.
///
/// This is necessary because if the transaction lasts less than 19.8ms, we know that the respective
/// profile data won't have enough samples to be of any use, hence we "unlink" the profile from the transaction.
#[cfg(feature = "processing")]
pub fn scrub_profiler_id(event: &mut Annotated<Event>) {
    let Some(event) = event.value_mut() else {
        return;
    };
    let transaction_duration = event
        .get_value("event.duration")
        .and_then(|duration| duration.as_f64());

    if !transaction_duration.is_some_and(|duration| duration < 19.8) {
        return;
    }
    if let Some(contexts) = event.contexts.value_mut().as_mut()
        && let Some(profiler_id) = contexts
            .get_mut::<ProfileContext>()
            .map(|ctx| &mut ctx.profiler_id)
    {
        let id = std::mem::take(profiler_id.value_mut());
        let remark = Remark::new(RemarkType::Removed, "transaction_duration");
        profiler_id.meta_mut().add_remark(remark);
        profiler_id.meta_mut().set_original_value(id);
    }
}

/// Processes profiles and set the profile ID in the profile context on the transaction if successful.
pub fn process(
    managed_envelope: &mut TypedEnvelope<TransactionGroup>,
    event: &mut Annotated<Event>,
    global_config: &GlobalConfig,
    config: &Config,
    project_info: &ProjectInfo,
) -> Option<ProfileId> {
    let client_ip = managed_envelope.envelope().meta().client_addr();
    let filter_settings = &project_info.config.filter_settings;

    let profiling_enabled = project_info.has_feature(Feature::Profiling);
    let mut profile_id = None;

    managed_envelope.retain_items(|item| match item.ty() {
        ItemType::Profile => {
            if !profiling_enabled {
                return ItemAction::DropSilently;
            }

            // There should always be an event/transaction available at this stage.
            // It is required to expand the profile. If it's missing, drop the item.
            let Some(event) = event.value() else {
                return ItemAction::DropSilently;
            };

            match expand_profile(
                item,
                event,
                config,
                client_ip,
                filter_settings,
                global_config,
            ) {
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
fn expand_profile(
    item: &mut Item,
    event: &Event,
    config: &Config,
    client_ip: Option<IpAddr>,
    filter_settings: &ProjectFiltersConfig,
    global_config: &GlobalConfig,
) -> Result<ProfileId, Outcome> {
    match relay_profiling::expand_profile(
        &item.payload(),
        event,
        client_ip,
        filter_settings,
        global_config,
    ) {
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
        Err(relay_profiling::ProfileError::Filtered(filter_stat_key)) => {
            Err(Outcome::Filtered(filter_stat_key))
        }
        Err(err) => Err(Outcome::Invalid(DiscardReason::Profiling(
            relay_profiling::discard_reason(err),
        ))),
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "processing")]
    use chrono::{Duration, TimeZone, Utc};
    #[cfg(feature = "processing")]
    use uuid::Uuid;

    #[cfg(feature = "processing")]
    use insta::assert_debug_snapshot;
    use relay_cogs::Token;
    #[cfg(not(feature = "processing"))]
    use relay_dynamic_config::Feature;
    use relay_event_schema::protocol::EventId;
    #[cfg(feature = "processing")]
    use relay_protocol::get_value;
    use relay_sampling::evaluation::ReservoirCounters;
    use relay_system::Addr;

    use crate::envelope::Envelope;
    use crate::extractors::RequestMeta;
    use crate::managed::ManagedEnvelope;
    use crate::processing;
    #[cfg(feature = "processing")]
    use crate::services::processor::Submit;
    use crate::services::processor::{ProcessEnvelopeGrouped, ProcessingGroup};
    use crate::services::projects::project::ProjectInfo;
    use crate::testutils::create_test_processor;

    use super::*;

    #[cfg(feature = "processing")]
    #[tokio::test]
    async fn test_profile_id_transfered() {
        let config = Config::from_json_value(serde_json::json!({
            "processing": {
                "enabled": true,
                "kafka_config": []
            }
        }))
        .unwrap();
        let processor = create_test_processor(config).await;
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

        let mut project_info = ProjectInfo::default();
        project_info.config.features.0.insert(Feature::Profiling);

        let mut envelopes = ProcessingGroup::split_envelope(*envelope, &Default::default());
        assert_eq!(envelopes.len(), 1);

        let (group, envelope) = envelopes.pop().unwrap();
        let envelope = ManagedEnvelope::new(envelope, Addr::dummy());

        let message = ProcessEnvelopeGrouped {
            group,
            envelope,
            ctx: processing::Context {
                project_info: &project_info,
                ..processing::Context::for_test()
            },
            reservoir_counters: &ReservoirCounters::default(),
        };

        let Ok(Some(Submit::Envelope(new_envelope))) =
            processor.process(&mut Token::noop(), message).await
        else {
            panic!();
        };
        let new_envelope = new_envelope.envelope();

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
        let processor = create_test_processor(config).await;
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

        let mut envelopes = ProcessingGroup::split_envelope(*envelope, &Default::default());
        assert_eq!(envelopes.len(), 1);

        let (group, envelope) = envelopes.pop().unwrap();
        let envelope = ManagedEnvelope::new(envelope, Addr::dummy());

        let message = ProcessEnvelopeGrouped {
            group,
            envelope,
            ctx: processing::Context {
                project_info: &project_info,
                ..processing::Context::for_test()
            },
            reservoir_counters: &ReservoirCounters::default(),
        };

        let Ok(Some(Submit::Envelope(new_envelope))) =
            processor.process(&mut Token::noop(), message).await
        else {
            panic!();
        };
        let new_envelope = new_envelope.envelope();

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
        let processor = create_test_processor(Default::default()).await;
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

        let mut project_info = ProjectInfo::default();
        project_info.config.features.0.insert(Feature::Profiling);

        let mut envelopes = ProcessingGroup::split_envelope(*envelope, &Default::default());
        assert_eq!(envelopes.len(), 1);

        let (group, envelope) = envelopes.pop().unwrap();
        let envelope = ManagedEnvelope::new(envelope.clone(), Addr::dummy());

        let message = ProcessEnvelopeGrouped {
            group,
            envelope,
            ctx: processing::Context {
                project_info: &project_info,
                ..processing::Context::for_test()
            },
            reservoir_counters: &ReservoirCounters::default(),
        };

        let envelope = processor
            .process(&mut Token::noop(), message)
            .await
            .unwrap();
        assert!(envelope.is_none());
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
        let processor = create_test_processor(config).await;
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

        let mut project_info = ProjectInfo::default();
        project_info.config.features.0.insert(Feature::Profiling);

        let mut envelopes = ProcessingGroup::split_envelope(*envelope, &Default::default());
        assert_eq!(envelopes.len(), 1);

        let (group, envelope) = envelopes.pop().unwrap();
        let envelope = ManagedEnvelope::new(envelope, Addr::dummy());

        let message = ProcessEnvelopeGrouped {
            group,
            envelope,
            ctx: processing::Context {
                project_info: &project_info,
                ..processing::Context::for_test()
            },
            reservoir_counters: &ReservoirCounters::default(),
        };

        let Ok(Some(Submit::Envelope(new_envelope))) =
            processor.process(&mut Token::noop(), message).await
        else {
            panic!();
        };
        let new_envelope = new_envelope.envelope();

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

    #[cfg(feature = "processing")]
    #[test]
    fn test_scrub_profiler_id_should_be_stripped() {
        let mut contexts = Contexts::new();
        contexts.add(ProfileContext {
            profiler_id: Annotated::new(EventId(
                Uuid::parse_str("52df9022835246eeb317dbd739ccd059").unwrap(),
            )),
            ..Default::default()
        });
        let mut event: Annotated<Event> = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0)
                    .unwrap()
                    .checked_add_signed(Duration::milliseconds(15))
                    .unwrap()
                    .into(),
            ),
            contexts: Annotated::new(contexts),
            ..Default::default()
        });

        scrub_profiler_id(&mut event);

        let profile_context = get_value!(event.contexts)
            .unwrap()
            .get::<ProfileContext>()
            .unwrap();

        assert!(
            profile_context
                .profiler_id
                .meta()
                .iter_remarks()
                .any(|remark| remark.rule_id == *"transaction_duration"
                    && remark.ty == RemarkType::Removed)
        )
    }

    #[cfg(feature = "processing")]
    #[test]
    fn test_scrub_profiler_id_should_not_be_stripped() {
        let mut contexts = Contexts::new();
        contexts.add(ProfileContext {
            profiler_id: Annotated::new(EventId(
                Uuid::parse_str("52df9022835246eeb317dbd739ccd059").unwrap(),
            )),
            ..Default::default()
        });
        let mut event: Annotated<Event> = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0)
                    .unwrap()
                    .checked_add_signed(Duration::milliseconds(20))
                    .unwrap()
                    .into(),
            ),
            contexts: Annotated::new(contexts),
            ..Default::default()
        });

        scrub_profiler_id(&mut event);

        let profile_context = get_value!(event.contexts)
            .unwrap()
            .get::<ProfileContext>()
            .unwrap();

        assert!(
            !profile_context
                .profiler_id
                .meta()
                .iter_remarks()
                .any(|remark| remark.rule_id == *"transaction_duration"
                    && remark.ty == RemarkType::Removed)
        )
    }
}
