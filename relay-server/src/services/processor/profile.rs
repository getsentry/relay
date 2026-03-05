//! Profiles related processor code.

use relay_dynamic_config::Feature;

use relay_config::Config;
use relay_profiling::ProfileError;

use crate::envelope::ItemType;
use crate::managed::{ItemAction, TypedEnvelope};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::should_filter;
use crate::services::projects::project::ProjectInfo;

pub fn filter<Group>(
    managed_envelope: &mut TypedEnvelope<Group>,
    config: &Config,
    project_info: &ProjectInfo,
) {
    let profiling_disabled = should_filter(config, project_info, Feature::Profiling);

    let mut saw_profile = false;
    managed_envelope.retain_items(|item| match item.ty() {
        ItemType::Profile if profiling_disabled => ItemAction::DropSilently,
        // First profile found in the envelope, we'll keep it if metadata are valid.
        ItemType::Profile if !saw_profile => {
            // Drop profile without a transaction in the same envelope,
            // except if unsampled profiles are allowed for this project.
            let profile_allowed = !item.sampled();
            if !profile_allowed {
                return ItemAction::DropSilently;
            }

            match relay_profiling::parse_metadata(&item.payload()) {
                Ok(_) => {
                    saw_profile = true;
                    ItemAction::Keep
                }
                Err(err) => ItemAction::Drop(Outcome::Invalid(DiscardReason::Profiling(
                    relay_profiling::discard_reason(&err),
                ))),
            }
        }
        // We found another profile, we'll drop it.
        ItemType::Profile => ItemAction::Drop(Outcome::Invalid(DiscardReason::Profiling(
            relay_profiling::discard_reason(&ProfileError::TooManyProfiles),
        ))),
        _ => ItemAction::Keep,
    });
}

#[cfg(test)]
mod tests {
    use crate::envelope::{ContentType, Envelope, Item};
    use crate::extractors::RequestMeta;
    use crate::managed::ManagedEnvelope;
    use crate::processing::{self, Outputs};
    use crate::services::processor::Submit;
    use crate::services::processor::{ProcessEnvelopeGrouped, ProcessingGroup};
    use crate::services::projects::project::ProjectInfo;
    use crate::testutils::create_test_processor;
    use insta::assert_debug_snapshot;
    use relay_dynamic_config::{ErrorBoundary, Feature, GlobalConfig, TransactionMetricsConfig};
    use relay_event_schema::protocol::{Event, EventId, ProfileContext};
    use relay_protocol::Annotated;
    use relay_system::Addr;

    use super::*;

    async fn process_event(envelope: Box<Envelope>) -> Option<Annotated<Event>> {
        let config = Config::from_json_value(serde_json::json!({
            "processing": {
                "enabled": true,
                "kafka_config": []
            }
        }))
        .unwrap();
        let processor = create_test_processor(config).await;
        let mut envelopes = ProcessingGroup::split_envelope(*envelope, &Default::default());
        assert_eq!(envelopes.len(), 1);
        let (group, envelope) = envelopes.pop().unwrap();

        let envelope = ManagedEnvelope::new(envelope, Addr::dummy());

        let mut project_info = ProjectInfo::default().sanitized(false);
        project_info.config.transaction_metrics =
            Some(ErrorBoundary::Ok(TransactionMetricsConfig::new()));
        project_info.config.features.0.insert(Feature::Profiling);

        let mut global_config = GlobalConfig::default();
        global_config.normalize();
        let message = ProcessEnvelopeGrouped {
            group,
            envelope,
            ctx: processing::Context {
                config: &processor.inner.config,
                project_info: &project_info,
                global_config: &global_config,
                ..processing::Context::for_test()
            },
        };

        let result = processor.process(message).await.unwrap()?;

        let Submit::Output {
            output: Outputs::Transactions(t),
            ctx: _,
        } = result
        else {
            panic!();
        };
        Some(t.event().unwrap())
    }

    #[tokio::test]
    async fn test_profile_id_transfered() {
        relay_log::init_test!();

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

        let event = process_event(envelope).await.unwrap();

        let context = event.value().unwrap().context::<ProfileContext>().unwrap();

        assert_debug_snapshot!(context, @r###"
        ProfileContext {
            profile_id: EventId(
                012d836b-15bb-49d7-bbf9-9e64295d995b,
            ),
            profiler_id: ~,
        }
        "###);
    }

    #[tokio::test]
    async fn test_invalid_profile_id_not_transfered() {
        // Setup
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

        let event = process_event(envelope).await.unwrap();
        let context = event.value().unwrap().context::<ProfileContext>().unwrap();

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

        let event = process_event(envelope).await;
        assert!(event.is_none());
    }

    #[tokio::test]
    async fn test_profile_id_removed_profiler_id_kept() {
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

        let event = process_event(envelope).await.unwrap();
        let context = event.value().unwrap().context::<ProfileContext>().unwrap();

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
