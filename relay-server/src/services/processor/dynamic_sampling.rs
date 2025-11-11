//! Dynamic sampling processor related code.

use relay_config::Config;
use relay_dynamic_config::ErrorBoundary;
use relay_event_schema::protocol::{Contexts, Event, TraceContext};
use relay_protocol::{Annotated, Empty};
use relay_quotas::DataCategory;

use crate::envelope::ItemType;
use crate::managed::TypedEnvelope;
use crate::services::outcome::Outcome;
use crate::services::processor::{
    EventProcessing, SpansExtracted, TransactionGroup, event_category,
};
use crate::services::projects::project::ProjectInfo;
use crate::utils::{self};

/// Ensures there is a valid dynamic sampling context and corresponding project state.
///
/// The dynamic sampling context (DSC) specifies the project_key of the project that initiated
/// the trace. That project state should have been loaded previously by the project cache and is
/// available on the `ProcessEnvelopeState`. Under these conditions, this cannot happen:
///
///  - There is no DSC in the envelope headers. This occurs with older or third-party SDKs.
///  - The project key does not exist. This can happen if the project key was disabled, the
///    project removed, or in rare cases when a project from another Sentry instance is referred
///    to.
///  - The project key refers to a project from another organization. In this case the project
///    cache does not resolve the state and instead leaves it blank.
///  - The project state could not be fetched. This is a runtime error, but in this case Relay
///    should fall back to the next-best sampling rule set.
///
/// In all of the above cases, this function will compute a new DSC using information from the
/// event payload, similar to how SDKs do this. The `sampling_project_state` is also switched to
/// the main project state.
///
/// If there is no transaction event in the envelope, this function will do nothing.
///
/// The function will return the sampling project information of the root project for the event. If
/// no sampling project information is specified, the project information of the event’s project
/// will be returned.
pub fn validate_and_set_dsc<'a, T>(
    managed_envelope: &mut TypedEnvelope<T>,
    event: &mut Annotated<Event>,
    project_info: &'a ProjectInfo,
    sampling_project_info: Option<&'a ProjectInfo>,
) -> Option<&'a ProjectInfo> {
    let original_dsc = managed_envelope.envelope().dsc();
    if original_dsc.is_some() && sampling_project_info.is_some() {
        return sampling_project_info;
    }

    // The DSC can only be computed if there's a transaction event. Note that `dsc_from_event`
    // below already checks for the event type.
    if let Some(event) = event.value()
        && let Some(key_config) = project_info.get_public_key_config()
        && let Some(mut dsc) =
            crate::processing::utils::dsc::dsc_from_event(key_config.public_key, event)
    {
        // All other information in the DSC must be discarded, but the sample rate was
        // actually applied by the client and is therefore correct.
        let original_sample_rate = original_dsc.and_then(|dsc| dsc.sample_rate);
        dsc.sample_rate = dsc.sample_rate.or(original_sample_rate);

        managed_envelope.envelope_mut().set_dsc(dsc);
        return Some(project_info);
    }

    // If we cannot compute a new DSC but the old one is incorrect, we need to remove it.
    managed_envelope.envelope_mut().remove_dsc();
    None
}

/// Apply the dynamic sampling decision from `compute_sampling_decision`.
pub fn drop_unsampled_items(
    managed_envelope: &mut TypedEnvelope<TransactionGroup>,
    event: Annotated<Event>,
    outcome: Outcome,
    spans_extracted: SpansExtracted,
) {
    // Remove all items from the envelope which need to be dropped due to dynamic sampling.
    let dropped_items = managed_envelope
        .envelope_mut()
        // Profiles are not dropped by dynamic sampling, they are all forwarded to storage and
        // later processed in Sentry and potentially dropped there.
        .take_items_by(|item| *item.ty() != ItemType::Profile);

    for item in dropped_items {
        for (category, quantity) in item.quantities() {
            // Dynamic sampling only drops indexed items.
            //
            // Only emit the base category, if the item does not have an indexed category.
            if category.index_category().is_none() {
                managed_envelope.track_outcome(outcome.clone(), category, quantity);
            }
        }
    }

    // Mark all remaining items in the envelope as un-sampled.
    for item in managed_envelope.envelope_mut().items_mut() {
        item.set_sampled(false);
    }

    // Another 'hack' to emit outcomes from the container item for the contained items (spans).
    //
    // The entire tracking outcomes for contained elements is not handled in a systematic way
    // and whenever an event/transaction is discarded, contained elements are tracked in a 'best
    // effort' basis (basically in all the cases where someone figured out this is a problem).
    //
    // This is yet another case, when the spans have not yet been separated from the transaction
    // also emit dynamic sampling outcomes for the contained spans.
    if !spans_extracted.0 {
        let spans = event.value().and_then(|e| e.spans.value());
        let span_count = spans.map_or(0, |s| s.len());

        // Track the amount of contained spans + 1 segment span (the transaction itself which would
        // be converted to a span).
        managed_envelope.track_outcome(outcome.clone(), DataCategory::SpanIndexed, span_count + 1);
    }

    // All items have been dropped, now make sure the event is also handled and dropped.
    if let Some(category) = event_category(&event) {
        let category = category.index_category().unwrap_or(category);
        managed_envelope.track_outcome(outcome, category, 1)
    }
}

/// Runs dynamic sampling on an incoming error and tags it in case of successful sampling
/// decision.
///
/// This execution of dynamic sampling is technically a "simulation" since we will use the result
/// only for tagging errors and not for actually sampling incoming events.
pub async fn tag_error_with_sampling_decision<Group: EventProcessing>(
    managed_envelope: &mut TypedEnvelope<Group>,
    event: &mut Annotated<Event>,
    sampling_project_info: Option<&ProjectInfo>,
    config: &Config,
) {
    let (Some(dsc), Some(event)) = (managed_envelope.envelope().dsc(), event.value_mut()) else {
        return;
    };

    let root_state = sampling_project_info.as_ref();
    let sampling_config = match root_state.and_then(|s| s.config.sampling.as_ref()) {
        Some(ErrorBoundary::Ok(config)) => config,
        _ => return,
    };

    if sampling_config.unsupported() {
        if config.processing_enabled() {
            relay_log::error!("found unsupported rules even as processing relay");
        }

        return;
    }

    let Some(sampled) = utils::is_trace_fully_sampled(sampling_config, dsc).await else {
        return;
    };

    // We want to get the trace context, in which we will inject the `sampled` field.
    let context = event
        .contexts
        .get_or_insert_with(Contexts::new)
        .get_or_default::<TraceContext>();

    // We want to update `sampled` only if it was not set, since if we don't check this
    // we will end up overriding the value set by downstream Relays and this will lead
    // to more complex debugging in case of problems.
    if context.sampled.is_empty() {
        relay_log::trace!("tagged error with `sampled = {}` flag", sampled);
        context.sampled = Annotated::new(sampled);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use relay_base_schema::project::ProjectKey;
    use relay_cogs::Token;
    use relay_event_schema::protocol::EventId;
    use relay_protocol::RuleCondition;
    use relay_sampling::config::{RuleId, RuleType, SamplingRule, SamplingValue};
    use relay_sampling::{DynamicSamplingContext, SamplingConfig};
    use relay_system::Addr;

    use crate::envelope::{ContentType, Envelope, Item};
    use crate::extractors::RequestMeta;
    use crate::managed::ManagedEnvelope;
    use crate::processing;
    use crate::services::processor::{ProcessEnvelopeGrouped, ProcessingGroup, Submit};
    use crate::services::projects::project::ProjectInfo;
    use crate::testutils::create_test_processor;

    use super::*;

    /// Always sets the processing item type to event.
    async fn process_envelope_with_root_project_state(
        envelope: Box<Envelope>,
        sampling_project_info: Option<&ProjectInfo>,
    ) -> Envelope {
        let processor = create_test_processor(Default::default()).await;
        let outcome_aggregator = Addr::dummy();

        let mut envelopes = ProcessingGroup::split_envelope(*envelope, &Default::default());
        assert_eq!(envelopes.len(), 1);
        let (group, envelope) = envelopes.pop().unwrap();

        let message = ProcessEnvelopeGrouped {
            group,
            envelope: ManagedEnvelope::new(envelope, outcome_aggregator),
            ctx: processing::Context {
                sampling_project_info,
                ..processing::Context::for_test()
            },
        };

        let Ok(Some(Submit::Envelope(envelope))) =
            processor.process(&mut Token::noop(), message).await
        else {
            panic!();
        };

        envelope.envelope().clone()
    }

    fn extract_first_event_from_envelope(envelope: Envelope) -> Event {
        let item = envelope.items().next().unwrap();
        let annotated_event: Annotated<Event> =
            Annotated::from_json_bytes(&item.payload()).unwrap();
        annotated_event.into_value().unwrap()
    }

    fn mocked_error_item() -> Item {
        let mut item = Item::new(ItemType::Event);
        item.set_payload(
            ContentType::Json,
            r#"{
              "event_id": "52df9022835246eeb317dbd739ccd059",
              "exception": {
                "values": [
                    {
                      "type": "mytype",
                      "value": "myvalue",
                      "module": "mymodule",
                      "thread_id": 42,
                      "other": "value"
                    }
                ]
              }
            }"#,
        );
        item
    }

    #[tokio::test]
    async fn test_error_is_tagged_correctly_if_trace_sampling_result_is_none() {
        let event_id = EventId::new();
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let request_meta = RequestMeta::new(dsn);

        // We test tagging when root project state and dsc are none.
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);
        envelope.add_item(mocked_error_item());
        let new_envelope = process_envelope_with_root_project_state(envelope, None).await;
        let event = extract_first_event_from_envelope(new_envelope);

        assert!(event.contexts.value().is_none());
    }

    fn project_state_with_single_rule(sample_rate: f64) -> ProjectInfo {
        let sampling_config = SamplingConfig {
            rules: vec![SamplingRule {
                condition: RuleCondition::all(),
                sampling_value: SamplingValue::SampleRate { value: sample_rate },
                ty: RuleType::Trace,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            }],
            ..SamplingConfig::new()
        };

        let mut sampling_project_state = ProjectInfo::default();
        sampling_project_state.config.sampling = Some(ErrorBoundary::Ok(sampling_config));
        sampling_project_state
    }

    fn mock_dsc() -> DynamicSamplingContext {
        DynamicSamplingContext {
            trace_id: "67e5504410b1426f9247bb680e5fe0c8".parse().unwrap(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_owned()),
            user: Default::default(),
            replay_id: None,
            environment: None,
            transaction: Some("transaction1".into()),
            sample_rate: Some(0.5),
            sampled: Some(true),
            other: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn test_error_is_tagged_correctly_if_trace_sampling_result_is_some() {
        let event_id = EventId::new();
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);
        envelope.set_dsc(mock_dsc());
        envelope.add_item(mocked_error_item());

        // We test with sample rate equal to 100%.
        let sampling_project_state = project_state_with_single_rule(1.0);
        let new_envelope = process_envelope_with_root_project_state(
            envelope.clone(),
            Some(&sampling_project_state),
        )
        .await;
        let event = extract_first_event_from_envelope(new_envelope);
        let trace_context = event.context::<TraceContext>().unwrap();
        assert!(trace_context.sampled.value().unwrap());

        // We test with sample rate equal to 0%.
        let sampling_project_state = project_state_with_single_rule(0.0);
        let new_envelope =
            process_envelope_with_root_project_state(envelope, Some(&sampling_project_state)).await;
        let event = extract_first_event_from_envelope(new_envelope);
        let trace_context = event.context::<TraceContext>().unwrap();
        assert!(!trace_context.sampled.value().unwrap());
    }

    #[tokio::test]
    async fn test_error_is_not_tagged_if_already_tagged() {
        let event_id = EventId::new();
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let request_meta = RequestMeta::new(dsn);

        // We test tagging with an incoming event that has already been tagged by downstream Relay.
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);
        let mut item = Item::new(ItemType::Event);
        item.set_payload(
            ContentType::Json,
            r#"{
              "event_id": "52df9022835246eeb317dbd739ccd059",
              "exception": {
                "values": [
                    {
                      "type": "mytype",
                      "value": "myvalue",
                      "module": "mymodule",
                      "thread_id": 42,
                      "other": "value"
                    }
                ]
              },
              "contexts": {
                "trace": {
                    "sampled": true
                }
              }
            }"#,
        );
        envelope.add_item(item);
        let sampling_project_state = project_state_with_single_rule(0.0);
        let new_envelope =
            process_envelope_with_root_project_state(envelope, Some(&sampling_project_state)).await;
        let event = extract_first_event_from_envelope(new_envelope);
        let trace_context = event.context::<TraceContext>().unwrap();
        assert!(trace_context.sampled.value().unwrap());
    }
}
