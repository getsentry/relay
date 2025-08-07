//! Dynamic sampling processor related code.
use std::ops::ControlFlow;
use std::sync::Arc;

use chrono::Utc;
use relay_config::Config;
use relay_dynamic_config::ErrorBoundary;
use relay_event_schema::protocol::{Contexts, Event, TraceContext};
use relay_protocol::{Annotated, Empty};
use relay_quotas::DataCategory;
use relay_sampling::config::RuleType;
use relay_sampling::evaluation::{ReservoirEvaluator, SamplingEvaluator};
use relay_sampling::{DynamicSamplingContext, SamplingConfig};

use crate::envelope::ItemType;
use crate::managed::TypedEnvelope;
use crate::services::outcome::Outcome;
use crate::services::processor::{
    EventProcessing, Sampling, SpansExtracted, TransactionGroup, event_category,
};
use crate::services::projects::project::ProjectInfo;
use crate::utils::{self, SamplingResult};

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
pub fn validate_and_set_dsc<T>(
    managed_envelope: &mut TypedEnvelope<T>,
    event: &mut Annotated<Event>,
    project_info: Arc<ProjectInfo>,
    sampling_project_info: Option<Arc<ProjectInfo>>,
) -> Option<Arc<ProjectInfo>> {
    let original_dsc = managed_envelope.envelope().dsc();
    if original_dsc.is_some() && sampling_project_info.is_some() {
        return sampling_project_info;
    }

    // The DSC can only be computed if there's a transaction event. Note that `dsc_from_event`
    // below already checks for the event type.
    if let Some(event) = event.value()
        && let Some(key_config) = project_info.get_public_key_config()
        && let Some(mut dsc) = utils::dsc_from_event(key_config.public_key, event)
    {
        // All other information in the DSC must be discarded, but the sample rate was
        // actually applied by the client and is therefore correct.
        let original_sample_rate = original_dsc.and_then(|dsc| dsc.sample_rate);
        dsc.sample_rate = dsc.sample_rate.or(original_sample_rate);

        managed_envelope.envelope_mut().set_dsc(dsc);
        return Some(project_info.clone());
    }

    // If we cannot compute a new DSC but the old one is incorrect, we need to remove it.
    managed_envelope.envelope_mut().remove_dsc();
    None
}

/// Computes the sampling decision on the incoming event
pub async fn run<Group>(
    managed_envelope: &mut TypedEnvelope<Group>,
    event: &mut Annotated<Event>,
    config: Arc<Config>,
    project_info: Arc<ProjectInfo>,
    sampling_project_info: Option<Arc<ProjectInfo>>,
    reservoir: &ReservoirEvaluator<'_>,
) -> SamplingResult
where
    Group: Sampling,
{
    if !Group::supports_sampling(&project_info) {
        return SamplingResult::Pending;
    }

    let sampling_config = match project_info.config.sampling {
        Some(ErrorBoundary::Ok(ref config)) if !config.unsupported() => Some(config),
        _ => None,
    };

    let root_state = sampling_project_info.as_ref();
    let root_config = match root_state.and_then(|s| s.config.sampling.as_ref()) {
        Some(ErrorBoundary::Ok(config)) if !config.unsupported() => Some(config),
        _ => None,
    };

    let reservoir = Group::supports_reservoir_sampling().then_some(reservoir);

    compute_sampling_decision(
        config.processing_enabled(),
        reservoir,
        sampling_config,
        event.value(),
        root_config,
        managed_envelope.envelope().dsc(),
    )
    .await
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
            // Dynamic sampling only drops indexed items. Upgrade the category to the index
            // category if one exists for this category, for example profiles will be upgraded to profiles indexed,
            // but attachments are still emitted as attachments.
            let category = category.index_category().unwrap_or(category);

            managed_envelope.track_outcome(outcome.clone(), category, quantity);
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

/// Computes the sampling decision on the incoming envelope.
async fn compute_sampling_decision(
    processing_enabled: bool,
    reservoir: Option<&ReservoirEvaluator<'_>>,
    sampling_config: Option<&SamplingConfig>,
    event: Option<&Event>,
    root_sampling_config: Option<&SamplingConfig>,
    dsc: Option<&DynamicSamplingContext>,
) -> SamplingResult {
    if (sampling_config.is_none() || event.is_none())
        && (root_sampling_config.is_none() || dsc.is_none())
    {
        return SamplingResult::NoMatch;
    }

    if sampling_config.is_some_and(|config| config.unsupported())
        || root_sampling_config.is_some_and(|config| config.unsupported())
    {
        if processing_enabled {
            relay_log::error!("found unsupported rules even as processing relay");
        } else {
            return SamplingResult::NoMatch;
        }
    }

    let mut evaluator = match reservoir {
        Some(reservoir) => SamplingEvaluator::new_with_reservoir(Utc::now(), reservoir),
        None => SamplingEvaluator::new(Utc::now()),
    };

    if let (Some(event), Some(sampling_state)) = (event, sampling_config)
        && let Some(seed) = event.id.value().map(|id| id.0)
    {
        let rules = sampling_state.filter_rules(RuleType::Transaction);
        evaluator = match evaluator.match_rules(seed, event, rules).await {
            ControlFlow::Continue(evaluator) => evaluator,
            ControlFlow::Break(sampling_match) => {
                return SamplingResult::Match(sampling_match);
            }
        }
    };

    if let (Some(dsc), Some(sampling_state)) = (dsc, root_sampling_config) {
        let rules = sampling_state.filter_rules(RuleType::Trace);
        return evaluator
            .match_rules(*dsc.trace_id, dsc, rules)
            .await
            .into();
    }

    SamplingResult::NoMatch
}

/// Runs dynamic sampling on an incoming error and tags it in case of successful sampling
/// decision.
///
/// This execution of dynamic sampling is technically a "simulation" since we will use the result
/// only for tagging errors and not for actually sampling incoming events.
pub async fn tag_error_with_sampling_decision<Group: EventProcessing>(
    managed_envelope: &mut TypedEnvelope<Group>,
    event: &mut Annotated<Event>,
    sampling_project_info: Option<Arc<ProjectInfo>>,
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
    use std::sync::Arc;

    use bytes::Bytes;
    use relay_base_schema::events::EventType;
    use relay_base_schema::project::ProjectKey;
    use relay_cogs::Token;
    use relay_dynamic_config::{MetricExtractionConfig, TransactionMetricsConfig};
    use relay_event_schema::protocol::{EventId, LenientString};
    use relay_protocol::RuleCondition;
    use relay_sampling::config::{
        DecayingFunction, RuleId, SamplingRule, SamplingValue, TimeRange,
    };
    use relay_sampling::evaluation::{ReservoirCounters, SamplingDecision, SamplingMatch};
    use relay_system::Addr;

    use crate::envelope::{ContentType, Envelope, Item};
    use crate::extractors::RequestMeta;
    use crate::managed::ManagedEnvelope;
    use crate::services::processor::{ProcessEnvelopeGrouped, ProcessingGroup, SpanGroup, Submit};
    use crate::services::projects::project::ProjectInfo;
    use crate::testutils::{
        self, create_test_processor, new_envelope, state_with_rule_and_condition,
    };

    use super::*;

    fn mocked_event(event_type: EventType, transaction: &str, release: &str) -> Event {
        Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(event_type),
            transaction: Annotated::new(transaction.to_owned()),
            release: Annotated::new(LenientString(release.to_owned())),
            ..Event::default()
        }
    }

    fn dummy_reservoir() -> ReservoirEvaluator<'static> {
        ReservoirEvaluator::new(ReservoirCounters::default())
    }

    // Helper to extract the sampling match from SamplingResult if thats the variant.
    fn get_sampling_match(sampling_result: SamplingResult) -> SamplingMatch {
        if let SamplingResult::Match(sampling_match) = sampling_result {
            sampling_match
        } else {
            panic!()
        }
    }

    /// Always sets the processing item type to event.
    async fn process_envelope_with_root_project_state(
        envelope: Box<Envelope>,
        sampling_project_info: Option<Arc<ProjectInfo>>,
    ) -> Envelope {
        let processor = create_test_processor(Default::default()).await;
        let (outcome_aggregator, test_store) = testutils::processor_services();

        let mut envelopes = ProcessingGroup::split_envelope(*envelope, &Default::default());
        assert_eq!(envelopes.len(), 1);
        let (group, envelope) = envelopes.pop().unwrap();

        let message = ProcessEnvelopeGrouped {
            group,
            envelope: ManagedEnvelope::new(envelope, outcome_aggregator, test_store),
            project_info: Arc::new(ProjectInfo::default()),
            rate_limits: Default::default(),
            sampling_project_info,
            reservoir_counters: ReservoirCounters::default(),
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

    #[tokio::test]
    async fn test_it_keeps_or_drops_transactions() {
        let event = Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("testing".to_owned()),
            ..Event::default()
        };

        for (sample_rate, should_keep) in [(0.0, false), (1.0, true)] {
            let sampling_config = SamplingConfig {
                rules: vec![SamplingRule {
                    condition: RuleCondition::all(),
                    sampling_value: SamplingValue::SampleRate { value: sample_rate },
                    ty: RuleType::Transaction,
                    id: RuleId(1),
                    time_range: Default::default(),
                    decaying_fn: DecayingFunction::Constant,
                }],
                ..SamplingConfig::new()
            };

            // TODO: This does not test if the sampling decision is actually applied. This should be
            // refactored to send a proper Envelope in and call process_state to cover the full
            // pipeline.
            let res = compute_sampling_decision(
                false,
                None,
                Some(&sampling_config),
                Some(&event),
                None,
                None,
            )
            .await;
            assert_eq!(res.decision().is_keep(), should_keep);
        }
    }

    #[tokio::test]
    async fn test_dsc_respects_metrics_extracted() {
        relay_test::setup();
        let (outcome_aggregator, test_store) = testutils::processor_services();

        let config = Arc::new(
            Config::from_json_value(serde_json::json!({
                "processing": {
                    "enabled": true,
                    "kafka_config": [],
                }
            }))
            .unwrap(),
        );

        let get_test_params = |version: Option<u16>| {
            let event = Event {
                id: Annotated::new(EventId::new()),
                ty: Annotated::new(EventType::Transaction),
                transaction: Annotated::new("testing".to_owned()),
                ..Event::default()
            };

            let mut project_info = state_with_rule_and_condition(
                Some(0.0),
                RuleType::Transaction,
                RuleCondition::all(),
            );

            if let Some(version) = version {
                project_info.config.transaction_metrics =
                    ErrorBoundary::Ok(relay_dynamic_config::TransactionMetricsConfig {
                        version,
                        ..Default::default()
                    })
                    .into();
            }

            let envelope = new_envelope(false, "foo");
            let managed_envelope: TypedEnvelope<TransactionGroup> = (
                ManagedEnvelope::new(envelope, outcome_aggregator.clone(), test_store.clone()),
                ProcessingGroup::Transaction,
            )
                .try_into()
                .unwrap();

            let event = Annotated::from(event);

            let project_info = Arc::new(project_info);

            (managed_envelope, event, project_info)
        };

        let reservoir = dummy_reservoir();

        // None represents no TransactionMetricsConfig, DS will not be run
        let (mut managed_envelope, mut event, project_info) = get_test_params(None);
        let sampling_result = run(
            &mut managed_envelope,
            &mut event,
            config.clone(),
            project_info,
            None,
            &reservoir,
        )
        .await;
        assert_eq!(sampling_result.decision(), SamplingDecision::Keep);

        // Current version is 3, so it won't run DS if it's outdated
        let (mut managed_envelope, mut event, project_info) = get_test_params(Some(2));
        let sampling_result = run(
            &mut managed_envelope,
            &mut event,
            config.clone(),
            project_info,
            None,
            &reservoir,
        )
        .await;
        assert_eq!(sampling_result.decision(), SamplingDecision::Keep);

        // Dynamic sampling is run, as the transaction metrics version is up to date.
        let (mut managed_envelope, mut event, project_info) = get_test_params(Some(3));
        let sampling_result = run(
            &mut managed_envelope,
            &mut event,
            config.clone(),
            project_info,
            None,
            &reservoir,
        )
        .await;
        assert_eq!(sampling_result.decision(), SamplingDecision::Drop);
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

    #[tokio::test]
    async fn test_error_is_tagged_correctly_if_trace_sampling_result_is_some() {
        let event_id = EventId::new();
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);
        let dsc = DynamicSamplingContext {
            trace_id: "67e5504410b1426f9247bb680e5fe0c8".parse().unwrap(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_owned()),
            user: Default::default(),
            replay_id: None,
            environment: None,
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: Some(true),
            other: BTreeMap::new(),
        };
        envelope.set_dsc(dsc);
        envelope.add_item(mocked_error_item());

        // We test with sample rate equal to 100%.
        let sampling_project_state = project_state_with_single_rule(1.0);
        let new_envelope = process_envelope_with_root_project_state(
            envelope.clone(),
            Some(Arc::new(sampling_project_state)),
        )
        .await;
        let event = extract_first_event_from_envelope(new_envelope);
        let trace_context = event.context::<TraceContext>().unwrap();
        assert!(trace_context.sampled.value().unwrap());

        // We test with sample rate equal to 0%.
        let sampling_project_state = project_state_with_single_rule(0.0);
        let new_envelope = process_envelope_with_root_project_state(
            envelope,
            Some(Arc::new(sampling_project_state)),
        )
        .await;
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
        let new_envelope = process_envelope_with_root_project_state(
            envelope,
            Some(Arc::new(sampling_project_state)),
        )
        .await;
        let event = extract_first_event_from_envelope(new_envelope);
        let trace_context = event.context::<TraceContext>().unwrap();
        assert!(trace_context.sampled.value().unwrap());
    }

    /// Happy path test for compute_sampling_decision.
    #[tokio::test]
    async fn test_compute_sampling_decision_matching() {
        let event = mocked_event(EventType::Transaction, "foo", "bar");
        let rule = SamplingRule {
            condition: RuleCondition::all(),
            sampling_value: SamplingValue::SampleRate { value: 1.0 },
            ty: RuleType::Transaction,
            id: RuleId(0),
            time_range: TimeRange::default(),
            decaying_fn: Default::default(),
        };

        let sampling_config = SamplingConfig {
            rules: vec![rule],
            ..SamplingConfig::new()
        };

        let res = compute_sampling_decision(
            false,
            None,
            Some(&sampling_config),
            Some(&event),
            None,
            None,
        )
        .await;
        assert!(res.is_match());
    }

    #[tokio::test]
    async fn test_matching_with_unsupported_rule() {
        let event = mocked_event(EventType::Transaction, "foo", "bar");
        let rule = SamplingRule {
            condition: RuleCondition::all(),
            sampling_value: SamplingValue::SampleRate { value: 1.0 },
            ty: RuleType::Transaction,
            id: RuleId(0),
            time_range: TimeRange::default(),
            decaying_fn: Default::default(),
        };

        let unsupported_rule = SamplingRule {
            condition: RuleCondition::all(),
            sampling_value: SamplingValue::SampleRate { value: 1.0 },
            ty: RuleType::Unsupported,
            id: RuleId(0),
            time_range: TimeRange::default(),
            decaying_fn: Default::default(),
        };

        let sampling_config = SamplingConfig {
            rules: vec![rule, unsupported_rule],
            ..SamplingConfig::new()
        };

        // Unsupported rule should result in no match if processing is not enabled.
        let res = compute_sampling_decision(
            false,
            None,
            Some(&sampling_config),
            Some(&event),
            None,
            None,
        )
        .await;
        assert!(res.is_no_match());

        // Match if processing is enabled.
        let res =
            compute_sampling_decision(true, None, Some(&sampling_config), Some(&event), None, None)
                .await;
        assert!(res.is_match());
    }

    #[tokio::test]
    async fn test_client_sample_rate() {
        let dsc = DynamicSamplingContext {
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
        };

        let rule = SamplingRule {
            condition: RuleCondition::all(),
            sampling_value: SamplingValue::SampleRate { value: 0.2 },
            ty: RuleType::Trace,
            id: RuleId(0),
            time_range: TimeRange::default(),
            decaying_fn: Default::default(),
        };

        let sampling_config = SamplingConfig {
            rules: vec![rule],
            ..SamplingConfig::new()
        };

        let res =
            compute_sampling_decision(false, None, None, None, Some(&sampling_config), Some(&dsc))
                .await;

        assert_eq!(get_sampling_match(res).sample_rate(), 0.2);
    }

    async fn run_with_reservoir_rule<Group>(processing_group: ProcessingGroup) -> SamplingResult
    where
        Group: Sampling + TryFrom<ProcessingGroup>,
    {
        let project_info = {
            let mut info = ProjectInfo::default();
            info.config.transaction_metrics = Some(ErrorBoundary::Ok(TransactionMetricsConfig {
                version: 1,
                ..Default::default()
            }));
            Arc::new(info)
        };

        let bytes = Bytes::from(
            r#"{"dsn":"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42","trace":{"trace_id":"89143b0763095bd9c9955e8175d1fb23","public_key":"e12d836b15bb49d7bbf99e64295d995b"}}"#,
        );
        let envelope = Envelope::parse_bytes(bytes).unwrap();
        let config = Arc::new(Config::default());

        let mut managed_envelope: TypedEnvelope<Group> = (
            ManagedEnvelope::new(envelope, Addr::dummy(), Addr::dummy()),
            processing_group,
        )
            .try_into()
            .unwrap();

        let mut event = Annotated::new(Event::default());

        let sampling_project_info = {
            let mut state = ProjectInfo::default();
            state.config.metric_extraction = ErrorBoundary::Ok(MetricExtractionConfig::default());
            state.config.sampling = Some(ErrorBoundary::Ok(SamplingConfig {
                version: 2,
                rules: vec![
                    // Set up a reservoir (only used for transactions):
                    SamplingRule {
                        condition: RuleCondition::all(),
                        sampling_value: SamplingValue::Reservoir { limit: 100 },
                        ty: RuleType::Trace,
                        id: RuleId(1),
                        time_range: Default::default(),
                        decaying_fn: Default::default(),
                    },
                    // Reject everything that does not go into the reservoir:
                    SamplingRule {
                        condition: RuleCondition::all(),
                        sampling_value: SamplingValue::SampleRate { value: 0.0 },
                        ty: RuleType::Trace,
                        id: RuleId(2),
                        time_range: Default::default(),
                        decaying_fn: Default::default(),
                    },
                ],
                rules_v2: vec![],
            }));
            Some(Arc::new(state))
        };

        let reservoir = dummy_reservoir();
        run::<Group>(
            &mut managed_envelope,
            &mut event,
            config,
            project_info,
            sampling_project_info,
            &reservoir,
        )
        .await
    }

    #[tokio::test]
    async fn test_reservoir_applied_for_transactions() {
        let result =
            run_with_reservoir_rule::<TransactionGroup>(ProcessingGroup::Transaction).await;
        // Default sampling rate is 0.0, but transaction is retained because of reservoir:
        assert_eq!(result.decision(), SamplingDecision::Keep);
    }

    #[tokio::test]
    async fn test_reservoir_not_applied_for_spans() {
        let result = run_with_reservoir_rule::<SpanGroup>(ProcessingGroup::Span).await;
        // Default sampling rate is 0.0, and the reservoir does not apply to spans:
        assert_eq!(result.decision(), SamplingDecision::Drop);
    }
}
