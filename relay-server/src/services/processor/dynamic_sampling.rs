//! Dynamic sampling processor related code.

use std::ops::ControlFlow;

use chrono::Utc;
use relay_config::Config;
use relay_dynamic_config::ErrorBoundary;
use relay_event_schema::protocol::{Contexts, Event, TraceContext};
use relay_protocol::{Annotated, Empty};
use relay_sampling::config::RuleType;
use relay_sampling::evaluation::{ReservoirEvaluator, SamplingEvaluator};
use relay_sampling::{DynamicSamplingContext, SamplingConfig};

use crate::envelope::ItemType;
use crate::services::outcome::Outcome;
use crate::services::processor::{
    EventProcessing, ProcessEnvelopeState, Sampling, TransactionGroup,
};
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
pub fn ensure_dsc(state: &mut ProcessEnvelopeState<TransactionGroup>) {
    if state.envelope().dsc().is_some() && state.sampling_project_state.is_some() {
        return;
    }

    // The DSC can only be computed if there's a transaction event. Note that `dsc_from_event`
    // below already checks for the event type.
    let Some(event) = state.event.value() else {
        return;
    };
    let Some(key_config) = state.project_state.get_public_key_config() else {
        return;
    };

    if let Some(dsc) = utils::dsc_from_event(key_config.public_key, event) {
        state.envelope_mut().set_dsc(dsc);
        state.sampling_project_state = Some(state.project_state.clone());
    }
}

/// Computes the sampling decision on the incoming event
pub fn run<Group>(state: &mut ProcessEnvelopeState<Group>) -> SamplingResult
where
    Group: Sampling,
{
    if !Group::supports_sampling(&state.project_state) {
        return SamplingResult::Pending;
    }

    let sampling_config = match state.project_state.config.sampling {
        Some(ErrorBoundary::Ok(ref config)) if !config.unsupported() => Some(config),
        _ => None,
    };

    let root_state = state.sampling_project_state.as_ref();
    let root_config = match root_state.and_then(|s| s.config.sampling.as_ref()) {
        Some(ErrorBoundary::Ok(ref config)) if !config.unsupported() => Some(config),
        _ => None,
    };

    let reservoir = Group::supports_reservoir_sampling().then_some(&state.reservoir);

    compute_sampling_decision(
        state.config.processing_enabled(),
        reservoir,
        sampling_config,
        state.event.value(),
        root_config,
        state.envelope().dsc(),
    )
}

/// Apply the dynamic sampling decision from `compute_sampling_decision`.
pub fn drop_unsampled_items(
    state: &mut ProcessEnvelopeState<TransactionGroup>,
    outcome: Outcome,
    keep_profiles: bool,
) {
    // Remove all items from the envelope which need to be dropped due to dynamic sampling.
    let dropped_items = state.envelope_mut().take_items_by(|item| match item.ty() {
        ItemType::Profile => !keep_profiles,
        _ => true,
    });

    for item in dropped_items {
        let Some(category) = item.outcome_category() else {
            continue;
        };

        // Dynamic sampling only drops indexed items. Upgrade the category to the index
        // category if one exists for this category, for example profiles will be upgraded to profiles indexed,
        // but attachments are still emitted as attachments.
        let category = category.index_category().unwrap_or(category);

        state
            .managed_envelope
            .track_outcome(outcome.clone(), category, item.quantity());
    }

    // Mark all remaining items in the envelope as unsampled.
    for item in state.envelope_mut().items_mut() {
        item.set_sampled(false);
    }

    // All items have been dropped, now make sure the event is also handled and dropped.
    if let Some(category) = state.event_category() {
        let category = category.index_category().unwrap_or(category);
        state.managed_envelope.track_outcome(outcome, category, 1)
    }
    state.remove_event();
}

/// Computes the sampling decision on the incoming envelope.
fn compute_sampling_decision(
    processing_enabled: bool,
    reservoir: Option<&ReservoirEvaluator>,
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

    if sampling_config.map_or(false, |config| config.unsupported())
        || root_sampling_config.map_or(false, |config| config.unsupported())
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

    if let (Some(event), Some(sampling_state)) = (event, sampling_config) {
        if let Some(seed) = event.id.value().map(|id| id.0) {
            let rules = sampling_state.filter_rules(RuleType::Transaction);
            evaluator = match evaluator.match_rules(seed, event, rules) {
                ControlFlow::Continue(evaluator) => evaluator,
                ControlFlow::Break(sampling_match) => {
                    return SamplingResult::Match(sampling_match);
                }
            }
        };
    }

    if let (Some(dsc), Some(sampling_state)) = (dsc, root_sampling_config) {
        let rules = sampling_state.filter_rules(RuleType::Trace);
        return evaluator.match_rules(dsc.trace_id, dsc, rules).into();
    }

    SamplingResult::NoMatch
}

/// Runs dynamic sampling on an incoming error and tags it in case of successful sampling
/// decision.
///
/// This execution of dynamic sampling is technically a "simulation" since we will use the result
/// only for tagging errors and not for actually sampling incoming events.
pub fn tag_error_with_sampling_decision<G: EventProcessing>(
    state: &mut ProcessEnvelopeState<G>,
    config: &Config,
) {
    let (Some(dsc), Some(event)) = (
        state.managed_envelope.envelope().dsc(),
        state.event.value_mut(),
    ) else {
        return;
    };

    let root_state = state.sampling_project_state.as_ref();
    let sampling_config = match root_state.and_then(|s| s.config.sampling.as_ref()) {
        Some(ErrorBoundary::Ok(ref config)) => config,
        _ => return,
    };

    if sampling_config.unsupported() {
        if config.processing_enabled() {
            relay_log::error!("found unsupported rules even as processing relay");
        }

        return;
    }

    let Some(sampled) = utils::is_trace_fully_sampled(sampling_config, dsc) else {
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
    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_dynamic_config::{MetricExtractionConfig, TransactionMetricsConfig};
    use relay_event_schema::protocol::{EventId, LenientString};
    use relay_protocol::RuleCondition;
    use relay_sampling::config::{
        DecayingFunction, RuleId, SamplingRule, SamplingValue, TimeRange,
    };
    use relay_sampling::evaluation::{ReservoirCounters, SamplingDecision, SamplingMatch};
    use relay_system::Addr;
    use uuid::Uuid;

    use crate::envelope::{ContentType, Envelope, Item};
    use crate::extractors::RequestMeta;
    use crate::services::processor::{
        ProcessEnvelope, ProcessingExtractedMetrics, ProcessingGroup, SpanGroup,
    };
    use crate::services::project::ProjectInfo;
    use crate::testutils::{
        self, create_test_processor, new_envelope, state_with_rule_and_condition,
    };
    use crate::utils::ManagedEnvelope;

    use super::*;

    fn mocked_event(event_type: EventType, transaction: &str, release: &str) -> Event {
        Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(event_type),
            transaction: Annotated::new(transaction.to_string()),
            release: Annotated::new(LenientString(release.to_string())),
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
    fn process_envelope_with_root_project_state(
        envelope: Box<Envelope>,
        sampling_project_info: Option<Arc<ProjectInfo>>,
    ) -> Envelope {
        let processor = create_test_processor(Default::default());
        let (outcome_aggregator, test_store) = testutils::processor_services();

        let mut envelopes = ProcessingGroup::split_envelope(*envelope);
        assert_eq!(envelopes.len(), 1);
        let (group, envelope) = envelopes.pop().unwrap();

        let message = ProcessEnvelope {
            envelope: ManagedEnvelope::new(envelope, outcome_aggregator, test_store, group),
            project_info: Arc::new(ProjectInfo::default()),
            sampling_project_info,
            reservoir_counters: ReservoirCounters::default(),
        };

        let envelope_response = processor.process(message).unwrap();
        let ctx = envelope_response.envelope.unwrap();
        ctx.envelope().clone()
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
        let new_envelope = process_envelope_with_root_project_state(envelope, None);
        let event = extract_first_event_from_envelope(new_envelope);

        assert!(event.contexts.value().is_none());
    }

    #[test]
    fn test_it_keeps_or_drops_transactions() {
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
            );
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

        // Gets a ProcessEnvelopeState, either with or without the metrics_exracted flag toggled.
        let get_state = |version: Option<u16>| {
            let event = Event {
                id: Annotated::new(EventId::new()),
                ty: Annotated::new(EventType::Transaction),
                transaction: Annotated::new("testing".to_owned()),
                ..Event::default()
            };

            let mut project_state = state_with_rule_and_condition(
                Some(0.0),
                RuleType::Transaction,
                RuleCondition::all(),
            );

            if let Some(version) = version {
                project_state.config.transaction_metrics =
                    ErrorBoundary::Ok(relay_dynamic_config::TransactionMetricsConfig {
                        version,
                        ..Default::default()
                    })
                    .into();
            }

            let project_state = Arc::new(project_state);
            let envelope = new_envelope(false, "foo");

            ProcessEnvelopeState::<TransactionGroup> {
                event: Annotated::from(event),
                metrics: Default::default(),
                sample_rates: None,
                extracted_metrics: ProcessingExtractedMetrics::new(
                    project_state.clone(),
                    Arc::new(GlobalConfig::default()),
                    envelope.dsc(),
                ),
                config: config.clone(),
                project_state,
                sampling_project_state: None,
                project_id: ProjectId::new(42),
                managed_envelope: ManagedEnvelope::new(
                    envelope,
                    outcome_aggregator.clone(),
                    test_store.clone(),
                    ProcessingGroup::Transaction,
                )
                .try_into()
                .unwrap(),
                event_metrics_extracted: false,
                reservoir: dummy_reservoir(),
                spans_extracted: false,
                event_fully_normalized: false,
            }
        };

        // None represents no TransactionMetricsConfig, DS will not be run
        let mut state = get_state(None);
        let sampling_result = run(&mut state);
        assert_eq!(sampling_result.decision(), SamplingDecision::Keep);

        // Current version is 3, so it won't run DS if it's outdated
        let mut state = get_state(Some(2));
        let sampling_result = run(&mut state);
        assert_eq!(sampling_result.decision(), SamplingDecision::Keep);

        // Dynamic sampling is run, as the transactionmetrics version is up to date.
        let mut state = get_state(Some(3));
        let sampling_result = run(&mut state);
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
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
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
        );
        let event = extract_first_event_from_envelope(new_envelope);
        let trace_context = event.context::<TraceContext>().unwrap();
        assert!(trace_context.sampled.value().unwrap());

        // We test with sample rate equal to 0%.
        let sampling_project_state = project_state_with_single_rule(0.0);
        let new_envelope = process_envelope_with_root_project_state(
            envelope,
            Some(Arc::new(sampling_project_state)),
        );
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
        );
        let event = extract_first_event_from_envelope(new_envelope);
        let trace_context = event.context::<TraceContext>().unwrap();
        assert!(trace_context.sampled.value().unwrap());
    }

    /// Happy path test for compute_sampling_decision.
    #[test]
    fn test_compute_sampling_decision_matching() {
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
        );
        assert!(res.is_match());
    }

    #[test]
    fn test_matching_with_unsupported_rule() {
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
        );
        assert!(res.is_no_match());

        // Match if processing is enabled.
        let res =
            compute_sampling_decision(true, None, Some(&sampling_config), Some(&event), None, None);
        assert!(res.is_match());
    }

    #[test]
    fn test_client_sample_rate() {
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
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
            compute_sampling_decision(false, None, None, None, Some(&sampling_config), Some(&dsc));

        assert_eq!(get_sampling_match(res).sample_rate(), 0.2);
    }

    fn run_with_reservoir_rule<G>(processing_group: ProcessingGroup) -> SamplingResult
    where
        G: Sampling + TryFrom<ProcessingGroup>,
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
        let mut state = ProcessEnvelopeState::<G> {
            event: Annotated::new(Event::default()),
            event_metrics_extracted: false,
            spans_extracted: false,
            metrics: Default::default(),
            sample_rates: Default::default(),
            extracted_metrics: ProcessingExtractedMetrics::new(
                project_info.clone(),
                Arc::new(GlobalConfig::default()),
                envelope.dsc(),
            ),
            config: Arc::new(Config::default()),
            project_state: project_info,
            sampling_project_state: {
                let mut state = ProjectInfo::default();
                state.config.metric_extraction =
                    ErrorBoundary::Ok(MetricExtractionConfig::default());
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
            },
            project_id: ProjectId::new(1),
            managed_envelope: ManagedEnvelope::new(
                envelope,
                Addr::dummy(),
                Addr::dummy(),
                processing_group,
            )
            .try_into()
            .unwrap(),
            reservoir: dummy_reservoir(),
            event_fully_normalized: false,
        };

        run(&mut state)
    }

    #[test]
    fn test_reservoir_applied_for_transactions() {
        let result = run_with_reservoir_rule::<TransactionGroup>(ProcessingGroup::Transaction);
        // Default sampling rate is 0.0, but transaction is retained because of reservoir:
        assert_eq!(result.decision(), SamplingDecision::Keep);
    }

    #[test]
    fn test_reservoir_not_applied_for_spans() {
        let result = run_with_reservoir_rule::<SpanGroup>(ProcessingGroup::Span);
        // Default sampling rate is 0.0, and the reservoir does not apply to spans:
        assert_eq!(result.decision(), SamplingDecision::Drop);
    }
}
