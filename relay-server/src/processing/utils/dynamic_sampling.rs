//! Dynamic sampling processor related code.
use std::ops::ControlFlow;

use chrono::Utc;
use relay_dynamic_config::ErrorBoundary;
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;
use relay_sampling::config::RuleType;
use relay_sampling::evaluation::{ReservoirEvaluator, SamplingEvaluator};
use relay_sampling::{DynamicSamplingContext, SamplingConfig};

use crate::processing::Context;
use crate::utils::SamplingResult;

/// Computes the sampling decision on the incoming event
pub async fn run(
    dsc: Option<&DynamicSamplingContext>,
    event: &mut Annotated<Event>,
    ctx: &Context<'_>,
    reservoir: Option<&ReservoirEvaluator<'_>>,
) -> SamplingResult {
    let sampling_config = match ctx.project_info.config.sampling {
        Some(ErrorBoundary::Ok(ref config)) if !config.unsupported() => Some(config),
        _ => None,
    };

    let root_state = ctx.sampling_project_info.as_ref();
    let root_config = match root_state.and_then(|s| s.config.sampling.as_ref()) {
        Some(ErrorBoundary::Ok(config)) if !config.unsupported() => Some(config),
        _ => None,
    };

    compute_sampling_decision(
        ctx.config.processing_enabled(),
        reservoir,
        sampling_config,
        event.value(),
        root_config,
        dsc,
    )
    .await
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

    if let (Some(dsc), Some(sampling_state)) = (dsc, sampling_config) {
        let rules = sampling_state.filter_rules(RuleType::Project);
        evaluator = match evaluator.match_rules(*dsc.trace_id, dsc, rules).await {
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use relay_base_schema::events::EventType;
    use relay_base_schema::project::ProjectKey;
    use relay_event_schema::protocol::{EventId, LenientString};
    use relay_protocol::RuleCondition;
    use relay_sampling::config::{
        DecayingFunction, RuleId, SamplingRule, SamplingValue, TimeRange,
    };
    use relay_sampling::evaluation::SamplingMatch;

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

    // Helper to extract the sampling match from SamplingResult if thats the variant.
    fn get_sampling_match(sampling_result: SamplingResult) -> SamplingMatch {
        if let SamplingResult::Match(sampling_match) = sampling_result {
            sampling_match
        } else {
            panic!()
        }
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

    /// Happy path test for compute_sampling_decision.
    #[tokio::test]
    async fn test_compute_sampling_decision_matching() {
        for rule_type in [RuleType::Transaction, RuleType::Project] {
            let event = mocked_event(EventType::Transaction, "foo", "bar");
            let rule = SamplingRule {
                condition: RuleCondition::all(),
                sampling_value: SamplingValue::SampleRate { value: 1.0 },
                ty: rule_type,
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
                Some(&mock_dsc()),
            )
            .await;
            assert!(res.is_match());
        }
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
        let dsc = mock_dsc();

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
}
