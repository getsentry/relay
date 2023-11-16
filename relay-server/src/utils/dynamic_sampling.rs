//! Functionality for calculating if a trace should be processed or dropped.
use std::ops::ControlFlow;

use chrono::Utc;
use relay_base_schema::events::EventType;
use relay_base_schema::project::ProjectKey;
use relay_event_schema::protocol::{Event, TraceContext};
use relay_sampling::config::{RuleType, SamplingConfig, SamplingMode};
use relay_sampling::dsc::{DynamicSamplingContext, TraceUserContext};
use relay_sampling::evaluation::{SamplingEvaluator, SamplingMatch};

use crate::envelope::{Envelope, ItemType};

/// Represents the specification for sampling an incoming event.
#[derive(Default, Clone, Debug, PartialEq)]
pub enum SamplingResult {
    /// The event matched a sampling condition.
    Match(SamplingMatch),
    /// The event did not match a sampling condition.
    NoMatch,
    /// The event has yet to be run a dynamic sampling decision.
    #[default]
    Pending,
}

impl SamplingResult {
    /// Returns `true` if the event matched on any rules.
    #[cfg(test)]
    pub fn is_no_match(&self) -> bool {
        matches!(self, &Self::NoMatch)
    }

    /// Returns `true` if the event did not match on any rules.
    #[cfg(test)]
    pub fn is_match(&self) -> bool {
        matches!(self, &Self::Match(_))
    }

    /// Returns `true` if the event should be dropped.
    pub fn should_drop(&self) -> bool {
        !self.should_keep()
    }

    /// Returns `true` if the event should be kept.
    pub fn should_keep(&self) -> bool {
        match self {
            SamplingResult::Match(sampling_match) => sampling_match.should_keep(),
            // If no rules matched on an event, we want to keep it.
            SamplingResult::NoMatch => true,
            SamplingResult::Pending => true,
        }
    }
}

impl From<ControlFlow<SamplingMatch, SamplingEvaluator<'_>>> for SamplingResult {
    fn from(value: ControlFlow<SamplingMatch, SamplingEvaluator>) -> Self {
        match value {
            ControlFlow::Break(sampling_match) => Self::Match(sampling_match),
            ControlFlow::Continue(_) => Self::NoMatch,
        }
    }
}

/// Runs dynamic sampling if the dsc and root project state are not None and returns whether the
/// transactions received with such dsc and project state would be kept or dropped by dynamic
/// sampling.
pub fn is_trace_fully_sampled(
    processing_enabled: bool,
    root_project_config: &SamplingConfig,
    dsc: &DynamicSamplingContext,
) -> Option<bool> {
    // If the sampled field is not set, we prefer to not tag the error since we have no clue on
    // whether the head of the trace was kept or dropped on the client side.
    // In addition, if the head of the trace was dropped on the client we will immediately mark
    // the trace as not fully sampled.
    if !(dsc.sampled?) {
        return Some(false);
    }

    if root_project_config.unsupported() {
        if processing_enabled {
            relay_log::error!("found unsupported rules even as processing relay");
        } else {
            return Some(true);
        }
    }

    let adjustment_rate = match root_project_config.mode {
        SamplingMode::Total => dsc.sample_rate,
        _ => None,
    };

    // TODO(tor): pass correct now timestamp
    let evaluator = SamplingEvaluator::new(Utc::now()).adjust_client_sample_rate(adjustment_rate);

    let rules = root_project_config.filter_rules(RuleType::Trace);

    let evaluation = evaluator.match_rules(dsc.trace_id, dsc, rules);
    Some(SamplingResult::from(evaluation).should_keep())
}

/// Returns the project key defined in the `trace` header of the envelope.
///
/// This function returns `None` if:
///  - there is no [`DynamicSamplingContext`] in the envelope headers.
///  - there are no transactions or events in the envelope, since in this case sampling by trace is redundant.
pub fn get_sampling_key(envelope: &Envelope) -> Option<ProjectKey> {
    // If the envelope item is not of type transaction or event, we will not return a sampling key
    // because it doesn't make sense to load the root project state if we don't perform trace
    // sampling.
    envelope
        .get_item_by(|item| item.ty() == &ItemType::Transaction || item.ty() == &ItemType::Event)?;
    envelope.dsc().map(|dsc| dsc.public_key)
}

/// Computes a dynamic sampling context from a transaction event.
///
/// Returns `None` if the passed event is not a transaction event, or if it does not contain a
/// trace ID in its trace context. All optional fields in the dynamic sampling context are
/// populated with the corresponding attributes from the event payload if they are available.
///
/// Since sampling information is not available in the event payload, the `sample_rate` field
/// cannot be set when computing the dynamic sampling context from a transaction event.
pub fn dsc_from_event(public_key: ProjectKey, event: &Event) -> Option<DynamicSamplingContext> {
    if event.ty.value() != Some(&EventType::Transaction) {
        return None;
    }

    let Some(trace) = event.context::<TraceContext>() else {
        return None;
    };
    let trace_id = trace.trace_id.value()?.0.parse().ok()?;
    let user = event.user.value();

    Some(DynamicSamplingContext {
        trace_id,
        public_key,
        release: event.release.as_str().map(str::to_owned),
        environment: event.environment.value().cloned(),
        transaction: event.transaction.value().cloned(),
        replay_id: None,
        sample_rate: None,
        user: TraceUserContext {
            user_segment: user
                .and_then(|u| u.segment.value().cloned())
                .unwrap_or_default(),
            user_id: user
                .and_then(|u| u.id.as_str())
                .unwrap_or_default()
                .to_owned(),
        },
        sampled: None,
        other: Default::default(),
    })
}

#[cfg(test)]
mod tests {
    use relay_base_schema::events::EventType;
    use relay_event_schema::protocol::{Event, EventId, LenientString};
    use relay_protocol::Annotated;
    use relay_protocol::RuleCondition;
    use relay_sampling::config::{
        RuleId, RuleType, SamplingConfig, SamplingMode, SamplingRule, SamplingValue,
    };
    use uuid::Uuid;

    fn mocked_event(event_type: EventType, transaction: &str, release: &str) -> Event {
        Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(event_type),
            transaction: Annotated::new(transaction.to_string()),
            release: Annotated::new(LenientString(release.to_string())),
            ..Event::default()
        }
    }

    use super::*;

    fn mocked_simple_dynamic_sampling_context(
        sample_rate: Option<f64>,
        release: Option<&str>,
        transaction: Option<&str>,
        environment: Option<&str>,
        sampled: Option<bool>,
    ) -> DynamicSamplingContext {
        DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: "12345678901234567890123456789012".parse().unwrap(),
            release: release.map(|value| value.to_string()),
            environment: environment.map(|value| value.to_string()),
            transaction: transaction.map(|value| value.to_string()),
            sample_rate,
            user: Default::default(),
            other: Default::default(),
            replay_id: None,
            sampled,
        }
    }

    fn mocked_sampling_rule(id: u32, ty: RuleType, sample_rate: f64) -> SamplingRule {
        SamplingRule {
            condition: RuleCondition::all(),
            sampling_value: SamplingValue::SampleRate { value: sample_rate },
            ty,
            id: RuleId(id),
            time_range: Default::default(),
            decaying_fn: Default::default(),
        }
    }

    #[test]
    /// Tests that an event is kept when there is a match and we have 100% sample rate.
    fn test_match_rules_return_keep_with_match_and_100_sample_rate() {
        let event = mocked_event(EventType::Transaction, "bar", "2.0");
        let rules = vec![mocked_sampling_rule(1, RuleType::Transaction, 1.0)];
        let seed = Uuid::default();

        let result: SamplingResult = SamplingEvaluator::new(Utc::now())
            .match_rules(seed, &event, rules.iter())
            .into();

        assert!(result.is_match());
        assert!(result.should_keep());
    }
    #[test]
    /// Tests that an event is dropped when there is a match and we have 0% sample rate.
    fn test_match_rules_return_drop_with_match_and_0_sample_rate() {
        let event = mocked_event(EventType::Transaction, "bar", "2.0");
        let rules = vec![mocked_sampling_rule(1, RuleType::Transaction, 0.0)];
        let seed = Uuid::default();

        let result: SamplingResult = SamplingEvaluator::new(Utc::now())
            .match_rules(seed, &event, rules.iter())
            .into();

        assert!(result.is_match());
        assert!(result.should_drop());
    }

    #[test]
    /// Tests that an event is kept when there is no match.
    fn test_match_rules_return_keep_with_no_match() {
        let rules = vec![SamplingRule {
            condition: RuleCondition::eq_ignore_case("event.transaction", "foo"),
            sampling_value: SamplingValue::SampleRate { value: 0.5 },
            ty: RuleType::Transaction,
            id: RuleId(3),
            time_range: Default::default(),
            decaying_fn: Default::default(),
        }];

        let event = mocked_event(EventType::Transaction, "bar", "2.0");
        let seed = Uuid::default();

        let result: SamplingResult = SamplingEvaluator::new(Utc::now())
            .match_rules(seed, &event, rules.iter())
            .into();

        assert!(result.is_no_match());
        assert!(result.should_keep());
    }

    #[test]
    /// Tests that an event is kept when there is a trace match and we have 100% sample rate.
    fn test_match_rules_with_traces_rules_return_keep_when_match() {
        let rules = vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)];
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, None);

        let result: SamplingResult = SamplingEvaluator::new(Utc::now())
            .match_rules(Uuid::default(), &dsc, rules.iter())
            .into();

        assert!(result.is_match());
        assert!(result.should_keep());
    }

    #[test]
    fn test_is_trace_fully_sampled_return_true_with_unsupported_rules() {
        let config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![
                mocked_sampling_rule(1, RuleType::Unsupported, 1.0),
                mocked_sampling_rule(1, RuleType::Trace, 0.0),
            ],
            mode: SamplingMode::Received,
        };

        let dsc = mocked_simple_dynamic_sampling_context(None, None, None, None, Some(true));

        // Return true if any unsupported rules.
        assert_eq!(is_trace_fully_sampled(false, &config, &dsc), Some(true));

        // If processing is enabled, we simply log an error and otherwise proceed as usual.
        assert_eq!(is_trace_fully_sampled(true, &config, &dsc), Some(false));
    }

    #[test]
    /// Tests that a trace is marked as fully sampled correctly when dsc and project state are set.
    fn test_is_trace_fully_sampled_with_valid_dsc_and_sampling_config() {
        // We test with `sampled = true` and 100% rule.

        let config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)],
            mode: SamplingMode::Received,
        };

        let dsc =
            mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, Some(true));

        let result = is_trace_fully_sampled(false, &config, &dsc).unwrap();
        assert!(result);

        // We test with `sampled = true` and 0% rule.
        let config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 0.0)],
            mode: SamplingMode::Received,
        };

        let dsc =
            mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, Some(true));

        let result = is_trace_fully_sampled(false, &config, &dsc).unwrap();
        assert!(!result);

        // We test with `sampled = false` and 100% rule.
        let config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)],
            mode: SamplingMode::Received,
        };

        let dsc =
            mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, Some(false));

        let result = is_trace_fully_sampled(false, &config, &dsc).unwrap();
        assert!(!result);
    }

    #[test]
    /// Tests that a trace is not marked as fully sampled or not if inputs are invalid.
    fn test_is_trace_fully_sampled_with_invalid_inputs() {
        // We test with missing `sampled`.
        let config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)],
            mode: SamplingMode::Received,
        };
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, None);

        let result = is_trace_fully_sampled(false, &config, &dsc);
        assert!(result.is_none());
    }
}
