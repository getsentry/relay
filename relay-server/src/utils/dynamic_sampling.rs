//! Functionality for calculating if a trace should be processed or dropped.
use std::sync::Arc;

use chrono::Utc;
use relay_base_schema::project::ProjectKey;
use relay_sampling::config::{RuleType, SamplingMode};
use relay_sampling::evaluation::{Evaluation, ReservoirStuff, SamplingEvaluator, SamplingMatch};
use relay_sampling::{DynamicSamplingContext, SamplingConfig};

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
    #[cfg(test)]
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

impl From<Evaluation> for SamplingResult {
    fn from(value: Evaluation) -> Self {
        match value {
            Evaluation::Matched(sampling_match) => Self::Match(sampling_match),
            Evaluation::Continue(_) => Self::NoMatch,
        }
    }
}

/// Runs dynamic sampling if the dsc and root project state are not None and returns whether the
/// transactions received with such dsc and project state would be kept or dropped by dynamic
/// sampling.
pub fn is_trace_fully_sampled(
    processing_enabled: bool,
    reservoir: Arc<ReservoirStuff>,
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
    let evaluator =
        SamplingEvaluator::new(Utc::now(), reservoir).adjust_client_sample_rate(adjustment_rate);

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

#[cfg(test)]
mod tests {
    use relay_base_schema::events::EventType;
    use relay_event_schema::protocol::{Event, EventId, LenientString};
    use relay_protocol::Annotated;
    use relay_sampling::condition::{EqCondOptions, EqCondition, RuleCondition};
    use relay_sampling::config::{
        RuleId, RuleType, SamplingConfig, SamplingMode, SamplingRule, SamplingValue,
    };
    use uuid::Uuid;

    fn dummy_reservoir() -> Arc<ReservoirStuff> {
        let project_key = "12345678123456781234567812345678"
            .parse::<ProjectKey>()
            .unwrap();
        ReservoirStuff::new(project_key).into()
    }

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

    fn eq(name: &str, value: &[&str], ignore_case: bool) -> RuleCondition {
        RuleCondition::Eq(EqCondition {
            name: name.to_owned(),
            value: value.iter().map(|s| s.to_string()).collect(),
            options: EqCondOptions { ignore_case },
        })
    }

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

        let result: SamplingResult = SamplingEvaluator::new(Utc::now(), dummy_reservoir())
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

        let result: SamplingResult = SamplingEvaluator::new(Utc::now(), dummy_reservoir())
            .match_rules(seed, &event, rules.iter())
            .into();

        assert!(result.is_match());
        assert!(result.should_drop());
    }

    #[test]
    /// Tests that an event is kept when there is no match.
    fn test_match_rules_return_keep_with_no_match() {
        let rules = vec![SamplingRule {
            condition: eq("event.transaction", &["foo"], true),
            sampling_value: SamplingValue::SampleRate { value: 0.5 },
            ty: RuleType::Transaction,
            id: RuleId(3),
            time_range: Default::default(),
            decaying_fn: Default::default(),
        }];

        let event = mocked_event(EventType::Transaction, "bar", "2.0");
        let seed = Uuid::default();

        let result: SamplingResult = SamplingEvaluator::new(Utc::now(), dummy_reservoir())
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

        let result: SamplingResult = SamplingEvaluator::new(Utc::now(), dummy_reservoir())
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
        assert_eq!(
            is_trace_fully_sampled(false, dummy_reservoir(), &config, &dsc),
            Some(true)
        );

        // If processing is enabled, we simply log an error and otherwise proceed as usual.
        assert_eq!(
            is_trace_fully_sampled(true, dummy_reservoir(), &config, &dsc),
            Some(false)
        );
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

        let result = is_trace_fully_sampled(false, dummy_reservoir(), &config, &dsc).unwrap();
        assert!(result);

        // We test with `sampled = true` and 0% rule.
        let config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 0.0)],
            mode: SamplingMode::Received,
        };

        let dsc =
            mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, Some(true));

        let result = is_trace_fully_sampled(false, dummy_reservoir(), &config, &dsc).unwrap();
        assert!(!result);

        // We test with `sampled = false` and 100% rule.
        let config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)],
            mode: SamplingMode::Received,
        };

        let dsc =
            mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, Some(false));

        let result = is_trace_fully_sampled(false, dummy_reservoir(), &config, &dsc).unwrap();
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

        let result = is_trace_fully_sampled(false, dummy_reservoir(), &config, &dsc);
        assert!(result.is_none());
    }
}
