//! Functionality for calculating if a trace should be processed or dropped.
use std::ops::ControlFlow;

use relay_sampling::evaluation::{SamplingDecision, SamplingEvaluator, SamplingMatch};

use crate::services::outcome::Outcome;

/// Represents the specification for sampling an incoming item.
#[derive(Default, Clone, Debug, PartialEq)]
pub enum SamplingResult {
    /// The item matched a sampling condition, whether the item is kept depends on the [`SamplingMatch`].
    Match(SamplingMatch),
    /// The item did not match a sampling condition and must be kept.
    NoMatch,
    /// The item has yet to be run a dynamic sampling decision, the item must be kept for now.
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

    /// Boolean decision whether to keep or drop the item.
    pub fn decision(&self) -> SamplingDecision {
        match self {
            Self::Match(sampling_match) => sampling_match.decision(),
            _ => SamplingDecision::Keep,
        }
    }

    /// Returns the contained sample rate.
    pub fn sample_rate(&self) -> Option<f64> {
        match self {
            SamplingResult::Match(sampling_match) => Some(sampling_match.sample_rate()),
            SamplingResult::NoMatch | SamplingResult::Pending => None,
        }
    }

    /// Consumes the sampling results and returns and outcome if the sampling decision is drop.
    pub fn into_dropped_outcome(self) -> Option<Outcome> {
        match self {
            SamplingResult::Match(sampling_match) if sampling_match.decision().is_drop() => Some(
                Outcome::FilteredSampling(sampling_match.into_matched_rules().into()),
            ),
            SamplingResult::Match(_) => None,
            SamplingResult::NoMatch => None,
            SamplingResult::Pending => None,
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

#[cfg(test)]
mod tests {
    use relay_base_schema::events::EventType;
    use relay_event_schema::protocol::Event;
    use relay_event_schema::protocol::{EventId, LenientString};
    use relay_protocol::Annotated;
    use relay_protocol::RuleCondition;
    use relay_sampling::config::{RuleId, SamplingRule, SamplingValue};
    use uuid::Uuid;

    fn mocked_event(event_type: EventType, transaction: &str, release: &str) -> Event {
        Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(event_type),
            transaction: Annotated::new(transaction.to_owned()),
            release: Annotated::new(LenientString(release.to_owned())),
            ..Event::default()
        }
    }

    use chrono::Utc;
    use relay_sampling::config::RuleType;
    use relay_sampling::dsc::DynamicSamplingContext;

    use super::*;

    fn mocked_simple_dynamic_sampling_context(
        sample_rate: Option<f64>,
        release: Option<&str>,
        transaction: Option<&str>,
        environment: Option<&str>,
        sampled: Option<bool>,
    ) -> DynamicSamplingContext {
        DynamicSamplingContext {
            trace_id: "67e5504410b1426f9247bb680e5fe0c8".parse().unwrap(),
            public_key: "12345678901234567890123456789012".parse().unwrap(),
            release: release.map(|value| value.to_owned()),
            environment: environment.map(|value| value.to_owned()),
            transaction: transaction.map(|value| value.to_owned()),
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

    #[tokio::test]
    /// Tests that an event is kept when there is a match and we have 100% sample rate.
    async fn test_match_rules_return_keep_with_match_and_100_sample_rate() {
        let event = mocked_event(EventType::Transaction, "bar", "2.0");
        let rules = [mocked_sampling_rule(1, RuleType::Transaction, 1.0)];

        let result: SamplingResult = SamplingEvaluator::new(Utc::now())
            .match_rules(Uuid::default(), &event, rules.iter())
            .await
            .into();

        assert!(result.is_match());
        assert!(result.decision().is_keep());
    }
    #[tokio::test]
    /// Tests that an event is dropped when there is a match and we have 0% sample rate.
    async fn test_match_rules_return_drop_with_match_and_0_sample_rate() {
        let event = mocked_event(EventType::Transaction, "bar", "2.0");
        let rules = [mocked_sampling_rule(1, RuleType::Transaction, 0.0)];

        let result: SamplingResult = SamplingEvaluator::new(Utc::now())
            .match_rules(Uuid::default(), &event, rules.iter())
            .await
            .into();

        assert!(result.is_match());
        assert!(result.decision().is_drop());
    }

    #[tokio::test]
    /// Tests that an event is kept when there is no match.
    async fn test_match_rules_return_keep_with_no_match() {
        let rules = [SamplingRule {
            condition: RuleCondition::eq_ignore_case("event.transaction", "foo"),
            sampling_value: SamplingValue::SampleRate { value: 0.5 },
            ty: RuleType::Transaction,
            id: RuleId(3),
            time_range: Default::default(),
            decaying_fn: Default::default(),
        }];

        let event = mocked_event(EventType::Transaction, "bar", "2.0");

        let result: SamplingResult = SamplingEvaluator::new(Utc::now())
            .match_rules(Uuid::default(), &event, rules.iter())
            .await
            .into();

        assert!(result.is_no_match());
        assert!(result.decision().is_keep());
    }

    #[tokio::test]
    /// Tests that an event is kept when there is a trace match and we have 100% sample rate.
    async fn test_match_rules_with_traces_rules_return_keep_when_match() {
        let rules = [mocked_sampling_rule(1, RuleType::Trace, 1.0)];
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, None);

        let result: SamplingResult = SamplingEvaluator::new(Utc::now())
            .match_rules(Uuid::default(), &dsc, rules.iter())
            .await
            .into();

        assert!(result.is_match());
        assert!(result.decision().is_keep());
    }
}
