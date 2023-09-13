//! Functionality for calculating if a trace should be processed or dropped.
use std::collections::BTreeMap;

use crate::actors::project::ProjectState;
use crate::envelope::{Envelope, ItemType};
use chrono::Utc;
use relay_base_schema::project::ProjectKey;
use relay_event_schema::protocol::Event;
use relay_sampling::config::RuleId;
use relay_sampling::evaluation::{
    check_unsupported_rules, merge_rules_from_configs, MatchedRuleIds, SamplingMatch,
};
use relay_sampling::{DynamicSamplingContext, SamplingConfig};
use uuid::Uuid;

pub type BiasedRuleId = RuleId;

/// The result of a sampling operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SamplingResult {
    /// Keep the event.
    ///
    /// Relay either applied sampling rules and decided to keep the event, or was unable to parse
    /// the rules.
    Keep(Option<BiasedRuleId>),
    /// Drop the event, due to a list of rules with provided identifiers.
    Drop(MatchedRuleIds),
}

impl Default for SamplingResult {
    fn default() -> Self {
        Self::Keep(None)
    }
}

impl SamplingResult {
    fn determine_from_sampling_match(
        sample_rate: f64,
        seed: Uuid,
        matched_rule_ids: MatchedRuleIds,
    ) -> Self {
        let random_number = relay_sampling::evaluation::pseudo_random_from_uuid(seed);
        relay_log::trace!(
            sample_rate,
            random_number,
            "applying dynamic sampling to matching event"
        );

        if random_number >= sample_rate {
            relay_log::trace!("dropping event that matched the configuration");
            SamplingResult::Drop(matched_rule_ids)
        } else {
            relay_log::trace!("keeping event that matched the configuration");
            SamplingResult::Keep(None)
        }
    }

    pub fn should_keep(&self) -> bool {
        matches!(self, &Self::Keep(_))
    }
}

/// Runs dynamic sampling on an incoming event/dsc and returns whether or not the event should be
/// kept or dropped.
pub fn get_sampling_result(
    processing_enabled: bool,
    sampling_config: Option<&SamplingConfig>,
    root_sampling_config: Option<&SamplingConfig>,
    dsc: Option<&DynamicSamplingContext>,
    event: Option<&Event>,
    reservoir_stuff: &BTreeMap<RuleId, usize>,
) -> SamplingResult {
    // If we have a match, we will try to derive the sample rate based on the sampling mode.
    //
    // Keep in mind that the sample rate received here has already been derived by the matching
    // logic, based on multiple matches and decaying functions.
    //
    // The determination of the sampling mode occurs with the following priority:
    // 1. Non-root project sampling mode
    // 2. Root project sampling mode
    let sampling_mode = match sampling_config.or(root_sampling_config) {
        Some(config) => config.mode,
        None => {
            relay_log::error!("cannot sample without at least one sampling config");
            return SamplingResult::Keep(None);
        }
    };

    // We check if there are unsupported rules in any of the two configurations.
    if check_unsupported_rules(processing_enabled, sampling_config, root_sampling_config).is_err() {
        return SamplingResult::Keep(None);
    };

    // We perform the rule matching with the multi-matching logic on the merged rules.
    let rules = merge_rules_from_configs(sampling_config, root_sampling_config);

    let sampling_match = SamplingMatch::get_sampling_match(
        rules,
        event,
        dsc,
        Utc::now(),
        reservoir_stuff,
        &sampling_mode,
    );

    match sampling_match {
        Some(SamplingMatch::Normal {
            sample_rate,
            seed,
            matched_rule_ids,
        }) => SamplingResult::determine_from_sampling_match(sample_rate, seed, matched_rule_ids),
        Some(SamplingMatch::Bias { rule_id }) => SamplingResult::Keep(Some(rule_id)),
        None => {
            relay_log::trace!("keeping event that didn't match the configuration");
            SamplingResult::Keep(None)
        }
    }
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
    use relay_event_schema::protocol::{EventId, LenientString};
    use relay_protocol::Annotated;
    use relay_sampling::condition::{EqCondOptions, EqCondition, RuleCondition};
    use relay_sampling::config::{
        RuleId, RuleType, SamplingConfig, SamplingMode, SamplingRule, SamplingValue,
    };
    use similar_asserts::assert_eq;
    use uuid::Uuid;

    use super::*;
    use crate::testutils::project_state_with_config;

    fn eq(name: &str, value: &[&str], ignore_case: bool) -> RuleCondition {
        RuleCondition::Eq(EqCondition {
            name: name.to_owned(),
            value: value.iter().map(|s| s.to_string()).collect(),
            options: EqCondOptions { ignore_case },
        })
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
    fn test_get_sampling_result_return_keep_with_match_and_100_sample_rate() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Transaction, 1.0)],
            mode: SamplingMode::Received,
        });
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = get_sampling_result(
            true,
            Some(&project_state),
            None,
            None,
            Some(&event),
            &BTreeMap::default(),
        );
        assert!(result.should_keep())
    }

    #[test]
    /// Tests that an event is dropped when there is a match and we have 0% sample rate.
    fn test_get_sampling_result_return_drop_with_match_and_0_sample_rate() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Transaction, 0.0)],
            mode: SamplingMode::Received,
        });
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = get_sampling_result(
            true,
            Some(&project_state),
            None,
            None,
            Some(&event),
            &BTreeMap::default(),
        );
        assert_eq!(
            result,
            SamplingResult::Drop(MatchedRuleIds(vec![RuleId(1)]))
        )
    }

    #[test]
    /// Tests that an event is kept when there is no match.
    fn test_get_sampling_result_return_keep_with_no_match() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![SamplingRule {
                condition: eq("event.transaction", &["foo"], true),
                sampling_value: SamplingValue::SampleRate { value: 0.5 },
                ty: RuleType::Transaction,
                id: RuleId(3),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            }],
            mode: SamplingMode::Received,
        });
        let event = mocked_event(EventType::Transaction, "bar", "2.0");

        let result = get_sampling_result(
            true,
            Some(&project_state),
            None,
            None,
            Some(&event),
            &BTreeMap::default(),
        );
        assert!(result.should_keep())
    }

    #[test]
    /// Tests that an event is kept when there are unsupported rules with no processing and vice versa.
    fn test_get_sampling_result_return_keep_with_unsupported_rule() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![
                mocked_sampling_rule(1, RuleType::Unsupported, 0.0),
                mocked_sampling_rule(2, RuleType::Transaction, 0.0),
            ],
            mode: SamplingMode::Received,
        });
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = get_sampling_result(
            false,
            Some(&project_state),
            None,
            None,
            Some(&event),
            &BTreeMap::default(),
        );
        assert!(result.should_keep());

        let result = get_sampling_result(
            true,
            Some(&project_state),
            None,
            None,
            Some(&event),
            &BTreeMap::default(),
        );
        assert_eq!(
            result,
            SamplingResult::Drop(MatchedRuleIds(vec![RuleId(2)]))
        )
    }

    #[test]
    /// Tests that an event is kept when there is a trace match and we have 100% sample rate.
    fn test_get_sampling_result_with_traces_rules_return_keep_when_match() {
        let root_project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)],
            mode: SamplingMode::Received,
        });
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, None);

        let result = get_sampling_result(
            true,
            None,
            Some(&root_project_state),
            Some(&dsc),
            None,
            &BTreeMap::default(),
        );
        assert!(result.should_keep())
    }

    #[test]
    /// Tests that a trace is marked as fully sampled correctly when dsc and project state are set.
    fn test_is_trace_fully_sampled_with_valid_dsc_and_project_state() {
        // We test with `sampled = true` and 100% rule.
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)],
            mode: SamplingMode::Received,
        });
        let dsc =
            mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, Some(true));

        let result = get_sampling_result(
            true,
            None,
            Some(&project_state),
            Some(&dsc),
            None,
            &BTreeMap::default(),
        );

        assert!(result.should_keep());

        // We test with `sampled = true` and 0% rule.
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 0.0)],
            mode: SamplingMode::Received,
        });
        let dsc =
            mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, Some(true));

        let result = get_sampling_result(
            true,
            None,
            Some(&project_state),
            Some(&dsc),
            None,
            &BTreeMap::default(),
        );
        assert!(!result.should_keep());

        // We test with `sampled = false` and 100% rule.
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)],
            mode: SamplingMode::Received,
        });
        let dsc =
            mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, Some(false));

        let result = get_sampling_result(
            true,
            None,
            Some(&project_state),
            Some(&dsc),
            None,
            &BTreeMap::default(),
        );
        assert!(!result.should_keep());
    }

    #[test]
    /// Tests that a trace is not marked as fully sampled or not if inputs are invalid.
    fn test_is_trace_fully_sampled_with_invalid_inputs() {
        // We test with missing `sampled`.
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)],
            mode: SamplingMode::Received,
        });
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, None);

        let result = get_sampling_result(
            true,
            None,
            Some(&project_state),
            Some(&dsc),
            None,
            &BTreeMap::default(),
        );
        assert!(result.should_keep());

        // We test with missing dsc and project config.
        let result = get_sampling_result(true, None, None, None, None, &BTreeMap::default());
        assert!(result.should_keep())
    }
}
