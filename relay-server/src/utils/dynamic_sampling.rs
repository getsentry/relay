//! Functionality for calculating if a trace should be processed or dropped.
use chrono::Utc;
use relay_base_schema::project::ProjectKey;
use relay_sampling::dsc::DynamicSamplingContext;
use relay_sampling::evaluation::match_rules;

use crate::actors::project::ProjectState;
use crate::envelope::{Envelope, ItemType};

/// Runs dynamic sampling if the dsc and root project state are not None and returns whether the
/// transactions received with such dsc and project state would be kept or dropped by dynamic
/// sampling.
pub fn is_trace_fully_sampled(
    processing_enabled: bool,
    root_project_state: &ProjectState,
    dsc: &DynamicSamplingContext,
) -> Option<bool> {
    // If the sampled field is not set, we prefer to not tag the error since we have no clue on
    // whether the head of the trace was kept or dropped on the client side.
    // In addition, if the head of the trace was dropped on the client we will immediately mark
    // the trace as not fully sampled.
    if !(dsc.sampled?) {
        return Some(false);
    }

    Some(
        match_rules(
            processing_enabled,
            None,
            root_project_state.config.dynamic_sampling.as_ref(),
            None,
            Some(dsc),
            Utc::now(),
        )
        .should_keep(),
    )
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
    fn test_match_rules_return_keep_with_match_and_100_sample_rate() {
        let config = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Transaction, 1.0)],
            mode: SamplingMode::Received,
        })
        .config
        .dynamic_sampling
        .unwrap();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = match_rules(true, Some(&config), None, Some(&event), None, Utc::now());
        assert!(result.should_keep());
    }

    #[test]
    /// Tests that an event is dropped when there is a match and we have 0% sample rate.
    fn test_match_rules_return_drop_with_match_and_0_sample_rate() {
        let config = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Transaction, 0.0)],
            mode: SamplingMode::Received,
        })
        .config
        .dynamic_sampling
        .unwrap();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = match_rules(true, Some(&config), None, Some(&event), None, Utc::now());
        assert!(result.should_drop());
    }

    #[test]
    /// Tests that an event is kept when there is no match.
    fn test_match_rules_return_keep_with_no_match() {
        let config = project_state_with_config(SamplingConfig {
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
        })
        .config
        .dynamic_sampling
        .unwrap();
        let event = mocked_event(EventType::Transaction, "bar", "2.0");

        let result = match_rules(true, Some(&config), None, Some(&event), None, Utc::now());
        assert!(result.should_keep())
    }

    #[test]
    /// Tests that an event is kept when there are unsupported rules with no processing and vice versa.
    fn test_match_rules_return_no_match_with_unsupported_rule() {
        let config = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![
                mocked_sampling_rule(1, RuleType::Unsupported, 0.0),
                mocked_sampling_rule(2, RuleType::Transaction, 0.0),
            ],
            mode: SamplingMode::Received,
        })
        .config
        .dynamic_sampling
        .unwrap();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = match_rules(true, Some(&config), None, Some(&event), None, Utc::now());
        assert!(result.is_match());

        let result = match_rules(false, Some(&config), None, Some(&event), None, Utc::now());
        assert!(result.is_no_match());
    }

    #[test]
    /// Tests that an event is kept when there is a trace match and we have 100% sample rate.
    fn test_match_rules_with_traces_rules_return_keep_when_match() {
        let config = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)],
            mode: SamplingMode::Received,
        })
        .config
        .dynamic_sampling
        .unwrap();
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, None);

        let result = match_rules(true, None, Some(&config), None, Some(&dsc), Utc::now());
        assert!(result.should_keep());
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

        let result = is_trace_fully_sampled(true, &project_state, &dsc).unwrap();
        assert!(result);

        // We test with `sampled = true` and 0% rule.
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 0.0)],
            mode: SamplingMode::Received,
        });
        let dsc =
            mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, Some(true));

        let result = is_trace_fully_sampled(true, &project_state, &dsc).unwrap();
        assert!(!result);

        // We test with `sampled = false` and 100% rule.
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)],
            mode: SamplingMode::Received,
        });
        let dsc =
            mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, Some(false));

        let result = is_trace_fully_sampled(true, &project_state, &dsc).unwrap();
        assert!(!result);
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

        let result = is_trace_fully_sampled(true, &project_state, &dsc);
        assert!(result.is_none());
    }
}
