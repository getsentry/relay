//! Functionality for calculating if a trace should be processed or dropped.
//!
use std::net::IpAddr;

use chrono::{DateTime, Utc};
use relay_common::ProjectKey;
use relay_general::protocol::Event;
use relay_sampling::{DynamicSamplingContext, MatchedRuleIds, SamplingConfig, SamplingMatch};

use crate::actors::project::ProjectState;
use crate::envelope::{Envelope, ItemType};

/// The result of a sampling operation.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum SamplingResult {
    /// Keep the event.
    ///
    /// Relay either applied sampling rules and decided to keep the event, or was unable to parse
    /// the rules.
    #[default]
    Keep,

    /// Drop the event, due to a list of rules with provided identifiers.
    Drop(MatchedRuleIds),
}

/// Matches the incoming event with the SamplingConfig(s) of the projects.
fn match_with_project_states(
    processing_enabled: bool,
    project_state: &ProjectState,
    root_project_state: Option<&ProjectState>,
    dsc: Option<&DynamicSamplingContext>,
    event: Option<&Event>,
    ip_addr: Option<IpAddr>,
    now: DateTime<Utc>,
) -> Option<SamplingMatch> {
    let event = event?;
    // We want to extract the SamplingConfig from each project state.
    let sampling_config = project_state.config.dynamic_sampling.as_ref()?;
    let root_sampling_config =
        root_project_state.and_then(|state| state.config.dynamic_sampling.as_ref());

    return SamplingConfig::match_against_configs(
        processing_enabled,
        sampling_config,
        root_sampling_config,
        dsc,
        event,
        ip_addr,
        now,
    );
}

/// Checks whether an incoming event should be kept or dropped based on the result of the sampling
/// configuration match.
pub fn should_keep_event(
    processing_enabled: bool,
    project_state: &ProjectState,
    root_project_state: Option<&ProjectState>,
    dsc: Option<&DynamicSamplingContext>,
    event: Option<&Event>,
    ip_addr: Option<IpAddr>,
) -> SamplingResult {
    match match_with_project_states(
        processing_enabled,
        project_state,
        root_project_state,
        dsc,
        event,
        ip_addr,
        // For consistency reasons we take a snapshot in time and use that time across all code that
        // requires it.
        Utc::now(),
    ) {
        Some(SamplingMatch {
            sample_rate,
            matched_rule_ids,
            seed,
        }) => {
            let random_number = relay_sampling::pseudo_random_from_uuid(seed);
            relay_log::trace!("sampling event with sample rate {}", sample_rate);
            if random_number >= sample_rate {
                relay_log::trace!("dropping event that matched the configuration");
                SamplingResult::Drop(matched_rule_ids)
            } else {
                relay_log::trace!("keeping event that matched the configuration");
                SamplingResult::Keep
            }
        }
        None => {
            relay_log::trace!("keeping event that didn't match the configuration");
            SamplingResult::Keep
        }
    }
}

/// Returns the project key defined in the `trace` header of the envelope.
///
/// This function returns `None` if:
///  - there is no [`DynamicSamplingContext`] in the envelope headers.
///  - there are no transactions in the envelope, since in this case sampling by trace is redundant.
pub fn get_sampling_key(envelope: &Envelope) -> Option<ProjectKey> {
    let transaction_item = envelope.get_item_by(|item| item.ty() == &ItemType::Transaction);

    // if there are no transactions to sample, return here
    transaction_item?;

    envelope.dsc().map(|dsc| dsc.public_key)
}

#[cfg(test)]
mod tests {
    use chrono::Duration as DateDuration;
    use relay_common::{EventType, Uuid};
    use relay_general::protocol::{EventId, LenientString};
    use relay_general::types::Annotated;
    use relay_sampling::{
        DecayingFunction, EqCondOptions, EqCondition, RuleCondition, RuleId, RuleType,
        SamplingConfig, SamplingMatch, SamplingMode, SamplingRule, SamplingValue, TimeRange,
    };
    use similar_asserts::assert_eq;

    use super::*;
    use crate::testutils::project_state_with_config;

    macro_rules! assert_transaction_match {
        ($res:expr, $sr:expr, $sd:expr, $( $id:expr ),*) => {
            assert_eq!(
                $res,
                Some(SamplingMatch {
                        sample_rate: $sr,
                        seed: $sd.id.value().unwrap().0,
                        matched_rule_ids: MatchedRuleIds(vec![$(RuleId($id),)*])
                        }                )

            )
        }
    }

    macro_rules! assert_trace_match {
        ($res:expr, $sr:expr, $sd:expr, $( $id:expr ),*) => {
            assert_eq!(
                $res,
                Some(SamplingMatch {
                        sample_rate: $sr,
                        seed: $sd.trace_id,
                        matched_rule_ids: MatchedRuleIds(vec![$(RuleId($id),)*])
        })

            )
        }
    }

    macro_rules! assert_no_match {
        ($res:expr) => {
            assert_eq!($res, None)
        };
    }

    fn eq(name: &str, value: &[&str], ignore_case: bool) -> RuleCondition {
        RuleCondition::Eq(EqCondition {
            name: name.to_owned(),
            value: value.iter().map(|s| s.to_string()).collect(),
            options: EqCondOptions { ignore_case },
        })
    }

    fn mocked_dynamic_sampling_context(
        sample_rate: Option<f64>,
        release: Option<&str>,
        transaction: Option<&str>,
        environment: Option<&str>,
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
        }
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

    fn mocked_project_state(mode: SamplingMode) -> ProjectState {
        project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![
                SamplingRule {
                    condition: eq("event.transaction", &["healthcheck"], true),
                    sampling_value: SamplingValue::SampleRate { value: 0.1 },
                    ty: RuleType::Transaction,
                    id: RuleId(1),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: eq("event.transaction", &["bar"], true),
                    sampling_value: SamplingValue::Factor { value: 1.0 },
                    ty: RuleType::Transaction,
                    id: RuleId(2),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: eq("event.transaction", &["foo"], true),
                    sampling_value: SamplingValue::SampleRate { value: 0.5 },
                    ty: RuleType::Transaction,
                    id: RuleId(3),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                // We put this trace rule here just for testing purposes, even though it will never
                // be considered if put within a non-root project.
                SamplingRule {
                    condition: RuleCondition::all(),
                    sampling_value: SamplingValue::SampleRate { value: 0.5 },
                    ty: RuleType::Trace,
                    id: RuleId(4),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
            ],
            mode,
        })
    }

    fn mocked_root_project_state(mode: SamplingMode) -> ProjectState {
        project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![
                SamplingRule {
                    condition: eq("trace.release", &["3.0"], true),
                    sampling_value: SamplingValue::Factor { value: 1.5 },
                    ty: RuleType::Trace,
                    id: RuleId(5),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: eq("trace.environment", &["dev"], true),
                    sampling_value: SamplingValue::SampleRate { value: 1.0 },
                    ty: RuleType::Trace,
                    id: RuleId(6),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: RuleCondition::all(),
                    sampling_value: SamplingValue::SampleRate { value: 0.5 },
                    ty: RuleType::Trace,
                    id: RuleId(7),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
            ],
            mode,
        })
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

    fn mocked_decaying_sampling_rule(
        id: u32,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        sampling_value: SamplingValue,
        decaying_fn: DecayingFunction,
    ) -> SamplingRule {
        SamplingRule {
            condition: RuleCondition::all(),
            sampling_value,
            ty: RuleType::Transaction,
            id: RuleId(id),
            time_range: TimeRange { start, end },
            decaying_fn,
        }
    }

    fn add_sampling_rule_to_project_state(
        project_state: &mut ProjectState,
        sampling_rule: SamplingRule,
    ) {
        project_state
            .config
            .dynamic_sampling
            .as_mut()
            .unwrap()
            .rules_v2
            .push(sampling_rule);
    }

    #[test]
    /// Tests that no match is done when there are no matching rules.
    fn test_get_sampling_match_result_with_no_match() {
        let project_state = mocked_project_state(SamplingMode::Received);
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = match_with_project_states(
            true,
            &project_state,
            None,
            None,
            Some(&event),
            None,
            Utc::now(),
        );
        assert_no_match!(result);
    }

    #[test]
    /// Tests that matching is still done on the transaction rules in case trace params are invalid.
    fn test_get_sampling_match_result_with_invalid_trace_params() {
        let project_state = mocked_project_state(SamplingMode::Received);
        let root_project_state = mocked_root_project_state(SamplingMode::Received);
        let dsc = mocked_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);

        let event = mocked_event(EventType::Transaction, "foo", "2.0");
        let result = match_with_project_states(
            true,
            &project_state,
            Some(&root_project_state),
            None,
            Some(&event),
            None,
            Utc::now(),
        );
        assert_transaction_match!(result, 0.5, event, 3);

        let event = mocked_event(EventType::Transaction, "healthcheck", "2.0");
        let result = match_with_project_states(
            true,
            &project_state,
            None,
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );
        assert_transaction_match!(result, 0.1, event, 1);
    }

    #[test]
    /// Tests that a match with early return is done in the project sampling config.
    fn test_get_sampling_match_result_with_project_config_match() {
        let project_state = mocked_project_state(SamplingMode::Received);
        let root_project_state = mocked_root_project_state(SamplingMode::Received);
        let dsc = mocked_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "healthcheck", "2.0");

        let result = match_with_project_states(
            true,
            &project_state,
            Some(&root_project_state),
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );
        assert_transaction_match!(result, 0.1, event, 1);
    }

    #[test]
    /// Tests that a match with early return is done in the root project sampling config.
    fn test_get_sampling_match_result_with_root_project_config_match() {
        let project_state = mocked_project_state(SamplingMode::Received);
        let root_project_state = mocked_root_project_state(SamplingMode::Received);
        let dsc = mocked_dynamic_sampling_context(Some(1.0), Some("1.0"), None, Some("dev"));
        let event = mocked_event(EventType::Transaction, "my_transaction", "2.0");

        let result = match_with_project_states(
            true,
            &project_state,
            Some(&root_project_state),
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );
        assert_trace_match!(result, 1.0, dsc, 6);
    }

    #[test]
    /// Tests that the multiple matches are done across root and non-root project sampling configs.
    fn test_get_sampling_match_result_with_both_project_configs_match() {
        let project_state = mocked_project_state(SamplingMode::Received);
        let root_project_state = mocked_root_project_state(SamplingMode::Received);
        let dsc = mocked_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None);
        let event = mocked_event(EventType::Transaction, "bar", "2.0");

        let result = match_with_project_states(
            true,
            &project_state,
            Some(&root_project_state),
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );
        assert_trace_match!(result, 0.75, dsc, 2, 5, 7);
    }

    #[test]
    /// Tests that a match is done when no dynamic sampling context and root project state are
    /// available.
    fn test_get_sampling_match_result_with_no_dynamic_sampling_context_and_no_root_project_state() {
        let project_state = mocked_project_state(SamplingMode::Received);
        let event = mocked_event(EventType::Transaction, "foo", "1.0");

        let result = match_with_project_states(
            true,
            &project_state,
            None,
            None,
            Some(&event),
            None,
            Utc::now(),
        );
        assert_transaction_match!(result, 0.5, event, 3);
    }

    #[test]
    /// Tests that a match is done and the sample rate is adjusted when sampling mode is total.
    fn test_get_sampling_match_result_with_total_sampling_mode_in_project_state() {
        let project_state = mocked_project_state(SamplingMode::Total);
        let root_project_state = mocked_root_project_state(SamplingMode::Total);
        let dsc = mocked_dynamic_sampling_context(Some(0.8), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "foo", "2.0");

        let result = match_with_project_states(
            true,
            &project_state,
            Some(&root_project_state),
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );
        assert_transaction_match!(result, 0.625, event, 3);
    }

    #[test]
    /// Tests that the correct match is raised in case we have unsupported rules with processing both
    /// enabled and disabled.
    fn test_get_sampling_match_result_with_unsupported_rules() {
        let mut project_state = mocked_project_state(SamplingMode::Received);
        add_sampling_rule_to_project_state(
            &mut project_state,
            SamplingRule {
                condition: RuleCondition::Unsupported,
                sampling_value: SamplingValue::SampleRate { value: 0.5 },
                ty: RuleType::Transaction,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
        );

        let root_project_state = mocked_root_project_state(SamplingMode::Received);
        let dsc = mocked_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "foo", "2.0");

        let result = match_with_project_states(
            false,
            &project_state,
            Some(&root_project_state),
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );
        assert_no_match!(result);

        let result = match_with_project_states(
            true,
            &project_state,
            Some(&root_project_state),
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );
        assert_transaction_match!(result, 0.5, event, 3);
    }

    #[test]
    /// Tests that a no match is raised in case we have an unsupported sampling mode and a match.
    fn test_get_sampling_match_result_with_unsupported_sampling_mode_and_match() {
        let project_state = mocked_project_state(SamplingMode::Unsupported);
        let root_project_state = mocked_root_project_state(SamplingMode::Unsupported);
        let dsc = mocked_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "foo", "2.0");

        let result = match_with_project_states(
            true,
            &project_state,
            Some(&root_project_state),
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );
        assert_no_match!(result);
    }

    #[test]
    /// Tests that only transaction rules are matched in case no root project or dsc are supplied.
    fn test_get_sampling_match_result_with_invalid_root_project_and_dsc_combination() {
        let project_state = mocked_project_state(SamplingMode::Received);
        let event = mocked_event(EventType::Transaction, "healthcheck", "2.0");

        let dsc = mocked_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let result = match_with_project_states(
            true,
            &project_state,
            None,
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );
        assert_transaction_match!(result, 0.1, event, 1);

        let root_project_state = mocked_root_project_state(SamplingMode::Received);
        let result = match_with_project_states(
            true,
            &project_state,
            Some(&root_project_state),
            None,
            Some(&event),
            None,
            Utc::now(),
        );
        assert_transaction_match!(result, 0.1, event, 1);
    }

    #[test]
    /// Tests that a match of a rule of type error with a transaction event results in no match.
    fn test_get_sampling_match_result_with_transaction_event_and_error_rule() {
        let mut project_state = mocked_project_state(SamplingMode::Received);
        add_sampling_rule_to_project_state(
            &mut project_state,
            SamplingRule {
                condition: RuleCondition::all(),
                sampling_value: SamplingValue::SampleRate { value: 0.5 },
                ty: RuleType::Error,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
        );
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = match_with_project_states(
            true,
            &project_state,
            None,
            None,
            Some(&event),
            None,
            Utc::now(),
        );
        assert_no_match!(result);
    }

    #[test]
    /// Tests that a match of a rule of type error with an error event results in a match.
    fn test_get_sampling_match_result_with_error_event_and_error_rule() {
        let mut project_state = mocked_project_state(SamplingMode::Received);
        add_sampling_rule_to_project_state(
            &mut project_state,
            SamplingRule {
                condition: RuleCondition::all(),
                sampling_value: SamplingValue::SampleRate { value: 0.5 },
                ty: RuleType::Error,
                id: RuleId(10),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
        );
        let event = mocked_event(EventType::Error, "transaction", "2.0");

        let result = match_with_project_states(
            true,
            &project_state,
            None,
            None,
            Some(&event),
            None,
            Utc::now(),
        );
        assert_transaction_match!(result, 0.5, event, 10);
    }

    #[test]
    /// Tests that a match of a rule of type default with an error event results in a match.
    fn test_get_sampling_match_result_with_default_event_and_error_rule() {
        let mut project_state = mocked_project_state(SamplingMode::Received);
        add_sampling_rule_to_project_state(
            &mut project_state,
            SamplingRule {
                condition: RuleCondition::all(),
                sampling_value: SamplingValue::SampleRate { value: 0.5 },
                ty: RuleType::Error,
                id: RuleId(10),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
        );
        let event = mocked_event(EventType::Default, "transaction", "2.0");

        let result = match_with_project_states(
            true,
            &project_state,
            None,
            None,
            Some(&event),
            None,
            Utc::now(),
        );
        assert_transaction_match!(result, 0.5, event, 10);
    }

    #[test]
    /// Tests that match is returned with sample rate value interpolated with linear decaying function.
    fn test_get_sampling_match_result_with_linear_decaying_function() {
        let now = Utc::now();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                Some(now - DateDuration::days(1)),
                Some(now + DateDuration::days(1)),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        });
        let result =
            match_with_project_states(true, &project_state, None, None, Some(&event), None, now);
        assert_transaction_match!(result, 0.75, event, 1);

        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                Some(now),
                Some(now + DateDuration::days(1)),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        });
        let result =
            match_with_project_states(true, &project_state, None, None, Some(&event), None, now);
        assert_transaction_match!(result, 1.0, event, 1);

        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                Some(now - DateDuration::days(1)),
                Some(now),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        });
        let result =
            match_with_project_states(true, &project_state, None, None, Some(&event), None, now);
        assert_no_match!(result);
    }

    #[test]
    /// Tests that no match is returned when the linear decaying function has invalid time range.
    fn test_get_sampling_match_result_with_linear_decaying_function_and_invalid_time_range() {
        let now = Utc::now();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                Some(now - DateDuration::days(1)),
                None,
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        });
        let result =
            match_with_project_states(true, &project_state, None, None, Some(&event), None, now);
        assert_no_match!(result);

        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                None,
                Some(now + DateDuration::days(1)),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        });
        let result =
            match_with_project_states(true, &project_state, None, None, Some(&event), None, now);
        assert_no_match!(result);

        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                None,
                None,
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        });
        let result =
            match_with_project_states(true, &project_state, None, None, Some(&event), None, now);
        assert_no_match!(result);
    }

    #[test]
    /// Tests that match is returned when there are multiple decaying rules with factor and sample rate.
    fn test_get_sampling_match_result_with_multiple_decaying_functions_with_factor_and_sample_rate()
    {
        let now = Utc::now();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![
                mocked_decaying_sampling_rule(
                    1,
                    Some(now - DateDuration::days(1)),
                    Some(now + DateDuration::days(1)),
                    SamplingValue::Factor { value: 5.0 },
                    DecayingFunction::Linear { decayed_value: 1.0 },
                ),
                mocked_decaying_sampling_rule(
                    2,
                    Some(now - DateDuration::days(1)),
                    Some(now + DateDuration::days(1)),
                    SamplingValue::SampleRate { value: 0.3 },
                    DecayingFunction::Constant,
                ),
            ],
            mode: SamplingMode::Received,
        });

        let result =
            match_with_project_states(true, &project_state, None, None, Some(&event), None, now);
        assert!(result.is_some());
        if let Some(SamplingMatch {
            sample_rate,
            seed,
            matched_rule_ids,
        }) = result
        {
            assert!((sample_rate - 0.9).abs() < f64::EPSILON);
            assert_eq!(seed, event.id.0.unwrap().0);
            assert_eq!(matched_rule_ids, MatchedRuleIds(vec![RuleId(1), RuleId(2)]))
        }
    }

    #[test]
    /// Tests that an event is kept when there is a match and we have 100% sample rate.
    fn test_should_keep_event_return_keep_with_match_and_100_sample_rate() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Transaction, 1.0)],
            mode: SamplingMode::Received,
        });
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = should_keep_event(true, &project_state, None, None, Some(&event), None);
        assert_eq!(result, SamplingResult::Keep)
    }

    #[test]
    /// Tests that an event is dropped when there is a match and we have 0% sample rate.
    fn test_should_keep_event_return_drop_with_match_and_0_sample_rate() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_sampling_rule(1, RuleType::Transaction, 0.0)],
            mode: SamplingMode::Received,
        });
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = should_keep_event(true, &project_state, None, None, Some(&event), None);
        assert_eq!(
            result,
            SamplingResult::Drop(MatchedRuleIds(vec![RuleId(1)]))
        )
    }

    #[test]
    /// Tests that an event is kept when there is no match.
    fn test_should_keep_event_return_keep_with_no_match() {
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

        let result = should_keep_event(true, &project_state, None, None, Some(&event), None);
        assert_eq!(result, SamplingResult::Keep)
    }

    #[test]
    /// Tests that an event is kept when there are unsupported rules with no processing and vice versa.
    fn test_should_keep_event_return_keep_with_unsupported_rule() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            rules_v2: vec![
                mocked_sampling_rule(1, RuleType::Unsupported, 0.0),
                mocked_sampling_rule(2, RuleType::Transaction, 0.0),
            ],
            mode: SamplingMode::Received,
        });
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = should_keep_event(false, &project_state, None, None, Some(&event), None);
        assert_eq!(result, SamplingResult::Keep);

        let result = should_keep_event(true, &project_state, None, None, Some(&event), None);
        assert_eq!(
            result,
            SamplingResult::Drop(MatchedRuleIds(vec![RuleId(2)]))
        )
    }
}
