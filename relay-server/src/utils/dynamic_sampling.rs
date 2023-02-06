//! Functionality for calculating if a trace should be processed or dropped.
//!
use std::net::IpAddr;

use chrono::{DateTime, Utc};

use relay_common::{ProjectKey, Uuid};
use relay_general::protocol::Event;
use relay_sampling::{
    DynamicSamplingContext, MatchedRuleIds, RuleType, SamplingConfig, SamplingMode,
};

use crate::actors::project::ProjectState;
use crate::envelope::{Envelope, ItemType};

macro_rules! no_match_if_none {
    ($e:expr) => {
        match $e {
            Some(x) => x,
            None => return SamplingMatchResult::NoMatch,
        }
    };
}

/// The result of a sampling operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SamplingResult {
    /// Keep the event. Relay either applied a sampling rule or was unable to parse all rules (so
    /// it bailed out)
    Keep,
    /// Drop the event, due to the rule with provided identifier.
    Drop(MatchedRuleIds),
}

/// The result of the dynamic sampling matching executed on the sampling config.
#[derive(Clone, Debug, PartialEq)]
enum SamplingMatchResult {
    /// The incoming event/dynamic sampling context matched the sampling configuration.
    Match {
        sample_rate: f64,
        matched_rule_ids: MatchedRuleIds,
        seed: Uuid,
    },
    /// The incoming event/dynamic sampling context didn't match the sampling configuration.
    NoMatch,
}

/// A combination of two sampling configurations of the root and non-root projects.
///
/// In case the incoming event is the head of the trace, both root and non-root projects will be
/// the same.
struct SamplingConfigs {
    sampling_config: SamplingConfig,
    root_sampling_config: Option<SamplingConfig>,
}

impl SamplingConfigs {
    fn new(sampling_config: &SamplingConfig) -> SamplingConfigs {
        SamplingConfigs {
            sampling_config: sampling_config.clone(),
            root_sampling_config: None,
        }
    }

    fn add_root_config(&mut self, root_project_state: Option<&ProjectState>) -> &mut Self {
        if let Some(root_project_state) = root_project_state {
            self.root_sampling_config = root_project_state.config.dynamic_sampling.clone()
        } else {
            relay_log::trace!("found dynamic sampling context, but no corresponding project state");
        }

        self
    }

    fn get_merged_config(&self) -> SamplingConfig {
        let event_rules = self
            .sampling_config
            .rules
            .clone()
            .into_iter()
            .filter(|rule| rule.ty != RuleType::Trace);

        let parent_rules = self
            .root_sampling_config
            .clone()
            .map_or(vec![], |config| config.rules)
            .into_iter()
            .filter(|rule| rule.ty == RuleType::Trace || rule.ty == RuleType::Unsupported);

        SamplingConfig {
            rules: event_rules.chain(parent_rules).collect(),
            // We want to take field priority on the fields from the sampling config of the project
            // to which the incoming transaction belongs.
            //
            // This code ignore the situation in which we have conflicting sampling config modes
            // between root and non-root projects.
            mode: self.sampling_config.mode,
        }
    }
}

/// Checks whether unsupported rules result in a direct keep of the event or depending on the
/// type of Relay an ignore of unsupported rules.
fn check_unsupported_rules(
    processing_enabled: bool,
    sampling_config: &SamplingConfig,
) -> Option<()> {
    // When we have unsupported rules disable sampling for non processing relays.
    if sampling_config.has_unsupported_rules() {
        if !processing_enabled {
            return None;
        } else {
            relay_log::error!("found unsupported rules even as processing relay");
        }
    }

    Some(())
}

/// Gets the sampling match result by creating the merged configuration and matching it against
/// the sampling configuration.
fn get_sampling_match_result(
    processing_enabled: bool,
    project_state: &ProjectState,
    root_project_state: Option<&ProjectState>,
    dsc: Option<&DynamicSamplingContext>,
    event: Option<&Event>,
    ip_addr: Option<IpAddr>,
    now: DateTime<Utc>,
) -> SamplingMatchResult {
    // We get all the required data for transaction-based dynamic sampling.
    let event = no_match_if_none!(event);
    let sampling_config = no_match_if_none!(&project_state.config.dynamic_sampling);

    // We obtain the merged sampling configuration with a concatenation of transaction rules
    // of the current project and trace rules of the root project.
    let merged_config = SamplingConfigs::new(sampling_config)
        .add_root_config(root_project_state)
        .get_merged_config();

    // We check if there are unsupported rules.
    no_match_if_none!(check_unsupported_rules(processing_enabled, &merged_config));

    // We perform the rule matching with the multi-matching logic.
    let result = no_match_if_none!(merged_config.match_against_rules(event, dsc, ip_addr, now));

    // If we have a match, we will try to derive the sample rate based on the sampling mode.
    //
    // Keep in mind that the sample rate received here has already been derived by the matching
    // logic, based on multiple matches and decaying functions.
    let sample_rate = match sampling_config.mode {
        SamplingMode::Received => result.sample_rate,
        SamplingMode::Total => match dsc {
            Some(dsc) => dsc.adjusted_sample_rate(result.sample_rate),
            None => result.sample_rate,
        },
        SamplingMode::Unsupported => {
            if processing_enabled {
                relay_log::error!("found unsupported sampling mode even as processing Relay");
            }

            return SamplingMatchResult::NoMatch;
        }
    };

    // Only if we arrive at this stage, it means that we have found a match and we want to prepare
    // the data for making the sampling decision.
    SamplingMatchResult::Match {
        sample_rate,
        seed: result.seed,
        matched_rule_ids: result.matched_rule_ids,
    }
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
    match get_sampling_match_result(
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
        SamplingMatchResult::Match {
            sample_rate,
            matched_rule_ids,
            seed,
        } => {
            let random_number = relay_sampling::pseudo_random_from_uuid(seed);
            relay_log::trace!("sampling envelope with {} sample rate", sample_rate);
            if random_number >= sample_rate {
                relay_log::trace!("dropping envelope that matched the configuration");
                SamplingResult::Drop(matched_rule_ids)
            } else {
                relay_log::trace!("keeping envelope that matched the configuration");
                SamplingResult::Keep
            }
        }
        SamplingMatchResult::NoMatch => {
            relay_log::trace!("keeping envelope that didn't match the configuration");
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
    use similar_asserts::assert_eq;

    use chrono::Duration as DateDuration;

    use relay_common::EventType;
    use relay_general::protocol::{EventId, LenientString};
    use relay_general::types::Annotated;
    use relay_sampling::{
        DecayingFunction, EqCondOptions, EqCondition, RuleCondition, RuleId, RuleType,
        SamplingConfig, SamplingRule, SamplingValue, TimeRange,
    };

    use crate::testutils::project_state_with_config;

    use super::*;

    macro_rules! match_rule_ids {
        ($exc:expr, $res:expr) => {
            if ($exc.len() != $res.len()) {
                panic!("The rule ids don't match.")
            }

            for (index, rule) in $res.iter().enumerate() {
                assert_eq!(rule.id.0, $exc[index])
            }
        };
    }

    macro_rules! transaction_match {
        ($res:expr, $sr:expr, $sd:expr, $( $id:expr ),*) => {
            assert_eq!(
                $res,
                SamplingMatchResult::Match {
                    sample_rate: $sr,
                    seed: $sd.id.0.unwrap().0,
                    matched_rule_ids: MatchedRuleIds(vec![$(RuleId($id),)*])
                }
            )
        }
    }

    macro_rules! trace_match {
        ($res:expr, $sr:expr, $sd:expr, $( $id:expr ),*) => {
            assert_eq!(
                $res,
                SamplingMatchResult::Match {
                    sample_rate: $sr,
                    seed: $sd.trace_id,
                    matched_rule_ids: MatchedRuleIds(vec![$(RuleId($id),)*])
                }
            )
        }
    }

    macro_rules! no_match {
        ($res:expr) => {
            assert_eq!($res, SamplingMatchResult::NoMatch)
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
            rules: vec![
                SamplingRule {
                    condition: eq("event.transaction", &["healthcheck"], true),
                    sample_rate: 1.0,
                    sampling_value: SamplingValue::SampleRate { value: 0.1 },
                    ty: RuleType::Transaction,
                    id: RuleId(1),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: eq("event.transaction", &["bar"], true),
                    sample_rate: 1.0,
                    sampling_value: SamplingValue::Factor { value: 1.0 },
                    ty: RuleType::Transaction,
                    id: RuleId(2),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: eq("event.transaction", &["foo"], true),
                    sample_rate: 1.0,
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
                    sample_rate: 1.0,
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
            rules: vec![
                SamplingRule {
                    condition: eq("trace.release", &["3.0"], true),
                    sample_rate: 1.0,
                    sampling_value: SamplingValue::Factor { value: 1.5 },
                    ty: RuleType::Trace,
                    id: RuleId(5),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: eq("trace.environment", &["dev"], true),
                    sample_rate: 1.0,
                    sampling_value: SamplingValue::SampleRate { value: 1.0 },
                    ty: RuleType::Trace,
                    id: RuleId(6),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: RuleCondition::all(),
                    sample_rate: 1.0,
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
            sample_rate: 1.0,
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
            sample_rate: 1.0,
            sampling_value,
            ty: RuleType::Transaction,
            id: RuleId(id),
            time_range: TimeRange { start, end },
            decaying_fn,
        }
    }

    fn merge_root_and_non_root_configs_with(
        rules: Vec<SamplingRule>,
        root_rules: Vec<SamplingRule>,
    ) -> Vec<SamplingRule> {
        let project_state = project_state_with_config(SamplingConfig {
            rules,
            mode: SamplingMode::Received,
        });
        let root_project_state = project_state_with_config(SamplingConfig {
            rules: root_rules,
            mode: SamplingMode::Received,
        });

        SamplingConfigs::new(project_state.config.dynamic_sampling.as_ref().unwrap())
            .add_root_config(Some(&root_project_state))
            .get_merged_config()
            .rules
    }

    #[test]
    /// Tests the merged config of the two configs with rules.
    fn test_get_merged_config_with_rules_in_both_project_config_and_root_project_config() {
        match_rule_ids!(
            [1, 2, 4, 7, 8],
            merge_root_and_non_root_configs_with(
                vec![
                    mocked_sampling_rule(1, RuleType::Transaction, 0.1),
                    mocked_sampling_rule(2, RuleType::Error, 0.2),
                    mocked_sampling_rule(3, RuleType::Trace, 0.3),
                    mocked_sampling_rule(4, RuleType::Unsupported, 0.1),
                ],
                vec![
                    mocked_sampling_rule(5, RuleType::Transaction, 0.4),
                    mocked_sampling_rule(6, RuleType::Error, 0.5),
                    mocked_sampling_rule(7, RuleType::Trace, 0.6),
                    mocked_sampling_rule(8, RuleType::Unsupported, 0.1),
                ],
            )
        );
    }

    #[test]
    /// Tests the merged config of the two configs without rules.
    fn test_get_merged_config_with_no_rules_in_both_project_config_and_root_project_config() {
        assert!(merge_root_and_non_root_configs_with(vec![], vec![]).is_empty());
    }

    #[test]
    /// Tests the merged config of the project config with rules and the root project config
    /// without rules.
    fn test_get_merged_config_with_rules_in_project_config_and_no_rules_in_root_project_config() {
        match_rule_ids!(
            [1, 2, 4],
            merge_root_and_non_root_configs_with(
                vec![
                    mocked_sampling_rule(1, RuleType::Transaction, 0.1),
                    mocked_sampling_rule(2, RuleType::Error, 0.2),
                    mocked_sampling_rule(3, RuleType::Trace, 0.3),
                    mocked_sampling_rule(4, RuleType::Unsupported, 0.1),
                ],
                vec![],
            )
        );
    }

    #[test]
    /// Tests the merged config of the project config without rules and the root project config
    /// with rules.
    fn test_get_merged_config_with_no_rules_in_project_config_and_with_rules_in_root_project_config(
    ) {
        match_rule_ids!(
            [6, 7],
            merge_root_and_non_root_configs_with(
                vec![],
                vec![
                    mocked_sampling_rule(4, RuleType::Transaction, 0.4),
                    mocked_sampling_rule(5, RuleType::Error, 0.5),
                    mocked_sampling_rule(6, RuleType::Trace, 0.6),
                    mocked_sampling_rule(7, RuleType::Unsupported, 0.1),
                ]
            )
        );
    }

    #[test]
    /// Tests that no match is done when there are no matching rules.
    fn test_get_sampling_match_result_with_no_match() {
        let project_state = mocked_project_state(SamplingMode::Received);
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = get_sampling_match_result(
            true,
            &project_state,
            None,
            None,
            Some(&event),
            None,
            Utc::now(),
        );
        no_match!(result);
    }

    #[test]
    /// Tests that matching is still done on the transaction rules in case trace params are invalid.
    fn test_get_sampling_match_result_with_invalid_trace_params() {
        let project_state = mocked_project_state(SamplingMode::Received);
        let root_project_state = mocked_root_project_state(SamplingMode::Received);
        let dsc = mocked_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);

        let event = mocked_event(EventType::Transaction, "foo", "2.0");
        let result = get_sampling_match_result(
            true,
            &project_state,
            Some(&root_project_state),
            None,
            Some(&event),
            None,
            Utc::now(),
        );
        transaction_match!(result, 0.5, event, 3);

        let event = mocked_event(EventType::Transaction, "healthcheck", "2.0");
        let result = get_sampling_match_result(
            true,
            &project_state,
            None,
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );
        transaction_match!(result, 0.1, event, 1);
    }

    #[test]
    /// Tests that a match with early return is done in the project sampling config.
    fn test_get_sampling_match_result_with_project_config_match() {
        let project_state = mocked_project_state(SamplingMode::Received);
        let root_project_state = mocked_root_project_state(SamplingMode::Received);
        let dsc = mocked_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "healthcheck", "2.0");

        let result = get_sampling_match_result(
            true,
            &project_state,
            Some(&root_project_state),
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );
        transaction_match!(result, 0.1, event, 1);
    }

    #[test]
    /// Tests that a match with early return is done in the root project sampling config.
    fn test_get_sampling_match_result_with_root_project_config_match() {
        let project_state = mocked_project_state(SamplingMode::Received);
        let root_project_state = mocked_root_project_state(SamplingMode::Received);
        let dsc = mocked_dynamic_sampling_context(Some(1.0), Some("1.0"), None, Some("dev"));
        let event = mocked_event(EventType::Transaction, "my_transaction", "2.0");

        let result = get_sampling_match_result(
            true,
            &project_state,
            Some(&root_project_state),
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );
        trace_match!(result, 1.0, dsc, 6);
    }

    #[test]
    /// Tests that the multiple matches are done across root and non-root project sampling configs.
    fn test_get_sampling_match_result_with_both_project_configs_match() {
        let project_state = mocked_project_state(SamplingMode::Received);
        let root_project_state = mocked_root_project_state(SamplingMode::Received);
        let dsc = mocked_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None);
        let event = mocked_event(EventType::Transaction, "bar", "2.0");

        let result = get_sampling_match_result(
            true,
            &project_state,
            Some(&root_project_state),
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );
        trace_match!(result, 0.75, dsc, 2, 5, 7);
    }

    #[test]
    /// Tests that a match is done when no dynamic sampling context and root project state are
    /// available.
    fn test_get_sampling_match_result_with_no_dynamic_sampling_context_and_no_root_project_state() {
        let project_state = mocked_project_state(SamplingMode::Received);
        let event = mocked_event(EventType::Transaction, "foo", "1.0");

        let result = get_sampling_match_result(
            true,
            &project_state,
            None,
            None,
            Some(&event),
            None,
            Utc::now(),
        );
        transaction_match!(result, 0.5, event, 3);
    }

    #[test]
    /// Tests that a match is done and the sample rate is adjusted when sampling mode is total.
    fn test_get_sampling_match_result_with_total_sampling_mode_in_project_state() {
        let project_state = mocked_project_state(SamplingMode::Total);
        let root_project_state = mocked_root_project_state(SamplingMode::Total);
        let dsc = mocked_dynamic_sampling_context(Some(0.8), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "foo", "2.0");

        let result = get_sampling_match_result(
            true,
            &project_state,
            Some(&root_project_state),
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );
        transaction_match!(result, 0.625, event, 3);
    }

    #[test]
    /// Tests that the correct match is raised in case we have unsupported rules with processing both
    /// enabled and disabled.
    fn test_get_sampling_match_result_with_unsupported_rules() {
        let mut project_state = mocked_project_state(SamplingMode::Received);
        // TODO: find a way to simplify this.
        project_state
            .config
            .dynamic_sampling
            .as_mut()
            .unwrap()
            .rules
            .push(SamplingRule {
                condition: RuleCondition::Unsupported,
                sample_rate: 1.0,
                sampling_value: SamplingValue::SampleRate { value: 0.5 },
                ty: RuleType::Transaction,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            });
        let root_project_state = mocked_root_project_state(SamplingMode::Received);
        let dsc = mocked_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "foo", "2.0");

        let result = get_sampling_match_result(
            false,
            &project_state,
            Some(&root_project_state),
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );
        no_match!(result);

        let result = get_sampling_match_result(
            true,
            &project_state,
            Some(&root_project_state),
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );
        transaction_match!(result, 0.5, event, 3);
    }

    #[test]
    /// Tests that a no match is raised in case we have an unsupported sampling mode and a match.
    fn test_get_sampling_match_result_with_unsupported_sampling_mode_and_match() {
        let project_state = mocked_project_state(SamplingMode::Unsupported);
        let root_project_state = mocked_root_project_state(SamplingMode::Unsupported);
        let dsc = mocked_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "foo", "2.0");

        let result = get_sampling_match_result(
            true,
            &project_state,
            Some(&root_project_state),
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );
        no_match!(result);
    }

    #[test]
    /// Tests that a match of a rule of type error with a transaction event results in no match.
    fn test_get_sampling_match_result_with_transaction_event_and_error_rule() {
        let mut project_state = mocked_project_state(SamplingMode::Received);
        project_state
            .config
            .dynamic_sampling
            .as_mut()
            .unwrap()
            .rules
            .push(SamplingRule {
                condition: RuleCondition::all(),
                sample_rate: 1.0,
                sampling_value: SamplingValue::SampleRate { value: 0.5 },
                ty: RuleType::Error,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            });
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = get_sampling_match_result(
            true,
            &project_state,
            None,
            None,
            Some(&event),
            None,
            Utc::now(),
        );
        no_match!(result);
    }

    #[test]
    /// Tests that a match of a rule of type error with an error event results in a match.
    fn test_get_sampling_match_result_with_error_event_and_error_rule() {
        let mut project_state = mocked_project_state(SamplingMode::Received);
        project_state
            .config
            .dynamic_sampling
            .as_mut()
            .unwrap()
            .rules
            .push(SamplingRule {
                condition: RuleCondition::all(),
                sample_rate: 1.0,
                sampling_value: SamplingValue::SampleRate { value: 0.5 },
                ty: RuleType::Error,
                id: RuleId(10),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            });
        let event = mocked_event(EventType::Error, "transaction", "2.0");

        let result = get_sampling_match_result(
            true,
            &project_state,
            None,
            None,
            Some(&event),
            None,
            Utc::now(),
        );
        transaction_match!(result, 0.5, event, 10);
    }

    #[test]
    /// Tests that a match of a rule of type default with an error event results in a match.
    fn test_get_sampling_match_result_with_default_event_and_error_rule() {
        let mut project_state = mocked_project_state(SamplingMode::Received);
        project_state
            .config
            .dynamic_sampling
            .as_mut()
            .unwrap()
            .rules
            .push(SamplingRule {
                condition: RuleCondition::all(),
                sample_rate: 1.0,
                sampling_value: SamplingValue::SampleRate { value: 0.5 },
                ty: RuleType::Error,
                id: RuleId(10),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            });
        let event = mocked_event(EventType::Default, "transaction", "2.0");

        let result = get_sampling_match_result(
            true,
            &project_state,
            None,
            None,
            Some(&event),
            None,
            Utc::now(),
        );
        transaction_match!(result, 0.5, event, 10);
    }

    #[test]
    /// Tests that match is returned with sample rate value interpolated with linear decaying function.
    fn test_get_sampling_match_result_with_linear_decaying_function() {
        let now = Utc::now();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![mocked_decaying_sampling_rule(
                1,
                Some(now - DateDuration::days(1)),
                Some(now + DateDuration::days(1)),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        });
        let result =
            get_sampling_match_result(true, &project_state, None, None, Some(&event), None, now);
        transaction_match!(result, 0.75, event, 1);

        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![mocked_decaying_sampling_rule(
                1,
                Some(now),
                Some(now + DateDuration::days(1)),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        });
        let result =
            get_sampling_match_result(true, &project_state, None, None, Some(&event), None, now);
        transaction_match!(result, 1.0, event, 1);

        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![mocked_decaying_sampling_rule(
                1,
                Some(now - DateDuration::days(1)),
                Some(now),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        });
        let result =
            get_sampling_match_result(true, &project_state, None, None, Some(&event), None, now);
        no_match!(result);
    }

    #[test]
    /// Tests that no match is returned when the linear decaying function has invalid time range.
    fn test_get_sampling_match_result_with_linear_decaying_function_and_invalid_time_range() {
        let now = Utc::now();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![mocked_decaying_sampling_rule(
                1,
                Some(now - DateDuration::days(1)),
                None,
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        });
        let result =
            get_sampling_match_result(true, &project_state, None, None, Some(&event), None, now);
        no_match!(result);

        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![mocked_decaying_sampling_rule(
                1,
                None,
                Some(now + DateDuration::days(1)),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        });
        let result =
            get_sampling_match_result(true, &project_state, None, None, Some(&event), None, now);
        no_match!(result);

        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![mocked_decaying_sampling_rule(
                1,
                None,
                None,
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        });
        let result =
            get_sampling_match_result(true, &project_state, None, None, Some(&event), None, now);
        no_match!(result);
    }

    #[test]
    /// Tests that match is returned when
    fn test_get_sampling_match_result_with_multiple_decaying_functions_with_factor_and_sample_rate()
    {
        let now = Utc::now();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![
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
            get_sampling_match_result(true, &project_state, None, None, Some(&event), None, now);
        assert!(matches!(result, SamplingMatchResult::Match { .. }));
        if let SamplingMatchResult::Match {
            sample_rate,
            seed,
            matched_rule_ids,
        } = result
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
            rules: vec![mocked_sampling_rule(1, RuleType::Transaction, 1.0)],
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
            rules: vec![mocked_sampling_rule(1, RuleType::Transaction, 0.0)],
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
            rules: vec![SamplingRule {
                condition: eq("event.transaction", &["foo"], true),
                sample_rate: 1.0,
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
            rules: vec![
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
