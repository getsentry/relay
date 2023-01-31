//! Functionality for calculating if a trace should be processed or dropped.
//!
use std::net::IpAddr;

use chrono::{DateTime, Utc};

use relay_common::{ProjectKey, Uuid};
use relay_general::protocol::{Event, EventId};
use relay_sampling::{
    DynamicSamplingContext, RuleId, RuleType, SamplingConfig, SamplingConfigMatchResult,
    SamplingMode,
};

use crate::actors::project::ProjectState;
use crate::envelope::{Envelope, ItemType};

macro_rules! or_ok_none {
    ($e:expr) => {
        match $e {
            Some(x) => x,
            None => return Ok(None),
        }
    };
}

/// The result of a sampling operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SamplingResult {
    /// Keep the event. Relay either applied a sampling rule or was unable to parse all rules (so
    /// it bailed out)
    Keep,
    /// Drop the event, due to the rule with provided identifier.
    Drop(RuleId),
}

fn check_unsupported_rules(
    processing_enabled: bool,
    sampling_config: &SamplingConfig,
) -> Result<(), SamplingResult> {
    // When we have unsupported rules disable sampling for non processing relays.
    if sampling_config.has_unsupported_rules() {
        if !processing_enabled {
            return Err(SamplingResult::Keep);
        } else {
            relay_log::error!("found unsupported rules even as processing relay");
        }
    }

    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
struct SamplingSpec {
    sample_rate: f64,
    rule_id: RuleId,
    seed: Uuid,
    matched_trace: bool,
}

fn get_trace_sampling_rule(
    processing_enabled: bool,
    sampling_project_state: Option<&ProjectState>,
    dsc: Option<&DynamicSamplingContext>,
    ip_addr: Option<IpAddr>,
    now: DateTime<Utc>,
) -> Result<Option<SamplingSpec>, SamplingResult> {
    let dsc = or_ok_none!(dsc);

    if sampling_project_state.is_none() {
        relay_log::trace!("found sampling context, but no corresponding project state");
    }
    let sampling_project_state = or_ok_none!(sampling_project_state);
    let sampling_config = or_ok_none!(&sampling_project_state.config.dynamic_sampling);
    check_unsupported_rules(processing_enabled, sampling_config)?;

    let rule = or_ok_none!(sampling_config.get_matching_trace_rule(dsc, ip_addr, now));
    let sample_rate = match sampling_config.mode {
        SamplingMode::Received => rule.get_sampling_strategy_value(now),
        SamplingMode::Total => dsc.adjusted_sample_rate(rule.get_sampling_strategy_value(now)),
        SamplingMode::Unsupported => {
            if processing_enabled {
                relay_log::error!("Found unsupported sampling mode even as processing Relay, keep");
            }
            return Err(SamplingResult::Keep);
        }
    };

    Ok(Some(SamplingSpec {
        sample_rate,
        rule_id: rule.id,
        seed: dsc.trace_id,
        matched_trace: false,
    }))
}

fn get_event_sampling_rule(
    processing_enabled: bool,
    project_state: &ProjectState,
    dsc: Option<&DynamicSamplingContext>,
    event: Option<&Event>,
    ip_addr: Option<IpAddr>,
    now: DateTime<Utc>,
) -> Result<Option<SamplingSpec>, SamplingResult> {
    let event = or_ok_none!(event);
    let event_id = or_ok_none!(event.id.value());

    let sampling_config = or_ok_none!(&project_state.config.dynamic_sampling);
    check_unsupported_rules(processing_enabled, sampling_config)?;

    let rule = or_ok_none!(sampling_config.get_matching_event_rule(event, ip_addr, now));
    let sample_rate = match (dsc, sampling_config.mode) {
        (Some(dsc), SamplingMode::Total) => {
            dsc.adjusted_sample_rate(rule.get_sampling_strategy_value(now))
        }
        _ => rule.get_sampling_strategy_value(now),
    };

    Ok(Some(SamplingSpec {
        sample_rate,
        rule_id: rule.id,
        seed: event_id.0,
        matched_trace: false,
    }))
}

/// Checks whether an event should be kept or removed by dynamic sampling.
///
/// This runs both trace- and event/transaction/error-based rules at once.
pub fn should_keep_event(
    dsc: Option<&DynamicSamplingContext>,
    event: Option<&Event>,
    ip_addr: Option<IpAddr>,
    project_state: &ProjectState,
    sampling_project_state: Option<&ProjectState>,
    processing_enabled: bool,
) -> SamplingResult {
    // For consistency reasons we take a snapshot in time and use that time across all code that
    // requires it.
    let now = Utc::now();

    // /hello release: 1.0
    // /world TKT
    // /hello -> /world
    let matching_trace_rule = match get_trace_sampling_rule(
        processing_enabled,
        sampling_project_state,
        dsc,
        ip_addr,
        now,
    ) {
        Ok(spec) => spec,
        Err(sampling_result) => return sampling_result,
    };

    let matching_event_rule = match get_event_sampling_rule(
        processing_enabled,
        project_state,
        dsc,
        event,
        ip_addr,
        now,
    ) {
        Ok(spec) => spec,
        Err(sampling_result) => return sampling_result,
    };

    // NOTE: Event rules take precedence over trace rules. If the event rule has a lower sample rate
    // than the trace rule, this means that traces will be incomplete.
    // We could guarantee consistent traces if trace rules took precedence over event rules,
    // but we need the current behavior to allow health check rules
    // to take precedence over the overall base rate, which is set on the trace.
    if let Some(spec) = matching_event_rule.or(matching_trace_rule) {
        let random_number = relay_sampling::pseudo_random_from_uuid(spec.seed);
        if random_number >= spec.sample_rate {
            return SamplingResult::Drop(spec.rule_id);
        }
    }

    SamplingResult::Keep
}

#[derive(Clone, Debug, PartialEq)]
enum SamplingMatchResult {
    Match {
        sample_rate: f64,
        rule_id: RuleId,
        seed: Uuid,
    },
    NoMatch,
    Override {
        sampling_result: SamplingResult,
    },
}

macro_rules! no_match_if_none {
    ($e:expr) => {
        match $e {
            Some(x) => x,
            None => return SamplingMatchResult::NoMatch,
        }
    };
}

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
            relay_log::trace!("found sampling context, but no corresponding project state");
        }

        self
    }

    fn get_merged_config(&self) -> SamplingConfig {
        // We get all the transaction rules of the sampling config of the project to which
        // the incoming transaction belongs.
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
            .filter(|rule| rule.ty == RuleType::Trace);

        SamplingConfig {
            // TODO: what do we do with rule ids? Do we re-create them?
            rules: event_rules.chain(parent_rules).collect(),
            // We want to take field priority on the fields from the sampling config of the project
            // to which the incoming transaction belongs.
            // TODO: which mode do we choose, do we want to choose the root project mode in case
            //  we match a trace?
            mode: self.sampling_config.mode,
            // TODO: do we want to keep this field, it seems unused.
            next_id: self.sampling_config.next_id,
        }
    }
}

fn get_seed(
    dsc: Option<&DynamicSamplingContext>,
    event_id: &EventId,
    result: &SamplingConfigMatchResult,
) -> Uuid {
    if result.has_matched_trace_rule {
        if let Some(dsc) = dsc {
            dsc.trace_id
        } else {
            event_id.0
        }
    } else {
        event_id.0
    }
}

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
    let event_id = no_match_if_none!(event.id.value());
    let sampling_config = no_match_if_none!(&project_state.config.dynamic_sampling);

    // We obtain the merged sampling configuration with a concatenation of transaction rules
    // of the current project and trace rules of the root project.
    let merged_config = SamplingConfigs::new(sampling_config)
        .add_root_config(root_project_state)
        .get_merged_config();

    if let Err(sampling_result) = check_unsupported_rules(processing_enabled, &merged_config) {
        return SamplingMatchResult::Override { sampling_result };
    }

    let result = no_match_if_none!(merged_config.match_against_rules(event, dsc, ip_addr, now));
    let sample_rate = match sampling_config.mode {
        SamplingMode::Received => result.sample_rate,
        SamplingMode::Total => match dsc {
            Some(dsc) => dsc.adjusted_sample_rate(result.sample_rate),
            None => result.sample_rate,
        },
        SamplingMode::Unsupported => {
            if processing_enabled {
                relay_log::error!("found unsupported sampling mode even as processing Relay, keep");
            }

            return SamplingMatchResult::Override {
                sampling_result: SamplingResult::Keep,
            };
        }
    };

    SamplingMatchResult::Match {
        sample_rate,
        seed: get_seed(dsc, event_id, &result),
        // TODO: decide what to do with rule ids because we have multiple matches.
        rule_id: RuleId(1),
    }
}

pub fn should_keep_event_new(
    processing_enabled: bool,
    project_state: &ProjectState,
    root_project_state: Option<&ProjectState>,
    dsc: Option<&DynamicSamplingContext>,
    event: Option<&Event>,
    ip_addr: Option<IpAddr>,
) -> SamplingResult {
    // TODO: can we have some root project state and none dsc?
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
            rule_id,
            seed,
        } => {
            let random_number = relay_sampling::pseudo_random_from_uuid(seed);
            if random_number >= sample_rate {
                SamplingResult::Drop(rule_id)
            } else {
                SamplingResult::Keep
            }
        }
        SamplingMatchResult::NoMatch => SamplingResult::Keep,
        SamplingMatchResult::Override { sampling_result } => sampling_result,
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
mod tests_new {
    use chrono::DateTime;
    use chrono::Duration as DateDuration;

    use relay_common::EventType;
    use relay_general::protocol::{EventId, LenientString};
    use relay_general::types::Annotated;
    use relay_sampling::{
        AndCondition, DecayingFunction, EqCondOptions, EqCondition, RuleCondition, RuleId,
        RuleType, SamplingConfig, SamplingRule, SamplingStrategy, TimeRange,
    };

    use crate::testutils::create_sampling_context;
    use crate::testutils::new_envelope;
    use crate::testutils::project_state_with_config;
    use crate::testutils::state_with_rule;
    use crate::testutils::state_with_rule_and_condition;

    use super::*;

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
                    sampling_strategy: SamplingStrategy::SampleRate { value: 0.1 },
                    ty: RuleType::Transaction,
                    id: RuleId(1),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: eq("event.transaction", &["bar"], true),
                    sampling_strategy: SamplingStrategy::Factor { value: 1.0 },
                    ty: RuleType::Transaction,
                    id: RuleId(2),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: eq("event.transaction", &["foo"], true),
                    sampling_strategy: SamplingStrategy::SampleRate { value: 0.5 },
                    ty: RuleType::Transaction,
                    id: RuleId(3),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                // We put this trace rule here just for testing purposes, even though it will never
                // be considered if put within a non-root project.
                SamplingRule {
                    condition: RuleCondition::all(),
                    sampling_strategy: SamplingStrategy::SampleRate { value: 0.5 },
                    ty: RuleType::Trace,
                    id: RuleId(4),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
            ],
            mode,
            next_id: None,
        })
    }

    fn mocked_root_project_state(mode: SamplingMode) -> ProjectState {
        project_state_with_config(SamplingConfig {
            rules: vec![
                SamplingRule {
                    condition: eq("trace.release", &["3.0"], true),
                    sampling_strategy: SamplingStrategy::Factor { value: 1.5 },
                    ty: RuleType::Trace,
                    id: RuleId(1),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: eq("trace.environment", &["dev"], true),
                    sampling_strategy: SamplingStrategy::SampleRate { value: 1.0 },
                    ty: RuleType::Trace,
                    id: RuleId(1),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: RuleCondition::all(),
                    sampling_strategy: SamplingStrategy::SampleRate { value: 0.5 },
                    ty: RuleType::Trace,
                    id: RuleId(2),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
            ],
            mode,
            next_id: None,
        })
    }

    fn mocked_sampling_rule(id: u32, ty: RuleType, sample_rate: f64) -> SamplingRule {
        SamplingRule {
            condition: RuleCondition::all(),
            sampling_strategy: SamplingStrategy::SampleRate { value: sample_rate },
            ty,
            id: RuleId(id),
            time_range: Default::default(),
            decaying_fn: Default::default(),
        }
    }

    // with both configs trace and transaction containing transaction
    // with empty project and root project
    // with full project and empty root project
    // with empty project and full root project

    #[test]
    /// Tests the merged config of the two configs with rules.
    fn test_get_merged_config_with_rules_in_both_project_config_and_root_project_config() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![
                mocked_sampling_rule(1, RuleType::Transaction, 0.1),
                mocked_sampling_rule(2, RuleType::Error, 0.2),
                mocked_sampling_rule(3, RuleType::Trace, 0.3),
            ],
            mode: SamplingMode::Received,
            next_id: None,
        });
        let root_project_state = project_state_with_config(SamplingConfig {
            rules: vec![
                mocked_sampling_rule(4, RuleType::Transaction, 0.4),
                mocked_sampling_rule(5, RuleType::Error, 0.5),
                mocked_sampling_rule(6, RuleType::Trace, 0.6),
            ],
            mode: SamplingMode::Received,
            next_id: None,
        });

        let result = SamplingConfigs::new(project_state.config.dynamic_sampling.as_ref().unwrap())
            .add_root_config(Some(&root_project_state))
            .get_merged_config();

        let expected_result = vec![1, 2, 6];
        for (index, rule) in result.rules.iter().enumerate() {
            assert_eq!(rule.id.0, expected_result[index])
        }
    }

    #[test]
    /// Tests the merged config of the two configs without rules.
    fn test_get_merged_config_with_no_rules_in_both_project_config_and_root_project_config() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            mode: SamplingMode::Received,
            next_id: None,
        });
        let root_project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            mode: SamplingMode::Received,
            next_id: None,
        });

        let result = SamplingConfigs::new(project_state.config.dynamic_sampling.as_ref().unwrap())
            .add_root_config(Some(&root_project_state))
            .get_merged_config();

        assert!(result.rules.is_empty());
    }

    #[test]
    /// Tests the merged config of the project config with rules and the root project config
    /// without rules.
    fn test_get_merged_config_with_rules_in_project_config_and_no_rules_in_root_project_config() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![
                mocked_sampling_rule(1, RuleType::Transaction, 0.1),
                mocked_sampling_rule(2, RuleType::Error, 0.2),
                mocked_sampling_rule(3, RuleType::Trace, 0.3),
            ],
            mode: SamplingMode::Received,
            next_id: None,
        });
        let root_project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            mode: SamplingMode::Received,
            next_id: None,
        });

        let result = SamplingConfigs::new(project_state.config.dynamic_sampling.as_ref().unwrap())
            .add_root_config(Some(&root_project_state))
            .get_merged_config();

        let expected_result = vec![1, 2];
        for (index, rule) in result.rules.iter().enumerate() {
            assert_eq!(rule.id.0, expected_result[index])
        }
    }

    #[test]
    /// Tests the merged config of the project config without rules and the root project config
    /// with rules.
    fn test_get_merged_config_with_no_rules_in_project_config_and_with_rules_in_root_project_config(
    ) {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![],
            mode: SamplingMode::Received,
            next_id: None,
        });
        let root_project_state = project_state_with_config(SamplingConfig {
            rules: vec![
                mocked_sampling_rule(4, RuleType::Transaction, 0.4),
                mocked_sampling_rule(5, RuleType::Error, 0.5),
                mocked_sampling_rule(6, RuleType::Trace, 0.6),
            ],
            mode: SamplingMode::Received,
            next_id: None,
        });

        let result = SamplingConfigs::new(project_state.config.dynamic_sampling.as_ref().unwrap())
            .add_root_config(Some(&root_project_state))
            .get_merged_config();

        let expected_result = vec![6];
        for (index, rule) in result.rules.iter().enumerate() {
            assert_eq!(rule.id.0, expected_result[index])
        }
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

        assert_eq!(result, SamplingMatchResult::NoMatch)
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

        assert_eq!(
            result,
            SamplingMatchResult::Match {
                sample_rate: 0.1,
                seed: event.id.0.unwrap().0,
                rule_id: RuleId(1)
            }
        )
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

        assert_eq!(
            result,
            SamplingMatchResult::Match {
                sample_rate: 1.0,
                seed: dsc.trace_id,
                rule_id: RuleId(1)
            }
        )
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

        assert_eq!(
            result,
            SamplingMatchResult::Match {
                sample_rate: 0.75,
                seed: dsc.trace_id,
                rule_id: RuleId(1)
            }
        )
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

        assert_eq!(
            result,
            SamplingMatchResult::Match {
                sample_rate: 0.5,
                seed: event.id.0.unwrap().0,
                rule_id: RuleId(1)
            }
        )
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

        assert_eq!(
            result,
            SamplingMatchResult::Match {
                sample_rate: 0.625,
                seed: event.id.0.unwrap().0,
                rule_id: RuleId(1)
            }
        )
    }

    #[test]
    /// Tests that an override is raised in case we have unsupported rules with processing both
    /// enabled and disabled.
    fn test_get_sampling_match_result_with_unsupported_rules() {
        let mut project_state = mocked_project_state(SamplingMode::Received);
        project_state
            .config
            .dynamic_sampling
            .as_mut()
            .unwrap()
            .rules
            .push(SamplingRule {
                condition: RuleCondition::Unsupported,
                sampling_strategy: SamplingStrategy::SampleRate { value: 0.5 },
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

        assert_eq!(
            result,
            SamplingMatchResult::Override {
                sampling_result: SamplingResult::Keep
            }
        );

        let result = get_sampling_match_result(
            true,
            &project_state,
            Some(&root_project_state),
            Some(&dsc),
            Some(&event),
            None,
            Utc::now(),
        );

        assert_eq!(
            result,
            SamplingMatchResult::Match {
                sample_rate: 0.5,
                seed: event.id.0.unwrap().0,
                rule_id: RuleId(1)
            }
        )
    }

    #[test]
    /// Tests that an override is raised in case we have an unsupported sampling mode and a match.
    fn test_get_sampling_match_result_with_unsupported_sampling_mode_and_match() {
        let mut project_state = mocked_project_state(SamplingMode::Unsupported);
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

        assert_eq!(
            result,
            SamplingMatchResult::Override {
                sampling_result: SamplingResult::Keep
            }
        )
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
                sampling_strategy: SamplingStrategy::SampleRate { value: 0.5 },
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

        assert_eq!(result, SamplingMatchResult::NoMatch);
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
                sampling_strategy: SamplingStrategy::SampleRate { value: 0.5 },
                ty: RuleType::Error,
                id: RuleId(1),
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

        assert_eq!(
            result,
            SamplingMatchResult::Match {
                sample_rate: 0.5,
                seed: event.id.0.unwrap().0,
                rule_id: RuleId(1)
            }
        );
    }

    #[test]
    fn test_should_keep_event_return_keep_with_match_and_100_sample_rate() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![mocked_sampling_rule(1, RuleType::Transaction, 1.0)],
            mode: SamplingMode::Received,
            next_id: None,
        });

        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = should_keep_event_new(true, &project_state, None, None, Some(&event), None);

        assert_eq!(result, SamplingResult::Keep)
    }

    #[test]
    fn test_should_keep_event_return_drop_with_match_and_0_sample_rate() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![mocked_sampling_rule(1, RuleType::Transaction, 0.0)],
            mode: SamplingMode::Received,
            next_id: None,
        });

        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = should_keep_event_new(true, &project_state, None, None, Some(&event), None);

        assert_eq!(result, SamplingResult::Drop(RuleId(1)))
    }

    #[test]
    fn test_should_keep_event_return_keep_with_no_match() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![SamplingRule {
                condition: eq("event.transaction", &["foo"], true),
                sampling_strategy: SamplingStrategy::SampleRate { value: 0.5 },
                ty: RuleType::Transaction,
                id: RuleId(3),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            }],
            mode: SamplingMode::Received,
            next_id: None,
        });

        let event = mocked_event(EventType::Transaction, "foo", "2.0");

        let result = should_keep_event_new(true, &project_state, None, None, Some(&event), None);

        assert_eq!(result, SamplingResult::Keep)
    }

    #[test]
    fn test_should_keep_event_return_keep_with_unsupported_rule() {
        let project_state = project_state_with_config(SamplingConfig {
            rules: vec![mocked_sampling_rule(1, RuleType::Unsupported, 0.0)],
            mode: SamplingMode::Received,
            next_id: None,
        });

        let event = mocked_event(EventType::Transaction, "transaction", "2.0");

        let result = should_keep_event_new(true, &project_state, None, None, Some(&event), None);

        assert_eq!(result, SamplingResult::Keep)
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use chrono::Duration as DateDuration;

    use relay_common::EventType;
    use relay_general::protocol::EventId;
    use relay_general::types::Annotated;
    use relay_sampling::{
        DecayingFunction, EqCondition, RuleCondition, RuleId, RuleType, SamplingConfig,
        SamplingRule, SamplingStrategy, TimeRange,
    };

    use crate::testutils::create_sampling_context;
    use crate::testutils::new_envelope;
    use crate::testutils::project_state_with_config;
    use crate::testutils::state_with_rule;
    use crate::testutils::state_with_rule_and_condition;

    use super::*;

    fn state_with_decaying_rule(
        sample_rate: Option<f64>,
        rule_type: RuleType,
        mode: SamplingMode,
        decaying_fn: DecayingFunction,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> ProjectState {
        let rules = match sample_rate {
            Some(sample_rate) => vec![SamplingRule {
                condition: RuleCondition::all(),
                sampling_strategy: SamplingStrategy::SampleRate { value: sample_rate },
                ty: rule_type,
                id: RuleId(1),
                time_range: TimeRange { start, end },
                decaying_fn,
            }],
            None => Vec::new(),
        };

        project_state_with_config(SamplingConfig {
            rules,
            mode,
            next_id: None,
        })
    }

    fn prepare_and_get_sampling_rule(
        client_sample_rate: f64,
        event_type: EventType,
        project_state: &ProjectState,
        now: DateTime<Utc>,
    ) -> Result<Option<SamplingSpec>, SamplingResult> {
        let sampling_context = create_sampling_context(Some(client_sample_rate));
        let event = Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(event_type),
            ..Event::default()
        };

        get_event_sampling_rule(
            true, // irrelevant, just skips unsupported rules
            project_state,
            Some(&sampling_context),
            Some(&event),
            None, // ip address not needed for uniform rule
            now,
        )
    }

    fn samplingresult_from_rules_and_proccessing_flag(
        rules: Vec<SamplingRule>,
        processing_enabled: bool,
    ) -> SamplingResult {
        let event_state = project_state_with_config(SamplingConfig {
            rules,
            mode: SamplingMode::Received,
            next_id: None,
        });

        let some_event = Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("testing".to_owned()),
            ..Event::default()
        };

        let some_envelope = new_envelope(true, "testing");

        should_keep_event(
            some_envelope.dsc(),
            Some(&some_event),
            None,
            &event_state,
            None,
            processing_enabled,
        )
    }

    /// Checks that events aren't dropped if they contain an unsupported rule,
    /// checks the cases with and without the process_enabled flag
    #[test]
    fn test_bad_dynamic_rules() {
        // adds a rule which should always match (meaning the event will be dropped)
        let mut rules = vec![SamplingRule {
            condition: RuleCondition::all(),
            sampling_strategy: SamplingStrategy::SampleRate { value: 0.0 },
            ty: RuleType::Transaction,
            id: RuleId(1),
            time_range: Default::default(),
            decaying_fn: Default::default(),
        }];

        // ensures the event is indeed dropped with and without processing enabled
        let res = samplingresult_from_rules_and_proccessing_flag(rules.clone(), false);
        assert!(matches!(res, SamplingResult::Drop(_)));

        let res = samplingresult_from_rules_and_proccessing_flag(rules.clone(), true);
        assert!(matches!(res, SamplingResult::Drop(_)));

        rules.push(SamplingRule {
            condition: RuleCondition::Unsupported,
            sampling_strategy: SamplingStrategy::SampleRate { value: 0.0 },
            ty: RuleType::Transaction,
            id: RuleId(1),
            time_range: Default::default(),
            decaying_fn: Default::default(),
        });

        // now that an unsupported rule has been pushed, it should keep the event if processing is disabled
        let res = samplingresult_from_rules_and_proccessing_flag(rules.clone(), false);
        assert!(matches!(res, SamplingResult::Keep));

        let res = samplingresult_from_rules_and_proccessing_flag(rules, true);
        assert!(matches!(res, SamplingResult::Drop(_))); // should also log an error
    }

    #[test]
    fn test_trace_rules_applied_after_event_rules() {
        // a transaction rule that drops everything
        let event_state = state_with_rule_and_condition(
            Some(0.0),
            RuleType::Transaction,
            SamplingMode::Received,
            RuleCondition::Eq(EqCondition {
                name: "event.transaction".to_owned(),
                value: "healthcheck".into(),
                options: Default::default(),
            }),
        );

        // a trace rule that keeps everything
        let trace_state = state_with_rule(Some(1.0), RuleType::Trace, SamplingMode::Received);

        let healthcheck_envelope = new_envelope(true, "healthcheck");
        let other_envelope = new_envelope(true, "test1");

        let healthcheck_event = Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("healthcheck".to_owned()),
            ..Event::default()
        };

        let other_event = Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("test1".to_owned()),
            ..Event::default()
        };

        // if it matches the transaction rule, the transaction should be dropped
        let should_drop = should_keep_event(
            healthcheck_envelope.dsc(),
            Some(&healthcheck_event),
            None,
            &event_state,
            Some(&trace_state),
            false,
        );

        // if it doesn't match the transaction rule, the transaction shouldn't be dropped
        let should_keep = should_keep_event(
            other_envelope.dsc(),
            Some(&other_event),
            None,
            &event_state,
            Some(&trace_state),
            false,
        );

        let now = Utc::now();

        // matching event should return an event rule
        assert!(get_event_sampling_rule(
            false,
            &event_state,
            healthcheck_envelope.dsc(),
            Some(&healthcheck_event),
            None,
            now,
        )
        .unwrap()
        .is_some());

        // non-matching event should not return an event rule
        assert!(get_event_sampling_rule(
            false,
            &event_state,
            other_envelope.dsc(),
            Some(&other_event),
            None,
            now,
        )
        .unwrap()
        .is_none());

        assert!(matches!(should_keep, SamplingResult::Keep));
        assert!(matches!(should_drop, SamplingResult::Drop(_)));
    }

    #[test]
    /// Should_keep_event returns the expected results.
    fn test_should_keep_event() {
        let event = Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(EventType::Error),
            ..Event::default()
        };

        let proj_state = state_with_rule(Some(0.0), RuleType::Error, SamplingMode::default());

        assert_eq!(
            SamplingResult::Drop(RuleId(1)),
            should_keep_event(None, Some(&event), None, &proj_state, None, true)
        );
        let proj_state = state_with_rule(Some(1.0), RuleType::Error, SamplingMode::default());
        assert_eq!(
            SamplingResult::Keep,
            should_keep_event(None, Some(&event), None, &proj_state, None, true)
        );
        let proj_state = state_with_rule(None, RuleType::Error, SamplingMode::default());
        assert_eq!(
            SamplingResult::Keep,
            should_keep_event(None, Some(&event), None, &proj_state, None, true)
        );
    }

    #[test]
    fn test_unsampled_envelope_with_sample_rate() {
        //create an envelope with a event and a transaction
        let envelope = new_envelope(true, "");
        let state = state_with_rule(Some(1.0), RuleType::Trace, SamplingMode::default());
        let sampling_state = state_with_rule(Some(0.0), RuleType::Trace, SamplingMode::default());
        let result = should_keep_event(
            envelope.dsc(),
            None,
            None,
            &state,
            Some(&sampling_state),
            true,
        );
        assert_eq!(result, SamplingResult::Drop(RuleId(1)));
    }

    #[test]
    /// Should keep transaction when no trace context is present
    fn test_should_keep_transaction_no_trace() {
        //create an envelope with a event and a transaction
        let envelope = new_envelope(false, "");
        let state = state_with_rule(Some(1.0), RuleType::Trace, SamplingMode::default());
        let sampling_state = state_with_rule(Some(0.0), RuleType::Trace, SamplingMode::default());

        let result = should_keep_event(
            envelope.dsc(),
            None,
            None,
            &state,
            Some(&sampling_state),
            true,
        );
        assert_eq!(result, SamplingResult::Keep);
        // both the event and the transaction item should have been left in the envelope
        assert_eq!(envelope.len(), 3);
    }

    #[test]
    /// When the envelope becomes empty due to sampling we should get back the rule that dropped the
    /// transaction
    fn test_should_signal_when_envelope_becomes_empty() {
        //create an envelope with a event and a transaction
        let envelope = new_envelope(true, "");
        let state = state_with_rule(Some(1.0), RuleType::Trace, SamplingMode::default());
        let sampling_state = state_with_rule(Some(0.0), RuleType::Trace, SamplingMode::default());

        let result = should_keep_event(
            envelope.dsc(),
            None,
            None,
            &state,
            Some(&sampling_state),
            true,
        );
        assert_eq!(result, SamplingResult::Drop(RuleId(1)));
    }

    #[test]
    /// When there's a mixture of event rules and trace rules, the event rules
    /// take precedence.
    fn test_event_rule_precedence() {
        let sampling_config = serde_json::json!(
            {
                "rules": [
                    {
                        "samplingStrategy": {"type": "sampleRate", "value": 0},
                        "type": "trace",
                        "active": true,
                        "condition": {
                            "op": "and",
                            "inner": []
                        },
                        "id": 1000
                    },
                    {
                        "samplingStrategy": {"type": "sampleRate", "value": 1},
                        "type": "transaction",
                        "condition": {
                            "op": "or",
                            "inner": [
                            {
                                "op": "glob",
                                "name": "event.transaction",
                                "value": [
                                    "my-important-transaction",
                                ],
                                "options": {
                                    "ignoreCase": true
                                }
                            }
                            ]
                        },
                        "active": true,
                        "id": 1002
                    }
                ]
            }
        );

        let sampling_config = serde_json::from_value(sampling_config).unwrap();
        let project_state = project_state_with_config(sampling_config);

        let envelope = new_envelope(true, "");

        let event = Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("my-important-transaction".to_owned()),
            ..Event::default()
        };

        let keep_event = should_keep_event(
            envelope.dsc(),
            Some(&event),
            None,
            &project_state,
            Some(&project_state),
            true,
        );

        assert_eq!(keep_event, SamplingResult::Keep);
    }

    #[test]
    fn test_trace_rule_received() {
        let project_state = state_with_rule(Some(0.1), RuleType::Trace, SamplingMode::Received);
        let sampling_context = create_sampling_context(Some(0.5));
        let spec = get_trace_sampling_rule(
            true, // irrelevant, just skips unsupported rules
            Some(&project_state),
            Some(&sampling_context),
            None,
            Utc::now(),
        );

        assert_eq!(spec.unwrap().unwrap().sample_rate, 0.1);
    }

    #[test]
    fn test_trace_rule_adjusted() {
        let project_state = state_with_rule(Some(0.1), RuleType::Trace, SamplingMode::Total);
        let sampling_context = create_sampling_context(Some(0.5));
        let spec = get_trace_sampling_rule(
            true, // irrelevant, just skips unsupported rules
            Some(&project_state),
            Some(&sampling_context),
            None,
            Utc::now(),
        );

        assert_eq!(spec.unwrap().unwrap().sample_rate, 0.2);
    }

    #[test]
    fn test_trace_rule_unsupported() {
        let project_state = state_with_rule(Some(0.1), RuleType::Trace, SamplingMode::Unsupported);
        let sampling_context = create_sampling_context(Some(0.5));
        let spec = get_trace_sampling_rule(
            true,
            Some(&project_state),
            Some(&sampling_context),
            None,
            Utc::now(),
        );

        assert!(matches!(spec, Err(SamplingResult::Keep)));
    }

    #[test]
    fn test_event_rule_received() {
        let project_state =
            state_with_rule(Some(0.1), RuleType::Transaction, SamplingMode::Received);
        let sampling_context = create_sampling_context(Some(0.5));
        let event = Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(EventType::Transaction),
            ..Event::default()
        };

        let spec = get_event_sampling_rule(
            true, // irrelevant, just skips unsupported rules
            &project_state,
            Some(&sampling_context),
            Some(&event),
            None, // ip address not needed for uniform rule
            Utc::now(),
        );

        assert_eq!(spec.unwrap().unwrap().sample_rate, 0.1);
    }

    #[test]
    fn test_event_rule_adjusted() {
        let project_state = state_with_rule(Some(0.1), RuleType::Transaction, SamplingMode::Total);
        let sampling_context = create_sampling_context(Some(0.5));
        let event = Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(EventType::Transaction),
            ..Event::default()
        };

        let spec = get_event_sampling_rule(
            true, // irrelevant, just skips unsupported rules
            &project_state,
            Some(&sampling_context),
            Some(&event),
            None, // ip address not needed for uniform rule
            Utc::now(),
        );

        assert_eq!(spec.unwrap().unwrap().sample_rate, 0.2);
    }

    #[test]
    fn test_sample_rate() {
        let event_state_drop = state_with_rule_and_condition(
            Some(0.0),
            RuleType::Transaction,
            SamplingMode::Received,
            RuleCondition::all(),
        );

        let envelope = new_envelope(true, "foo");

        let event = Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("foo".to_owned()),
            ..Event::default()
        };

        // if it matches the transaction rule, the transaction should be dropped
        let should_drop = should_keep_event(
            envelope.dsc(),
            Some(&event),
            None,
            &event_state_drop,
            Some(&event_state_drop),
            false,
        );

        assert!(matches!(should_drop, SamplingResult::Drop(_)));

        let event_state_keep = state_with_rule_and_condition(
            Some(1.0),
            RuleType::Transaction,
            SamplingMode::Received,
            RuleCondition::all(),
        );

        let should_keep = should_keep_event(
            envelope.dsc(),
            Some(&event),
            None,
            &event_state_keep,
            Some(&event_state_keep),
            false,
        );

        assert!(matches!(should_keep, SamplingResult::Keep));
    }

    #[test]
    fn test_event_decaying_rule_with_linear_function() {
        let now = Utc::now();
        let project_state = state_with_decaying_rule(
            Some(0.7),
            RuleType::Transaction,
            SamplingMode::Total,
            DecayingFunction::Linear { decayed_value: 0.2 },
            Some(now - DateDuration::days(1)),
            Some(now + DateDuration::days(1)),
        );

        let sample_rate =
            prepare_and_get_sampling_rule(1.0, EventType::Transaction, &project_state, now)
                .unwrap()
                .unwrap()
                .sample_rate;
        let expected_sample_rate = 0.44999999999999996;

        // Workaround against floating point precision differences.
        // https://rust-lang.github.io/rust-clippy/master/#float_cmp
        assert!((sample_rate - expected_sample_rate).abs() < f64::EPSILON)
    }

    #[test]
    fn test_event_decaying_rule_with_open_time_range_and_linear_function() {
        let now = Utc::now();
        let project_state = state_with_decaying_rule(
            Some(0.7),
            RuleType::Transaction,
            SamplingMode::Total,
            DecayingFunction::Linear { decayed_value: 0.2 },
            Some(now - DateDuration::days(1)),
            None,
        );

        assert!(
            prepare_and_get_sampling_rule(1.0, EventType::Transaction, &project_state, now)
                .unwrap()
                .is_none()
        );

        let project_state = state_with_decaying_rule(
            Some(0.7),
            RuleType::Transaction,
            SamplingMode::Total,
            DecayingFunction::Linear { decayed_value: 0.2 },
            None,
            Some(now + DateDuration::days(1)),
        );

        assert!(
            prepare_and_get_sampling_rule(1.0, EventType::Transaction, &project_state, now)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_event_decaying_rule_with_no_time_range_and_linear_function() {
        let now = Utc::now();
        let project_state = state_with_decaying_rule(
            Some(0.7),
            RuleType::Transaction,
            SamplingMode::Total,
            DecayingFunction::Linear { decayed_value: 0.2 },
            None,
            None,
        );

        assert!(
            prepare_and_get_sampling_rule(1.0, EventType::Transaction, &project_state, now)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_event_decaying_rule_with_now_equal_start_and_linear_function() {
        let now = Utc::now();
        let project_state = state_with_decaying_rule(
            Some(0.7),
            RuleType::Transaction,
            SamplingMode::Total,
            DecayingFunction::Linear { decayed_value: 0.2 },
            Some(now),
            Some(now + DateDuration::days(1)),
        );

        assert_eq!(
            prepare_and_get_sampling_rule(1.0, EventType::Transaction, &project_state, now)
                .unwrap()
                .unwrap()
                .sample_rate,
            0.7
        );
    }

    #[test]
    fn test_event_decaying_rule_with_now_equal_end_and_linear_function() {
        let now = Utc::now();
        let project_state = state_with_decaying_rule(
            Some(0.7),
            RuleType::Transaction,
            SamplingMode::Total,
            DecayingFunction::Linear { decayed_value: 0.2 },
            Some(now - DateDuration::days(1)),
            Some(now),
        );

        assert!(
            prepare_and_get_sampling_rule(1.0, EventType::Transaction, &project_state, now)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_event_decaying_rule_with_base_less_then_decayed_and_linear_function() {
        let now = Utc::now();
        let project_state = state_with_decaying_rule(
            Some(0.3),
            RuleType::Transaction,
            SamplingMode::Total,
            DecayingFunction::Linear { decayed_value: 0.7 },
            Some(now - DateDuration::days(1)),
            Some(now + DateDuration::days(1)),
        );

        assert!(
            prepare_and_get_sampling_rule(1.0, EventType::Transaction, &project_state, now)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_event_decaying_rule_with_constant_function() {
        let now = Utc::now();
        let project_state = state_with_decaying_rule(
            Some(0.6),
            RuleType::Transaction,
            SamplingMode::Total,
            DecayingFunction::Constant,
            Some(now - DateDuration::days(1)),
            Some(now + DateDuration::days(1)),
        );

        assert_eq!(
            prepare_and_get_sampling_rule(1.0, EventType::Transaction, &project_state, now)
                .unwrap()
                .unwrap()
                .sample_rate,
            0.6
        );
    }

    #[test]
    fn test_event_decaying_rule_with_open_time_range_and_constant_function() {
        let now = Utc::now();
        let project_state = state_with_decaying_rule(
            Some(0.7),
            RuleType::Transaction,
            SamplingMode::Total,
            DecayingFunction::Constant,
            Some(now - DateDuration::days(1)),
            None,
        );

        assert_eq!(
            prepare_and_get_sampling_rule(1.0, EventType::Transaction, &project_state, now)
                .unwrap()
                .unwrap()
                .sample_rate,
            0.7
        );

        let project_state = state_with_decaying_rule(
            Some(0.7),
            RuleType::Transaction,
            SamplingMode::Total,
            DecayingFunction::Constant,
            None,
            Some(now + DateDuration::days(1)),
        );

        assert_eq!(
            prepare_and_get_sampling_rule(1.0, EventType::Transaction, &project_state, now)
                .unwrap()
                .unwrap()
                .sample_rate,
            0.7
        );
    }

    #[test]
    fn test_event_decaying_rule_with_no_time_range_and_constant_function() {
        let now = Utc::now();
        let project_state = state_with_decaying_rule(
            Some(0.7),
            RuleType::Transaction,
            SamplingMode::Total,
            DecayingFunction::Constant,
            None,
            None,
        );

        assert_eq!(
            prepare_and_get_sampling_rule(1.0, EventType::Transaction, &project_state, now)
                .unwrap()
                .unwrap()
                .sample_rate,
            0.7
        );
    }

    #[test]
    fn test_event_decaying_rule_with_inverse_time_range_and_constant_function() {
        let now = Utc::now();
        let project_state = state_with_decaying_rule(
            Some(0.6),
            RuleType::Transaction,
            SamplingMode::Total,
            DecayingFunction::Constant,
            Some(now + DateDuration::days(1)),
            Some(now - DateDuration::days(1)),
        );

        assert!(
            prepare_and_get_sampling_rule(1.0, EventType::Transaction, &project_state, now)
                .unwrap()
                .is_none()
        );
    }
}
