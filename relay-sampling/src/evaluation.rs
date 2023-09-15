//! Evaluation of dynamic sampling rules.

use std::fmt;
use std::num::ParseIntError;

use chrono::{DateTime, Utc};
use rand::distributions::Uniform;
use rand::Rng;
use rand_pcg::Pcg32;
use relay_base_schema::events::EventType;
use relay_event_schema::protocol::Event;
use serde::Serialize;
use uuid::Uuid;

use crate::config::{RuleId, RuleType, SamplingConfig, SamplingMode, SamplingRule, SamplingValue};
use crate::DynamicSamplingContext;

/// Generates a pseudo random number by seeding the generator with the given id.
///
/// The return is deterministic, always generates the same number from the same id.
fn pseudo_random_from_uuid(id: Uuid) -> f64 {
    let big_seed = id.as_u128();
    let mut generator = Pcg32::new((big_seed >> 64) as u64, big_seed as u64);
    let dist = Uniform::new(0f64, 1f64);
    generator.sample(dist)
}

/// Returns an iterator of references that chains together and merges rules.
///
/// The chaining logic will take all the non-trace rules from the project and all the trace/unsupported
/// rules from the root project and concatenate them.
fn merge_rules_from_configs<'a>(
    sampling_config: Option<&'a SamplingConfig>,
    root_sampling_config: Option<&'a SamplingConfig>,
) -> impl Iterator<Item = &'a SamplingRule> {
    let transaction_rules = sampling_config
        .into_iter()
        .flat_map(|config| config.rules_v2.iter())
        .filter(|&rule| rule.ty == RuleType::Transaction);

    let trace_rules = root_sampling_config
        .into_iter()
        .flat_map(|config| config.rules_v2.iter())
        .filter(|&rule| rule.ty == RuleType::Trace);

    transaction_rules.chain(trace_rules)
}

/// Checks whether unsupported rules result in a direct keep of the event or depending on the
/// type of Relay an ignore of unsupported rules.
fn check_unsupported_rules(
    processing_enabled: bool,
    sampling_config: Option<&SamplingConfig>,
    root_sampling_config: Option<&SamplingConfig>,
) -> Result<(), ()> {
    // When we have unsupported rules disable sampling for non processing relays.
    if sampling_config.map_or(false, |config| config.unsupported())
        || root_sampling_config.map_or(false, |config| config.unsupported())
    {
        if !processing_enabled {
            return Err(());
        } else {
            relay_log::error!("found unsupported rules even as processing relay");
        }
    }

    Ok(())
}

fn verify_and_merge_rules<'a>(
    processing_enabled: bool,
    sampling_config: Option<&'a SamplingConfig>,
    root_sampling_config: Option<&'a SamplingConfig>,
) -> Option<impl Iterator<Item = &'a SamplingRule>> {
    check_unsupported_rules(processing_enabled, sampling_config, root_sampling_config).ok()?;

    Some(merge_rules_from_configs(
        sampling_config,
        root_sampling_config,
    ))
}

fn sampling_match(sample_rate: f64, seed: Uuid) -> bool {
    let random_number = pseudo_random_from_uuid(seed);
    relay_log::trace!(
        sample_rate,
        random_number,
        "applying dynamic sampling to matching event"
    );

    if random_number >= sample_rate {
        relay_log::trace!("dropping event that matched the configuration");
        false
    } else {
        relay_log::trace!("keeping event that matched the configuration");
        true
    }
}

/// Gets the sampling match for an event.
pub fn match_rules<'a>(
    processing_enabled: bool,
    sampling_config: Option<&'a SamplingConfig>,
    root_sampling_config: Option<&'a SamplingConfig>,
    event: Option<&Event>,
    dsc: Option<&DynamicSamplingContext>,
    now: DateTime<Utc>,
) -> SamplingResult {
    let adjust_sample_rate = match sampling_config
        .or(root_sampling_config)
        .map(|config| config.mode)
    {
        Some(SamplingMode::Total) => true,
        Some(SamplingMode::Received) => false,
        Some(SamplingMode::Unsupported) => {
            if processing_enabled {
                relay_log::error!("found unsupported sampling mode even as processing Relay");
            }
            return SamplingResult::NoMatch;
        }
        None => {
            relay_log::info!("cannot sample without at least one sampling config");
            return SamplingResult::NoMatch;
        }
    };

    // We perform the rule matching with the multi-matching logic on the merged rules.
    let Some(rules) =
        verify_and_merge_rules(processing_enabled, sampling_config, root_sampling_config)
    else {
        return SamplingResult::NoMatch;
    };

    get_sampling_match(rules, event, dsc, now, adjust_sample_rate)
}

/// Represents the specification for sampling an incoming event.
#[derive(Default, Clone, Debug, PartialEq)]
pub enum SamplingResult {
    /// The event matched a sampling condition.
    Match {
        /// The sample rate to use for the incoming event.
        sample_rate: f64,
        /// The seed to feed to the random number generator which allows the same number to be
        /// generated given the same seed.
        ///
        /// This is especially important for trace sampling, even though we can have inconsistent
        /// traces due to multi-matching.
        seed: Uuid,
        /// The list of rule ids that have matched the incoming event and/or dynamic sampling context.
        matched_rules: MatchedRuleIds,
        /// Whether this sampling match results in the item getting sampled.
        /// It's essentially a cache, as the value can be deterministically derived from
        /// the sample rate and the seed.
        is_kept: bool,
    },
    /// The event did not match a sampling condition.
    #[default]
    NoMatch,
}

impl SamplingResult {
    /// Returns the sample rate.
    pub fn sample_rate(&self) -> Option<f64> {
        if let Self::Match { sample_rate, .. } = self {
            return Some(*sample_rate);
        }
        None
    }

    /// Returns true if the event matched on any rules.
    pub fn no_match(&self) -> bool {
        matches!(self, &Self::NoMatch)
    }

    /// Returns true if the event did not match on any rules.
    pub fn matches(&self) -> bool {
        !self.no_match()
    }

    /// Returns true if the event should be kept.
    pub fn should_keep(&self) -> bool {
        match self {
            SamplingResult::Match { is_kept, .. } => *is_kept,
            // If no rules matched on an event, we want to keep it.
            SamplingResult::NoMatch => true,
        }
    }

    /// Returns true if the event should be dropped.
    pub fn should_drop(&self) -> bool {
        !self.should_keep()
    }
}

/// Matches an event and/or dynamic sampling context against the rules of the sampling configuration.
///
/// The multi-matching algorithm used iterates by collecting and multiplying factor rules until
/// it finds a sample rate rule. Once a sample rate rule is found, the final sample rate is
/// computed by multiplying it with the previously accumulated factors.
///
/// The default accumulated factors equal to 1 because it is the identity of the multiplication
/// operation, thus in case no factor rules are matched, the final result will just be the
/// sample rate of the matching rule.
fn get_sampling_match<'a>(
    rules: impl Iterator<Item = &'a SamplingRule>,
    event: Option<&Event>,
    dsc: Option<&DynamicSamplingContext>,
    now: DateTime<Utc>,
    adjust_sample_rate: bool,
) -> SamplingResult {
    let mut matched_rule_ids = vec![];
    // Even though this seed is changed based on whether we match event or trace rules, we will
    // still incur in inconsistent trace sampling because of multi-matching of rules across event
    // and trace rules.
    //
    // An example of inconsistent trace sampling could be:
    // /hello -> /world -> /transaction belong to trace_id = abc
    // * /hello has uniform rule with 0.2 sample rate which will match all the transactions of the trace
    // * each project has a single transaction rule with different factors (2, 3, 4)
    //
    // 1. /hello is matched with a transaction rule with a factor of 2 and uses as seed abc -> 0.2 * 2 = 0.4 sample rate
    // 2. /world is matched with a transaction rule with a factor of 3 and uses as seed abc -> 0.2 * 3 = 0.6 sample rate
    // 3. /transaction is matched with a transaction rule with a factor of 4 and uses as seed abc -> 0.2 * 4 = 0.8 sample rate
    //
    // We can see that we have 3 different samples rates but given the same seed, the random number generated will be the same.
    let mut seed = event.and_then(|e| e.id.value()).map(|id| id.0);
    let mut accumulated_factors = 1.0;

    for rule in rules {
        let matches = match rule.ty {
            RuleType::Trace => match dsc {
                Some(dsc) => rule.condition.matches(dsc),
                _ => false,
            },
            RuleType::Transaction => event.map_or(false, |event| match event.ty.0 {
                Some(EventType::Transaction) => rule.condition.matches(event),
                _ => false,
            }),
            _ => false,
        };

        if matches {
            if let Some(value) = rule.sample_rate(now) {
                matched_rule_ids.push(rule.id);

                if rule.ty == RuleType::Trace {
                    if let Some(dsc) = dsc {
                        seed = Some(dsc.trace_id);
                    }
                }

                match rule.sampling_value {
                    SamplingValue::Factor { .. } => accumulated_factors *= value,
                    SamplingValue::SampleRate { .. } => {
                        let sample_rate = {
                            let base = (value * accumulated_factors).clamp(0.0, 1.0);
                            if adjust_sample_rate {
                                dsc.and_then(|d| d.sample_rate)
                                    .map(|client_sample_rate| {
                                        DynamicSamplingContext::adjust_sample_rate(
                                            client_sample_rate,
                                            base,
                                        )
                                    })
                                    .unwrap_or(base)
                            } else {
                                base
                            }
                        };

                        let Some(seed) = seed else {
                            return SamplingResult::NoMatch;
                        };

                        let is_kept = sampling_match(sample_rate, seed);

                        return SamplingResult::Match {
                            sample_rate,
                            seed,
                            matched_rules: MatchedRuleIds(matched_rule_ids),
                            is_kept,
                        };
                    }
                }
            }
        }
    }

    // In case no match is available, we won't return any specification.
    relay_log::trace!("keeping event that didn't match the configuration");
    SamplingResult::NoMatch
}

/// Represents a list of rule ids which is used for outcomes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct MatchedRuleIds(pub Vec<RuleId>);

impl MatchedRuleIds {
    /// Parses `MatchedRuleIds` from a string with concatenated rule identifiers.
    ///
    /// The format it parses from is:
    ///
    /// ```text
    /// rule_id_1,rule_id_2,...
    /// ```
    pub fn parse(value: &str) -> Result<MatchedRuleIds, ParseIntError> {
        let mut rule_ids = vec![];

        for rule_id in value.split(',') {
            rule_ids.push(RuleId(rule_id.parse()?));
        }

        Ok(MatchedRuleIds(rule_ids))
    }
}

impl fmt::Display for MatchedRuleIds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, rule_id) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{rule_id}")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Duration as DateDuration, TimeZone, Utc};
    use relay_base_schema::events::EventType;
    use relay_base_schema::project::ProjectKey;
    use relay_common::glob3::GlobPatterns;
    use serde_json::Value;
    use similar_asserts::assert_eq;
    use uuid::Uuid;

    use relay_event_schema::protocol::{
        Event, EventId, Exception, Headers, IpAddr, JsonLenientString, LenientString, LogEntry,
        PairList, Request, User, Values,
    };
    use relay_protocol::Annotated;

    use crate::condition::RuleCondition;
    use crate::condition::{
        AndCondition, EqCondOptions, EqCondition, GlobCondition, NotCondition, OrCondition,
    };
    use crate::config::{
        DecayingFunction, RuleId, RuleType, SamplingMode, SamplingRule, SamplingValue, TimeRange,
    };
    use crate::dsc::TraceUserContext;
    use crate::evaluation::{get_sampling_match, match_rules, MatchedRuleIds, SamplingResult};

    use super::*;

    fn eq(name: &str, value: &[&str], ignore_case: bool) -> RuleCondition {
        RuleCondition::Eq(EqCondition {
            name: name.to_owned(),
            value: value.iter().map(|s| s.to_string()).collect(),
            options: EqCondOptions { ignore_case },
        })
    }

    fn eq_null(name: &str) -> RuleCondition {
        RuleCondition::Eq(EqCondition {
            name: name.to_owned(),
            value: Value::Null,
            options: EqCondOptions { ignore_case: true },
        })
    }

    fn glob(name: &str, value: &[&str]) -> RuleCondition {
        RuleCondition::Glob(GlobCondition {
            name: name.to_owned(),
            value: GlobPatterns::new(value.iter().map(|s| s.to_string()).collect()),
        })
    }

    fn and(conds: Vec<RuleCondition>) -> RuleCondition {
        RuleCondition::And(AndCondition { inner: conds })
    }

    fn or(conds: Vec<RuleCondition>) -> RuleCondition {
        RuleCondition::Or(OrCondition { inner: conds })
    }

    fn not(cond: RuleCondition) -> RuleCondition {
        RuleCondition::Not(NotCondition {
            inner: Box::new(cond),
        })
    }

    fn mocked_sampling_config_with_rules(rules: Vec<SamplingRule>) -> SamplingConfig {
        SamplingConfig {
            rules: vec![],
            rules_v2: rules,
            mode: SamplingMode::Received,
        }
    }

    fn mocked_event(
        event_type: EventType,
        transaction: &str,
        release: &str,
        environment: &str,
    ) -> Event {
        Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(event_type),
            transaction: Annotated::new(transaction.to_string()),
            release: Annotated::new(LenientString(release.to_string())),
            environment: Annotated::new(environment.to_string()),
            ..Event::default()
        }
    }

    fn mocked_dsc(environment: &str, user_segment: &str, release: &str) -> DynamicSamplingContext {
        DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: "12345678901234567890123456789012".parse().unwrap(),
            release: Some(release.to_string()),
            environment: Some(environment.to_string()),
            transaction: Some("root_transaction".to_string()),
            sample_rate: Some(1.0),
            user: TraceUserContext {
                user_segment: user_segment.to_string(),
                user_id: "user-id".to_string(),
            },
            replay_id: None,
            sampled: None,
            other: Default::default(),
        }
    }

    fn mocked_simple_dynamic_sampling_context(
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
            replay_id: None,
            sampled: None,
        }
    }

    fn mocked_sampling_config(mode: SamplingMode) -> SamplingConfig {
        SamplingConfig {
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
        }
    }

    fn mocked_root_project_sampling_config(mode: SamplingMode) -> SamplingConfig {
        SamplingConfig {
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

    fn add_sampling_rule_to_config(
        sampling_config: &mut SamplingConfig,
        sampling_rule: SamplingRule,
    ) {
        sampling_config.rules_v2.push(sampling_rule);
    }

    fn match_against_rules(
        config: &SamplingConfig,
        event: &Event,
        dsc: &DynamicSamplingContext,
        now: DateTime<Utc>,
    ) -> SamplingResult {
        // This essentially bypasses the verification of the rules, which won't happen in prod.
        // Todo(tor): figure out if we want this behaviour.
        get_sampling_match(config.rules_v2.iter(), Some(event), Some(dsc), now, false)
    }

    fn sampling_match_from_parts(
        sample_rate: f64,
        seed: Uuid,
        matched_rules: MatchedRuleIds,
    ) -> SamplingResult {
        let is_kept = sampling_match(sample_rate, seed);
        SamplingResult::Match {
            sample_rate,
            seed,
            matched_rules,
            is_kept,
        }
    }

    fn transaction_match(
        sample_rate: f64,
        event: &Event,
        matched_rule_ids: &[u32],
    ) -> SamplingResult {
        sampling_match_from_parts(
            sample_rate,
            event.id.value().unwrap().0,
            MatchedRuleIds(matched_rule_ids.iter().map(|id| RuleId(*id)).collect()),
        )
    }

    fn trace_match(
        sample_rate: f64,
        dsc: &DynamicSamplingContext,
        matched_rule_ids: &[u32],
    ) -> SamplingResult {
        sampling_match_from_parts(
            sample_rate,
            dsc.trace_id,
            MatchedRuleIds(matched_rule_ids.iter().map(|id| RuleId(*id)).collect()),
        )
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

    fn merge_root_and_non_root_configs_with(
        rules: Vec<SamplingRule>,
        root_rules: Vec<SamplingRule>,
    ) -> Vec<SamplingRule> {
        crate::evaluation::merge_rules_from_configs(
            Some(&SamplingConfig {
                rules: vec![],
                rules_v2: rules,
                mode: SamplingMode::Received,
            }),
            Some(&SamplingConfig {
                rules: vec![],
                rules_v2: root_rules,
                mode: SamplingMode::Received,
            }),
        )
        .cloned()
        .collect()
    }

    #[test]
    /// Test that the we get the same sampling decision from the same trace id
    fn test_repeatable_seed() {
        let id = "4a106cf6-b151-44eb-9131-ae7db1a157a3".parse().unwrap();

        let val1 = pseudo_random_from_uuid(id);
        let val2 = pseudo_random_from_uuid(id);
        assert!(val1 + f64::EPSILON > val2 && val2 + f64::EPSILON > val1);
    }

    #[test]
    /// Tests if the MatchedRuleIds struct is displayed correctly as string.
    fn matched_rule_ids_display() {
        let matched_rule_ids = MatchedRuleIds(vec![RuleId(123), RuleId(456)]);
        assert_eq!(matched_rule_ids.to_string(), "123,456");

        let matched_rule_ids = MatchedRuleIds(vec![RuleId(123)]);
        assert_eq!(matched_rule_ids.to_string(), "123");

        let matched_rule_ids = MatchedRuleIds(vec![]);
        assert_eq!(matched_rule_ids.to_string(), "")
    }

    #[test]
    /// Tests if the MatchRuleIds struct is created correctly from its string representation.
    fn matched_rule_ids_parse() {
        assert_eq!(
            MatchedRuleIds::parse("123,456"),
            Ok(MatchedRuleIds(vec![RuleId(123), RuleId(456)]))
        );

        assert_eq!(
            MatchedRuleIds::parse("123"),
            Ok(MatchedRuleIds(vec![RuleId(123)]))
        );

        assert!(MatchedRuleIds::parse("").is_err());

        assert!(MatchedRuleIds::parse(",").is_err());

        assert!(MatchedRuleIds::parse("123.456").is_err());

        assert!(MatchedRuleIds::parse("a,b").is_err());
    }

    macro_rules! assert_rule_ids_eq {
        ($exc:expr, $res:expr) => {
            if ($exc.len() != $res.len()) {
                panic!("The rule ids don't match.")
            }

            for (index, rule) in $res.iter().enumerate() {
                assert_eq!(rule.id.0, $exc[index])
            }
        };
    }

    #[test]
    /// Tests the merged config of the two configs with rules.
    fn test_get_merged_config_with_rules_in_both_project_config_and_root_project_config() {
        assert_rule_ids_eq!(
            [1, 7],
            merge_root_and_non_root_configs_with(
                vec![
                    mocked_sampling_rule(1, RuleType::Transaction, 0.1),
                    mocked_sampling_rule(3, RuleType::Trace, 0.3),
                    mocked_sampling_rule(4, RuleType::Unsupported, 0.1),
                ],
                vec![
                    mocked_sampling_rule(5, RuleType::Transaction, 0.4),
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
        assert_rule_ids_eq!(
            [1],
            merge_root_and_non_root_configs_with(
                vec![
                    mocked_sampling_rule(1, RuleType::Transaction, 0.1),
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
        assert_rule_ids_eq!(
            [6],
            merge_root_and_non_root_configs_with(
                vec![],
                vec![
                    mocked_sampling_rule(4, RuleType::Transaction, 0.4),
                    mocked_sampling_rule(6, RuleType::Trace, 0.6),
                    mocked_sampling_rule(7, RuleType::Unsupported, 0.1),
                ]
            )
        );
    }

    #[test]
    /// Tests that the multi-matching works for a mixture of trace and transaction rules with interleaved strategies.
    fn test_match_against_rules_with_multiple_event_types_non_decaying_rules_and_matches() {
        let config = mocked_sampling_config_with_rules(vec![
            SamplingRule {
                condition: and(vec![glob("event.transaction", &["*healthcheck*"])]),
                sampling_value: SamplingValue::SampleRate { value: 0.1 },
                ty: RuleType::Transaction,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
            SamplingRule {
                condition: and(vec![glob("trace.environment", &["*dev*"])]),
                sampling_value: SamplingValue::SampleRate { value: 1.0 },
                ty: RuleType::Trace,
                id: RuleId(2),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
            SamplingRule {
                condition: and(vec![eq("event.transaction", &["foo"], true)]),
                sampling_value: SamplingValue::Factor { value: 2.0 },
                ty: RuleType::Transaction,
                id: RuleId(3),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
            SamplingRule {
                condition: and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.user.segment", &["vip"], true),
                ]),
                sampling_value: SamplingValue::SampleRate { value: 0.5 },
                ty: RuleType::Trace,
                id: RuleId(4),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
            SamplingRule {
                condition: and(vec![
                    eq("trace.release", &["1.1.1"], true),
                    eq("trace.environment", &["prod"], true),
                ]),
                sampling_value: SamplingValue::Factor { value: 1.5 },
                ty: RuleType::Trace,
                id: RuleId(5),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
            SamplingRule {
                condition: and(vec![]),
                sampling_value: SamplingValue::SampleRate { value: 0.02 },
                ty: RuleType::Trace,
                id: RuleId(6),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
        ]);

        // early return of first rule
        let event = mocked_event(EventType::Transaction, "healthcheck", "1.1.1", "testing");
        let dsc = mocked_dsc("debug", "vip", "1.1.1");
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_eq!(result, transaction_match(0.1, &event, &[1]));

        // early return of second rule
        let event = mocked_event(EventType::Transaction, "foo", "1.1.1", "testing");
        let dsc = mocked_dsc("dev", "vip", "1.1.1");
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_eq!(result, trace_match(1.0, &dsc, &[2]));

        // factor match third rule and early return sixth rule
        let event = mocked_event(EventType::Transaction, "foo", "1.1.1", "testing");
        let dsc = mocked_dsc("testing", "non-vip", "1.1.2");
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_eq!(result, trace_match(0.04, &dsc, &[3, 6]));

        // factor match third rule and early return fourth rule
        let event = mocked_event(EventType::Transaction, "foo", "1.1.1", "testing");
        let dsc = mocked_dsc("prod", "vip", "1.1.1");
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_eq!(result, trace_match(1.0, &dsc, &[3, 4]));

        // factor match third, fifth rule and early return sixth rule
        let event = mocked_event(EventType::Transaction, "foo", "1.1.1", "testing");
        let dsc = mocked_dsc("prod", "non-vip", "1.1.1");
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_eq!(result, trace_match(0.06, &dsc, &[3, 5, 6]));

        // factor match fifth and early return sixth rule
        let event = mocked_event(EventType::Transaction, "transaction", "1.1.1", "testing");
        let dsc = mocked_dsc("prod", "non-vip", "1.1.1");
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_eq!(result, trace_match(0.03, &dsc, &[5, 6]));
    }

    #[test]
    /// test matching for various rules
    fn test_matches() {
        let conditions = [
            (
                "simple",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                    eq("trace.transaction", &["transaction1"], true),
                ]),
            ),
            (
                "glob releases",
                and(vec![
                    glob("trace.release", &["1.*"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "glob transaction",
                and(vec![glob("trace.transaction", &["trans*"])]),
            ),
            (
                "multiple releases",
                and(vec![
                    glob("trace.release", &["2.1.1", "1.1.*"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "multiple user segments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["paid", "vip", "free"], true),
                ]),
            ),
            (
                "multiple transactions",
                and(vec![glob("trace.transaction", &["t22", "trans*", "t33"])]),
            ),
            (
                "case insensitive user segments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["ViP", "FrEe"], true),
                ]),
            ),
            (
                "multiple user environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq(
                        "trace.environment",
                        &["integration", "debug", "production"],
                        true,
                    ),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "case insensitive environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["DeBuG", "PrOd"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "all environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "undefined environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            ("match no conditions", and(vec![])),
        ];

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".into()),
            user: TraceUserContext {
                user_segment: "vip".into(),
                user_id: "user-id".into(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".into()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        for (rule_test_name, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&dsc), "{failure_name}");
        }
    }

    #[test]
    /// test matching for various rules
    fn test_matches_events() {
        let conditions = [
            ("release", and(vec![glob("event.release", &["1.1.1"])])),
            (
                "transaction",
                and(vec![glob("event.transaction", &["trans*"])]),
            ),
            (
                "environment",
                or(vec![eq("event.environment", &["prod"], true)]),
            ),
        ];

        let evt = Event {
            release: Annotated::new(LenientString("1.1.1".to_owned())),
            environment: Annotated::new("prod".to_owned()),
            user: Annotated::new(User {
                ip_address: Annotated::new(IpAddr("127.0.0.1".to_owned())),
                ..Default::default()
            }),
            exceptions: Annotated::new(Values {
                values: Annotated::new(vec![Annotated::new(Exception {
                    value: Annotated::new(JsonLenientString::from(
                        "canvas.contentDocument".to_owned(),
                    )),
                    ..Default::default()
                })]),
                ..Default::default()
            }),
            request: Annotated::new(Request {
                headers: Annotated::new(Headers(
                    PairList(vec![Annotated::new((
                        Annotated::new("user-agent".into()),
                        Annotated::new("Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 7.0; InfoPath.3; .NET CLR 3.1.40767; Trident/6.0; en-IN)".into()),
                    ))]))),
                ..Default::default()
            }),
            logentry: Annotated::new(LogEntry {
                formatted: Annotated::new("abc".to_owned().into()),
                ..Default::default()
            }),
            transaction: Annotated::new("transaction1".into()),
            ..Default::default()
        };

        for (rule_test_name, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&evt), "{failure_name}");
        }
    }

    #[test]
    fn test_or_combinator() {
        let conditions = [
            (
                "both",
                true,
                or(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "first",
                true,
                or(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["all"], true),
                ]),
            ),
            (
                "second",
                true,
                or(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "none",
                false,
                or(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user.segment", &["all"], true),
                ]),
            ),
            ("empty", false, or(vec![])),
        ];

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&dsc) == *expected, "{failure_name}");
        }
    }

    #[test]
    fn test_and_combinator() {
        let conditions = [
            (
                "both",
                true,
                and(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "first",
                false,
                and(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["all"], true),
                ]),
            ),
            (
                "second",
                false,
                and(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "none",
                false,
                and(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user.segment", &["all"], true),
                ]),
            ),
            ("empty", true, and(vec![])),
        ];

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&dsc) == *expected, "{failure_name}");
        }
    }

    #[test]
    fn test_not_combinator() {
        let conditions = [
            (
                "not true",
                false,
                not(eq("trace.environment", &["debug"], true)),
            ),
            (
                "not false",
                true,
                not(eq("trace.environment", &["prod"], true)),
            ),
        ];

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&dsc) == *expected, "{failure_name}");
        }
    }

    #[test]
    /// test various rules that do not match
    fn test_does_not_match() {
        let conditions = [
            (
                "release",
                and(vec![
                    glob("trace.release", &["1.1.2"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            (
                "user segment",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user", &["all"], true),
                ]),
            ),
            (
                "environment",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            (
                "transaction",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    glob("trace.transaction", &["t22"]),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
        ];

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        for (rule_test_name, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(!condition.matches(&dsc), "{failure_name}");
        }
    }

    #[test]
    fn test_partial_trace_matches() {
        let condition = and(vec![
            eq("trace.environment", &["debug"], true),
            eq("trace.user.segment", &["vip"], true),
        ]);
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        assert!(
            condition.matches(&dsc),
            "did not match with missing release"
        );

        let condition = and(vec![
            glob("trace.release", &["1.1.1"]),
            eq("trace.environment", &["debug"], true),
        ]);
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext::default(),
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        assert!(
            condition.matches(&dsc),
            "did not match with missing user segment"
        );

        let condition = and(vec![
            glob("trace.release", &["1.1.1"]),
            eq("trace.user.segment", &["vip"], true),
        ]);
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: None,
            sampled: None,
            environment: None,
            transaction: Some("transaction1".into()),
            sample_rate: None,
            other: BTreeMap::new(),
        };

        assert!(
            condition.matches(&dsc),
            "did not match with missing environment"
        );

        let condition = and(vec![
            glob("trace.release", &["1.1.1"]),
            eq("trace.user.segment", &["vip"], true),
        ]);
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: None,
            sampled: None,
            environment: Some("debug".to_string()),
            transaction: None,
            sample_rate: None,
            other: BTreeMap::new(),
        };

        assert!(
            condition.matches(&dsc),
            "did not match with missing transaction"
        );
        let condition = and(vec![]);
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user: TraceUserContext::default(),
            replay_id: None,
            sampled: None,
            environment: None,
            transaction: None,
            sample_rate: None,
            other: BTreeMap::new(),
        };

        assert!(
            condition.matches(&dsc),
            "did not match with missing release, user segment, environment and transaction"
        );
    }

    #[test]
    /// test that the multi-matching returns none in case there is no match.
    fn test_multi_matching_with_transaction_event_non_decaying_rules_and_no_match() {
        let result = match_rules(
            true,
            Some(&mocked_sampling_config_with_rules(vec![
                SamplingRule {
                    condition: and(vec![eq("event.transaction", &["foo"], true)]),
                    sampling_value: SamplingValue::Factor { value: 2.0 },
                    ty: RuleType::Transaction,
                    id: RuleId(1),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.1"]),
                        eq("trace.environment", &["prod"], true),
                    ]),
                    sampling_value: SamplingValue::SampleRate { value: 0.5 },
                    ty: RuleType::Trace,
                    id: RuleId(2),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
            ])),
            None,
            Some(&mocked_event(
                EventType::Transaction,
                "healthcheck",
                "1.1.1",
                "testing",
            )),
            Some(&mocked_dsc("debug", "vip", "1.1.1")),
            Utc::now(),
        );
        assert!(result.no_match(), "did not return none for no match");
    }

    #[test]
    /// Test that the multi-matching works for a mixture of decaying and non-decaying rules.
    fn test_match_against_rules_with_trace_event_type_decaying_rules_and_matches() {
        let sampling_config = mocked_sampling_config_with_rules(vec![
            SamplingRule {
                condition: and(vec![
                    eq("trace.release", &["1.1.1"], true),
                    eq("trace.environment", &["dev"], true),
                ]),
                sampling_value: SamplingValue::Factor { value: 2.0 },
                ty: RuleType::Trace,
                id: RuleId(1),
                time_range: TimeRange {
                    start: Some(Utc.with_ymd_and_hms(1970, 10, 10, 0, 0, 0).unwrap()),
                    end: Some(Utc.with_ymd_and_hms(1970, 10, 12, 0, 0, 0).unwrap()),
                },
                decaying_fn: DecayingFunction::Linear { decayed_value: 1.0 },
            },
            SamplingRule {
                condition: and(vec![
                    eq("trace.release", &["1.1.1"], true),
                    eq("trace.environment", &["prod"], true),
                ]),
                sampling_value: SamplingValue::SampleRate { value: 0.6 },
                ty: RuleType::Trace,
                id: RuleId(2),
                time_range: TimeRange {
                    start: Some(Utc.with_ymd_and_hms(1970, 10, 10, 0, 0, 0).unwrap()),
                    end: Some(Utc.with_ymd_and_hms(1970, 10, 12, 0, 0, 0).unwrap()),
                },
                decaying_fn: DecayingFunction::Linear { decayed_value: 0.3 },
            },
            SamplingRule {
                condition: and(vec![]),
                sampling_value: SamplingValue::SampleRate { value: 0.02 },
                ty: RuleType::Trace,
                id: RuleId(3),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
        ]);

        let event = mocked_event(EventType::Transaction, "transaction", "1.1.1", "testing");

        // factor match first rule and early return third rule
        // We will use a time in the middle of 10th and 11th.
        let dsc = mocked_dsc("dev", "vip", "1.1.1");
        let result = match_against_rules(
            &sampling_config,
            &event,
            &dsc,
            Utc.with_ymd_and_hms(1970, 10, 11, 0, 0, 0).unwrap(),
        );
        assert_eq!(result, trace_match(0.03, &dsc, &[1, 3]));

        // early return second rule
        // We will use a time in the middle of 10th and 11th.
        let dsc = mocked_dsc("prod", "vip", "1.1.1");
        let result = match_against_rules(
            &sampling_config,
            &event,
            &dsc,
            Utc.with_ymd_and_hms(1970, 10, 11, 0, 0, 0).unwrap(),
        );
        assert!(result.matches());

        assert!(
            (result.sample_rate().unwrap() - 0.45).abs() < f64::EPSILON, // 0.45
            "did not use the sample rate of the second rule"
        );

        // early return third rule
        // We will use a time in the middle of 10th and 11th.
        let dsc = mocked_dsc("testing", "vip", "1.1.1");
        let result = match_against_rules(
            &sampling_config,
            &event,
            &dsc,
            Utc.with_ymd_and_hms(1970, 10, 11, 0, 0, 0).unwrap(),
        );
        assert_eq!(result, trace_match(0.02, &dsc, &[3]));
    }

    #[test]
    /// test that the correct match is performed when replay id is present in the dsc.
    fn test_sampling_match_with_trace_replay_id() {
        let event = mocked_event(EventType::Transaction, "healthcheck", "1.1.1", "testing");
        let mut dsc = mocked_dsc("prod", "vip", "1.1.1");
        dsc.replay_id = Some(Uuid::new_v4());

        let result = match_rules(
            true,
            None,
            Some(&mocked_sampling_config_with_rules(vec![SamplingRule {
                condition: and(vec![not(eq_null("trace.replay_id"))]),
                sampling_value: SamplingValue::SampleRate { value: 1.0 },
                ty: RuleType::Trace,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            }])),
            Some(&event),
            Some(&dsc),
            Utc::now(),
        );
        assert_eq!(result, trace_match(1.0, &dsc, &[1]));
    }

    #[test]
    /// Tests that no match is done when there are no matching rules.
    fn test_get_sampling_match_result_with_no_match() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let event = mocked_event(EventType::Transaction, "transaction", "2.0", "");

        let result = match_rules(
            true,
            Some(&sampling_config),
            None,
            Some(&event),
            None,
            Utc::now(),
        );
        assert!(result.no_match());
    }

    #[test]
    /// Tests that matching is still done on the transaction rules in case trace params are invalid.
    fn test_get_sampling_match_result_with_invalid_trace_params() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);

        let event = mocked_event(EventType::Transaction, "foo", "2.0", "");
        let result = match_rules(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&event),
            None,
            Utc::now(),
        );
        assert_eq!(result, transaction_match(0.5, &event, &[3]));

        let event = mocked_event(EventType::Transaction, "healthcheck", "2.0", "");
        let result = match_rules(
            true,
            Some(&sampling_config),
            None,
            Some(&event),
            Some(&dsc),
            Utc::now(),
        );
        assert_eq!(result, transaction_match(0.1, &event, &[1]));
    }

    #[test]
    /// Tests that a match with early return is done in the project sampling config.
    fn test_get_sampling_match_result_with_project_config_match() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "healthcheck", "2.0", "");

        let result = match_rules(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&event),
            Some(&dsc),
            Utc::now(),
        );
        assert_eq!(result, transaction_match(0.1, &event, &[1]));
    }

    #[test]
    /// Tests that a match with early return is done in the root project sampling config.
    fn test_get_sampling_match_result_with_root_project_config_match() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, Some("dev"));
        let event = mocked_event(EventType::Transaction, "my_transaction", "2.0", "");

        let result = match_rules(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&event),
            Some(&dsc),
            Utc::now(),
        );
        assert_eq!(result, trace_match(1.0, &dsc, &[6]));
    }

    #[test]
    /// Tests that the multiple matches are done across root and non-root project sampling configs.
    fn test_get_sampling_match_result_with_both_project_configs_match() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None);
        let event = mocked_event(EventType::Transaction, "bar", "2.0", "");

        let result = match_rules(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&event),
            Some(&dsc),
            Utc::now(),
        );
        assert_eq!(result, trace_match(0.75, &dsc, &[2, 5, 7]));
    }

    #[test]
    /// Tests that a match is done when no dynamic sampling context and root project state are
    /// available.
    fn test_get_sampling_match_result_with_no_dynamic_sampling_context_and_no_root_project_state() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let event = mocked_event(EventType::Transaction, "foo", "1.0", "");

        let result = match_rules(
            true,
            Some(&sampling_config),
            None,
            Some(&event),
            None,
            Utc::now(),
        );
        assert_eq!(result, transaction_match(0.5, &event, &[3]));
    }

    #[test]
    /// Tests that a match is done and the sample rate is adjusted when sampling mode is total.
    fn test_get_sampling_match_result_with_total_sampling_mode_in_project_state() {
        let sampling_config = mocked_sampling_config(SamplingMode::Total);
        let root_project_sampling_config = mocked_root_project_sampling_config(SamplingMode::Total);
        let dsc = mocked_simple_dynamic_sampling_context(Some(0.8), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "foo", "2.0", "");

        let result = match_rules(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&event),
            Some(&dsc),
            Utc::now(),
        );
        assert_eq!(result, transaction_match(0.625, &event, &[3]));
    }

    #[test]
    /// Tests that the correct match is raised in case we have unsupported rules with processing both
    /// enabled and disabled.
    fn test_get_sampling_match_result_with_unsupported_rules() {
        let mut sampling_config = mocked_sampling_config(SamplingMode::Received);
        add_sampling_rule_to_config(
            &mut sampling_config,
            SamplingRule {
                condition: RuleCondition::Unsupported,
                sampling_value: SamplingValue::SampleRate { value: 0.5 },
                ty: RuleType::Transaction,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
        );

        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "foo", "2.0", "");

        let result = match_rules(
            false,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&event),
            Some(&dsc),
            Utc::now(),
        );
        assert!(result.no_match());

        let result = match_rules(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&event),
            Some(&dsc),
            Utc::now(),
        );
        assert!(result.matches());
    }

    #[test]
    /// Tests that a no match is raised in case we have an unsupported sampling mode and a match.
    fn test_get_sampling_match_result_with_unsupported_sampling_mode_and_match() {
        let sampling_config = mocked_sampling_config(SamplingMode::Unsupported);
        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Unsupported);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "foo", "2.0", "");

        let result = match_rules(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&event),
            Some(&dsc),
            Utc::now(),
        );
        assert!(result.no_match());
    }

    #[test]
    /// Tests that only transaction rules are matched in case no root project or dsc are supplied.
    fn test_get_sampling_match_result_with_invalid_root_project_and_dsc_combination() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let event = mocked_event(EventType::Transaction, "healthcheck", "2.0", "");

        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let result = match_rules(
            true,
            Some(&sampling_config),
            None,
            Some(&event),
            Some(&dsc),
            Utc::now(),
        );
        assert_eq!(result, transaction_match(0.1, &event, &[1]));

        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let result = match_rules(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&event),
            None,
            Utc::now(),
        );
        assert_eq!(result, transaction_match(0.1, &event, &[1]));
    }

    #[test]
    /// Tests that match is returned with sample rate value interpolated with linear decaying function.
    fn test_get_sampling_match_result_with_linear_decaying_function() {
        let now = Utc::now();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0", "");

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                Some(now - DateDuration::days(1)),
                Some(now + DateDuration::days(1)),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result = match_rules(true, Some(&sampling_config), None, Some(&event), None, now);
        assert_eq!(result, transaction_match(0.75, &event, &[1]));

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                Some(now),
                Some(now + DateDuration::days(1)),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result = match_rules(true, Some(&sampling_config), None, Some(&event), None, now);
        assert_eq!(result, transaction_match(1.0, &event, &[1]));

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                Some(now - DateDuration::days(1)),
                Some(now),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result = match_rules(true, Some(&sampling_config), None, Some(&event), None, now);
        assert!(result.no_match());
    }

    #[test]
    /// Tests that no match is returned when the linear decaying function has invalid time range.
    fn test_get_sampling_match_result_with_linear_decaying_function_and_invalid_time_range() {
        let now = Utc::now();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0", "");

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                Some(now - DateDuration::days(1)),
                None,
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result = match_rules(true, Some(&sampling_config), None, Some(&event), None, now);
        assert!(result.no_match());

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                None,
                Some(now + DateDuration::days(1)),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result = match_rules(true, Some(&sampling_config), None, Some(&event), None, now);
        assert!(result.no_match());

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                None,
                None,
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result = match_rules(true, Some(&sampling_config), None, Some(&event), None, now);
        assert!(result.no_match());
    }

    #[test]
    /// Tests that match is returned when there are multiple decaying rules with factor and sample rate.
    fn test_get_sampling_match_result_with_multiple_decaying_functions_with_factor_and_sample_rate()
    {
        let now = Utc::now();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0", "");

        let sampling_config = SamplingConfig {
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
        };

        let result = match_rules(true, Some(&sampling_config), None, Some(&event), None, now);
        match result {
            SamplingResult::Match {
                sample_rate,
                seed,
                matched_rules,
                ..
            } => {
                assert!((sample_rate - 0.9).abs() < f64::EPSILON);
                assert_eq!(seed, event.id.0.unwrap().0);
                assert_eq!(matched_rules, MatchedRuleIds(vec![RuleId(1), RuleId(2)]))
            }
            SamplingResult::NoMatch => panic!("should have matched"),
        }
    }

    #[test]
    /// Tests that match is returned when there are only dsc and root sampling config.
    fn test_get_sampling_match_result_with_no_event_and_with_dsc() {
        let now = Utc::now();

        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, Some("dev"));
        let root_sampling_config = mocked_root_project_sampling_config(SamplingMode::Total);
        let result = match_rules(
            true,
            None,
            Some(&root_sampling_config),
            None,
            Some(&dsc),
            now,
        );
        assert_eq!(result, trace_match(1.0, &dsc, &[6]));
    }

    #[test]
    /// Tests that no match is returned when no event and no dsc are passed.
    fn test_get_sampling_match_result_with_no_event_and_no_dsc() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let root_sampling_config = mocked_root_project_sampling_config(SamplingMode::Total);

        let result = match_rules(
            true,
            Some(&sampling_config),
            Some(&root_sampling_config),
            None,
            None,
            Utc::now(),
        );
        assert!(result.no_match());
    }

    #[test]
    /// Tests that no match is returned when no sampling configs are passed.
    fn test_get_sampling_match_result_with_no_sampling_configs() {
        let event = mocked_event(EventType::Transaction, "transaction", "2.0", "");
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, Some("dev"));

        let result = match_rules(true, None, None, Some(&event), Some(&dsc), Utc::now());
        assert!(result.no_match());
    }
}
