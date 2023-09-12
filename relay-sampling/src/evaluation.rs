//! Evaluation of dynamic sampling rules.

use std::collections::BTreeMap;
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

use crate::config::{
    DecayingFunction, RuleId, RuleType, SamplingConfig, SamplingMode, SamplingRule, SamplingValue,
    TimeRange,
};
use crate::dsc::DynamicSamplingContext;

/// Generates a pseudo random number by seeding the generator with the given id.
///
/// The return is deterministic, always generates the same number from the same id.
pub fn pseudo_random_from_uuid(id: Uuid) -> f64 {
    let big_seed = id.as_u128();
    let mut generator = Pcg32::new((big_seed >> 64) as u64, big_seed as u64);
    let dist = Uniform::new(0f64, 1f64);
    generator.sample(dist)
}

/// Returns an iterator of references that chains together and merges rules.
///
/// The chaining logic will take all the non-trace rules from the project and all the trace/unsupported
/// rules from the root project and concatenate them.
pub fn merge_rules_from_configs<'a>(
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

/// Gets the sampling match result by creating the merged configuration and matching it against
/// the sampling configuration.
pub fn merge_configs_and_match(
    processing_enabled: bool,
    sampling_config: Option<&SamplingConfig>,
    root_sampling_config: Option<&SamplingConfig>,
    dsc: Option<&DynamicSamplingContext>,
    event: Option<&Event>,
    now: DateTime<Utc>,
    bias_counters: &BTreeMap<RuleId, usize>,
) -> Option<SamplingMatch> {
    // We check if there are unsupported rules in any of the two configurations.
    check_unsupported_rules(processing_enabled, sampling_config, root_sampling_config).ok()?;

    // We perform the rule matching with the multi-matching logic on the merged rules.
    let rules = merge_rules_from_configs(sampling_config, root_sampling_config);
    let mut match_result =
        SamplingMatch::match_against_rules(rules, event, dsc, now, bias_counters)?;

    let sample_rate = match match_result {
        SamplingMatch::Bias { .. } => return Some(match_result),
        SamplingMatch::Other { sample_rate, .. } => sample_rate,
    };

    // If we have a match, we will try to derive the sample rate based on the sampling mode.
    //
    // Keep in mind that the sample rate received here has already been derived by the matching
    // logic, based on multiple matches and decaying functions.
    //
    // The determination of the sampling mode occurs with the following priority:
    // 1. Non-root project sampling mode
    // 2. Root project sampling mode
    let Some(primary_config) = sampling_config.or(root_sampling_config) else {
        relay_log::error!("cannot sample without at least one sampling config");
        return None;
    };

    let adjusted_sample_rate = match primary_config.mode {
        SamplingMode::Received => sample_rate,
        SamplingMode::Total => match dsc {
            Some(dsc) => dsc.adjusted_sample_rate(sample_rate),
            None => sample_rate,
        },
        SamplingMode::Unsupported => {
            if processing_enabled {
                relay_log::error!("found unsupported sampling mode even as processing Relay");
            }

            return None;
        }
    };

    match_result.set_sample_rate(adjusted_sample_rate);

    // Only if we arrive at this stage, it means that we have found a match and we want to prepare
    // the data for making the sampling decision.
    Some(match_result)
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ReservoirMessage {
    Update(RuleId),
}

/// Represents the specification for sampling an incoming event.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum SamplingMatch {
    /// The rule is an inspection bias.
    Bias { rule_id: RuleId },
    /// Normal sampling.
    Other {
        /// The sample rate to use for the incoming event.
        sample_rate: f64,

        /// The seed to feed to the random number generator which allows the same number to be
        /// generated given the same seed.
        ///
        /// This is especially important for trace sampling, even though we can have inconsistent
        /// traces due to multi-matching.
        seed: Uuid,

        /// The list of rule ids that have matched the incoming event and/or dynamic sampling context.
        matched_rule_ids: MatchedRuleIds,
    },
}

impl SamplingMatch {
    /// Setter for `sample_rate`.
    pub fn set_sample_rate(&mut self, new_sample_rate: f64) {
        if let Self::Other {
            ref mut sample_rate,
            ..
        } = self
        {
            *sample_rate = new_sample_rate;
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
    ///
    /// In case no sample rate rule is matched, we are going to return a None, signaling that no
    /// match has been found.
    pub fn match_against_rules<'a, I>(
        rules: I,
        event: Option<&Event>,
        dsc: Option<&DynamicSamplingContext>,
        now: DateTime<Utc>,
        reservoir_stuff: &BTreeMap<RuleId, usize>,
    ) -> Option<SamplingMatch>
    where
        I: Iterator<Item = &'a SamplingRule>,
    {
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
                if let Some(evaluator) = SamplingValueEvaluator::create(
                    rule,
                    now,
                    reservoir_stuff.get(&rule.id).copied(),
                ) {
                    matched_rule_ids.push(rule.id);

                    if rule.ty == RuleType::Trace {
                        if let Some(dsc) = dsc {
                            seed = Some(dsc.trace_id);
                        }
                    }

                    if let SamplingValueEvaluator::InspectBias { target_is_reached } = evaluator {
                        // When target is reached, we disable this rule, and sample accoridng to
                        // other rules. This branch should only be reached in the time interval
                        // between the target getting reached, and sentry removing the rule
                        // through [`SamplingConfig`].
                        if target_is_reached {
                            continue;
                        } else {
                            return Some(SamplingMatch::Bias { rule_id: rule.id });
                        }
                    }

                    let value = evaluator.evaluate(now);
                    match rule.sampling_value {
                        SamplingValue::Factor { .. } => accumulated_factors *= value,
                        SamplingValue::SampleRate { .. } => {
                            return Some(SamplingMatch::Other {
                                sample_rate: (value * accumulated_factors).clamp(0.0, 1.0),
                                seed: seed?,
                                matched_rule_ids: MatchedRuleIds(matched_rule_ids),
                            });
                        }
                    }
                }
            }
        }

        // In case no match is available, we won't return any specification.
        None
    }
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

/// A struct representing the evaluation context of a sample rate.
#[derive(Debug, Clone, Copy)]
enum SamplingValueEvaluator {
    Linear {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        initial_value: f64,
        decayed_value: f64,
    },
    Constant {
        initial_value: f64,
    },
    InspectBias {
        target_is_reached: bool,
    },
}

impl SamplingValueEvaluator {
    /// Returns a [`SamplingValueEvaluator`] if the rule is active at the given time.
    fn create(rule: &SamplingRule, now: DateTime<Utc>, counter: Option<usize>) -> Option<Self> {
        let sampling_base_value = rule.sampling_value.value();

        match rule.decaying_fn {
            DecayingFunction::Linear { decayed_value } => {
                if let TimeRange {
                    start: Some(start),
                    end: Some(end),
                } = rule.time_range
                {
                    // As in the TimeRange::contains method we use a right non-inclusive time bound.
                    if sampling_base_value > decayed_value && start <= now && now < end {
                        return Some(Self::Linear {
                            start,
                            end,
                            initial_value: sampling_base_value,
                            decayed_value,
                        });
                    }
                }
            }
            DecayingFunction::InspectionBias { reservoir_limit } => {
                let counter = counter.unwrap_or_default();

                if counter >= reservoir_limit {
                    // the counter has reached the target.
                    return Some(Self::InspectBias {
                        target_is_reached: true,
                    });
                } else {
                    // still below target :)
                    return Some(Self::InspectBias {
                        target_is_reached: false,
                    });
                }
            }
            DecayingFunction::Constant => {
                if rule.time_range.contains(now) {
                    return Some(Self::Constant {
                        initial_value: rule.sampling_value.value(),
                    });
                }
            }
        }

        None
    }

    /// Evaluates the value of the sampling strategy given a the current time.
    fn evaluate(&self, now: DateTime<Utc>) -> f64 {
        match self {
            Self::Linear {
                start,
                end,
                initial_value,
                decayed_value,
            } => {
                let now_timestamp = now.timestamp() as f64;
                let start_timestamp = start.timestamp() as f64;
                let end_timestamp = end.timestamp() as f64;
                let progress_ratio = ((now_timestamp - start_timestamp)
                    / (end_timestamp - start_timestamp))
                    .clamp(0.0, 1.0);

                // This interval will always be < 0.
                let interval = decayed_value - initial_value;
                initial_value + (interval * progress_ratio)
            }
            Self::Constant { initial_value } => *initial_value,
            Self::InspectBias { target_is_reached } => {
                if *target_is_reached {
                    0.0
                } else {
                    1.0
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::condition::RuleCondition;

    use super::*;

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
}
