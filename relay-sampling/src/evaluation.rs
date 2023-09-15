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

fn get_and_verify_rules<'a>(
    processing_enabled: bool,
    sampling_config: Option<&'a SamplingConfig>,
    root_sampling_config: Option<&'a SamplingConfig>,
) -> Option<impl Iterator<Item = &'a SamplingRule>> {
    (sampling_config.is_some() || root_sampling_config.is_some()).then_some(())?;

    check_unsupported_rules(processing_enabled, sampling_config, root_sampling_config).ok()?;

    Some(merge_rules_from_configs(
        sampling_config,
        root_sampling_config,
    ))
}

fn get_adjusted_sample_rate(
    base_sample_rate: f64,
    client_sample_rate: Option<f64>,
    sampling_mode: SamplingMode,
) -> Option<f64> {
    match sampling_mode {
        SamplingMode::Total => match client_sample_rate {
            Some(client_sample_rate) => Some(DynamicSamplingContext::adjusted_sample_rate(
                client_sample_rate,
                base_sample_rate,
            )),
            None => Some(base_sample_rate),
        },
        SamplingMode::Received => Some(base_sample_rate),
        SamplingMode::Unsupported => {
            #[cfg(feature = "processing")]
            relay_log::error!("found unsupported sampling mode even as processing Relay");

            None
        }
    }
}

pub(crate) fn sampling_match(sample_rate: f64, seed: Uuid) -> bool {
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

/// Get the sampling result.
pub fn match_rules<'a>(
    processing_enabled: bool,
    sampling_config: Option<&'a SamplingConfig>,
    root_sampling_config: Option<&'a SamplingConfig>,
    event: Option<&Event>,
    dsc: Option<&DynamicSamplingContext>,
    now: DateTime<Utc>,
) -> SamplingMatch {
    // We perform the rule matching with the multi-matching logic on the merged rules.
    let Some(rules) =
        get_and_verify_rules(processing_enabled, sampling_config, root_sampling_config)
    else {
        return SamplingMatch::NoMatch;
    };

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
            return SamplingMatch::NoMatch;
        }
    };

    get_sampling_match(rules, event, dsc, now, sampling_mode)
}

/// Represents the specification for sampling an incoming event.
#[derive(Default, Clone, Debug, PartialEq)]
pub enum SamplingMatch {
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

impl SamplingMatch {
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
            SamplingMatch::Match { is_kept, .. } => *is_kept,
            // If no rules matched on an event, we want to keep it.
            SamplingMatch::NoMatch => true,
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
///
/// In case no sample rate rule is matched, we are going to return a None, signaling that no
/// match has been found.
pub(crate) fn get_sampling_match<'a>(
    rules: impl Iterator<Item = &'a SamplingRule>,
    event: Option<&Event>,
    dsc: Option<&DynamicSamplingContext>,
    now: DateTime<Utc>,
    sampling_mode: SamplingMode,
) -> SamplingMatch {
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
                            match get_adjusted_sample_rate(
                                base,
                                dsc.and_then(|dsc| dsc.sample_rate),
                                sampling_mode,
                            ) {
                                Some(adjusted_sample_rate) => adjusted_sample_rate,
                                None => return SamplingMatch::NoMatch,
                            }
                        };
                        let Some(seed) = seed else {
                            return SamplingMatch::NoMatch;
                        };

                        let is_kept = sampling_match(sample_rate, seed);

                        return SamplingMatch::Match {
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
    SamplingMatch::NoMatch
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
