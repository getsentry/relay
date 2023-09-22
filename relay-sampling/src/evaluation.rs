//! Evaluation of dynamic sampling rules.

use std::fmt;
use std::num::ParseIntError;

use chrono::{DateTime, Utc};
use rand::distributions::Uniform;
use rand::Rng;
use rand_pcg::Pcg32;
use relay_protocol::Getter;
use serde::Serialize;
use uuid::Uuid;

use crate::config::{RuleId, SamplingRule, SamplingValue};

/// State of sampling.
#[derive(Debug)]
pub enum MatchResult {
    /// No match has been found.
    Evaluator(SamplingEvaluator),
    /// Rule(s) have been matched.
    SamplingMatch(SamplingMatch),
}

impl MatchResult {
    /// Returns true if a rule has matched.
    pub fn is_match(&self) -> bool {
        matches!(self, &Self::SamplingMatch(_))
    }

    /// Returns true if no rule have matched.
    pub fn is_no_match(&self) -> bool {
        !self.is_match()
    }
}

/// State machine for dynamic sampling.
#[derive(Debug)]
pub struct SamplingEvaluator {
    now: DateTime<Utc>,
    rule_ids: Vec<RuleId>,
    factor: f64,
    adjustment_rate: Option<f64>,
}

impl SamplingEvaluator {
    /// Constructor for [`SamplingEvaluator`].
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            now,
            rule_ids: vec![],
            factor: 1.0,
            adjustment_rate: None,
        }
    }

    /// Sets a new client sample rate value.
    pub fn adjust_rate(mut self, adjustment_rate: Option<f64>) -> Self {
        self.adjustment_rate = adjustment_rate;
        self
    }

    /// Attemps to find a match for sampling rules.
    pub fn match_rules<'a, I, G>(mut self, seed: Uuid, instance: &G, rules: I) -> MatchResult
    where
        G: Getter,
        I: Iterator<Item = &'a SamplingRule>,
    {
        for rule in rules {
            if !rule.condition.matches(instance) {
                continue;
            };

            let Some(sampling_value) = rule.sample_rate(self.now) else {
                continue;
            };

            self.rule_ids.push(rule.id);

            match sampling_value {
                SamplingValue::Factor { value } => self.factor *= value,
                SamplingValue::SampleRate { value } => {
                    let base = (value * self.factor).clamp(0.0, 1.0);

                    return MatchResult::SamplingMatch(SamplingMatch::new(
                        self.adjusted_sample_rate(base),
                        seed,
                        self.rule_ids,
                    ));
                }
            }
        }
        MatchResult::Evaluator(self)
    }

    /// Compute the effective sampling rate based on the random "diceroll" and the sample rate from
    /// the matching rule.
    fn adjusted_sample_rate(&self, rule_sample_rate: f64) -> f64 {
        let Some(client_sample_rate) = self.adjustment_rate else {
            return rule_sample_rate;
        };

        if client_sample_rate <= 0.0 {
            // client_sample_rate is 0, which is bogus because the SDK should've dropped the
            // envelope. In that case let's pretend the sample rate was not sent, because clearly
            // the sampling decision across the trace is still 1. The most likely explanation is
            // that the SDK is reporting its own sample rate setting instead of the one from the
            // continued trace.
            //
            // since we write back the client_sample_rate into the event's trace context, it should
            // be possible to find those values + sdk versions via snuba
            relay_log::warn!("client sample rate is <= 0");
            rule_sample_rate
        } else {
            let adjusted_sample_rate = (rule_sample_rate / client_sample_rate).clamp(0.0, 1.0);
            if adjusted_sample_rate.is_infinite() || adjusted_sample_rate.is_nan() {
                relay_log::error!("adjusted sample rate ended up being nan/inf");
                debug_assert!(false);
                rule_sample_rate
            } else {
                adjusted_sample_rate
            }
        }
    }
}

/// Generates a pseudo random number by seeding the generator with the given id.
///
/// The return is deterministic, always generates the same number from the same id.
fn pseudo_random_from_uuid(id: Uuid) -> f64 {
    let big_seed = id.as_u128();
    let mut generator = Pcg32::new((big_seed >> 64) as u64, big_seed as u64);
    let dist = Uniform::new(0f64, 1f64);
    generator.sample(dist)
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

/// Represents the specification for sampling an incoming event.
#[derive(Clone, Debug, PartialEq)]
pub struct SamplingMatch {
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
}

impl SamplingMatch {
    fn new(sample_rate: f64, seed: Uuid, matched_rules: Vec<RuleId>) -> Self {
        let matched_rules = MatchedRuleIds(matched_rules);
        let is_kept = sampling_match(sample_rate, seed);
        Self {
            sample_rate,
            seed,
            matched_rules,
            is_kept,
        }
    }

    /// Returns the sample rate.
    pub fn sample_rate(&self) -> f64 {
        self.sample_rate
    }

    /// Returns the matched rules for the sampling match.
    ///
    /// Takes ownership, useful if you don't need the [`SamplingMatch`] anymore
    /// and you want to avoid allocations.
    pub fn take_matched_rules(self) -> MatchedRuleIds {
        self.matched_rules
    }

    /// Returns true if event should be kept.
    pub fn should_keep(&self) -> bool {
        self.is_kept
    }

    /// Returns true if event should be dropped.
    pub fn should_drop(&self) -> bool {
        !self.should_keep()
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
#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::{TimeZone, Utc};
    use relay_base_schema::events::EventType;
    use similar_asserts::assert_eq;
    use uuid::Uuid;

    use relay_event_schema::protocol::{Event, EventId, LenientString};
    use relay_protocol::Annotated;

    use crate::condition::RuleCondition;
    use crate::config::{
        DecayingFunction, RuleId, RuleType, SamplingRule, SamplingValue, TimeRange,
    };
    use crate::dsc::TraceUserContext;
    use crate::evaluation::MatchedRuleIds;
    use crate::tests::{and, eq, glob};
    use crate::DynamicSamplingContext;

    use super::*;

    fn get_sampling_match(rules: &[SamplingRule], instance: &impl Getter) -> Option<SamplingMatch> {
        let res =
            SamplingEvaluator::new(Utc::now()).match_rules(Uuid::default(), instance, rules.iter());

        let MatchResult::SamplingMatch(sampling_match) = res else {
            return None;
        };

        Some(sampling_match)
    }

    fn matches_rule_ids(rule_ids: &[u32], rules: &[SamplingRule], instance: &impl Getter) -> bool {
        let matched_rule_ids = MatchedRuleIds(rule_ids.iter().map(|num| RuleId(*num)).collect());
        let sampling_match = get_sampling_match(rules, instance).unwrap();
        matched_rule_ids == dbg!(sampling_match.matched_rules)
    }

    fn mocked_dynamic_sampling_context(vals: Vec<(&str, &str)>) -> DynamicSamplingContext {
        let mut def = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: "12345678901234567890123456789012".parse().unwrap(),
            release: None,
            environment: None,
            transaction: None,
            sample_rate: Some(1.0),
            user: TraceUserContext {
                user_segment: "".to_string(),
                user_id: "".to_string(),
            },
            replay_id: None,
            sampled: None,
            other: Default::default(),
        };

        for val in vals {
            let (key, val) = val;
            match key {
                "trace.release" => def.release = Some(val.to_owned()),
                "trace.environment" => def.environment = Some(val.to_owned()),
                "trace.user.id" => def.user.user_id = val.to_owned(),
                "trace.user.segment" => def.user.user_segment = val.to_owned(),
                "trace.transaction" => def.transaction = Some(val.to_owned()),
                "trace.replay_id" => def.replay_id = Some(Uuid::from_str(val).unwrap()),
                _ => panic!("invalid key"),
            }
        }
        def
    }

    #[test]
    fn test_decaying_rule() {
        let rule = SamplingRule {
            condition: and(vec![]),
            sampling_value: SamplingValue::SampleRate { value: 1.0 },
            ty: RuleType::Trace,
            id: RuleId(0),
            time_range: TimeRange {
                start: Some(Utc.with_ymd_and_hms(1970, 10, 10, 0, 0, 0).unwrap()),
                end: Some(Utc.with_ymd_and_hms(1970, 10, 12, 0, 0, 0).unwrap()),
            },
            decaying_fn: DecayingFunction::Linear { decayed_value: 0.5 },
        };

        let start = Utc.with_ymd_and_hms(1970, 10, 10, 0, 0, 0).unwrap();
        let halfway = Utc.with_ymd_and_hms(1970, 10, 11, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(1970, 10, 11, 23, 59, 59).unwrap();
        let out_of_range = Utc.with_ymd_and_hms(1971, 10, 11, 23, 59, 59).unwrap();

        assert_eq!(
            rule.sample_rate(start).unwrap(),
            SamplingValue::SampleRate { value: 1.0 }
        );

        assert_eq!(
            rule.sample_rate(halfway).unwrap(),
            SamplingValue::SampleRate { value: 0.75 }
        );

        assert_eq!(
            rule.sample_rate(end).unwrap(),
            SamplingValue::SampleRate {
                value: 0.5000028935185186
            }
        );

        assert!(rule.sample_rate(out_of_range).is_none());
    }

    #[test]
    fn test_client_sample_rate() {
        let rules = simple_sampling_rules(vec![
            (and(vec![]), SamplingValue::Factor { value: 0.5 }),
            (and(vec![]), SamplingValue::SampleRate { value: 0.25 }),
        ]);

        let dsc = mocked_dynamic_sampling_context(vec![]);

        let res = SamplingEvaluator::new(Utc::now())
            .adjust_rate(Some(0.2))
            .match_rules(Uuid::default(), &dsc, rules.iter());

        let MatchResult::SamplingMatch(sampling_match) = res else {
            panic!();
        };

        assert_eq!(sampling_match.sample_rate(), 0.625);
    }

    #[test]
    fn test_factors() {
        let rules = simple_sampling_rules(vec![
            (and(vec![]), SamplingValue::Factor { value: 0.5 }),
            (and(vec![]), SamplingValue::SampleRate { value: 0.25 }),
        ]);
        let dsc = mocked_dynamic_sampling_context(vec![]);

        assert_eq!(
            get_sampling_match(&rules, &dsc).unwrap().sample_rate(),
            0.125
        );
    }

    fn simple_sampling_rules(vals: Vec<(RuleCondition, SamplingValue)>) -> Vec<SamplingRule> {
        let mut vec = vec![];

        for (i, val) in vals.into_iter().enumerate() {
            let (condition, sampling_value) = val;
            vec.push(SamplingRule {
                condition,
                sampling_value,
                ty: RuleType::Trace,
                id: RuleId(i as u32),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            });
        }
        vec
    }

    #[test]
    fn test_expired_rules() {
        let rule = SamplingRule {
            condition: and(vec![]),
            sampling_value: SamplingValue::SampleRate { value: 1.0 },
            ty: RuleType::Trace,
            id: RuleId(0),
            time_range: TimeRange {
                start: Some(Utc.with_ymd_and_hms(1970, 10, 10, 0, 0, 0).unwrap()),
                end: Some(Utc.with_ymd_and_hms(1970, 10, 12, 0, 0, 0).unwrap()),
            },
            decaying_fn: Default::default(),
        };

        let dsc = mocked_dynamic_sampling_context(vec![]);

        let within_range = Utc.with_ymd_and_hms(1970, 10, 11, 0, 0, 0).unwrap();
        assert!(SamplingEvaluator::new(within_range)
            .match_rules(Uuid::default(), &dsc, [rule.clone()].iter())
            .is_match());

        let outside_range = Utc.with_ymd_and_hms(1971, 1, 1, 0, 0, 0).unwrap();
        assert!(SamplingEvaluator::new(outside_range)
            .match_rules(Uuid::default(), &dsc, [rule].iter())
            .is_no_match());
    }

    #[test]
    fn condition_matching() {
        let rules = simple_sampling_rules(vec![
            (
                and(vec![glob("trace.transaction", &["*healthcheck*"])]),
                SamplingValue::SampleRate { value: 0.1 },
            ),
            (
                and(vec![glob("trace.environment", &["*dev*"])]),
                SamplingValue::SampleRate { value: 1.0 },
            ),
            (
                and(vec![eq("trace.transaction", &["raboof"], true)]),
                SamplingValue::Factor { value: 2.0 },
            ),
            (
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.user.segment", &["vip"], true),
                ]),
                SamplingValue::SampleRate { value: 0.5 },
            ),
            (
                and(vec![
                    eq("trace.release", &["1.1.1"], true),
                    eq("trace.environment", &["prod"], true),
                ]),
                SamplingValue::Factor { value: 1.5 },
            ),
            (and(vec![]), SamplingValue::SampleRate { value: 0.02 }),
        ]);

        // early return of first rule
        let dsc = mocked_dynamic_sampling_context(vec![("trace.transaction", "foohealthcheckbar")]);
        assert!(matches_rule_ids(&[0], &rules, &dsc));

        // early return of second rule
        let dsc = mocked_dynamic_sampling_context(vec![("trace.environment", "dev")]);
        assert!(matches_rule_ids(&[1], &rules, &dsc));

        // factor match third rule and early return sixth rule
        let dsc = mocked_dynamic_sampling_context(vec![("trace.transaction", "raboof")]);
        assert!(matches_rule_ids(&[2, 5], &rules, &dsc));

        // factor match third rule and early return fourth rule
        let dsc = mocked_dynamic_sampling_context(vec![
            ("trace.transaction", "raboof"),
            ("trace.release", "1.1.1"),
            ("trace.user.segment", "vip"),
        ]);
        assert!(matches_rule_ids(&[2, 3], &rules, &dsc));

        // factor match third, fifth rule and early return sixth rule
        let dsc = mocked_dynamic_sampling_context(vec![
            ("trace.transaction", "raboof"),
            ("trace.release", "1.1.1"),
            ("trace.environment", "prod"),
        ]);
        assert!(matches_rule_ids(&[2, 4, 5], &rules, &dsc));

        // factor match fifth and early return sixth rule
        let dsc = mocked_dynamic_sampling_context(vec![
            ("trace.release", "1.1.1"),
            ("trace.environment", "prod"),
        ]);
        assert!(matches_rule_ids(&[4, 5], &rules, &dsc));
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

    #[test]
    /// Tests that no match is done when there are no matching rules.
    fn test_get_sampling_match_result_with_no_match() {
        let event = mocked_event(EventType::Transaction, "transaction", "2.0", "");

        let res =
            SamplingEvaluator::new(Utc::now()).match_rules(Uuid::default(), &event, [].iter());

        assert!(res.is_no_match());
    }
}
