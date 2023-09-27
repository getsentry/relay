//! Evaluation of dynamic sampling rules.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::ParseIntError;
use std::sync::{Arc, Mutex, MutexGuard};

use chrono::{DateTime, Utc};
use rand::distributions::Uniform;
use rand::Rng;
use rand_pcg::Pcg32;
use relay_base_schema::project::ProjectKey;
use relay_protocol::Getter;
use relay_redis::{RedisError, RedisPool};
use serde::Serialize;
use uuid::Uuid;

use crate::config::{RuleId, SamplingRule, SamplingValue};
use crate::SamplingConfig;

/// Generates a pseudo random number by seeding the generator with the given id.
///
/// The return is deterministic, always generates the same number from the same id.
fn pseudo_random_from_uuid(id: Uuid) -> f64 {
    let big_seed = id.as_u128();
    let mut generator = Pcg32::new((big_seed >> 64) as u64, big_seed as u64);
    let dist = Uniform::new(0f64, 1f64);
    generator.sample(dist)
}

/// State of the [`SamplingEvaluator`]'s rule matching.
///
/// Enforces a pattern to avoid matching on more rules if a match has already been found.
#[derive(Debug)]
pub enum Evaluation {
    /// Matching has not completed and more rules can be evaluated.
    ///
    /// This can happen either if no active rules match the provided data, or if the matched rules
    /// are factors that require a final sampling value. In this case, the returned evaluator stores
    /// the state of the matched rules as well as the accumulated sampling factor and can be used to
    /// evaluate more rules.
    ///
    /// If evaluation returns `Continue` and there are no more rules to match, this should be
    /// considered as "no match".
    Continue(SamplingEvaluator),
    /// One or more rules have matched.
    Matched(SamplingMatch),
}

impl Evaluation {
    /// Returns true if a rule has matched.
    pub fn is_match(&self) -> bool {
        matches!(self, &Self::Matched(_))
    }

    /// Returns true if no rule have matched.
    pub fn is_no_match(&self) -> bool {
        !self.is_match()
    }
}

pub struct BiasRedisKey(String);

impl BiasRedisKey {
    pub fn new(project_key: u64, rule_id: RuleId) -> Self {
        Self(format!("bias:{}:{}", project_key, rule_id))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Increments the count, it will be initialized automatically if it doesn't exist.
///
/// INCR docs: [`https://redis.io/commands/incr/`]
#[cfg(feature = "redis")]
fn increment_bias_rule_count(
    redis: &RedisPool,
    project_key: u64,
    rule_id: RuleId,
) -> Result<i64, RedisError> {
    let key = BiasRedisKey::new(project_key, rule_id);
    let mut command = relay_redis::redis::cmd("INCR");
    command.arg(key.as_str());
    let new_count: i64 = command.query(&mut redis.client()?.connection()?).unwrap();
    Ok(new_count)
}

#[derive(Debug)]
pub struct ReservoirStuff {
    org_id: u64,
    map: Mutex<BTreeMap<RuleId, i64>>,
}

impl ReservoirStuff {
    pub fn new(org_id: u64) -> Self {
        Self {
            org_id,
            map: Mutex::default(),
        }
    }

    #[cfg(not(feature = "redis"))]
    pub fn evaluate_rule(&self, redis_pool: Option<()>, rule: RuleId, limit: i64) -> bool {
        let Ok(mut map_guard) = self.map.try_lock() else {
            return false;
        };

        self.increment(&mut map_guard, rule, redis_pool);

        if Self::limit_exceeded(&mut map_guard, rule, limit).unwrap_or(true) {
            return false;
        }

        true
    }

    #[cfg(feature = "redis")]
    pub fn evaluate_rule(&self, redis_pool: Option<&RedisPool>, rule: RuleId, limit: i64) -> bool {
        let Ok(mut map_guard) = self.map.try_lock() else {
            return false;
        };

        self.increment(&mut map_guard, rule, redis_pool);

        if Self::limit_exceeded(&mut map_guard, rule, limit).unwrap_or(true) {
            return false;
        }

        true
    }

    fn limit_exceeded(
        guard: &mut MutexGuard<BTreeMap<RuleId, i64>>,
        rule: RuleId,
        limit: i64,
    ) -> Option<bool> {
        guard.get(&rule).map(|val| *val > limit)
    }

    // if limit isn't reached yet, we wanna increment.
    // when incrementing, if processing is enabled, we increment redis and insert back the value
    // otherwise we just increment directly
    #[cfg(feature = "redis")]
    fn increment(
        &self,
        map_guard: &mut MutexGuard<BTreeMap<RuleId, i64>>,
        rule: RuleId,
        redis_pool: Option<&RedisPool>,
    ) {
        match redis_pool {
            Some(redis_pool) => {
                let new_value = increment_bias_rule_count(redis_pool, self.org_id, rule)
                    .ok()
                    .unwrap();

                if let Some(val) = map_guard.get_mut(&rule) {
                    *val = new_value;
                }
            }
            None => {
                if let Some(val) = map_guard.get_mut(&rule) {
                    *val += 1;
                }
            }
        };
    }

    #[cfg(not(feature = "redis"))]
    fn increment(
        &self,
        map_guard: &mut MutexGuard<BTreeMap<RuleId, i64>>,
        rule: RuleId,
        _: Option<()>,
    ) {
        if let Some(val) = map_guard.get_mut(&rule) {
            *val += 1;
        }
    }

    // if a rule is no longer in the sampling config, we can assume it's deleted and no longer needed.
    pub fn delete_expired_rules(&self, config: &SamplingConfig) {
        let reservoir_rules: BTreeSet<RuleId> = config
            .rules_v2
            .iter()
            .filter_map(|rule| rule.is_reservoir().then_some(rule.id))
            .collect();

        self.map
            .try_lock()
            .unwrap()
            .retain(|key, _| reservoir_rules.contains(key));
    }
}

/// State machine for dynamic sampling.
#[derive(Debug)]
pub struct SamplingEvaluator {
    now: DateTime<Utc>,
    rule_ids: Vec<RuleId>,
    factor: f64,
    client_sample_rate: Option<f64>,
    reservoir: Arc<ReservoirStuff>,
    #[cfg(feature = "redis")]
    redis_pool: Option<Arc<RedisPool>>,
}

impl SamplingEvaluator {
    /// Constructor for [`SamplingEvaluator`].
    pub fn new(now: DateTime<Utc>, reservoir: Arc<ReservoirStuff>) -> Self {
        Self {
            now,
            rule_ids: vec![],
            factor: 1.0,
            client_sample_rate: None,
            reservoir,
            #[cfg(feature = "redis")]
            redis_pool: None,
        }
    }

    #[cfg(feature = "redis")]
    pub fn set_redis_pool(mut self, redis: Option<Arc<RedisPool>>) -> Self {
        self.redis_pool = redis;
        self
    }

    /// Sets a new client sample rate value.
    pub fn adjust_client_sample_rate(mut self, client_sample_rate: Option<f64>) -> Self {
        self.client_sample_rate = client_sample_rate;
        self
    }

    /// Attemps to find a match for sampling rules.
    pub fn match_rules<'b, I, G>(mut self, seed: Uuid, instance: &'b G, rules: I) -> Evaluation
    where
        G: Getter,
        I: Iterator<Item = &'b SamplingRule>,
    {
        for rule in rules {
            if !rule.condition.matches(instance) {
                continue;
            };

            let Some(sampling_value) = rule.sample_rate(self.now, self.reservoir.clone()) else {
                continue;
            };

            self.rule_ids.push(rule.id);

            match sampling_value {
                SamplingValue::Factor { value } => self.factor *= value,
                SamplingValue::SampleRate { value } => {
                    let sample_rate = (value * self.factor).clamp(0.0, 1.0);

                    return Evaluation::Matched(SamplingMatch::new(
                        self.adjusted_sample_rate(sample_rate),
                        seed,
                        self.rule_ids,
                    ));
                }
                SamplingValue::Reservoir { .. } => {
                    return Evaluation::Matched(SamplingMatch::new(1.0, seed, vec![rule.id]));
                }
            }
        }
        Evaluation::Continue(self)
    }

    /// Tries to negate the client side sampling if the evaluator has been provided
    /// with a client sample rate.
    fn adjusted_sample_rate(&self, rule_sample_rate: f64) -> f64 {
        let Some(client_sample_rate) = self.client_sample_rate else {
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

fn sampling_match(sample_rate: f64, seed: Uuid) -> bool {
    if sample_rate == 0.0 {
        return false;
    } else if sample_rate == 1.0 {
        return true;
    }

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
    should_keep: bool,
}

impl SamplingMatch {
    fn new(sample_rate: f64, seed: Uuid, matched_rules: Vec<RuleId>) -> Self {
        let matched_rules = MatchedRuleIds(matched_rules);

        let should_keep = sampling_match(sample_rate, seed);

        Self {
            sample_rate,
            seed,
            matched_rules,
            should_keep,
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
    pub fn into_matched_rules(self) -> MatchedRuleIds {
        self.matched_rules
    }

    /// Returns true if event should be kept.
    pub fn should_keep(&self) -> bool {
        self.should_keep
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
/*
#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::{TimeZone, Utc};
    use similar_asserts::assert_eq;
    use uuid::Uuid;

    use crate::condition::RuleCondition;
    use crate::config::{RuleId, RuleType, SamplingRule, SamplingValue, TimeRange};
    use crate::dsc::TraceUserContext;
    use crate::evaluation::MatchedRuleIds;
    use crate::tests::{and, eq, glob};
    use crate::DynamicSamplingContext;

    use super::*;

    fn dummy_reservoir() -> Arc<ReservoirStuff> {
        let project_key = "12345678123456781234567812345678"
            .parse::<ProjectKey>()
            .unwrap();
        ReservoirStuff::new(project_key).into()
    }

    /// Helper to extract the sampling match after evaluating rules.
    fn get_sampling_match(rules: &[SamplingRule], instance: &impl Getter) -> SamplingMatch {
        match SamplingEvaluator::new(Utc::now(), dummy_reservoir()).match_rules(
            Uuid::default(),
            instance,
            rules.iter(),
        ) {
            Evaluation::Matched(sampling_match) => sampling_match,
            Evaluation::Continue(_) => panic!("no match found"),
        }
    }

    /// Helper to check if certain rules are matched on.
    fn matches_rule_ids(rule_ids: &[u32], rules: &[SamplingRule], instance: &impl Getter) -> bool {
        let matched_rule_ids = MatchedRuleIds(rule_ids.iter().map(|num| RuleId(*num)).collect());
        let sampling_match = get_sampling_match(rules, instance);
        matched_rule_ids == sampling_match.into_matched_rules()
    }

    /// Helper function to create a dsc with the provided getter-values set.
    fn mocked_dsc_with_getter_values(
        paths_and_values: Vec<(&str, &str)>,
    ) -> DynamicSamplingContext {
        let mut dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: "12345678123456781234567812345678".parse().unwrap(),
            release: None,
            environment: None,
            transaction: None,
            sample_rate: None,
            user: TraceUserContext::default(),
            replay_id: None,
            sampled: None,
            other: Default::default(),
        };

        for (path, value) in paths_and_values {
            match path {
                "trace.release" => dsc.release = Some(value.to_owned()),
                "trace.environment" => dsc.environment = Some(value.to_owned()),
                "trace.user.id" => dsc.user.user_id = value.to_owned(),
                "trace.user.segment" => dsc.user.user_segment = value.to_owned(),
                "trace.transaction" => dsc.transaction = Some(value.to_owned()),
                "trace.replay_id" => dsc.replay_id = Some(Uuid::from_str(value).unwrap()),
                _ => panic!("invalid path"),
            }
        }

        dsc
    }

    #[test]
    fn test_adjust_sample_rate() {
        // return the same as input if no client sample rate set in the sampling evaluator.
        let eval = SamplingEvaluator::new(Utc::now(), dummy_reservoir());
        assert_eq!(eval.adjusted_sample_rate(0.2), 0.2);

        let eval = eval.adjust_client_sample_rate(Some(0.5));
        assert_eq!(eval.adjusted_sample_rate(0.2), 0.4);

        // tests that it doesn't exceed 1.0.
        let eval = eval.adjust_client_sample_rate(Some(0.005));
        assert_eq!(eval.adjusted_sample_rate(0.2), 1.0);

        // tests that it doesn't go below 0.0.
        let eval = eval.adjust_client_sample_rate(Some(0.005));
        assert_eq!(eval.adjusted_sample_rate(-0.2), 0.0);
    }

    /// Checks that server side sampling can correctly negate any client side sampling if configured to.
    #[test]
    fn test_adjust_by_client_sample_rate() {
        let rules = simple_sampling_rules(vec![
            (RuleCondition::all(), SamplingValue::Factor { value: 0.5 }),
            (
                RuleCondition::all(),
                SamplingValue::SampleRate { value: 0.25 },
            ),
        ]);

        let dsc = mocked_dsc_with_getter_values(vec![]);

        let res = SamplingEvaluator::new(Utc::now(), dummy_reservoir())
            .adjust_client_sample_rate(Some(0.2))
            .match_rules(Uuid::default(), &dsc, rules.iter());

        let Evaluation::Matched(sampling_match) = res else {
            panic!();
        };

        // ((0.5 * 0.25) / 0.2) == 0.625
        assert_eq!(sampling_match.sample_rate(), 0.625);
    }

    #[test]
    fn test_sample_rate_compounding() {
        let rules = simple_sampling_rules(vec![
            (RuleCondition::all(), SamplingValue::Factor { value: 0.8 }),
            (RuleCondition::all(), SamplingValue::Factor { value: 0.5 }),
            (
                RuleCondition::all(),
                SamplingValue::SampleRate { value: 0.25 },
            ),
        ]);
        let dsc = mocked_dsc_with_getter_values(vec![]);

        // 0.8 * 0.5 * 0.25 == 0.1
        assert_eq!(get_sampling_match(&rules, &dsc).sample_rate(), 0.1);
    }

    /// Helper function to quickly construct many rules with their condition and value, and a unique id,
    /// so the caller can easily check which rules are matching.
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

    /// Checks that rules don't match if the time is outside the time range.
    #[test]
    fn test_expired_rules() {
        let rule = SamplingRule {
            condition: RuleCondition::all(),
            sampling_value: SamplingValue::SampleRate { value: 1.0 },
            ty: RuleType::Trace,
            id: RuleId(0),
            time_range: TimeRange {
                start: Some(Utc.with_ymd_and_hms(1970, 10, 10, 0, 0, 0).unwrap()),
                end: Some(Utc.with_ymd_and_hms(1970, 10, 12, 0, 0, 0).unwrap()),
            },
            decaying_fn: Default::default(),
        };

        let dsc = mocked_dsc_with_getter_values(vec![]);

        // Baseline test.
        let within_timerange = Utc.with_ymd_and_hms(1970, 10, 11, 0, 0, 0).unwrap();
        assert!(SamplingEvaluator::new(within_timerange, dummy_reservoir())
            .match_rules(Uuid::default(), &dsc, [rule.clone()].iter())
            .is_match());

        let before_timerange = Utc.with_ymd_and_hms(1969, 1, 1, 0, 0, 0).unwrap();
        assert!(SamplingEvaluator::new(before_timerange, dummy_reservoir())
            .match_rules(Uuid::default(), &dsc, [rule.clone()].iter())
            .is_no_match());

        let after_timerange = Utc.with_ymd_and_hms(1971, 1, 1, 0, 0, 0).unwrap();
        assert!(SamplingEvaluator::new(after_timerange, dummy_reservoir())
            .match_rules(Uuid::default(), &dsc, [rule].iter())
            .is_no_match());
    }

    /// Checks that `SamplingValueEvaluator` correctly matches the right rules.
    #[test]
    fn test_condition_matching() {
        let rules = simple_sampling_rules(vec![
            (
                and(vec![glob("trace.transaction", &["*healthcheck*"])]),
                SamplingValue::SampleRate { value: 1.0 },
            ),
            (
                and(vec![glob("trace.environment", &["*dev*"])]),
                SamplingValue::SampleRate { value: 1.0 },
            ),
            (
                and(vec![eq("trace.transaction", &["raboof"], true)]),
                SamplingValue::Factor { value: 1.0 },
            ),
            (
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.user.segment", &["vip"], true),
                ]),
                SamplingValue::SampleRate { value: 1.0 },
            ),
            (
                and(vec![
                    eq("trace.release", &["1.1.1"], true),
                    eq("trace.environment", &["prod"], true),
                ]),
                SamplingValue::Factor { value: 1.0 },
            ),
            (
                RuleCondition::all(),
                SamplingValue::SampleRate { value: 1.0 },
            ),
        ]);

        // early return of first rule
        let dsc = mocked_dsc_with_getter_values(vec![("trace.transaction", "foohealthcheckbar")]);
        assert!(matches_rule_ids(&[0], &rules, &dsc));

        // early return of second rule
        let dsc = mocked_dsc_with_getter_values(vec![("trace.environment", "dev")]);
        assert!(matches_rule_ids(&[1], &rules, &dsc));

        // factor match third rule and early return sixth rule
        let dsc = mocked_dsc_with_getter_values(vec![("trace.transaction", "raboof")]);
        assert!(matches_rule_ids(&[2, 5], &rules, &dsc));

        // factor match third rule and early return fourth rule
        let dsc = mocked_dsc_with_getter_values(vec![
            ("trace.transaction", "raboof"),
            ("trace.release", "1.1.1"),
            ("trace.user.segment", "vip"),
        ]);
        assert!(matches_rule_ids(&[2, 3], &rules, &dsc));

        // factor match third, fifth rule and early return sixth rule
        let dsc = mocked_dsc_with_getter_values(vec![
            ("trace.transaction", "raboof"),
            ("trace.release", "1.1.1"),
            ("trace.environment", "prod"),
        ]);
        assert!(matches_rule_ids(&[2, 4, 5], &rules, &dsc));

        // factor match fifth and early return sixth rule
        let dsc = mocked_dsc_with_getter_values(vec![
            ("trace.release", "1.1.1"),
            ("trace.environment", "prod"),
        ]);
        assert!(matches_rule_ids(&[4, 5], &rules, &dsc));
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
        let dsc = mocked_dsc_with_getter_values(vec![]);

        let res = SamplingEvaluator::new(Utc::now(), dummy_reservoir()).match_rules(
            Uuid::default(),
            &dsc,
            [].iter(),
        );

        assert!(res.is_no_match());
    }
}
*/
