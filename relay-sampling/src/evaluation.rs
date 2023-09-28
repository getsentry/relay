//! Evaluation of dynamic sampling rules.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::num::ParseIntError;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use rand::distributions::Uniform;
use rand::Rng;
use rand_pcg::Pcg32;
use relay_protocol::Getter;
#[cfg(feature = "redis")]
use relay_redis::RedisPool;
use serde::Serialize;
use uuid::Uuid;

use crate::config::{RuleId, SamplingRule, SamplingValue};
#[cfg(feature = "redis")]
use crate::redis_sampling::{self, ReservoirRuleKey};

/// Generates a pseudo random number by seeding the generator with the given id.
///
/// The return is deterministic, always generates the same number from the same id.
fn pseudo_random_from_uuid(id: Uuid) -> f64 {
    let big_seed = id.as_u128();
    let mut generator = Pcg32::new((big_seed >> 64) as u64, big_seed as u64);
    let dist = Uniform::new(0f64, 1f64);
    generator.sample(dist)
}

/// The amount of matches for each reservoir rule in a given project.
pub type ReservoirCounters = Arc<Mutex<BTreeMap<RuleId, i64>>>;

/// Utility for evaluating reservoir-based sampling rules.
///
/// A "reservoir limit" rule samples every match until its limit is reached, after which
/// the rule is disabled.
///
/// This utility uses a dual-counter system for enforcing this limit:
///
/// - Local Counter: Each relay instance maintains a local counter to track sampled events.
///
/// - Redis Counter: For processing relays, a Redis-based counter provides synchronization
///   across multiple relay-instances. When incremented, the Redis counter returns the current global
///   count for the given rule, which is then used to update the local counter.
#[derive(Debug)]
pub struct ReservoirEvaluator<'a> {
    counters: ReservoirCounters,
    #[cfg(feature = "redis")]
    redis_pool: Option<&'a RedisPool>,
    #[cfg(feature = "redis")]
    org_id: Option<u64>,
    // Using PhantomData because the lifetimes are behind a processing flag.
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> ReservoirEvaluator<'a> {
    /// Constructor for [`ReservoirEvaluator`].
    pub fn new(counters: ReservoirCounters) -> Self {
        Self {
            counters,
            #[cfg(feature = "redis")]
            org_id: None,
            #[cfg(feature = "redis")]
            redis_pool: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Sets the Redis pool and organiation ID for the [`ReservoirEvaluator`].
    ///
    /// These values are needed to synchronize with Redis.
    #[cfg(feature = "redis")]
    pub fn set_redis(&mut self, org_id: u64, redis_pool: &'a RedisPool) {
        self.org_id = Some(org_id);
        self.redis_pool = Some(redis_pool);
    }

    #[cfg(feature = "redis")]
    fn redis_count(
        &self,
        key: &ReservoirRuleKey,
        redis_pool: &RedisPool,
        rule_expiry: Option<&DateTime<Utc>>,
    ) -> anyhow::Result<i64> {
        let mut redis_client = redis_pool.client()?;
        let mut redis_connection = redis_client.connection()?;

        let val = match redis_sampling::increment_redis_reservoir_count(&mut redis_connection, key)
        {
            Ok(val) => val,
            Err(e) => {
                relay_log::error!("failed to increment redis value: {:?}", e);
                return Err(e);
            }
        };

        if let Err(e) = redis_sampling::set_redis_expiry(&mut redis_connection, key, rule_expiry) {
            relay_log::error!("failed to set redis reservoir rule expiry");
            return Err(e);
        }

        Ok(val)
    }

    #[cfg(feature = "redis")]
    fn update_counter(&self, rule: RuleId, new_value: i64) {
        let Ok(mut map_guard) = self.counters.lock() else {
            return;
        };

        match map_guard.get_mut(&rule) {
            Some(value) => *value = new_value,
            // Logging an error because at this point the value should definitively be here.
            None => relay_log::error!("failed to retrieve counter entry"),
        }
    }

    /// Gets the local count of a reserovir rule and increments it, if the limit has yet to be reached
    fn local_count(&self, rule: RuleId, limit: i64) -> Option<i64> {
        let Ok(mut map_guard) = self.counters.lock() else {
            relay_log::error!("failed to lock reservoir counter mutex");
            return None;
        };

        let counter_value = map_guard.entry(rule).or_insert(0);

        match (*counter_value).cmp(&limit) {
            // Limit not yet reached. Eagerly incrementing to avoid an additional lock
            // in the case where it doesn't get overrwritten by the redis count.
            Ordering::Less => {
                *counter_value += 1;
                Some(*counter_value)
            }
            // Limit has already been reached.
            Ordering::Equal | Ordering::Greater => None,
        }
    }

    /// Evaluates a reservoir rule, returning `true` if it should be sampled.
    pub fn evaluate(&self, rule: RuleId, limit: i64, _rule_expiry: Option<&DateTime<Utc>>) -> bool {
        let Some(incremented_local_count) = self.local_count(rule, limit) else {
            return false;
        };

        #[cfg(feature = "redis")]
        if let (Some(org_id), Some(redis_pool)) = (self.org_id, self.redis_pool.as_ref()) {
            let key = ReservoirRuleKey::new(org_id, rule);

            let Ok(redis_count) = self.redis_count(&key, redis_pool, _rule_expiry) else {
                // We don't sample at all if we lost access to redis.
                // Therefore we revert the previous increment.
                // Seems inefficient, but this should be a rare occurence.
                self.update_counter(rule, incremented_local_count - 1);
                return false;
            };

            if redis_count > incremented_local_count {
                self.update_counter(rule, redis_count);
                return redis_count <= limit;
            };
        }

        incremented_local_count <= limit
    }
}

/// State machine for dynamic sampling.
#[derive(Debug)]
pub struct SamplingEvaluator<'a> {
    now: DateTime<Utc>,
    rule_ids: Vec<RuleId>,
    factor: f64,
    client_sample_rate: Option<f64>,
    reservoir: Option<&'a ReservoirEvaluator<'a>>,
}

impl<'a> SamplingEvaluator<'a> {
    /// Constructor for [`SamplingEvaluator`].
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            now,
            rule_ids: vec![],
            factor: 1.0,
            client_sample_rate: None,
            reservoir: None,
        }
    }

    /// Sets a [`ReservoirEvaluator`].
    pub fn set_reservoir(mut self, reservoir: &'a ReservoirEvaluator) -> Self {
        self.reservoir = Some(reservoir);
        self
    }

    /// Sets a new client sample rate value.
    pub fn adjust_client_sample_rate(mut self, client_sample_rate: Option<f64>) -> Self {
        self.client_sample_rate = client_sample_rate;
        self
    }

    /// Attempts to find a match for sampling rules using `ControlFlow`.
    ///
    /// This function returns a `ControlFlow` to provide control over the matching process.
    ///
    /// - `ControlFlow::Continue`: Indicates that matching is incomplete, and more rules can be evaluated.
    ///    - This state occurs either if no active rules match the provided data, or if the matched rules
    ///      are factors requiring a final sampling value.
    ///    - The returned evaluator contains the state of the matched rules and the accumulated sampling factor.
    ///    - If this value is returned and there are no more rules to evaluate, it should be interpreted as "no match."
    ///
    /// - `ControlFlow::Break`: Indicates that one or more rules have successfully matched.
    pub fn match_rules<'b, I, G>(
        mut self,
        seed: Uuid,
        instance: &G,
        rules: I,
    ) -> ControlFlow<SamplingMatch, Self>
    where
        G: Getter,
        I: Iterator<Item = &'b SamplingRule>,
    {
        for rule in rules {
            if !rule.condition.matches(instance) {
                continue;
            };

            let Some(sampling_value) = rule.evaluate(self.now, self.reservoir) else {
                continue;
            };

            self.rule_ids.push(rule.id);

            match sampling_value {
                SamplingValue::Factor { value } => self.factor *= value,
                SamplingValue::SampleRate { value } => {
                    let sample_rate = (value * self.factor).clamp(0.0, 1.0);

                    return ControlFlow::Break(SamplingMatch::new(
                        self.adjusted_sample_rate(sample_rate),
                        seed,
                        self.rule_ids,
                    ));
                }
                SamplingValue::Reservoir { .. } => {
                    return ControlFlow::Break(SamplingMatch::new(1.0, seed, vec![rule.id]));
                }
            }
        }
        ControlFlow::Continue(self)
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
    use crate::DynamicSamplingContext;

    use super::*;

    /// Helper to extract the sampling match after evaluating rules.
    fn get_sampling_match(rules: &[SamplingRule], instance: &impl Getter) -> SamplingMatch {
        match SamplingEvaluator::new(Utc::now()).match_rules(
            Uuid::default(),
            instance,
            rules.iter(),
        ) {
            ControlFlow::Break(sampling_match) => sampling_match,
            ControlFlow::Continue(_) => panic!("no match found"),
        }
    }

    fn evaluation_is_match(res: ControlFlow<SamplingMatch, SamplingEvaluator>) -> bool {
        matches!(res, ControlFlow::Break(_))
    }

    /// Helper to check if certain rules are matched on.
    fn matches_rule_ids(rule_ids: &[u32], rules: &[SamplingRule], instance: &impl Getter) -> bool {
        let matched_rule_ids = MatchedRuleIds(rule_ids.iter().map(|num| RuleId(*num)).collect());
        let sampling_match = get_sampling_match(rules, instance);
        matched_rule_ids == sampling_match.matched_rules
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
        let eval = SamplingEvaluator::new(Utc::now());
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

        let res = SamplingEvaluator::new(Utc::now())
            .adjust_client_sample_rate(Some(0.2))
            .match_rules(Uuid::default(), &dsc, rules.iter());

        let ControlFlow::Break(sampling_match) = res else {
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
        let res = SamplingEvaluator::new(within_timerange).match_rules(
            Uuid::default(),
            &dsc,
            [rule.clone()].iter(),
        );

        assert!(evaluation_is_match(res));

        let before_timerange = Utc.with_ymd_and_hms(1969, 1, 1, 0, 0, 0).unwrap();
        let res = SamplingEvaluator::new(before_timerange).match_rules(
            Uuid::default(),
            &dsc,
            [rule.clone()].iter(),
        );
        assert!(!evaluation_is_match(res));

        let after_timerange = Utc.with_ymd_and_hms(1971, 1, 1, 0, 0, 0).unwrap();
        let res = SamplingEvaluator::new(after_timerange).match_rules(
            Uuid::default(),
            &dsc,
            [rule].iter(),
        );
        assert!(!evaluation_is_match(res));
    }

    /// Checks that `SamplingValueEvaluator` correctly matches the right rules.
    #[test]
    fn test_condition_matching() {
        let rules = simple_sampling_rules(vec![
            (
                RuleCondition::glob("trace.transaction", "*healthcheck*"),
                SamplingValue::SampleRate { value: 1.0 },
            ),
            (
                RuleCondition::glob("trace.environment", "*dev*"),
                SamplingValue::SampleRate { value: 1.0 },
            ),
            (
                RuleCondition::eq_ignore_case("trace.transaction", "raboof"),
                SamplingValue::Factor { value: 1.0 },
            ),
            (
                RuleCondition::glob("trace.release", "1.1.1")
                    & RuleCondition::eq_ignore_case("trace.user.segment", "vip"),
                SamplingValue::SampleRate { value: 1.0 },
            ),
            (
                RuleCondition::eq_ignore_case("trace.release", "1.1.1")
                    & RuleCondition::eq_ignore_case("trace.environment", "prod"),
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

        let res = SamplingEvaluator::new(Utc::now()).match_rules(Uuid::default(), &dsc, [].iter());

        assert!(!evaluation_is_match(res));
    }
}
