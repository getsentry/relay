//! Evaluation of dynamic sampling rules.

use std::collections::BTreeMap;
use std::fmt;
use std::num::ParseIntError;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use rand::Rng;
use rand::distr::StandardUniform;
use rand_pcg::Pcg32;
#[cfg(feature = "redis")]
use relay_base_schema::organization::OrganizationId;
use relay_protocol::Getter;
#[cfg(feature = "redis")]
use relay_redis::AsyncRedisClient;
use serde::Serialize;
use uuid::Uuid;

use crate::config::{RuleId, SamplingRule, SamplingValue};
#[cfg(feature = "redis")]
use crate::redis_sampling::{self, ReservoirRuleKey};

/// Generates a pseudo random number by seeding the generator with the given id.
///
/// The return is deterministic, always generates the same number from the same id.
fn pseudo_random_from_seed(seed: Uuid) -> f64 {
    let seed_number = seed.as_u128();
    let mut generator = Pcg32::new((seed_number >> 64) as u64, seed_number as u64);
    generator.sample(StandardUniform)
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
    org_id_and_client: Option<(OrganizationId, &'a AsyncRedisClient)>,
    // Using PhantomData because the lifetimes are behind a feature flag.
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl ReservoirEvaluator<'_> {
    /// Constructor for [`ReservoirEvaluator`].
    pub fn new(counters: ReservoirCounters) -> Self {
        Self {
            counters,
            #[cfg(feature = "redis")]
            org_id_and_client: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Gets shared ownership of the reservoir counters.
    pub fn counters(&self) -> ReservoirCounters {
        Arc::clone(&self.counters)
    }

    #[cfg(feature = "redis")]
    async fn redis_incr(
        &self,
        key: &ReservoirRuleKey,
        client: &AsyncRedisClient,
        rule_expiry: Option<&DateTime<Utc>>,
    ) -> anyhow::Result<i64> {
        relay_statsd::metric!(timer(crate::statsd::SamplingTimers::RedisReservoir), {
            let mut connection = client.get_connection().await?;

            let val = redis_sampling::increment_redis_reservoir_count(&mut connection, key).await?;
            redis_sampling::set_redis_expiry(&mut connection, key, rule_expiry).await?;

            Ok(val)
        })
    }

    /// Evaluates a reservoir rule, returning `true` if it should be sampled.
    pub fn incr_local(&self, rule: RuleId, limit: i64) -> bool {
        let Ok(mut map_guard) = self.counters.lock() else {
            relay_log::error!("failed to lock reservoir counter mutex");
            return false;
        };

        let counter_value = map_guard.entry(rule).or_insert(0);

        if *counter_value < limit {
            *counter_value += 1;
            true
        } else {
            false
        }
    }

    /// Evaluates a reservoir rule, returning `true` if it should be sampled.
    pub async fn evaluate(
        &self,
        rule: RuleId,
        limit: i64,
        _rule_expiry: Option<&DateTime<Utc>>,
    ) -> bool {
        #[cfg(feature = "redis")]
        if let Some((org_id, client)) = self.org_id_and_client {
            if let Ok(guard) = self.counters.lock() {
                if *guard.get(&rule).unwrap_or(&0) > limit {
                    return false;
                }
            }

            let key = ReservoirRuleKey::new(org_id, rule);
            let redis_count = match self.redis_incr(&key, client, _rule_expiry).await {
                Ok(redis_count) => redis_count,
                Err(e) => {
                    relay_log::error!(error = &*e, "failed to increment reservoir rule");
                    return false;
                }
            };

            if let Ok(mut map_guard) = self.counters.lock() {
                // If the rule isn't present, it has just been cleaned up by a project state update.
                // In that case, it is no longer relevant so we ignore it.
                if let Some(value) = map_guard.get_mut(&rule) {
                    *value = redis_count.max(*value);
                }
            }
            return redis_count <= limit;
        }

        self.incr_local(rule, limit)
    }
}

#[cfg(feature = "redis")]
impl<'a> ReservoirEvaluator<'a> {
    /// Sets the Redis pool and organization ID for the [`ReservoirEvaluator`].
    ///
    /// These values are needed to synchronize with Redis.
    pub fn set_redis(&mut self, org_id: OrganizationId, client: &'a AsyncRedisClient) {
        self.org_id_and_client = Some((org_id, client));
    }
}

/// State machine for dynamic sampling.
#[derive(Debug)]
pub struct SamplingEvaluator<'a> {
    now: DateTime<Utc>,
    rule_ids: Vec<RuleId>,
    factor: f64,
    minimum_sample_rate: Option<f64>,
    reservoir: Option<&'a ReservoirEvaluator<'a>>,
}

impl<'a> SamplingEvaluator<'a> {
    /// Constructs an evaluator with reservoir sampling.
    pub fn new_with_reservoir(now: DateTime<Utc>, reservoir: &'a ReservoirEvaluator<'a>) -> Self {
        Self {
            now,
            rule_ids: vec![],
            factor: 1.0,
            minimum_sample_rate: None,
            reservoir: Some(reservoir),
        }
    }

    /// Constructs an evaluator without reservoir sampling.
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            now,
            rule_ids: vec![],
            factor: 1.0,
            minimum_sample_rate: None,
            reservoir: None,
        }
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
    pub async fn match_rules<'b, I, G>(
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
            if !rule.time_range.contains(self.now) || !rule.condition.matches(instance) {
                continue;
            };

            if let Some(sample_rate) = self.try_compute_sample_rate(rule).await {
                return ControlFlow::Break(SamplingMatch::new(sample_rate, seed, self.rule_ids));
            };
        }

        ControlFlow::Continue(self)
    }

    /// Attempts to compute the sample rate for a given [`SamplingRule`].
    ///
    /// # Returns
    ///
    /// - `None` if the sampling rule is invalid, expired, or if the final sample rate has not been
    ///   determined yet.
    /// - `Some` if the computed sample rate should be applied directly.
    async fn try_compute_sample_rate(&mut self, rule: &SamplingRule) -> Option<f64> {
        match rule.sampling_value {
            SamplingValue::Factor { value } => {
                self.factor *= rule.apply_decaying_fn(value, self.now)?;
                self.rule_ids.push(rule.id);
                None
            }
            SamplingValue::SampleRate { value } => {
                let sample_rate = rule.apply_decaying_fn(value, self.now)?;
                let minimum_sample_rate = self.minimum_sample_rate.unwrap_or(0.0);
                let adjusted = (sample_rate.max(minimum_sample_rate) * self.factor).clamp(0.0, 1.0);

                self.rule_ids.push(rule.id);
                Some(adjusted)
            }
            SamplingValue::Reservoir { limit } => {
                let reservoir = self.reservoir?;
                if !reservoir
                    .evaluate(rule.id, limit, rule.time_range.end.as_ref())
                    .await
                {
                    return None;
                }

                // Clearing the previously matched rules because reservoir overrides them.
                self.rule_ids.clear();
                self.rule_ids.push(rule.id);
                // If the reservoir has not yet reached its limit, we want to sample 100%.
                Some(1.0)
            }
            SamplingValue::MinimumSampleRate { value } => {
                if self.minimum_sample_rate.is_none() {
                    self.minimum_sample_rate = Some(rule.apply_decaying_fn(value, self.now)?);
                    self.rule_ids.push(rule.id);
                }
                None
            }
        }
    }
}

fn sampling_match(sample_rate: f64, seed: Uuid) -> SamplingDecision {
    if sample_rate <= 0.0 {
        return SamplingDecision::Drop;
    } else if sample_rate >= 1.0 {
        return SamplingDecision::Keep;
    }

    let random_number = pseudo_random_from_seed(seed);
    relay_log::trace!(
        sample_rate,
        random_number,
        "applying dynamic sampling to matching event"
    );

    if random_number >= sample_rate {
        relay_log::trace!("dropping event that matched the configuration");
        SamplingDecision::Drop
    } else {
        relay_log::trace!("keeping event that matched the configuration");
        SamplingDecision::Keep
    }
}

/// A sampling decision.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SamplingDecision {
    /// The item is sampled and should not be dropped.
    Keep,
    /// The item is not sampled and should be dropped.
    Drop,
}

impl SamplingDecision {
    /// Returns `true` if the sampling decision is [`Self::Keep`].
    pub fn is_keep(self) -> bool {
        matches!(self, Self::Keep)
    }

    /// Returns `true` if the sampling decision is [`Self::Drop`].
    pub fn is_drop(self) -> bool {
        matches!(self, Self::Drop)
    }

    /// Returns a string representation of the sampling decision.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Keep => "keep",
            Self::Drop => "drop",
        }
    }
}

impl fmt::Display for SamplingDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
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
    decision: SamplingDecision,
}

impl SamplingMatch {
    fn new(sample_rate: f64, seed: Uuid, matched_rules: Vec<RuleId>) -> Self {
        let matched_rules = MatchedRuleIds(matched_rules);
        let decision = sampling_match(sample_rate, seed);

        Self {
            sample_rate,
            seed,
            matched_rules,
            decision,
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

    /// Returns the sampling decision.
    pub fn decision(&self) -> SamplingDecision {
        self.decision
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
    use chrono::TimeZone;
    use relay_protocol::RuleCondition;
    use similar_asserts::assert_eq;
    use std::str::FromStr;
    use uuid::Uuid;

    use crate::DynamicSamplingContext;
    use crate::config::{DecayingFunction, RuleType, TimeRange};
    use crate::dsc::TraceUserContext;

    use super::*;

    fn mock_reservoir_evaluator(vals: Vec<(u32, i64)>) -> ReservoirEvaluator<'static> {
        let mut map = BTreeMap::default();

        for (rule_id, count) in vals {
            map.insert(RuleId(rule_id), count);
        }

        let map = Arc::new(Mutex::new(map));

        ReservoirEvaluator::new(map)
    }

    /// Helper to extract the sampling match after evaluating rules.
    async fn get_sampling_match(rules: &[SamplingRule], instance: &impl Getter) -> SamplingMatch {
        match SamplingEvaluator::new(Utc::now())
            .match_rules(Uuid::default(), instance, rules.iter())
            .await
        {
            ControlFlow::Break(sampling_match) => sampling_match,
            ControlFlow::Continue(_) => panic!("no match found"),
        }
    }

    fn evaluation_is_match(res: ControlFlow<SamplingMatch, SamplingEvaluator>) -> bool {
        matches!(res, ControlFlow::Break(_))
    }

    /// Helper to check if certain rules are matched on.
    async fn matches_rule_ids(
        rule_ids: &[u32],
        rules: &[SamplingRule],
        instance: &impl Getter,
    ) -> bool {
        let matched_rule_ids = MatchedRuleIds(rule_ids.iter().map(|num| RuleId(*num)).collect());
        let sampling_match = get_sampling_match(rules, instance).await;
        matched_rule_ids == sampling_match.matched_rules
    }

    // Helper method to "unwrap" the sampling match.
    fn get_matched_rules(
        sampling_evaluator: &ControlFlow<SamplingMatch, SamplingEvaluator>,
    ) -> Vec<u32> {
        match sampling_evaluator {
            ControlFlow::Continue(_) => panic!("expected a sampling match"),
            ControlFlow::Break(m) => m.matched_rules.0.iter().map(|rule_id| rule_id.0).collect(),
        }
    }

    /// Helper function to create a dsc with the provided getter-values set.
    fn mocked_dsc_with_getter_values(
        paths_and_values: Vec<(&str, &str)>,
    ) -> DynamicSamplingContext {
        let mut dsc = DynamicSamplingContext {
            trace_id: "67e5504410b1426f9247bb680e5fe0c8".parse().unwrap(),
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
                "trace.user.id" => value.clone_into(&mut dsc.user.user_id),
                "trace.user.segment" => value.clone_into(&mut dsc.user.user_segment),
                "trace.transaction" => dsc.transaction = Some(value.to_owned()),
                "trace.replay_id" => dsc.replay_id = Some(Uuid::from_str(value).unwrap()),
                _ => panic!("invalid path"),
            }
        }

        dsc
    }

    async fn is_match(
        now: DateTime<Utc>,
        rule: &SamplingRule,
        dsc: &DynamicSamplingContext,
    ) -> bool {
        SamplingEvaluator::new(now)
            .match_rules(Uuid::default(), dsc, std::iter::once(rule))
            .await
            .is_break()
    }

    #[tokio::test]
    async fn test_reservoir_evaluator_limit() {
        let evaluator = mock_reservoir_evaluator(vec![(1, 0)]);

        let rule = RuleId(1);
        let limit = 3;

        assert!(evaluator.evaluate(rule, limit, None).await);
        assert!(evaluator.evaluate(rule, limit, None).await);
        assert!(evaluator.evaluate(rule, limit, None).await);
        // After 3 samples we have reached the limit, and the following rules are not sampled.
        assert!(!evaluator.evaluate(rule, limit, None).await);
        assert!(!evaluator.evaluate(rule, limit, None).await);
    }

    #[tokio::test]
    async fn test_sample_rate_compounding() {
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
        assert_eq!(get_sampling_match(&rules, &dsc).await.sample_rate(), 0.1);
    }

    #[tokio::test]
    async fn test_minimum_sample_rate() {
        let rules = simple_sampling_rules(vec![
            (RuleCondition::all(), SamplingValue::Factor { value: 1.5 }),
            (
                RuleCondition::all(),
                SamplingValue::MinimumSampleRate { value: 0.5 },
            ),
            // Only the first matching minimum is applied.
            (
                RuleCondition::all(),
                SamplingValue::MinimumSampleRate { value: 1.0 },
            ),
            (
                RuleCondition::all(),
                SamplingValue::SampleRate { value: 0.05 },
            ),
        ]);
        let dsc = mocked_dsc_with_getter_values(vec![]);

        // max(0.05, 0.5) * 1.5 = 0.75
        assert_eq!(get_sampling_match(&rules, &dsc).await.sample_rate(), 0.75);
    }

    fn mocked_sampling_rule() -> SamplingRule {
        SamplingRule {
            condition: RuleCondition::all(),
            sampling_value: SamplingValue::SampleRate { value: 1.0 },
            ty: RuleType::Trace,
            id: RuleId(0),
            time_range: Default::default(),
            decaying_fn: Default::default(),
        }
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

    /// Tests that reservoir rules override the other rules.
    ///
    /// Here all 3 rules are a match. But when the reservoir
    /// rule (id = 1) has not yet reached its limit of "2" matches, the
    /// previous rule(s) will not be present in the matched rules output.
    /// After the limit has been reached, the reservoir rule is ignored
    /// and the output is the two other rules (id = 0, id = 2).
    #[tokio::test]
    async fn test_reservoir_override() {
        let dsc = mocked_dsc_with_getter_values(vec![]);
        let rules = simple_sampling_rules(vec![
            (RuleCondition::all(), SamplingValue::Factor { value: 0.5 }),
            // The reservoir has a limit of 2, meaning it should be sampled twice
            // before it is ignored.
            (RuleCondition::all(), SamplingValue::Reservoir { limit: 2 }),
            (
                RuleCondition::all(),
                SamplingValue::SampleRate { value: 0.5 },
            ),
        ]);

        // The reservoir keeps the counter state behind a mutex, which is how it
        // shares state among multiple evaluator instances.
        let reservoir = mock_reservoir_evaluator(vec![]);

        let evaluator = SamplingEvaluator::new_with_reservoir(Utc::now(), &reservoir);
        let matched_rules = get_matched_rules(
            &evaluator
                .match_rules(Uuid::default(), &dsc, rules.iter())
                .await,
        );
        // Reservoir rule overrides 0 and 2.
        assert_eq!(&matched_rules, &[1]);

        let evaluator = SamplingEvaluator::new_with_reservoir(Utc::now(), &reservoir);
        let matched_rules = get_matched_rules(
            &evaluator
                .match_rules(Uuid::default(), &dsc, rules.iter())
                .await,
        );
        // Reservoir rule overrides 0 and 2.
        assert_eq!(&matched_rules, &[1]);

        let evaluator = SamplingEvaluator::new_with_reservoir(Utc::now(), &reservoir);
        let matched_rules = get_matched_rules(
            &evaluator
                .match_rules(Uuid::default(), &dsc, rules.iter())
                .await,
        );
        // Reservoir rule reached its limit, rule 0 and 2 are now matched instead.
        assert_eq!(&matched_rules, &[0, 2]);
    }

    /// Checks that rules don't match if the time is outside the time range.
    #[tokio::test]
    async fn test_expired_rules() {
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
        let res = SamplingEvaluator::new(within_timerange)
            .match_rules(Uuid::default(), &dsc, [rule.clone()].iter())
            .await;
        assert!(evaluation_is_match(res));

        let before_timerange = Utc.with_ymd_and_hms(1969, 1, 1, 0, 0, 0).unwrap();
        let res = SamplingEvaluator::new(before_timerange)
            .match_rules(Uuid::default(), &dsc, [rule.clone()].iter())
            .await;
        assert!(!evaluation_is_match(res));

        let after_timerange = Utc.with_ymd_and_hms(1971, 1, 1, 0, 0, 0).unwrap();
        let res = SamplingEvaluator::new(after_timerange)
            .match_rules(Uuid::default(), &dsc, [rule].iter())
            .await;
        assert!(!evaluation_is_match(res));
    }

    /// Checks that `SamplingValueEvaluator` correctly matches the right rules.
    #[tokio::test]
    async fn test_condition_matching() {
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
        assert!(matches_rule_ids(&[0], &rules, &dsc).await);

        // early return of second rule
        let dsc = mocked_dsc_with_getter_values(vec![("trace.environment", "dev")]);
        assert!(matches_rule_ids(&[1], &rules, &dsc).await);

        // factor match third rule and early return sixth rule
        let dsc = mocked_dsc_with_getter_values(vec![("trace.transaction", "raboof")]);
        assert!(matches_rule_ids(&[2, 5], &rules, &dsc).await);

        // factor match third rule and early return fourth rule
        let dsc = mocked_dsc_with_getter_values(vec![
            ("trace.transaction", "raboof"),
            ("trace.release", "1.1.1"),
            ("trace.user.segment", "vip"),
        ]);
        assert!(matches_rule_ids(&[2, 3], &rules, &dsc).await);

        // factor match third, fifth rule and early return sixth rule
        let dsc = mocked_dsc_with_getter_values(vec![
            ("trace.transaction", "raboof"),
            ("trace.release", "1.1.1"),
            ("trace.environment", "prod"),
        ]);
        assert!(matches_rule_ids(&[2, 4, 5], &rules, &dsc).await);

        // factor match fifth and early return sixth rule
        let dsc = mocked_dsc_with_getter_values(vec![
            ("trace.release", "1.1.1"),
            ("trace.environment", "prod"),
        ]);
        assert!(matches_rule_ids(&[4, 5], &rules, &dsc).await);
    }

    #[test]
    /// Test that we get the same sampling decision from the same trace id
    fn test_repeatable_seed() {
        let val1 = pseudo_random_from_seed(Uuid::default());
        let val2 = pseudo_random_from_seed(Uuid::default());
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

    #[tokio::test]
    /// Tests that no match is done when there are no matching rules.
    async fn test_get_sampling_match_result_with_no_match() {
        let dsc = mocked_dsc_with_getter_values(vec![]);

        let res = SamplingEvaluator::new(Utc::now())
            .match_rules(Uuid::default(), &dsc, [].iter())
            .await;

        assert!(!evaluation_is_match(res));
    }

    /// Validates the early return (and hence no match) of the `match_rules` function if the current
    /// time is out of bounds of the time range.
    /// When the `start` or `end` of the range is missing, it defaults to always include
    /// times before the `end` or after the `start`, respectively.
    #[tokio::test]
    async fn test_sample_rate_valid_time_range() {
        let dsc = mocked_dsc_with_getter_values(vec![]);
        let time_range = TimeRange {
            start: Some(Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap()),
            end: Some(Utc.with_ymd_and_hms(1980, 1, 1, 0, 0, 0).unwrap()),
        };

        let before_time_range = Utc.with_ymd_and_hms(1969, 1, 1, 0, 0, 0).unwrap();
        let during_time_range = Utc.with_ymd_and_hms(1975, 1, 1, 0, 0, 0).unwrap();
        let after_time_range = Utc.with_ymd_and_hms(1981, 1, 1, 0, 0, 0).unwrap();

        let rule = SamplingRule {
            condition: RuleCondition::all(),
            sampling_value: SamplingValue::SampleRate { value: 1.0 },
            ty: RuleType::Trace,
            id: RuleId(0),
            time_range,
            decaying_fn: DecayingFunction::Constant,
        };

        // [start..end]
        assert!(!is_match(before_time_range, &rule, &dsc).await);
        assert!(is_match(during_time_range, &rule, &dsc).await);
        assert!(!is_match(after_time_range, &rule, &dsc).await);

        // [start..]
        let mut rule_without_end = rule.clone();
        rule_without_end.time_range.end = None;
        assert!(!is_match(before_time_range, &rule_without_end, &dsc).await);
        assert!(is_match(during_time_range, &rule_without_end, &dsc).await);
        assert!(is_match(after_time_range, &rule_without_end, &dsc).await);

        // [..end]
        let mut rule_without_start = rule.clone();
        rule_without_start.time_range.start = None;
        assert!(is_match(before_time_range, &rule_without_start, &dsc).await);
        assert!(is_match(during_time_range, &rule_without_start, &dsc).await);
        assert!(!is_match(after_time_range, &rule_without_start, &dsc).await);

        // [..]
        let mut rule_without_range = rule.clone();
        rule_without_range.time_range = TimeRange::default();
        assert!(is_match(before_time_range, &rule_without_range, &dsc).await);
        assert!(is_match(during_time_range, &rule_without_range, &dsc).await);
        assert!(is_match(after_time_range, &rule_without_range, &dsc).await);
    }

    /// Checks that `validate_match` yields the correct controlflow given the SamplingValue variant.
    #[tokio::test]
    async fn test_validate_match() {
        let mut rule = mocked_sampling_rule();

        let reservoir = ReservoirEvaluator::new(ReservoirCounters::default());
        let mut eval = SamplingEvaluator::new_with_reservoir(Utc::now(), &reservoir);

        rule.sampling_value = SamplingValue::SampleRate { value: 1.0 };
        assert_eq!(eval.try_compute_sample_rate(&rule).await, Some(1.0));

        rule.sampling_value = SamplingValue::Factor { value: 1.0 };
        assert_eq!(eval.try_compute_sample_rate(&rule).await, None);

        rule.sampling_value = SamplingValue::Reservoir { limit: 1 };
        assert_eq!(eval.try_compute_sample_rate(&rule).await, Some(1.0));
    }
}
