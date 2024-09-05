use std::sync::{Mutex, OnceLock, PoisonError};

use itertools::Itertools;
use relay_base_schema::metrics::MetricNamespace;
use relay_redis::redis::Script;
use relay_redis::{PooledClient, RedisError};

use crate::RedisQuota;

/// Default percentage of the quota limit to reserve from Redis as a local cache.
const DEFAULT_BUDGET_RATIO: f32 = 0.001;

fn load_global_lua_script() -> &'static Script {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    SCRIPT.get_or_init(|| Script::new(include_str!("global_quota.lua")))
}

/// A rate limiter for global rate limits.
#[derive(Default)]
pub struct GlobalRateLimits {
    limits: Mutex<hashbrown::HashMap<Key, GlobalRateLimit>>,
}

impl GlobalRateLimits {
    /// Returns a vector of the [`RedisQuota`]'s that should be ratelimited.
    ///
    /// We don't know if an item should be ratelimited or not until we've checked all the quotas.
    /// Therefore we only start decrementing the budgets of the various quotas when we know
    /// that None of the quotas hit the ratelimit.
    pub fn filter_rate_limited<'a>(
        &self,
        client: &mut PooledClient,
        quotas: &'a [RedisQuota<'a>],
        quantity: usize,
    ) -> Result<Vec<&RedisQuota<'a>>, RedisError> {
        let mut guard = self.limits.lock().unwrap_or_else(PoisonError::into_inner);

        let mut ratelimited = vec![];
        let mut not_ratelimited = vec![];

        let min_by_keyref = quotas
            .iter()
            .into_grouping_map_by(|q| KeyRef::new(q))
            .min_by_key(|_, q| q.limit());

        for (key, quota) in min_by_keyref {
            let val = guard.entry_ref(&key).or_default();

            if val.is_rate_limited(client, quota, key, quantity as u64)? {
                ratelimited.push(quota);
            } else {
                not_ratelimited.push(quota);
            }
        }

        if ratelimited.is_empty() {
            for quota in not_ratelimited {
                if let Some(val) = guard.get_mut(&KeyRef::new(quota)) {
                    val.budget -= quantity as u64;
                }
            }
        }

        Ok(ratelimited)
    }
}

/// Key for storing global quota-budgets locally.
///
/// Note: must not be used in redis. For that, use RedisQuota.key().
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    prefix: String,
    window: u64,
    namespace: Option<MetricNamespace>,
}

impl From<&KeyRef<'_>> for Key {
    fn from(value: &KeyRef<'_>) -> Self {
        Key {
            prefix: value.prefix.to_owned(),
            window: value.window,
            namespace: value.namespace,
        }
    }
}

#[derive(Debug)]
struct RedisKey(String);

impl RedisKey {
    fn new(key: &KeyRef<'_>, slot: u64) -> Self {
        Self(format!(
            "global_quota:{id}{window}{namespace:?}:{slot}",
            id = key.prefix,
            window = key.window,
            namespace = key.namespace,
            slot = slot,
        ))
    }
}

/// Used to look up a hashmap of [`keys`](Key) without a string allocation.
///
/// This works due to the [`hashbrown::Equivalent`] trait.
#[derive(Clone, Copy, Hash, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct KeyRef<'a> {
    prefix: &'a str,
    window: u64,
    namespace: Option<MetricNamespace>,
}

impl<'a> KeyRef<'a> {
    fn new(quota: &'a RedisQuota<'a>) -> Self {
        Self {
            prefix: quota.prefix(),
            window: quota.window(),
            namespace: quota.namespace,
        }
    }

    fn redis_key(&self, slot: u64) -> RedisKey {
        RedisKey::new(self, slot)
    }
}

impl hashbrown::Equivalent<Key> for KeyRef<'_> {
    fn equivalent(&self, key: &Key) -> bool {
        let Key {
            prefix,
            window,
            namespace,
        } = key;

        self.prefix == prefix && self.window == *window && self.namespace == *namespace
    }
}

/// A single global rate limit.
///
/// When we want to ratelimit across all relay-instances, we need to use Redis to synchronize.
/// Calling Redis every time we want to check if an item should be ratelimited would be very expensive,
/// which is why we have this cache. It works by 'taking' a certain budget from Redis, by pre-incrementing
/// a global counter. We put the amount we pre-incremented into this local cache and count down until
/// we have no more budget, then we ask for more from Redis. If we find the global counter is above
/// the quota limit, we will ratelimit the item.
#[derive(Debug)]
struct GlobalRateLimit {
    budget: u64,
    last_seen_redis_value: u64,
    slot: u64,
}

impl GlobalRateLimit {
    fn new() -> Self {
        Self {
            budget: 0,
            last_seen_redis_value: 0,
            slot: 0,
        }
    }

    /// Returns `true` if quota should be ratelimited.
    pub fn is_rate_limited(
        &mut self,
        client: &mut PooledClient,
        quota: &RedisQuota,
        key: KeyRef<'_>,
        quantity: u64,
    ) -> Result<bool, RedisError> {
        let quota_slot = quota.slot();

        // There is 2 cases we are handling here:
        //
        // 1. The quota slot is newer than the currently stored slot.
        //    This just means we advanced to the next slot/window. The expected case.
        // 2. The quota slot is (much) older than the currently stored slot.
        //    This means the system time changed, we reset to that older slot
        //    because we assume the system time will stay changed.
        //    If the slot is not much older (+1), keep using the same slot.
        if quota_slot > self.slot || quota_slot + 1 < self.slot {
            self.budget = 0;
            self.last_seen_redis_value = 0;
            self.slot = quota_slot;
        }

        if self.budget >= quantity {
            return Ok(false);
        }

        let redis_key = key.redis_key(quota_slot);
        let reserved = self.try_reserve(client, quantity, quota, redis_key)?;
        self.budget += reserved;

        Ok(self.budget < quantity)
    }

    fn try_reserve(
        &mut self,
        client: &mut PooledClient,
        quantity: u64,
        quota: &RedisQuota,
        redis_key: RedisKey,
    ) -> Result<u64, RedisError> {
        let min_required_budget = quantity.saturating_sub(self.budget);
        let max_available_budget = quota
            .limit
            .unwrap_or(u64::MAX)
            .saturating_sub(self.last_seen_redis_value);

        if min_required_budget > max_available_budget {
            return Ok(0);
        }

        let budget_to_reserve = min_required_budget.max(self.default_request_size(quantity, quota));

        let (budget, value): (u64, u64) = load_global_lua_script()
            .prepare_invoke()
            .key(redis_key.0)
            .arg(budget_to_reserve)
            .arg(quota.limit())
            .arg(quota.key_expiry())
            .invoke(&mut client.connection()?)
            .map_err(RedisError::Redis)?;

        self.last_seen_redis_value = value;

        Ok(budget)
    }

    fn default_request_size(&self, quantity: u64, quota: &RedisQuota) -> u64 {
        match quota.limit {
            Some(limit) => (limit as f32 * DEFAULT_BUDGET_RATIO) as u64,
            // On average `DEFAULT_BUDGET_RATIO` percent calls go to Redis for an infinite budget.
            None => (quantity as f32 / DEFAULT_BUDGET_RATIO) as u64,
        }
    }
}

impl Default for GlobalRateLimit {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use super::*;

    use relay_base_schema::data_category::DataCategory;
    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_common::time::UnixTimestamp;
    use relay_redis::{RedisConfigOptions, RedisPool};

    use crate::{DataCategories, Quota, QuotaScope, Scoping};

    fn build_redis_pool() -> RedisPool {
        let url = std::env::var("RELAY_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());

        RedisPool::single(&url, RedisConfigOptions::default()).unwrap()
    }

    fn build_quota(window: u64, limit: impl Into<Option<u64>>) -> Quota {
        Quota {
            id: Some(uuid::Uuid::new_v4().to_string()),
            categories: DataCategories::new(),
            scope: QuotaScope::Global,
            scope_id: None,
            window: Some(window),
            limit: limit.into(),
            reason_code: None,
            namespace: None,
        }
    }

    fn build_scoping() -> Scoping {
        Scoping {
            organization_id: 69420,
            project_id: ProjectId::new(42),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(4711),
        }
    }

    fn build_redis_quota<'a>(quota: &'a Quota, scoping: &'a Scoping) -> RedisQuota<'a> {
        let scoping = scoping.item(DataCategory::MetricBucket);
        RedisQuota::new(quota, scoping, UnixTimestamp::now()).unwrap()
    }

    #[test]
    fn test_multiple_ratelimits() {
        let scoping = build_scoping();

        let quota1 = build_quota(10, 100);
        let quota2 = build_quota(10, 150);
        let quota3 = build_quota(10, 200);
        let quantity = 175;

        let redis_quotas = [
            build_redis_quota(&quota1, &scoping),
            build_redis_quota(&quota2, &scoping),
            build_redis_quota(&quota3, &scoping),
        ];

        let pool = build_redis_pool();
        let mut client = pool.client().unwrap();
        let counter = GlobalRateLimits::default();

        let rate_limited_quotas = counter
            .filter_rate_limited(&mut client, &redis_quotas, quantity)
            .unwrap();

        // Only the quotas that are less than the quantity gets ratelimited.
        assert_eq!(
            BTreeSet::from([100, 150]),
            rate_limited_quotas
                .iter()
                .map(|quota| quota.limit())
                .collect()
        );
    }

    /// Checks that if two quotas are identical but with different limits, we only use
    /// the one with the smaller limit.
    #[test]
    fn test_use_smaller_limit() {
        let smaller_limit = 100;
        let bigger_limit = 200;

        let scoping = build_scoping();

        let mut smaller_quota = build_quota(10, smaller_limit);
        let mut bigger_quota = build_quota(10, bigger_limit);

        smaller_quota.id = Some("foobar".into());
        bigger_quota.id = Some("foobar".into());

        let redis_quotas = [
            build_redis_quota(&smaller_quota, &scoping),
            build_redis_quota(&bigger_quota, &scoping),
        ];

        let pool = build_redis_pool();
        let mut client = pool.client().unwrap();
        let counter = GlobalRateLimits::default();

        let rate_limited_quotas = counter
            .filter_rate_limited(&mut client, &redis_quotas, (bigger_limit * 2) as usize)
            .unwrap();

        assert_eq!(rate_limited_quotas.len(), 1);

        assert_eq!(
            rate_limited_quotas.first().unwrap().limit(),
            smaller_limit as i64
        );
    }

    #[test]
    fn test_global_ratelimit() {
        let limit = 200;

        let quota = build_quota(10, limit);
        let scoping = build_scoping();
        let redis_quota = [build_redis_quota(&quota, &scoping)];

        let pool = build_redis_pool();
        let mut client = pool.client().unwrap();
        let counter = GlobalRateLimits::default();

        let expected_ratelimit_result = [false, false, true, true].to_vec();

        // The limit is 200, while we take 90 at a time. So the first two times we call, we'll
        // still be under the limit. 90 < 200 -> 180 < 200 -> 270 > 200 -> 360 > 200.
        for should_ratelimit in expected_ratelimit_result {
            let is_ratelimited = counter
                .filter_rate_limited(&mut client, &redis_quota, 90)
                .unwrap();

            assert_eq!(should_ratelimit, !is_ratelimited.is_empty());
        }
    }

    #[test]
    fn test_global_ratelimit_over_under() {
        let limit = 10;

        let quota = build_quota(10, limit);
        let scoping = build_scoping();

        let pool = build_redis_pool();
        let mut client = pool.client().unwrap();
        let rl = GlobalRateLimits::default();

        let redis_quota = [build_redis_quota(&quota, &scoping)];
        assert!(!rl
            .filter_rate_limited(&mut client, &redis_quota, 11)
            .unwrap()
            .is_empty());

        assert!(rl
            .filter_rate_limited(&mut client, &redis_quota, 10)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_multiple_global_ratelimit() {
        let limit = 91_337;

        let quota = build_quota(10, limit as u64);
        let scoping = build_scoping();
        let quota = [build_redis_quota(&quota, &scoping)];

        let pool = build_redis_pool();
        let mut client = pool.client().unwrap();

        let counter1 = GlobalRateLimits::default();
        let counter2 = GlobalRateLimits::default();

        let mut total = 0;
        let mut total_counter_1 = 0;
        let mut total_counter_2 = 0;
        for i in 0.. {
            let quantity = i % 17;

            if counter1
                .filter_rate_limited(&mut client, &quota, quantity)
                .unwrap()
                .is_empty()
            {
                total += quantity;
                total_counter_1 += quantity;
            }

            if counter2
                .filter_rate_limited(&mut client, &quota, quantity)
                .unwrap()
                .is_empty()
            {
                total += quantity;
                total_counter_2 += quantity;
            }

            assert!(total <= limit);
            if total == limit {
                break;
            }
        }

        assert_eq!(total, limit);

        // Assert that each limiter got about an equal amount of rate limit quota.
        // This works because we are working with a rather big limit and small quantities.
        let diff = (total_counter_1 as f32 - total_counter_2 as f32).abs();
        assert!(diff <= limit as f32 * DEFAULT_BUDGET_RATIO);
    }

    #[test]
    fn test_global_ratelimit_slots() {
        let limit = 200;
        let window = 10;

        let ts = UnixTimestamp::now();
        let quota = build_quota(window, limit);
        let scoping = build_scoping();
        let item_scoping = scoping.item(DataCategory::MetricBucket);

        let pool = build_redis_pool();
        let mut client = pool.client().unwrap();

        let rl = GlobalRateLimits::default();

        let redis_quota = [RedisQuota::new(&quota, item_scoping, ts).unwrap()];
        assert!(rl
            .filter_rate_limited(&mut client, &redis_quota, 200)
            .unwrap()
            .is_empty());

        assert!(!rl
            .filter_rate_limited(&mut client, &redis_quota, 1)
            .unwrap()
            .is_empty());

        // Fast forward time.
        let redis_quota =
            [
                RedisQuota::new(&quota, item_scoping, ts + Duration::from_secs(window + 1))
                    .unwrap(),
            ];
        assert!(rl
            .filter_rate_limited(&mut client, &redis_quota, 200)
            .unwrap()
            .is_empty());

        assert!(!rl
            .filter_rate_limited(&mut client, &redis_quota, 1)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_global_ratelimit_infinite() {
        let limit = None;

        let timestamp = UnixTimestamp::now();

        let mut quota = build_quota(100, limit);
        let scoping = build_scoping();
        let item_scoping = scoping.item(DataCategory::MetricBucket);

        let pool = build_redis_pool();
        let mut client = pool.client().unwrap();

        let rl = GlobalRateLimits::default();

        let quantity = 2;
        let redis_threshold = (quantity as f32 / DEFAULT_BUDGET_RATIO) as u64;
        for _ in 0..redis_threshold + 10 {
            let redis_quota = RedisQuota::new(&quota, item_scoping, timestamp).unwrap();
            assert!(rl
                .filter_rate_limited(&mut client, &[redis_quota], quantity)
                .unwrap()
                .is_empty());
        }

        // Grab a new rate limiter and make sure even with the infinite limit,
        // the quantity was still synchronized via Redis.
        let rl = GlobalRateLimits::default();

        quota.limit = Some(redis_threshold);
        let redis_quota = RedisQuota::new(&quota, item_scoping, timestamp).unwrap();

        assert!(!rl
            .filter_rate_limited(&mut client, &[redis_quota], quantity)
            .unwrap()
            .is_empty());
    }
}
