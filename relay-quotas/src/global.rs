use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use relay_base_schema::metrics::MetricNamespace;
use relay_redis::redis::Script;
use relay_redis::{PooledClient, RedisError};

use crate::{QuotaScope, RedisQuota};

/// Default percentage of the quota limit to reserve from Redis as a local cache.
const DEFAULT_BUDGET_PERCENTAGE: f32 = 0.01;

fn load_global_lua_script() -> &'static Script {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    SCRIPT.get_or_init(|| Script::new(include_str!("global_quota.lua")))
}

/// A rate limiter for global rate limits.
#[derive(Default)]
pub struct GlobalRateLimits {
    limits: Mutex<hashbrown::HashMap<Key, Arc<Mutex<GlobalRateLimit>>>>,
}

impl GlobalRateLimits {
    /// Returns `Ok(true)` if the global quota should be ratelimited.
    ///
    /// Certain errors can be resolved by syncing to redis, so in those cases
    /// we try again to decrement the budget after syncing.
    pub fn is_rate_limited(
        &self,
        client: &mut PooledClient,
        quota: &RedisQuota,
        quantity: usize,
    ) -> Result<bool, RedisError> {
        let keys = KeyRef::new(quota);

        let mut should_ratelimit = false;

        for key in keys {
            let val = {
                let mut limits = self.limits.lock().unwrap_or_else(PoisonError::into_inner);
                Arc::clone(limits.entry_ref(&key).or_default())
            };

            let mut val = val.lock().unwrap_or_else(PoisonError::into_inner);

            if val.is_rate_limited(client, quota, key, quantity as u64)? {
                should_ratelimit = true;
            }
        }

        Ok(should_ratelimit)
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
#[derive(Clone, Copy, Hash)]
struct KeyRef<'a> {
    prefix: &'a str,
    window: u64,
    namespace: Option<MetricNamespace>,
}

impl<'a> KeyRef<'a> {
    fn redis_key(&self, slot: u64) -> RedisKey {
        RedisKey::new(self, slot)
    }

    fn new(quota: &'a RedisQuota<'a>) -> Vec<Self> {
        let mut keys = vec![];

        if quota.scope == QuotaScope::Global {
            let without_namespace = KeyRef {
                prefix: quota.prefix(),
                window: quota.window(),
                namespace: None,
            };
            keys.push(without_namespace);
        }

        if let Some(namespace) = quota.namespace {
            let global_ns = KeyRef {
                prefix: quota.prefix(),
                window: quota.window(),
                namespace: Some(namespace),
            };

            keys.push(global_ns);
        }

        keys
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
        //    If the slot is not much older (+1), keep using the same slot,
        if quota_slot > self.slot || quota_slot + 1 < self.slot {
            self.budget = 0;
            self.last_seen_redis_value = 0;
            self.slot = quota_slot;
        }

        if self.budget >= quantity {
            self.budget -= quantity;
            return Ok(false);
        }

        let redis_key = key.redis_key(quota_slot);
        let reserved = self.try_reserve(client, quantity, quota, redis_key)?;
        self.budget += reserved;

        if self.budget >= quantity {
            self.budget -= quantity;
            Ok(false)
        } else {
            Ok(true)
        }
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
            Some(limit) => (limit as f32 * DEFAULT_BUDGET_PERCENTAGE) as u64,
            // On average `DEFAULT_BUDGET_PERCENTAGE` calls go to Redis for infinite budget.
            None => (quantity as f32 / DEFAULT_BUDGET_PERCENTAGE) as u64,
        }
    }
}

impl Default for GlobalRateLimit {
    fn default() -> Self {
        Self::new()
    }
}

/*

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    use relay_base_schema::data_category::DataCategory;
    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_common::time::UnixTimestamp;
    use relay_redis::{RedisConfigOptions, RedisPool};

    use crate::{DataCategories, ItemScoping, Quota, QuotaScope, Scoping};

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
        let scoping = ItemScoping {
            category: DataCategory::MetricBucket,
            scoping,
        };
        RedisQuota::new(quota, scoping, UnixTimestamp::now()).unwrap()
    }

    #[test]
    fn test_global_ratelimit() {
        let limit = 200;

        let quota = build_quota(10, limit);
        let scoping = build_scoping();
        let redis_quota = build_redis_quota(&quota, &scoping);

        let pool = build_redis_pool();
        let mut client = pool.client().unwrap();
        let counter = GlobalRateLimits::default();

        let expected_ratelimit_result = [false, false, true, true].to_vec();

        // The limit is 200, while we take 90 at a time. So the first two times we call, we'll
        // still be under the limit. 90 < 200 -> 180 < 200 -> 270 > 200 -> 360 > 200.
        for should_ratelimit in expected_ratelimit_result {
            let is_ratelimited = counter
                .is_rate_limited(&mut client, &redis_quota, 90)
                .unwrap();

            assert_eq!(should_ratelimit, is_ratelimited);
        }
    }

    #[test]
    fn test_multiple_global_ratelimit() {
        let limit = 91_337;

        let quota = build_quota(10, limit as u64);
        let scoping = build_scoping();
        let quota = build_redis_quota(&quota, &scoping);

        let pool = build_redis_pool();
        let mut client = pool.client().unwrap();

        let counter1 = GlobalRateLimits::default();
        let counter2 = GlobalRateLimits::default();

        let mut total = 0;
        let mut total_counter_1 = 0;
        let mut total_counter_2 = 0;
        for i in 0.. {
            let quantity = i % 17;

            if !counter1
                .is_rate_limited(&mut client, &quota, quantity)
                .unwrap()
            {
                total += quantity;
                total_counter_1 += quantity;
            }
            if !counter2
                .is_rate_limited(&mut client, &quota, quantity)
                .unwrap()
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
        assert!(diff <= limit as f32 * DEFAULT_BUDGET_PERCENTAGE);
    }

    #[test]
    fn test_global_ratelimit_slots() {
        let limit = 200;
        let window = 10;

        let ts = UnixTimestamp::now();
        let quota = build_quota(window, limit);
        let scoping = build_scoping();
        let scoping = ItemScoping {
            category: DataCategory::MetricBucket,
            scoping: &scoping,
        };
        let redis_quota = RedisQuota::new(&quota, scoping, ts).unwrap();

        let pool = build_redis_pool();
        let mut client = pool.client().unwrap();

        let rl = GlobalRateLimits::default();

        assert!(!rl.is_rate_limited(&mut client, &redis_quota, 200).unwrap());
        assert!(rl.is_rate_limited(&mut client, &redis_quota, 1).unwrap());

        // Fast forward time.
        let redis_quota =
            RedisQuota::new(&quota, scoping, ts + Duration::from_secs(window + 1)).unwrap();
        assert!(!rl.is_rate_limited(&mut client, &redis_quota, 200).unwrap());
        assert!(rl.is_rate_limited(&mut client, &redis_quota, 1).unwrap());
    }

    #[test]
    fn test_global_ratelimit_infinite() {
        let limit = None;

        let mut quota = build_quota(10, limit);
        let scoping = build_scoping();
        let redis_quota = build_redis_quota(&quota, &scoping);

        let pool = build_redis_pool();
        let mut client = pool.client().unwrap();

        let rl = GlobalRateLimits::default();

        let quantity = 2;
        let redis_threshold = (quantity as f32 / DEFAULT_BUDGET_PERCENTAGE) as u64;
        for _ in 0..redis_threshold + 10 {
            assert!(!rl
                .is_rate_limited(&mut client, &redis_quota, quantity)
                .unwrap());
        }

        // Grab a new rate limiter and make sure even with the infinite limit,
        // the quantity was still synchronized via Redis.
        let rl = GlobalRateLimits::default();

        quota.limit = Some(redis_threshold);
        let redis_quota = build_redis_quota(&quota, &scoping);

        assert!(rl
            .is_rate_limited(&mut client, &redis_quota, quantity)
            .unwrap());
    }
}

*/
