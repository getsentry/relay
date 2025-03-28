use std::future::Future;

use itertools::Itertools;
use relay_base_schema::metrics::MetricNamespace;
use relay_redis::{AsyncRedisConnection, AsyncRedisPool, RedisError, RedisScripts};

use crate::redis::RedisQuota;
use crate::RateLimitingError;

/// Default percentage of the quota limit to reserve from Redis as a local cache.
const DEFAULT_BUDGET_RATIO: f32 = 0.001;

/// A trait that exposes methods to check global rate limits.
pub trait GlobalLimiter {
    /// Returns the [`RedisQuota`]s that should be rate limited.
    fn check_global_rate_limits<'a>(
        &self,
        global_quotas: &'a [RedisQuota<'a>],
        quantity: usize,
    ) -> impl Future<Output = Result<Vec<&'a RedisQuota<'a>>, RateLimitingError>> + Send;
}

/// A rate limiter for global rate limits.
///
/// This struct maintains local caches of rate limits to reduce the number of
/// Redis operations required.
#[derive(Debug, Default)]
pub struct GlobalRateLimiter {
    limits: hashbrown::HashMap<Key, GlobalRateLimit>,
}

impl GlobalRateLimiter {
    /// Returns the [`RedisQuota`]s that should be rate limited.
    ///
    /// This method checks all quotas against their limits and returns those that
    /// exceed their limits. The rate limits are checked globally using Redis.
    ///
    /// Budgets are only decremented when none of the quotas hit their rate limits,
    /// which ensures consistent behavior across all quotas.
    pub async fn filter_rate_limited<'a>(
        &mut self,
        client: &AsyncRedisPool,
        quotas: &'a [RedisQuota<'a>],
        quantity: usize,
    ) -> Result<Vec<&'a RedisQuota<'a>>, RateLimitingError> {
        let mut connection = client.get_connection().await?;

        let mut rate_limited = vec![];
        let mut not_rate_limited = vec![];

        let min_by_keyref = quotas
            .iter()
            .into_grouping_map_by(|q| KeyRef::new(q))
            .min_by_key(|_, q| q.limit());

        for (key, quota) in min_by_keyref {
            let global_rate_limit = self.limits.entry_ref(&key).or_default();

            if global_rate_limit
                .check_rate_limited(&mut connection, quota, key, quantity as u64)
                .await?
            {
                rate_limited.push(quota);
            } else {
                not_rate_limited.push(quota);
            }
        }

        if rate_limited.is_empty() {
            for quota in not_rate_limited {
                if let Some(val) = self.limits.get_mut(&KeyRef::new(quota)) {
                    val.budget -= quantity as u64;
                }
            }
        }

        Ok(rate_limited)
    }
}

/// Reference to a rate limit key without requiring string allocation.
///
/// This struct is used to look up rate limits in a hashmap efficiently by
/// implementing the [`hashbrown::Equivalent`] trait.
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

/// Key for storing global quota-budgets locally.
///
/// This is used to identify unique rate limits in the local cache.
/// Note that this is distinct from the keys used in Redis.
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

/// Redis key representation for global rate limits.
///
/// This struct formats rate limit keys for use in Redis operations.
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

/// A single global rate limit.
///
/// This struct implements a local cache of a global rate limit. It reserves a budget
/// from Redis that can be consumed locally, reducing the number of Redis operations
/// required. When the local budget is exhausted, a new budget is requested from Redis.
///
/// This approach significantly reduces the overhead of checking rate limits while
/// still maintaining global synchronization across multiple instances.
#[derive(Debug)]
struct GlobalRateLimit {
    budget: u64,
    last_seen_redis_value: u64,
    slot: u64,
}

impl GlobalRateLimit {
    /// Creates a new [`GlobalRateLimit`] with empty budget.
    fn new() -> Self {
        Self {
            budget: 0,
            last_seen_redis_value: 0,
            slot: 0,
        }
    }

    /// Checks if the quota should be rate limited.
    ///
    /// Returns `true` if the quota has exceeded its limit and should be rate limited.
    /// This method handles time slot transitions and requests additional budget from
    /// Redis when necessary.
    pub async fn check_rate_limited(
        &mut self,
        connection: &mut AsyncRedisConnection,
        quota: &RedisQuota<'_>,
        key: KeyRef<'_>,
        quantity: u64,
    ) -> Result<bool, RateLimitingError> {
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
        let reserved = self
            .try_reserve(connection, quantity, quota, redis_key)
            .await
            .map_err(RateLimitingError::Redis)?;
        self.budget += reserved;

        Ok(self.budget < quantity)
    }

    /// Attempts to reserve budget from Redis for this rate limit.
    ///
    /// This method calculates how much budget to request based on the current needs
    /// and quota limits, then communicates with Redis to reserve this budget.
    async fn try_reserve(
        &mut self,
        connection: &mut AsyncRedisConnection,
        quantity: u64,
        quota: &RedisQuota<'_>,
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

        let (budget, value): (u64, u64) = RedisScripts::load_global_quota()
            .prepare_invoke()
            .key(redis_key.0)
            .arg(budget_to_reserve)
            .arg(quota.limit())
            .arg(quota.key_expiry())
            .invoke_async(connection)
            .await
            .map_err(RedisError::Redis)?;

        self.last_seen_redis_value = value;

        Ok(budget)
    }

    /// Calculates the default amount of budget to request from Redis.
    ///
    /// This method determines an appropriate budget size based on the quota limit
    /// and the configured budget ratio, balancing Redis communication overhead
    /// against local memory usage.
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

    use relay_base_schema::data_category::DataCategory;
    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_common::time::UnixTimestamp;
    use relay_redis::{AsyncRedisPool, RedisConfigOptions};

    use super::*;
    use crate::{DataCategories, Quota, QuotaScope, Scoping};

    fn build_redis_client() -> AsyncRedisPool {
        let url = std::env::var("RELAY_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());

        AsyncRedisPool::single(&url, &RedisConfigOptions::default()).unwrap()
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
            organization_id: OrganizationId::new(69420),
            project_id: ProjectId::new(42),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(4711),
        }
    }

    fn build_redis_quota<'a>(quota: &'a Quota, scoping: &'a Scoping) -> RedisQuota<'a> {
        let scoping = scoping.item(DataCategory::MetricBucket);
        RedisQuota::new(quota, scoping, UnixTimestamp::now()).unwrap()
    }

    #[tokio::test]
    async fn test_multiple_rate_limits() {
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

        let client = build_redis_client();
        let mut counter = GlobalRateLimiter::default();

        let rate_limited_quotas = counter
            .filter_rate_limited(&client, &redis_quotas, quantity)
            .await
            .unwrap();

        // Only the quotas that are less than the quantity gets rate_limited.
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
    #[tokio::test]
    async fn test_use_smaller_limit() {
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

        let client = build_redis_client();
        let mut counter = GlobalRateLimiter::default();

        let rate_limited_quotas = counter
            .filter_rate_limited(&client, &redis_quotas, (bigger_limit * 2) as usize)
            .await
            .unwrap();

        assert_eq!(rate_limited_quotas.len(), 1);

        assert_eq!(
            rate_limited_quotas.first().unwrap().limit(),
            smaller_limit as i64
        );
    }

    #[tokio::test]
    async fn test_global_rate_limit() {
        let limit = 200;

        let quota = build_quota(10, limit);
        let scoping = build_scoping();
        let redis_quota = [build_redis_quota(&quota, &scoping)];

        let client = build_redis_client();
        let mut counter = GlobalRateLimiter::default();

        let expected_rate_limit_result = [false, false, true, true].to_vec();

        // The limit is 200, while we take 90 at a time. So the first two times we call, we'll
        // still be under the limit. 90 < 200 -> 180 < 200 -> 270 > 200 -> 360 > 200.
        for should_rate_limit in expected_rate_limit_result {
            let is_rate_limited = counter
                .filter_rate_limited(&client, &redis_quota, 90)
                .await
                .unwrap();

            assert_eq!(should_rate_limit, !is_rate_limited.is_empty());
        }
    }

    #[tokio::test]
    async fn test_global_rate_limit_over_under() {
        let limit = 10;

        let quota = build_quota(10, limit);
        let scoping = build_scoping();

        let client = build_redis_client();
        let mut rl = GlobalRateLimiter::default();

        let redis_quota = [build_redis_quota(&quota, &scoping)];
        assert!(!rl
            .filter_rate_limited(&client, &redis_quota, 11)
            .await
            .unwrap()
            .is_empty());

        assert!(rl
            .filter_rate_limited(&client, &redis_quota, 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn test_multiple_global_rate_limit() {
        let limit = 91_337;

        let quota = build_quota(10, limit as u64);
        let scoping = build_scoping();
        let quota = [build_redis_quota(&quota, &scoping)];

        let client = build_redis_client();

        let mut counter1 = GlobalRateLimiter::default();
        let mut counter2 = GlobalRateLimiter::default();

        let mut total = 0;
        let mut total_counter_1 = 0;
        let mut total_counter_2 = 0;
        for i in 0.. {
            let quantity = i % 17;

            if counter1
                .filter_rate_limited(&client, &quota, quantity)
                .await
                .unwrap()
                .is_empty()
            {
                total += quantity;
                total_counter_1 += quantity;
            }

            if counter2
                .filter_rate_limited(&client, &quota, quantity)
                .await
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

    #[tokio::test]
    async fn test_global_rate_limit_slots() {
        let limit = 200;
        let window = 10;

        let ts = UnixTimestamp::now();
        let quota = build_quota(window, limit);
        let scoping = build_scoping();
        let item_scoping = scoping.item(DataCategory::MetricBucket);

        let client = build_redis_client();

        let mut rl = GlobalRateLimiter::default();

        let redis_quota = [RedisQuota::new(&quota, item_scoping, ts).unwrap()];
        assert!(rl
            .filter_rate_limited(&client, &redis_quota, 200)
            .await
            .unwrap()
            .is_empty());

        assert!(!rl
            .filter_rate_limited(&client, &redis_quota, 1)
            .await
            .unwrap()
            .is_empty());

        // Fast forward time.
        let redis_quota =
            [
                RedisQuota::new(&quota, item_scoping, ts + Duration::from_secs(window + 1))
                    .unwrap(),
            ];
        assert!(rl
            .filter_rate_limited(&client, &redis_quota, 200)
            .await
            .unwrap()
            .is_empty());

        assert!(!rl
            .filter_rate_limited(&client, &redis_quota, 1)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn test_global_rate_limit_infinite() {
        let limit = None;

        let timestamp = UnixTimestamp::now();

        let mut quota = build_quota(100, limit);
        let scoping = build_scoping();
        let item_scoping = scoping.item(DataCategory::MetricBucket);

        let client = build_redis_client();

        let mut rl = GlobalRateLimiter::default();

        let quantity = 2;
        let redis_threshold = (quantity as f32 / DEFAULT_BUDGET_RATIO) as u64;
        for _ in 0..redis_threshold + 10 {
            let redis_quota = RedisQuota::new(&quota, item_scoping, timestamp).unwrap();
            assert!(rl
                .filter_rate_limited(&client, &[redis_quota], quantity)
                .await
                .unwrap()
                .is_empty());
        }

        // Grab a new rate limiter and make sure even with the infinite limit,
        // the quantity was still synchronized via Redis.
        let mut rl = GlobalRateLimiter::default();

        quota.limit = Some(redis_threshold);
        let redis_quota = RedisQuota::new(&quota, item_scoping, timestamp).unwrap();

        assert!(!rl
            .filter_rate_limited(&client, &[redis_quota], quantity)
            .await
            .unwrap()
            .is_empty());
    }
}
