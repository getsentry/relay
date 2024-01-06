use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use relay_common::time::UnixTimestamp;
use relay_log::protocol::value;
use relay_redis::redis::Script;
use relay_redis::{PooledClient, RedisError, RedisPool};
use thiserror::Error;

use crate::quota::{ItemScoping, Quota, QuotaScope};
use crate::rate_limit::{RateLimit, RateLimits, RetryAfter};
use crate::REJECT_ALL_SECS;

/// The `grace` period allows accomodating for clock drift in TTL
/// calculation since the clock on the Redis instance used to store quota
/// metrics may not be in sync with the computer running this code.
const GRACE: u64 = 60;

const DEFAULT_BUDGET_REQUEST: usize = 100;

/// An error returned by `RedisRateLimiter`.
#[derive(Debug, Error)]
pub enum RateLimitingError {
    /// Failed to communicate with Redis.
    #[error("failed to communicate with redis")]
    Redis(#[source] RedisError),
}

fn load_lua_script() -> Script {
    Script::new(include_str!("is_rate_limited.lua"))
}

fn load_global_lua_script() -> Script {
    Script::new(include_str!("global_quota.lua"))
}

fn get_refunded_quota_key(counter_key: &str) -> String {
    format!("r:{counter_key}")
}

/// A transparent wrapper around an Option that only displays `Some`.
struct OptionalDisplay<T>(Option<T>);

impl<T> fmt::Display for OptionalDisplay<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(ref value) => write!(f, "{value}"),
            None => Ok(()),
        }
    }
}

/// Reference to information required for tracking quotas in Redis.
#[derive(Debug)]
struct RedisQuota<'a> {
    /// The original quota.
    quota: &'a Quota,
    /// Scopes of the item being tracked.
    scoping: ItemScoping<'a>,
    /// The Redis key prefix mapped from the quota id.
    prefix: &'a str,
    /// The redis window in seconds mapped from the quota.
    window: u64,
    /// The ingestion timestamp determining the rate limiting bucket.
    timestamp: UnixTimestamp,
}

impl<'a> RedisQuota<'a> {
    fn new(quota: &'a Quota, scoping: ItemScoping<'a>, timestamp: UnixTimestamp) -> Option<Self> {
        // These fields indicate that we *can* track this quota.
        let prefix = quota.id.as_deref()?;
        let window = quota.window?;

        Some(Self {
            quota,
            scoping,
            prefix,
            window,
            timestamp,
        })
    }

    /// Returns the limit value for Redis (`-1` for unlimited, otherwise the limit value).
    fn limit(&self) -> i64 {
        self.limit
            // If it does not fit into i64, treat as unlimited:
            .and_then(|limit| limit.try_into().ok())
            .unwrap_or(-1)
    }

    fn shift(&self) -> u64 {
        self.scoping.organization_id % self.window
    }

    fn slot(&self) -> u64 {
        (self.timestamp.as_secs() - self.shift()) / self.window
    }

    fn expiry(&self) -> UnixTimestamp {
        let next_slot = self.slot() + 1;
        let next_start = next_slot * self.window + self.shift();
        UnixTimestamp::from_secs(next_start)
    }

    fn key(&self) -> String {
        // The subscope id is only formatted into the key if the quota is not organization-scoped.
        // The organization id is always included.
        let subscope = match self.quota.scope {
            QuotaScope::Organization => None,
            scope => self.scoping.scope_id(scope),
        };

        // 0 is arbitrary, we just need to ensure global quotas from different orgs have the same key.
        let org = if self.quota.scope == QuotaScope::Global {
            0
        } else {
            self.scoping.organization_id
        };

        format!(
            "quota:{id}{{{org}}}{subscope}:{slot}",
            id = self.prefix,
            org = org,
            subscope = OptionalDisplay(subscope),
            slot = self.slot(),
        )
    }
}

impl std::ops::Deref for RedisQuota<'_> {
    type Target = Quota;

    fn deref(&self) -> &Self::Target {
        self.quota
    }
}

#[derive(Debug, thiserror::Error)]
enum GlobalRateLimitError {
    #[error("quota key missing")]
    KeyMissing,
    #[error("failed to access RWLock")]
    PoisonedLock,
    #[error("slot expired")]
    SlotExpired,
    #[error("budget empty")]
    BudgetEmpty,
    #[error("failed to communicate with redis")]
    Redis,
}

/// Counters used as a cache for global quotas.
///
/// We use an RwLock because the counter itself is an `AtomicUsize`. Atomics can be modified with
/// a shared reference, meaning that we can have multiple threads updating the counter concurrently,
/// as long as there's no writers.
#[derive(Clone)]
struct GlobalCounters {
    script: Arc<Script>,
    counters: Arc<RwLock<hashbrown::HashMap<BudgetKey, BudgetState>>>,
}

impl Default for GlobalCounters {
    fn default() -> Self {
        Self {
            script: Arc::new(load_global_lua_script()),
            counters: Default::default(),
        }
    }
}

impl GlobalCounters {
    /// Returns `true` if the global quota should be ratelimited.
    ///
    /// We first check the local limits if we have any budget left. If we don't, or the
    /// slot is expired, we have to sync with redis, after which we try again.
    fn evaluate_quota(
        &self,
        client: &mut PooledClient,
        quota: &RedisQuota,
        quantity: usize,
    ) -> Result<bool, GlobalRateLimitError> {
        debug_assert!(quota.scope == QuotaScope::Global);

        match self.decrement_budget(quota, quantity) {
            ok @ Ok(_) => return ok,
            err @ Err(GlobalRateLimitError::Redis | GlobalRateLimitError::PoisonedLock) => {
                return err
            }
            Err(
                GlobalRateLimitError::BudgetEmpty
                | GlobalRateLimitError::KeyMissing
                | GlobalRateLimitError::SlotExpired,
            ) => self.redis_sync(client, quota, quantity)?,
        };

        self.decrement_budget(quota, quantity)
    }

    /// Attempts to decrement the budget by a certain amount.
    ///
    /// Returns `true` if items should be ratelimited.
    fn decrement_budget(
        &self,
        quota: &RedisQuota,
        decrement_qty: usize,
    ) -> Result<bool, GlobalRateLimitError> {
        let key = BudgetKeyRef::new(quota);
        let current_slot = quota.slot() as usize;
        let Some(limit) = quota.limit.map(|limit| limit as usize) else {
            return Ok(false);
        };

        let map = self
            .counters
            .read()
            .map_err(|_| GlobalRateLimitError::PoisonedLock)?;
        let val = map.get(&key).ok_or(GlobalRateLimitError::KeyMissing)?;

        if current_slot != val.slot.load(Ordering::SeqCst) {
            // Expired slot implies the the time window has passed, and we need to
            // ask for a new budget for the new slot.
            return Err(GlobalRateLimitError::SlotExpired);
        }

        let redis_count = val.last_seen_redis_value.load(Ordering::SeqCst);
        let mut current_budget = val.budget.load(Ordering::SeqCst);

        // Since the redis count was pre-incremented by the amount of budget we took, the actual
        // total count have to subtract the current budget.
        let total_count = redis_count.saturating_sub(current_budget);

        if total_count >= limit {
            return Ok(true);
        }

        loop {
            if current_budget < decrement_qty {
                return Err(GlobalRateLimitError::BudgetEmpty);
            }

            // When setting the new value, we must ensure that it hasn't already been
            // decremented in another thread.
            //
            // https://doc.rust-lang.org/std/sync/atomic/struct.AtomicU64.html#method.compare_exchange_weak
            if val
                .budget
                .compare_exchange_weak(
                    current_budget,
                    current_budget - decrement_qty,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                return Ok(false);
            }

            current_budget = val.budget.load(Ordering::SeqCst);
        }
    }

    /// Ensures the local counter is up to date with the redis counter.
    fn redis_sync(
        &self,
        client: &mut PooledClient,
        quota: &RedisQuota,
        quantity: usize,
    ) -> Result<(), GlobalRateLimitError> {
        // We don't want to reject a metrics batch just because it's too large, so we increase the
        // requested budget if the default is less than the metric bucket quantity.
        let requested_budget = std::cmp::max(DEFAULT_BUDGET_REQUEST, quantity);
        let redis_key = quota.key();
        let budget_key = BudgetKeyRef::new(quota);

        let (new_budget, current_redis_count): (usize, usize) = self
            .script
            .prepare_invoke()
            .key(redis_key.as_str())
            .arg(quota.limit)
            .arg(quota.expiry().as_secs() + GRACE)
            .arg(requested_budget)
            .invoke(
                &mut client
                    .connection()
                    .map_err(|_| GlobalRateLimitError::Redis)?,
            )
            .map_err(|_| GlobalRateLimitError::Redis)?;

        self.update_limit(
            budget_key,
            quota.slot() as usize,
            new_budget,
            current_redis_count,
        )
    }

    /// Updates the local counter after having synced with redis.
    fn update_limit(
        &self,
        budget_key: BudgetKeyRef,
        current_slot: usize,
        new_budget: usize,
        redis_value: usize,
    ) -> Result<(), GlobalRateLimitError> {
        match self
            .counters
            .read()
            .map_err(|_| GlobalRateLimitError::PoisonedLock)?
            .get(&budget_key)
        {
            // If the key/val exist, we can update the values with just a read lock. This allows
            // the counter to operate concurrently in different threads.
            Some(val) => {
                let old_budget = if val.slot.load(Ordering::SeqCst) == current_slot {
                    val.budget.load(Ordering::SeqCst)
                } else {
                    0
                };

                val.budget.store(new_budget + old_budget, Ordering::SeqCst);
                val.slot.store(current_slot, Ordering::SeqCst);
                val.last_seen_redis_value
                    .store(redis_value, Ordering::SeqCst);
            }
            // If no key/val exist, we have to momentarily block the counter in other threads.
            None => {
                self.counters
                    .write()
                    .map_err(|_| GlobalRateLimitError::PoisonedLock)?
                    .insert(
                        budget_key.to_owned(),
                        BudgetState::new(new_budget, redis_value, current_slot),
                    );
            }
        };

        Ok(())
    }
}

/// Key for storing global quota-budgets locally.
///
/// Note: must not be used in redis. For that, use RedisQuota.key().
#[derive(Clone, PartialEq, Eq, Hash)]
struct BudgetKey {
    prefix: String,
    window: u64,
}

/// Used to look up a hashmap of [`BudgetKey`]-keys without a string allocation.
///
/// This works due to the 'Equivalent' trait in the hashbrown crate.
#[derive(Clone, Copy, Hash)]
struct BudgetKeyRef<'a> {
    prefix: &'a str,
    window: u64,
}

impl<'a> BudgetKeyRef<'a> {
    fn new(quota: &'a RedisQuota) -> Self {
        Self {
            prefix: quota.prefix,
            window: quota.window,
        }
    }

    fn to_owned(self) -> BudgetKey {
        BudgetKey {
            prefix: self.prefix.to_owned(),
            window: self.window,
        }
    }
}

impl hashbrown::Equivalent<BudgetKey> for BudgetKeyRef<'_> {
    fn equivalent(&self, key: &BudgetKey) -> bool {
        self.prefix == key.prefix && self.window == key.window
    }
}

/// Represents the local budget taken from a global quota.
///
/// It works by taking a certain amount of quota by pre-incrementing the redis counter before
/// actually using it. Then we count down to zero before we ask for more. We store the value of the
/// redis counter as it was when we previously took a budget, so that if we the count reach zero
/// but we know the redis counter is still above the limit, we won't ask for more from redis.
/// The counter works per slot, so when there's a new slot this value will be invalidated and we
/// will check with redis again and create a new [`BudgetState`] for the local counter.
struct BudgetState {
    budget: AtomicUsize,
    last_seen_redis_value: AtomicUsize,
    slot: AtomicUsize,
}

impl BudgetState {
    fn new(budget: usize, redis_value: usize, slot: usize) -> Self {
        Self {
            budget: AtomicUsize::new(budget),
            last_seen_redis_value: AtomicUsize::new(redis_value),
            slot: AtomicUsize::new(slot),
        }
    }
}

/// A service that executes quotas and checks for rate limits in a shared cache.
///
/// Quotas handle tracking a project's usage and respond whether or not a project has been
/// configured to throttle incoming data if they go beyond the specified quota.
///
/// Quotas can specify a window to be tracked in, such as per minute or per hour. Additionally,
/// quotas allow to specify the data categories they apply to, for example error events or
/// attachments. For more information on quota parameters, see `QuotaConfig`.
///
/// Requires the `redis` feature.
#[derive(Clone)]
pub struct RedisRateLimiter {
    pool: RedisPool,
    script: Arc<Script>,
    max_limit: Option<u64>,
    counters: GlobalCounters,
}

impl RedisRateLimiter {
    /// Creates a new `RedisRateLimiter` instance.
    pub fn new(pool: RedisPool) -> Self {
        RedisRateLimiter {
            pool,
            script: Arc::new(load_lua_script()),
            max_limit: None,
            counters: GlobalCounters::default(),
        }
    }

    /// Sets the maximum rate limit in seconds.
    ///
    /// By default, this rate limiter will return rate limits based on the quotas' `window` fields.
    /// If a maximum rate limit is set, this limit is bounded.
    pub fn max_limit(mut self, max_limit: Option<u64>) -> Self {
        self.max_limit = max_limit;
        self
    }

    /// Checks whether any of the quotas in effect for the given project and project key has been
    /// exceeded and records consumption of the quota.
    ///
    /// By invoking this method, the caller signals that data is being ingested and needs to be
    /// counted against the quota. This increment happens atomically if none of the quotas have been
    /// exceeded. Otherwise, a rate limit is returned and data is not counted against the quotas.
    ///
    /// If no key is specified, then only organization-wide and project-wide quotas are checked. If
    /// a key is specified, then key-quotas are also checked.
    ///
    /// If the current consumed quotas are still under the limit and the current quantity would put
    /// it over the limit, which normaly would return the _rejection_, setting `over_accept_once`
    /// to `true` will allow accept the incoming data even if the limit is exceeded once.
    ///
    /// The passed `quantity` may be `0`. In this case, the rate limiter will check if the quota
    /// limit has been reached or exceeded without incrementing it in the success case. This can be
    /// useful to check for required quotas in a different data category.
    pub fn is_rate_limited(
        &self,
        quotas: &[Quota],
        item_scoping: ItemScoping<'_>,
        quantity: usize,
        over_accept_once: bool,
    ) -> Result<RateLimits, RateLimitingError> {
        let mut client = self.pool.client().map_err(RateLimitingError::Redis)?;
        let timestamp = UnixTimestamp::now();
        let mut invocation = self.script.prepare_invoke();
        let mut tracked_quotas = Vec::new();
        let mut rate_limits = RateLimits::new();

        for quota in quotas {
            if !quota.matches(item_scoping) {
                // Silently skip all quotas that do not apply to this item.
            } else if quota.limit == Some(0) {
                // A zero-sized quota is strongest. Do not call into Redis at all, and do not
                // increment any keys, as one quota has reached capacity (this is how regular quotas
                // behave as well).
                let retry_after = self.retry_after(REJECT_ALL_SECS);
                rate_limits.add(RateLimit::from_quota(quota, &item_scoping, retry_after));
            } else if let Some(quota) = RedisQuota::new(quota, item_scoping, timestamp) {
                if quota.scope == QuotaScope::Global {
                    match self.counters.evaluate_quota(&mut client, &quota, quantity) {
                        Ok(true) => {
                            rate_limits.add(RateLimit::from_quota(
                                &quota,
                                item_scoping.scoping,
                                self.retry_after((quota.expiry() - timestamp).as_secs()),
                            ));
                        }
                        Ok(false) => continue,
                        Err(e) => relay_log::error!(
                            error = &e as &dyn std::error::Error,
                            "failed to check global rate limit"
                        ),
                    }
                } else {
                    let key = quota.key();
                    // Remaining quotas are expected to be trackable in Redis.
                    let refund_key = get_refunded_quota_key(&key);

                    invocation.key(key);
                    invocation.key(refund_key);

                    invocation.arg(quota.limit());
                    invocation.arg(quota.expiry().as_secs() + GRACE);
                    invocation.arg(quantity);
                    invocation.arg(over_accept_once);

                    tracked_quotas.push(quota);
                }
            } else {
                // This quota is neither a static reject-all, nor can it be tracked in Redis due to
                // missing fields. We're skipping this for forward-compatibility.
                relay_log::with_scope(
                    |scope| scope.set_extra("quota", value::to_value(quota).unwrap()),
                    || relay_log::warn!("skipping unsupported quota"),
                )
            }
        }

        // Either there are no quotas to run against Redis, or we already have a rate limit from a
        // zero-sized quota. In either cases, skip invoking the script and return early.
        if tracked_quotas.is_empty() || rate_limits.is_limited() {
            return Ok(rate_limits);
        }

        let rejections: Vec<bool> = invocation
            .invoke(&mut client.connection().map_err(RateLimitingError::Redis)?)
            .map_err(RedisError::Redis)
            .map_err(RateLimitingError::Redis)?;

        for (quota, is_rejected) in tracked_quotas.iter().zip(rejections) {
            if is_rejected {
                let retry_after = self.retry_after((quota.expiry() - timestamp).as_secs());
                rate_limits.add(RateLimit::from_quota(quota, &item_scoping, retry_after));
            }
        }

        Ok(rate_limits)
    }

    /// Creates a rate limit bounded by `max_limit`.
    fn retry_after(&self, mut seconds: u64) -> RetryAfter {
        if let Some(max_limit) = self.max_limit {
            seconds = std::cmp::min(seconds, max_limit);
        }

        RetryAfter::from_secs(seconds)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_redis::redis::Commands;
    use relay_redis::RedisConfigOptions;

    use super::*;
    use crate::quota::{DataCategories, DataCategory, ReasonCode, Scoping};
    use crate::rate_limit::RateLimitScope;

    fn build_rate_limiter() -> RedisRateLimiter {
        let url = std::env::var("RELAY_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());

        RedisRateLimiter {
            pool: RedisPool::single(&url, RedisConfigOptions::default()).unwrap(),
            script: Arc::new(load_lua_script()),
            max_limit: None,
            counters: GlobalCounters::default(),
        }
    }

    #[test]
    fn test_zero_size_quotas() {
        let quotas = &[
            Quota {
                id: None,
                categories: DataCategories::new(),
                scope: QuotaScope::Organization,
                scope_id: None,
                limit: Some(0),
                window: None,
                reason_code: Some(ReasonCode::new("get_lost")),
            },
            Quota {
                id: Some("42".to_owned()),
                categories: DataCategories::new(),
                scope: QuotaScope::Organization,
                scope_id: None,
                limit: None,
                window: Some(42),
                reason_code: Some(ReasonCode::new("unlimited")),
            },
        ];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
        };

        let rate_limits: Vec<RateLimit> = build_rate_limiter()
            .is_rate_limited(quotas, scoping, 1, false)
            .expect("rate limiting failed")
            .into_iter()
            .collect();

        assert_eq!(
            rate_limits,
            vec![RateLimit {
                categories: DataCategories::new(),
                scope: RateLimitScope::Organization(42),
                reason_code: Some(ReasonCode::new("get_lost")),
                retry_after: rate_limits[0].retry_after,
            }]
        );
    }

    #[test]
    fn test_simple_quota() {
        let quotas = &[Quota {
            id: Some(format!("test_simple_quota_{:?}", SystemTime::now())),
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(5),
            window: Some(60),
            reason_code: Some(ReasonCode::new("get_lost")),
        }];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
        };

        let rate_limiter = build_rate_limiter();

        for i in 0..10 {
            let rate_limits: Vec<RateLimit> = rate_limiter
                .is_rate_limited(quotas, scoping, 1, false)
                .expect("rate limiting failed")
                .into_iter()
                .collect();

            if i >= 5 {
                assert_eq!(
                    rate_limits,
                    vec![RateLimit {
                        categories: DataCategories::new(),
                        scope: RateLimitScope::Organization(42),
                        reason_code: Some(ReasonCode::new("get_lost")),
                        retry_after: rate_limits[0].retry_after,
                    }]
                );
            } else {
                assert_eq!(rate_limits, vec![]);
            }
        }
    }

    #[test]
    fn test_quantity_0() {
        let quotas = &[Quota {
            id: Some(format!("test_quantity_0_{:?}", SystemTime::now())),
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(1),
            window: Some(60),
            reason_code: Some(ReasonCode::new("get_lost")),
        }];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
        };

        let rate_limiter = build_rate_limiter();

        // limit is 1, so first call not rate limited
        assert!(!rate_limiter
            .is_rate_limited(quotas, scoping, 1, false)
            .unwrap()
            .is_limited());

        // quota is now exhausted
        assert!(rate_limiter
            .is_rate_limited(quotas, scoping, 1, false)
            .unwrap()
            .is_limited());

        // quota is exhausted, regardless of the quantity
        assert!(rate_limiter
            .is_rate_limited(quotas, scoping, 0, false)
            .unwrap()
            .is_limited());

        // quota is exhausted, regardless of the quantity
        assert!(rate_limiter
            .is_rate_limited(quotas, scoping, 1, false)
            .unwrap()
            .is_limited());
    }

    #[test]
    fn test_quota_go_over() {
        let quotas = &[Quota {
            id: Some(format!("test_quota_go_over{:?}", SystemTime::now())),
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(2),
            window: Some(60),
            reason_code: Some(ReasonCode::new("get_lost")),
        }];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
        };

        let rate_limiter = build_rate_limiter();

        // limit is 2, so first call not rate limited
        let is_limited = rate_limiter
            .is_rate_limited(quotas, scoping, 1, true)
            .unwrap()
            .is_limited();
        assert!(!is_limited);

        // go over limit, but first call is over-accepted
        let is_limited = rate_limiter
            .is_rate_limited(quotas, scoping, 2, true)
            .unwrap()
            .is_limited();
        assert!(!is_limited);

        // quota is exhausted, regardless of the quantity
        let is_limited = rate_limiter
            .is_rate_limited(quotas, scoping, 0, true)
            .unwrap()
            .is_limited();
        assert!(is_limited);

        // quota is exhausted, regardless of the quantity
        let is_limited = rate_limiter
            .is_rate_limited(quotas, scoping, 1, true)
            .unwrap()
            .is_limited();
        assert!(is_limited);
    }

    #[test]
    fn test_bails_immediately_without_any_quota() {
        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
        };

        let rate_limits: Vec<RateLimit> = build_rate_limiter()
            .is_rate_limited(&[], scoping, 1, false)
            .expect("rate limiting failed")
            .into_iter()
            .collect();

        assert_eq!(rate_limits, vec![]);
    }

    #[test]
    fn test_limited_with_unlimited_quota() {
        let quotas = &[
            Quota {
                id: Some("q0".to_string()),
                categories: DataCategories::new(),
                scope: QuotaScope::Organization,
                scope_id: None,
                limit: None,
                window: Some(1),
                reason_code: Some(ReasonCode::new("project_quota0")),
            },
            Quota {
                id: Some("q1".to_string()),
                categories: DataCategories::new(),
                scope: QuotaScope::Organization,
                scope_id: None,
                limit: Some(1),
                window: Some(1),
                reason_code: Some(ReasonCode::new("project_quota1")),
            },
        ];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
        };

        let rate_limiter = build_rate_limiter();

        for i in 0..1 {
            let rate_limits: Vec<RateLimit> = rate_limiter
                .is_rate_limited(quotas, scoping, 1, false)
                .expect("rate limiting failed")
                .into_iter()
                .collect();

            if i == 0 {
                assert_eq!(rate_limits, &[]);
            } else {
                assert_eq!(
                    rate_limits,
                    vec![RateLimit {
                        categories: DataCategories::new(),
                        scope: RateLimitScope::Organization(42),
                        reason_code: Some(ReasonCode::new("project_quota1")),
                        retry_after: rate_limits[0].retry_after,
                    }]
                );
            }
        }
    }

    #[test]
    fn test_quota_with_quantity() {
        let quotas = &[Quota {
            id: Some(format!("test_quantity_quota_{:?}", SystemTime::now())),
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(500),
            window: Some(60),
            reason_code: Some(ReasonCode::new("get_lost")),
        }];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
        };

        let rate_limiter = build_rate_limiter();

        for i in 0..10 {
            let rate_limits: Vec<RateLimit> = rate_limiter
                .is_rate_limited(quotas, scoping, 100, false)
                .expect("rate limiting failed")
                .into_iter()
                .collect();

            if i >= 5 {
                assert_eq!(
                    rate_limits,
                    vec![RateLimit {
                        categories: DataCategories::new(),
                        scope: RateLimitScope::Organization(42),
                        reason_code: Some(ReasonCode::new("get_lost")),
                        retry_after: rate_limits[0].retry_after,
                    }]
                );
            } else {
                assert_eq!(rate_limits, vec![]);
            }
        }
    }

    #[test]
    fn test_get_redis_key_scoped() {
        let quota = Quota {
            id: Some("foo".to_owned()),
            categories: DataCategories::new(),
            scope: QuotaScope::Project,
            scope_id: Some("42".to_owned()),
            window: Some(2),
            limit: Some(0),
            reason_code: None,
        };

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 69420,
                project_id: ProjectId::new(42),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(4711),
            },
        };

        let timestamp = UnixTimestamp::from_secs(123_123_123);
        let redis_quota = RedisQuota::new(&quota, scoping, timestamp).unwrap();
        assert_eq!(redis_quota.key(), "quota:foo{69420}42:61561561");
    }

    #[test]
    fn test_get_redis_key_unscoped() {
        let quota = Quota {
            id: Some("foo".to_owned()),
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            window: Some(10),
            limit: Some(0),
            reason_code: None,
        };

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 69420,
                project_id: ProjectId::new(42),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(4711),
            },
        };

        let timestamp = UnixTimestamp::from_secs(234_531);
        let redis_quota = RedisQuota::new(&quota, scoping, timestamp).unwrap();
        assert_eq!(redis_quota.key(), "quota:foo{69420}:23453");
    }

    #[test]
    fn test_large_redis_limit_large() {
        let quota = Quota {
            id: Some("foo".to_owned()),
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            window: Some(10),
            limit: Some(9223372036854775808), // i64::MAX + 1
            reason_code: None,
        };

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 69420,
                project_id: ProjectId::new(42),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(4711),
            },
        };

        let timestamp = UnixTimestamp::from_secs(234_531);
        let redis_quota = RedisQuota::new(&quota, scoping, timestamp).unwrap();
        assert_eq!(redis_quota.limit(), -1);
    }

    #[test]
    #[allow(clippy::disallowed_names, clippy::let_unit_value)]
    fn test_is_rate_limited_script() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap();

        let rate_limiter = build_rate_limiter();
        let mut client = rate_limiter.pool.client().expect("get client");
        let mut conn = client.connection().expect("Redis connection");

        // define a few keys with random seed such that they do not collide with repeated test runs
        let foo = format!("foo___{now}");
        let r_foo = format!("r:foo___{now}");
        let bar = format!("bar___{now}");
        let r_bar = format!("r:bar___{now}");
        let apple = format!("apple___{now}");
        let orange = format!("orange___{now}");
        let baz = format!("baz___{now}");

        let script = load_lua_script();

        let mut invocation = script.prepare_invoke();
        invocation
            .key(&foo) // key
            .key(&r_foo) // refund key
            .key(&bar) // key
            .key(&r_bar) // refund key
            .arg(1) // limit
            .arg(now + 60) // expiry
            .arg(1) // quantity
            .arg(false) // over accept once
            .arg(2) // limit
            .arg(now + 120) // expiry
            .arg(1) // quantity
            .arg(false); // over accept once

        // The item should not be rate limited by either key.
        assert_eq!(
            invocation.invoke::<Vec<bool>>(&mut conn).unwrap(),
            vec![false, false]
        );

        // The item should be rate limited by the first key (1).
        assert_eq!(
            invocation.invoke::<Vec<bool>>(&mut conn).unwrap(),
            vec![true, false]
        );

        // The item should still be rate limited by the first key (1), but *not*
        // rate limited by the second key (2) even though this is the third time
        // we've checked the quotas. This ensures items that are rejected by a lower
        // quota don't affect unrelated items that share a parent quota.
        assert_eq!(
            invocation.invoke::<Vec<bool>>(&mut conn).unwrap(),
            vec![true, false]
        );

        assert_eq!(conn.get::<_, String>(&foo).unwrap(), "1");
        let ttl: u64 = conn.ttl(&foo).unwrap();
        assert!(ttl >= 59);
        assert!(ttl <= 60);

        assert_eq!(conn.get::<_, String>(&bar).unwrap(), "1");
        let ttl: u64 = conn.ttl(&bar).unwrap();
        assert!(ttl >= 119);
        assert!(ttl <= 120);

        // make sure "refund/negative" keys haven't been incremented
        let () = conn.get(r_foo).unwrap();
        let () = conn.get(r_bar).unwrap();

        // Test that refunded quotas work
        let () = conn.set(&apple, 5).unwrap();

        let mut invocation = script.prepare_invoke();
        invocation
            .key(&orange) // key
            .key(&baz) // refund key
            .arg(1) // limit
            .arg(now + 60) // expiry
            .arg(1) // quantity
            .arg(false);

        // increment
        assert_eq!(
            invocation.invoke::<Vec<bool>>(&mut conn).unwrap(),
            vec![false]
        );

        // test that it's rate limited without refund
        assert_eq!(
            invocation.invoke::<Vec<bool>>(&mut conn).unwrap(),
            vec![true]
        );

        let mut invocation = script.prepare_invoke();
        invocation
            .key(&orange) // key
            .key(&apple) // refund key
            .arg(1) // limit
            .arg(now + 60) // expiry
            .arg(1) // quantity
            .arg(false);

        // test that refund key is used
        assert_eq!(
            invocation.invoke::<Vec<bool>>(&mut conn).unwrap(),
            vec![false]
        );
    }
}
