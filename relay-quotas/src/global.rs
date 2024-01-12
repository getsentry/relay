use std::sync::{Arc, Mutex, OnceLock, RwLock};

use relay_common::time::UnixTimestamp;
use relay_redis::redis::Script;
use relay_redis::{PooledClient, RedisError};

use crate::RedisQuota;

fn load_global_lua_script() -> &'static Script {
    static SCRIPT: OnceLock<Script> = OnceLock::new();
    SCRIPT.get_or_init(|| Script::new(include_str!("global_quota.lua")))
}

fn current_slot(window: u64) -> usize {
    UnixTimestamp::now()
        .as_secs()
        .checked_div(window)
        .unwrap_or_default() as usize
}

/// Counters used as a cache for global quotas.
///
/// When we want to ratelimit across all relay-instances, we need to use redis to synchronize.
/// Calling Redis every time we want to check if an item should be ratelimited would be very expensive,
/// which is why we have this cache. It works by 'taking' a certain budget from redis, by pre-incrementing
/// a global counter. We Put the amount we pre-incremented into this local cache and count down until
/// we have no more budget, then we ask for more from redis. If we find the global counter is above
/// the quota limit, we will ratelimit the item.
#[derive(Default)]
pub struct GlobalRateLimits {
    limits: RwLock<hashbrown::HashMap<BudgetKey, Arc<Mutex<GlobalRateLimit>>>>,
}

impl GlobalRateLimits {
    /// Returns `true` if the global quota should be ratelimited.
    ///
    /// Certain errors can be resolved by syncing to redis, so in those cases
    /// we try again to decrement the budget after syncing.
    pub fn is_rate_limited(
        &self,
        client: &mut PooledClient,
        quota: &RedisQuota,
        quantity: usize,
    ) -> Result<bool, RedisError> {
        let Some(limit) = quota.limit else {
            return Ok(false);
        };

        let key = BudgetKeyRef::new(quota);

        let opt_val = self.limits.read().unwrap().get(&key).cloned();
        let val = match opt_val {
            Some(val) => val,
            None => Arc::clone(self.limits.write().unwrap().entry_ref(&key).or_default()),
        };

        let mut lock = val.lock().unwrap();
        lock.is_rate_limited(client, quota, quantity, limit)
    }
}

/// Key for storing global quota-budgets locally.
///
/// Note: must not be used in redis. For that, use RedisQuota.key().
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BudgetKey {
    prefix: String,
    window: u64,
}

impl From<&BudgetKeyRef<'_>> for BudgetKey {
    fn from(value: &BudgetKeyRef<'_>) -> Self {
        BudgetKey {
            prefix: value.prefix.to_owned(),
            window: value.window,
        }
    }
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
            prefix: quota.prefix(),
            window: quota.window(),
        }
    }
}

impl hashbrown::Equivalent<BudgetKey> for BudgetKeyRef<'_> {
    fn equivalent(&self, key: &BudgetKey) -> bool {
        self.prefix == key.prefix && self.window == key.window
    }
}

/// Represents the local budget taken from a global quota.
struct GlobalRateLimit {
    local_counter: LocalCounter,
    redis_counter: RedisCounter,
    slot: usize,
}

impl GlobalRateLimit {
    pub fn new() -> Self {
        Self {
            local_counter: LocalCounter::new(),
            redis_counter: RedisCounter::new(),
            slot: 0,
        }
    }

    pub fn is_rate_limited(
        &mut self,
        client: &mut PooledClient,
        quota: &RedisQuota,
        quantity: usize,
        limit: u64,
    ) -> Result<bool, RedisError> {
        let current_slot = current_slot(quota.window());

        if current_slot < self.slot {
            // TODO double check logic here
            // in theory this should never happen only if someone messes with the system time
            // be safe and dont rate limit
            relay_log::error!("budget slot ahead of current slot");
            return Ok(false);
        }

        if current_slot > self.slot {
            // We moved to the next slot and need to reset the local counter,
            // Redis already handles this by having the slot in the key.
            self.local_counter = LocalCounter::new();
            self.slot = current_slot;
        }

        if self.local_counter.try_consume(quantity) {
            return Ok(false);
        }

        if !self.redis_counter.can_satisfy(quantity, limit) {
            return Ok(true);
        }

        let budget_to_reserve = quantity.max(self.default_request_size_based_on_limit());
        let reserved = self
            .redis_counter
            .try_reserve(client, budget_to_reserve, limit, quota)? as usize;

        self.local_counter.increase_budget(reserved);
        Ok(!self.local_counter.try_consume(quantity))
    }

    fn default_request_size_based_on_limit(&self) -> usize {
        100
    }
}

impl Default for GlobalRateLimit {
    fn default() -> Self {
        Self::new()
    }
}

struct LocalCounter {
    budget: usize,
}

impl LocalCounter {
    fn new() -> Self {
        Self { budget: 0 }
    }

    fn try_consume(&mut self, quantity: usize) -> bool {
        if self.budget >= quantity {
            self.budget -= quantity;
            true
        } else {
            false
        }
    }

    fn increase_budget(&mut self, quantity: usize) {
        self.budget = self.budget.saturating_add(quantity);
    }
}

struct RedisCounter {
    last_seen: u64,
}

impl RedisCounter {
    fn new() -> Self {
        Self { last_seen: 0 }
    }

    fn can_satisfy(&self, quantity: usize, limit: u64) -> bool {
        self.last_seen + quantity as u64 <= limit
    }

    fn try_reserve(
        &mut self,
        client: &mut PooledClient,
        quantity: usize,
        limit: u64,
        quota: &RedisQuota,
    ) -> Result<u64, RedisError> {
        let script = load_global_lua_script();

        let redis_key = quota.key();
        let expiry = UnixTimestamp::now().as_secs() + quota.window();

        let (budget, redis_count): (u64, u64) = script
            .prepare_invoke()
            .key(redis_key.as_str())
            .arg(limit)
            .arg(expiry)
            .arg(quantity)
            .invoke(&mut client.connection()?)
            .map_err(RedisError::Redis)?;

        self.last_seen = redis_count;

        Ok(budget)
    }
}
