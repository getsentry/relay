use std::cmp::Ordering;
use std::sync::{Mutex, OnceLock, RwLock};

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
    counters: RwLock<hashbrown::HashMap<BudgetKey, Mutex<SlottedFooBar>>>,
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
        let key = BudgetKeyRef::new(quota);

        if let Some(foobar) = self.counters.read().unwrap().get(&key) {
            let mut foobar = foobar.lock().unwrap();
            foobar.is_rate_limited(client, quota, quantity)
        } else {
            self.counters.write().unwrap().entry_ref(&key).or_default();

            // TODO think about the recursion
            self.is_rate_limited(client, quota, quantity)
        }
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

trait Counter {}

struct SlottedFooBar {
    slot: usize,
    foobar: FooBar,
}

impl SlottedFooBar {
    pub fn new() -> Self {
        Self {
            slot: 0,
            foobar: FooBar::new(),
        }
    }

    pub fn is_rate_limited(
        &mut self,
        client: &mut PooledClient,
        quota: &RedisQuota,
        quantity: usize,
    ) -> Result<bool, RedisError> {
        let quota_slot = current_slot(quota.window());

        match self.slot.cmp(&quota_slot) {
            Ordering::Greater => {
                // TODO double check logic here
                // in theory this should never happen only if someone messes with the system time
                // be safe and dont rate limit
                return Ok(false);
            }
            Ordering::Less => self.foobar = FooBar::new(),
            Ordering::Equal => {}
        }

        self.foobar.is_rate_limited(client, quota, quantity)
    }
}

impl Default for SlottedFooBar {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents the local budget taken from a global quota.
struct FooBar {
    local_counter: LocalCounter,
    redis_counter: RedisCounter,
}

impl FooBar {
    pub fn new() -> Self {
        Self {
            local_counter: LocalCounter::new(),
            redis_counter: RedisCounter::new(),
        }
    }

    pub fn is_rate_limited(
        &mut self,
        client: &mut PooledClient,
        quota: &RedisQuota,
        quantity: usize,
    ) -> Result<bool, RedisError> {
        let limit = quota.limit.unwrap(); // TODO should maybe an argument, THINK ABOUT THIS

        if self.local_counter.try_consume(quantity) {
            return Ok(false);
        }

        // TODO
        if !self.redis_counter.can_satisfy(quantity, limit) {
            return Ok(true);
        }

        let budget_to_reserve = quantity.max(self.default_request_size_based_on_limit());
        let reserved = self.redis_counter.try_reserve(budget_to_reserve, limit) as usize;

        self.local_counter.increase_budget(reserved);
        Ok(self.local_counter.try_consume(quantity))
    }

    fn default_request_size_based_on_limit(&self) -> usize {
        // TODO
        100
    }
}

struct LocalCounter {
    budget: usize,
}

impl LocalCounter {
    fn new() -> Self {
        Self { budget: 0 }
    }

    ///
    fn try_consume(&mut self, quantity: usize) -> bool {
        if self.budget >= quantity {
            self.budget -= quantity;
            true
        } else {
            false
        }
    }

    fn increase_budget(&mut self, quantity: usize) {
        let _ = self.budget.saturating_add(quantity);
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

    fn try_reserve(&mut self, quantity: usize, limit: u64) -> u64 {
        let _script = load_global_lua_script();

        todo!()
    }
}
