use std::fmt::Debug;
use std::sync::{Arc, Mutex, RwLock};

use relay_common::time::UnixTimestamp;
use relay_redis::redis::Script;
use relay_redis::PooledClient;

use crate::RedisQuota;

fn current_slot(window: u64) -> usize {
    UnixTimestamp::now()
        .as_secs()
        .checked_div(window)
        .unwrap_or_default() as usize
}

const DEFAULT_BUDGET_REQUEST: usize = 100;

/// The possible errors from checking global rate limits.
#[derive(thiserror::Error, Debug)]
pub enum GlobalCountErrors {
    /// Failure in Redis communication.
    #[error("failed to communicate with redis")]
    Redis,

    /// Invalid budget slot.
    #[error("budget slot ahead of current slot")]
    InvalidSlot,
}

/// Counters used as a cache for global quotas.
///
/// When we want to ratelimit across all relay-instances, we need to use redis to synchronize.
/// Calling Redis every time we want to check if an item should be ratelimited would be very expensive,
/// which is why we have this cache. It works by 'taking' a certain budget from redis, by pre-incrementing
/// a global counter. We Put the amount we pre-incremented into this local cache and count down until
/// we have no more budget, then we ask for more from redis. If we find the global counter is above
/// the quota limit, we will ratelimit the item.
pub struct GlobalCounters {
    redis_script: Script,
    counters: RwLock<hashbrown::HashMap<Key, Arc<Mutex<BudgetState>>>>,
}

impl Default for GlobalCounters {
    fn default() -> Self {
        Self {
            redis_script: Script::new(include_str!("global_quota.lua")),
            counters: Default::default(),
        }
    }
}

impl GlobalCounters {
    /// Returns `true` if the global quota should be ratelimited.
    pub fn is_rate_limited(
        &self,
        client: &mut PooledClient,
        quota: &RedisQuota,
        quantity: usize,
    ) -> Result<bool, GlobalCountErrors> {
        let state = self.get_state(KeyRef::new(quota));
        let mut lock = state.lock().unwrap();

        lock.reset_if_expired(quota.window())?;

        if lock.cached_global_count_exceeded(quantity, quota.limit) {
            return Ok(true);
        }

        if lock.try_consume_budget(quantity) {
            return Ok(false);
        }

        let (budget, redis_count) = self.take_budget_from_redis(client, quota, quantity)?;
        lock.update_budget(budget, redis_count);

        Ok(!lock.try_consume_budget(quantity))
    }

    fn get_state(&self, key: KeyRef) -> Arc<Mutex<BudgetState>> {
        let state_opt = self.counters.read().unwrap().get(&key).map(Arc::clone);
        match state_opt {
            Some(state) => state,
            None => {
                let new_state = || Arc::new(Mutex::new(BudgetState::new(key.window)));
                Arc::clone(
                    self.counters
                        .write()
                        .unwrap()
                        .entry(key.into_owned())
                        .or_insert_with(new_state),
                )
            }
        }
    }

    /// Ask redis for more budget.
    fn take_budget_from_redis(
        &self,
        client: &mut PooledClient,
        quota: &RedisQuota,
        quantity: usize,
    ) -> Result<(usize, usize), GlobalCountErrors> {
        let requested_budget = DEFAULT_BUDGET_REQUEST.max(quantity);
        let expiry = UnixTimestamp::now().as_secs() + quota.window();

        let (received_budget, redis_count): (usize, usize) = self
            .redis_script
            .prepare_invoke()
            .key(quota.key())
            .arg(quota.limit())
            .arg(expiry)
            .arg(requested_budget)
            .invoke(&mut client.connection().map_err(|_| GlobalCountErrors::Redis)?)
            .map_err(|_| GlobalCountErrors::Redis)?;

        Ok((received_budget, redis_count))
    }
}

/// Key for storing global quota-budgets locally.
///
/// Note: must not be used in redis. For that, use RedisQuota.key().
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    prefix: String,
    window: u64,
}

/// Used to look up a hashmap of [`BudgetKey`]-keys without a string allocation.
///
/// This works due to the 'Equivalent' trait in the hashbrown crate.
#[derive(Clone, Copy, Hash)]
struct KeyRef<'a> {
    prefix: &'a str,
    window: u64,
}

impl<'a> KeyRef<'a> {
    fn new(quota: &'a RedisQuota) -> Self {
        Self {
            prefix: quota.prefix(),
            window: quota.window(),
        }
    }

    fn into_owned(self) -> Key {
        Key {
            prefix: self.prefix.to_owned(),
            window: self.window,
        }
    }
}

impl hashbrown::Equivalent<Key> for KeyRef<'_> {
    fn equivalent(&self, key: &Key) -> bool {
        self.prefix == key.prefix && self.window == key.window
    }
}

/// Represents the local budget taken from a global quota.
struct BudgetState {
    slot: usize,
    budget: usize,
    last_seen_redis_value: usize,
}

impl BudgetState {
    fn new(window: u64) -> Self {
        Self {
            slot: current_slot(window),
            budget: 0,
            last_seen_redis_value: 0,
        }
    }

    fn update_budget(&mut self, extra_budget: usize, redis_count: usize) {
        self.budget += extra_budget;
        self.last_seen_redis_value = redis_count;
    }

    /// Returns `true` if slot has expired.
    fn is_expired(&self, window: u64) -> Result<bool, GlobalCountErrors> {
        use std::cmp::Ordering::*;
        let current_slot = current_slot(window);

        match self.slot.cmp(&current_slot) {
            Less => Ok(true),
            Equal => Ok(false),
            Greater => Err(GlobalCountErrors::InvalidSlot),
        }
    }

    fn reset_if_expired(&mut self, window: u64) -> Result<(), GlobalCountErrors> {
        if self.is_expired(window)? {
            *self = Self::new(window);
        }
        Ok(())
    }

    /// Returns `true` if we succesfully decreased the budget by `quantity`.
    ///
    /// `false` means there is not enough budget.
    fn try_consume_budget(&mut self, quantity: usize) -> bool {
        if quantity > self.budget {
            false
        } else {
            self.budget -= quantity;
            true
        }
    }

    fn cached_global_count_exceeded(&self, quantity: usize, limit: Option<u64>) -> bool {
        match limit {
            Some(limit) => {
                let cached_global_count = self.last_seen_redis_value - self.budget;
                cached_global_count + quantity > limit as usize
            }

            None => false,
        }
    }
}
