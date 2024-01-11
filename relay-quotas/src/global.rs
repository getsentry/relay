use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

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
    counters: RwLock<hashbrown::HashMap<BudgetKey, Arc<RwLock<BudgetState>>>>,
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
        let budget_state = self.try_get_state(quota)?;

        {
            let read_lock = budget_state.read().unwrap();

            if read_lock.cached_global_count_exceeded(quantity, quota.limit) {
                return Ok(true);
            }

            if read_lock.try_consume_budget(quantity) {
                return Ok(false);
            }
        }

        let mut write_lock = budget_state.write().unwrap();

        if !write_lock.fits_budget(quantity) {
            let (budget, redis_count) = self.take_budget_from_redis(client, quota, quantity)?;
            write_lock.update_budget(budget, redis_count);
        };

        Ok(!write_lock.try_consume_budget(quantity))
    }

    /// Retrieves a valid [`BudgetState`] from the map.
    ///
    /// If it's missing or outdated, a new one is inserted before being returned.
    fn try_get_state(
        &self,
        quota: &RedisQuota,
    ) -> Result<Arc<RwLock<BudgetState>>, GlobalCountErrors> {
        let key = BudgetKeyRef::new(quota);
        let window = quota.window();

        let state_opt = self.counters.read().unwrap().get(&key).cloned();
        match state_opt {
            Some(state) => {
                if state.read().unwrap().is_valid(window)? {
                    return Ok(state);
                };

                // The 'if expired' check is there in the case of multiple writers waiting.
                state.write().unwrap().reset_if_expired(window)?;

                Ok(state)
            }
            None => {
                let mut write_guard = self.counters.write().unwrap();
                let new_state = || Arc::new(RwLock::new(BudgetState::new(window)));

                let state = write_guard
                    .entry(key.into_owned())
                    .or_insert_with(new_state);

                Ok(Arc::clone(state))
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
        let redis_key = quota.key();
        let expiry = UnixTimestamp::now().as_secs() + quota.window();
        let limit = quota.limit();

        let (received_budget, redis_count): (usize, usize) = self
            .redis_script
            .prepare_invoke()
            .key(redis_key.as_str())
            .arg(limit)
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

impl From<&BudgetKeyRef<'_>> for BudgetKey {
    fn from(value: &BudgetKeyRef<'_>) -> Self {
        Self {
            prefix: value.prefix.to_owned(),
            window: value.window,
        }
    }
}

impl<'a> BudgetKeyRef<'a> {
    fn new(quota: &'a RedisQuota) -> Self {
        Self {
            prefix: quota.prefix(),
            window: quota.window(),
        }
    }

    fn into_owned(self) -> BudgetKey {
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
struct BudgetState {
    slot: usize,
    budget: AtomicUsize,
    last_seen_redis_value: usize,
}

impl BudgetState {
    fn new(window: u64) -> Self {
        Self {
            slot: current_slot(window),
            budget: AtomicUsize::new(0),
            last_seen_redis_value: 0,
        }
    }

    /// Returns `true` if slot has not expired.
    fn is_valid(&self, window: u64) -> Result<bool, GlobalCountErrors> {
        use std::cmp::Ordering::*;
        let current_slot = current_slot(window);

        match self.slot.cmp(&current_slot) {
            Less => Ok(false),
            Equal => Ok(true),
            Greater => Err(GlobalCountErrors::InvalidSlot),
        }
    }

    fn reset_if_expired(&mut self, window: u64) -> Result<(), GlobalCountErrors> {
        if !self.is_valid(window)? {
            *self = Self::new(window);
        }
        Ok(())
    }

    /// Returns `true` if we succesfully decreased the budget by `quantity`.
    fn try_consume_budget(&self, quantity: usize) -> bool {
        use Ordering::SeqCst;

        if !self.fits_budget(quantity) {
            return false;
        }

        self.budget
            .fetch_update(SeqCst, SeqCst, |val| val.checked_sub(quantity))
            .is_ok()
    }

    fn cached_global_count_exceeded(&self, quantity: usize, limit: Option<u64>) -> bool {
        match limit {
            Some(limit) => {
                let cached_global_count = self.last_seen_redis_value - self.current_budget();
                cached_global_count + quantity > limit as usize
            }
            None => false,
        }
    }

    fn fits_budget(&self, quantity: usize) -> bool {
        self.current_budget() >= quantity
    }

    fn current_budget(&self) -> usize {
        self.budget.load(Ordering::SeqCst)
    }

    fn update_budget(&mut self, budget: usize, redis_count: usize) {
        self.budget.fetch_add(budget, Ordering::SeqCst);
        self.last_seen_redis_value = redis_count;
    }
}
