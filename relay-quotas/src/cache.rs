use std::num::NonZeroI64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use relay_common::time::UnixTimestamp;

/// A quota to be checked with the [`OpportunisticQuotaCache`].
#[derive(Debug, Clone, Copy)]
pub struct Quota<T> {
    /// The quota limit.
    pub limit: i64,
    /// A unique identifier for the quota bucket.
    pub key: T,
    /// The expiry of the bucket.
    pub expiry: UnixTimestamp,
}

/// An opportunistic cache for quotas.
#[derive(Debug)]
pub struct OpportunisticQuotaCache<T>
where
    T: std::hash::Hash + Eq,
{
    /// A cache for all currently consumed quotas.
    ///
    /// The cache is keyed with the individual quota, the value is the last known, currently
    /// consumed amount of the quota.
    cache: papaya::HashMap<T, CachedQuota>,

    /// The amount the cache is allowed to opportunistically over-accept based on the remaining
    /// quota.
    ///
    /// For example: Setting this to `10` means, if there is 100 quota remaining, the cache will
    /// opportunistically accept the next 10 items, if there is a quota of 90 remaining, the cache
    /// will accept the next 9 items.
    max_over_spend_divisor: NonZeroI64,

    /// Minimum interval between vacuum of the cache.
    vacuum_interval: Duration,
    /// Last time then vacuum was ran.
    ///
    /// This is a [`UnixTimestamp`] stored in an atomic.
    next_vacuum: AtomicU64,
}

impl<T> OpportunisticQuotaCache<T>
where
    T: std::hash::Hash + Eq,
{
    /// Creates a new [`Self`], with the configured maximum amount the cache accepts per quota
    /// until it requires synchronization.
    pub fn new(max_over_spend_divisor: NonZeroI64) -> Self {
        Self {
            cache: Default::default(),
            max_over_spend_divisor,
            vacuum_interval: Duration::from_secs(30),
            // Initialize to 0, this means a vacuum run immediately, but it is going to be fast
            // (empty cache) and it requires us to be time/environment independent, time is purely
            // driven by the user of the cache.
            next_vacuum: AtomicU64::new(0),
        }
    }

    /// Checks a quota with quantity against the cache.
    ///
    /// The cache may return [`Action::Accept`] indicating the quantity should be accepted.
    /// If the cache can not make a decision it returns [`Action::Check`] indicating the returned
    /// quantity needs to be synchronized with a centralized store.
    ///
    /// Whenever the cache returns [`Action::Check`], the cache requires a call to [`Self::update_quota`],
    /// with a synchronized 'consumed' amount.
    pub fn check_quota(&self, quota: Quota<T>, quantity: i64) -> Action {
        let cache = self.cache.pin();

        let value = cache.update(quota.key, |q| {
            let (consumed, local_use) = match q {
                // There is already a sync requested which we need to wait for to get consistent
                // data, we cannot make a decision here at this time.
                CachedQuota::NeedsSync => return CachedQuota::NeedsSync,
                // This variant only exists as a way of returning a quantity on the first
                // `NeedsSync` decision. We can safely erase it into `NeedsSync` without a value.
                CachedQuota::NeedsSyncWithQuantity(_) => return CachedQuota::NeedsSync,
                CachedQuota::Active {
                    consumed,
                    local_use,
                    expiry: _,
                } => (*consumed, *local_use),
            };

            let remaining = quota.limit.saturating_sub(consumed).max(0);
            let max_allowed_spend = remaining / self.max_over_spend_divisor.get();

            let total_local_use = local_use + quantity;
            match total_local_use > max_allowed_spend {
                true => NonZeroI64::new(total_local_use)
                    .map(CachedQuota::NeedsSyncWithQuantity)
                    .unwrap_or(CachedQuota::NeedsSync),
                false => CachedQuota::Active {
                    consumed,
                    local_use: total_local_use,
                    expiry: quota.expiry,
                },
            }
        });

        match value {
            Some(CachedQuota::NeedsSyncWithQuantity(q)) => Action::Check(q.get()),
            Some(CachedQuota::NeedsSync) | None => Action::Check(quantity),
            Some(CachedQuota::Active { .. }) => Action::Accept,
        }
    }

    /// Updates the quota state in the cache for the specified quota.
    ///
    /// The cache will use the synchronized `consumed` value to derive future decisions whether it
    /// can accept quota requests.
    pub fn update_quota(&self, quota: Quota<T>, consumed: i64) {
        let cache = self.cache.pin();

        cache.update_or_insert(
            quota.key,
            |q| match q {
                // We got the sync, start fresh!
                CachedQuota::NeedsSync | CachedQuota::NeedsSyncWithQuantity(_) => {
                    CachedQuota::Active {
                        consumed,
                        local_use: 0,
                        expiry: quota.expiry,
                    }
                }
                // This is slightly tricky, one could assume quotas are only growing and we should use
                // the bigger `consumed` count here. The problem is refunds, a quota may decrease again
                // due to refunds.
                //
                // Without a way to differentiate which `consumed` value is actually more recent, we
                // just have the implicit assumption that the value arriving last is the most recent.
                //
                // This is not perfect, but for our best effort opportunistic cache, this is not a big
                // problem, at worst, we over accept a little bit more for the next synchronization
                // round.
                CachedQuota::Active {
                    consumed: _,
                    local_use,
                    expiry: _,
                } => CachedQuota::Active {
                    consumed,
                    local_use: *local_use,
                    expiry: quota.expiry,
                },
            },
            CachedQuota::Active {
                consumed,
                local_use: 0,
                expiry: quota.expiry,
            },
        );
    }

    /// Attempts to run a vacuum on the internal cache.
    ///
    /// The vacuum is internally debounced and may not run. Callers should call attempt to vacuum
    /// periodically.
    ///
    /// Returns `true` if a vacuum was performed.
    pub fn try_vacuum(&self, now: UnixTimestamp) -> bool {
        // Check for the lucky winner to run an expiration.
        let next_vacuum = self.next_vacuum.load(Ordering::Relaxed);

        if next_vacuum > now.as_secs() {
            // It's not yet time to vacuum.
            return false;
        }

        let exchange = self.next_vacuum.compare_exchange(
            next_vacuum,
            now.as_secs() + self.vacuum_interval.as_secs(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );

        if !exchange.is_ok() {
            // Somebody else took the vacuum, that's fine.
            return false;
        }

        let mut cache = self.cache.pin();
        cache.retain(|_, v| v.is_still_needed(now));

        true
    }
}

/// Action to take when accessing cached quotas via [`OpportunisticQuotaCache::check_quota`].
#[derive(Debug)]
pub enum Action {
    /// Accept the quota request.
    Accept,
    /// Synchronize the quota with the returned quantity.
    Check(i64),
}

/// State of a cached quota.
#[derive(Debug)]
enum CachedQuota {
    /// The cache currently cannot make a decision and needs an updated consumption value from the
    /// synchronized store.
    NeedsSync,
    /// Like [`Self::NeedsSync`], but also carries a total quantity which needs to be synchronized
    /// with the store.
    NeedsSyncWithQuantity(NonZeroI64),
    /// The cache is active and can still make decisions without a synchronization.
    Active {
        consumed: i64,
        local_use: i64,
        expiry: UnixTimestamp,
    },
}

impl CachedQuota {
    /// Returns `true` when the entry is no longer needed in the cache.
    ///
    /// An item is no longer needed when it is expired or a sync is required.
    fn is_still_needed(&self, now: UnixTimestamp) -> bool {
        match self {
            Self::NeedsSync => false,
            Self::NeedsSyncWithQuantity(_) => false,
            Self::Active { expiry, .. } => now <= *expiry,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_opp_quota() {
        let cache = OpportunisticQuotaCache::new(NonZeroI64::new(10).unwrap());

        let q1 = Quota {
            limit: 100,
            key: "k1",
            expiry: UnixTimestamp::from_secs(300),
        };
        let q2 = Quota {
            limit: 50,
            key: "k2",
            expiry: UnixTimestamp::from_secs(300),
        };

        // First access must always go to synchronized store.
        assert!(matches!(cache.check_quota(q1, 10), Action::Check(10)));
        // So does any following access until there was a sync.
        assert!(matches!(cache.check_quota(q1, 9), Action::Check(9)));

        // Same for other keys.
        assert!(matches!(cache.check_quota(q2, 12), Action::Check(12)));

        // Sync internal state to 30, for this test, there will be 70 remaining, meaning the cache
        // is expected to accept 7 more items without requiring another sync.
        cache.update_quota(q1, 30);
        // TODO: test next call with > 7, < 7, = 7, 6 + large

        // A different key still needs a sync.
        assert!(matches!(cache.check_quota(q2, 12), Action::Check(12)));

        // 70 remaining, 10% -> 7 accepted.
        assert!(matches!(cache.check_quota(q1, 5), Action::Accept));
        assert!(matches!(cache.check_quota(q1, 2), Action::Accept));
        // This one goes over the limit, we need to now check a total of 8.
        assert!(matches!(cache.check_quota(q1, 1), Action::Check(8)));

        // A new sync is required
        assert!(matches!(cache.check_quota(q1, 1), Action::Check(1)));
        assert!(matches!(cache.check_quota(q1, 2), Action::Check(2)));
        // A different key is still a different key.
        assert!(matches!(cache.check_quota(q2, 3), Action::Check(3)));

        // Consumed state is absolute not relative.
        cache.update_quota(q1, 50);
        // This is too much.
        assert!(matches!(cache.check_quota(q2, 6), Action::Check(6)));
        // Need another sync again.
        assert!(matches!(cache.check_quota(q2, 1), Action::Check(1)));

        // Negative state can exist due to refunds.
        cache.update_quota(q1, -100);
        // We now have `200` remaining quota -> 20 (= 10%).
        assert!(matches!(cache.check_quota(q1, 20), Action::Accept));
        // Too much, check the entire local usage.
        assert!(matches!(cache.check_quota(q1, 1), Action::Check(21)));
    }

    #[test]
    fn test_opp_quota_vacuum() {
        let cache = OpportunisticQuotaCache::new(NonZeroI64::new(10).unwrap());

        let q1 = Quota {
            limit: 100,
            key: "k1",
            expiry: UnixTimestamp::from_secs(100),
        };
        let q2 = Quota {
            limit: 100,
            key: "k2",
            expiry: UnixTimestamp::from_secs(200),
        };

        // Initialize the cache.
        cache.update_quota(q1, 0);
        cache.update_quota(q2, 0);

        // Make sure some elements are stored in the cache.
        assert!(matches!(cache.check_quota(q1, 9), Action::Accept));
        assert!(matches!(cache.check_quota(q2, 9), Action::Accept));

        // This vacuum should succeed, no concurrent vacuums.
        assert!(cache.try_vacuum(UnixTimestamp::from_secs(150)));
        // A vacuum was just ran, there is no need to run it again.
        assert!(!cache.try_vacuum(UnixTimestamp::from_secs(150)));

        // This entry was purged from the cache, now with the same quota, we should no longer have
        // an old entry.
        assert!(matches!(cache.check_quota(q1, 6), Action::Check(6)));
        // This entry was not purged (different expiry) and should therefor return the full amount.
        assert!(matches!(cache.check_quota(q2, 6), Action::Check(15)));
    }
}
