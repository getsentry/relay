use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use relay_common::time::UnixTimestamp;

/// A ratio is converted to a divisor to perform integer arithmetic, instead of floating point.
///
/// This is done with the configured precision here.
const RATIO_PRECISION: u64 = 10;

/// A quota to be checked with the [`OpportunisticQuotaCache`].
#[derive(Debug, Clone, Copy)]
pub struct Quota<T> {
    /// The quota limit.
    ///
    /// A negative limit is treated as infinite/unlimited.
    pub limit: i64,
    /// The quota window size in seconds.
    pub window: u64,
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
    /// For example: Setting this to `10 * RATIO_PRECISION` means, if there is 100 quota remaining,
    /// the cache will opportunistically accept the next 10 items, if there is a quota of 90 remaining,
    /// the cache will accept the next 9 items.
    max_over_spend_divisor: NonZeroU64,

    /// The maximum amount of quota the cache considers for activation.
    ///
    /// See also: [`Self::with_limit_threshold`].
    limit_threshold_divisor: Option<NonZeroU64>,

    /// Minimum interval between vacuum of the cache.
    vacuum_interval: Duration,
    /// Unix timestamp of the next time the vacuum should be run.
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
    ///
    /// The configured ratio must be in range `[0, 1]`.
    pub fn new(max_over_spend: f32) -> Self {
        let max_over_spend_divisor = 1.0f32 / max_over_spend * RATIO_PRECISION as f32;
        let max_over_spend_divisor =
            NonZeroU64::new(max_over_spend_divisor as u64).unwrap_or(NonZeroU64::MIN);

        Self {
            cache: Default::default(),
            max_over_spend_divisor,
            // Can invert the percentage here to loose less precision (95% -> 5%)
            limit_threshold_divisor: None,
            vacuum_interval: Duration::from_secs(30),
            // Initialize to 0, this means a vacuum run immediately, but it is going to be fast
            // (empty cache) and it requires us to be time/environment independent, time is purely
            // driven by the user of the cache.
            next_vacuum: AtomicU64::new(0),
        }
    }

    /// Configures an optional limit threshold ratio.
    ///
    /// The limit threshold is a cut-off applied to the quota limit, if exceeded the cache will no
    /// longer cache the quota.
    ///
    /// Lowering the limit threshold reduces the amount of quota which may incorrectly be
    /// over-accepted.
    ///
    /// For example: A quota with limit `100` and a configured limit threshold of `0.7` (70%),
    /// will no longer be considered for caching if 70% (70) of the quota is consumed.
    ///
    /// By default, no limit threshold is configured.
    pub fn with_limit_threshold(mut self, limit_threshold: Option<f32>) -> Self {
        self.limit_threshold_divisor = limit_threshold.map(|v| {
            // Inverting the threshold here simplifies the checking code, but also retains more
            // precision for the integer division, since we can expect this value to be large.
            //
            // 1.0 / 0.95 * 10 = 10
            // 1.0 / (1.0 - 0.95) * 10 = 200
            //
            // 100 * 10 / 10 = 100
            // 100 - (100 * 10 / 200) = 95
            let min_remaining_divisor =
                1.0f32 / (1.0f32 - v.clamp(0.0, 1.0)) * RATIO_PRECISION as f32;
            NonZeroU64::new(min_remaining_divisor as u64).unwrap_or(NonZeroU64::MAX)
        });

        self
    }

    /// Checks a quota with quantity against the cache.
    ///
    /// The cache may return [`Action::Accept`] indicating the quantity should be accepted.
    /// If the cache can not make a decision it returns [`Action::Check`] indicating the returned
    /// quantity needs to be synchronized with a centralized store.
    ///
    /// Whenever the cache returns [`Action::Check`], the cache requires a call to [`Self::update_quota`],
    /// with a synchronized 'consumed' amount.
    pub fn check_quota(&self, quota: Quota<T>, quantity: u64) -> Action {
        let cache = self.cache.pin();

        let Ok(limit) = u64::try_from(quota.limit) else {
            // Negative limits are infinite.
            return Action::Accept;
        };

        // We can potentially short circuit here with a simple read, the cases:
        // 1. `NeedsSync`
        // 2. Active with `consumed >= limit`
        // 3. No entry.
        //
        // This may be faster because the map does not require a mutation.
        // Needs more investigation if the additional lookup is worth it.

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

            let total_local_use = local_use + quantity;

            // Upper limit threshold, where the cache is no longer active
            let threshold = match self.limit_threshold_divisor.map(NonZeroU64::get) {
                Some(div) => limit * RATIO_PRECISION / div,
                None => 0,
            };

            // Can short circuit here already if consumed is already above or equal to the limit.
            //
            // We could also propagate this out to the caller as a definitive negative in the
            // future. This does require some additional consideration how this would interact with
            // refunds, which can reduce the consumed.
            if consumed >= limit.saturating_sub(threshold) {
                return CachedQuota::new_needs_sync(total_local_use);
            }

            let remaining = limit.saturating_sub(consumed);
            let max_allowed_spend = remaining
                // Normalize the remaining quota with the window size, to apply the ratio/divisor to the
                // per second rate.
                //
                // This means we get a consistent behaviour for short (10s) quotas (e.g. abuse) as well
                // as long (1h) quotas (e.g. spike protection) with a more predictable error.
                / quota.window.max(1)
                // Apply ratio precision, which is already pre-multiplied into `max_over_spend_divisor`.
                * RATIO_PRECISION
                // Apply the actual ratio with the pre-computed divisor.
                / self.max_over_spend_divisor.get();

            match total_local_use > max_allowed_spend {
                true => CachedQuota::new_needs_sync(total_local_use),
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

        // Consumed quota can be negative due to refunds, we choose to deal with negative quotas
        // like they are simply unused.
        //
        // This only makes the cache stricter and less likely to over accept.
        let consumed = u64::try_from(consumed).unwrap_or(0);

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

        if exchange.is_err() {
            // Somebody else took the vacuum, that's fine.
            return false;
        }

        let mut cache = self.cache.pin();
        cache.retain(|_, v| v.is_still_needed(now));

        true
    }
}

/// Action to take when accessing cached quotas via [`OpportunisticQuotaCache::check_quota`].
#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    /// Accept the quota request.
    Accept,
    /// Synchronize the quota with the returned quantity.
    Check(u64),
}

/// State of a cached quota.
#[derive(Debug)]
enum CachedQuota {
    /// The cache currently cannot make a decision and needs an updated consumption value from the
    /// synchronized store.
    NeedsSync,
    /// Like [`Self::NeedsSync`], but also carries a total quantity which needs to be synchronized
    /// with the store.
    NeedsSyncWithQuantity(NonZeroU64),
    /// The cache is active and can still make decisions without a synchronization.
    Active {
        consumed: u64,
        local_use: u64,
        expiry: UnixTimestamp,
    },
}

impl CachedQuota {
    /// Creates [`Self::NeedsSync`] for a quantity of `0`, [`Self::NeedsSyncWithQuantity`] otherwise.
    pub fn new_needs_sync(quantity: u64) -> Self {
        NonZeroU64::new(quantity)
            .map(Self::NeedsSyncWithQuantity)
            .unwrap_or(Self::NeedsSync)
    }

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

    /// Returns a simple quota with a limit and a window of 1 second.
    fn simple_quota(limit: i64) -> Quota<&'static str> {
        Quota {
            limit,
            window: 1,
            key: "simple_quota_key",
            expiry: UnixTimestamp::from_secs(300),
        }
    }

    #[test]
    fn test_opp_quota() {
        let cache = OpportunisticQuotaCache::new(0.1);

        let q1 = Quota {
            limit: 100,
            window: 1,
            key: "k1",
            expiry: UnixTimestamp::from_secs(300),
        };
        let q2 = Quota {
            limit: 50,
            window: 1,
            key: "k2",
            expiry: UnixTimestamp::from_secs(300),
        };

        // First access must always go to synchronized store.
        assert_eq!(cache.check_quota(q1, 10), Action::Check(10));
        // So does any following access until there was a sync.
        assert_eq!(cache.check_quota(q1, 9), Action::Check(9));

        // Same for other keys.
        assert_eq!(cache.check_quota(q2, 12), Action::Check(12));

        // Sync internal state to 30, for this test, there will be 70 remaining, meaning the cache
        // is expected to accept 7 more items without requiring another sync.
        cache.update_quota(q1, 30);

        // A different key still needs a sync.
        assert_eq!(cache.check_quota(q2, 12), Action::Check(12));

        // 70 remaining, 10% -> 7 accepted.
        assert_eq!(cache.check_quota(q1, 5), Action::Accept);
        assert_eq!(cache.check_quota(q1, 2), Action::Accept);
        // This one goes over the limit, we need to now check a total of 8.
        assert_eq!(cache.check_quota(q1, 1), Action::Check(8));

        // A new sync is required
        assert_eq!(cache.check_quota(q1, 1), Action::Check(1));
        assert_eq!(cache.check_quota(q1, 2), Action::Check(2));
        // A different key is still a different key.
        assert_eq!(cache.check_quota(q2, 3), Action::Check(3));

        // Consumed state is absolute not relative.
        cache.update_quota(q1, 50);
        // This is too much.
        assert_eq!(cache.check_quota(q2, 6), Action::Check(6));
        // Need another sync again.
        assert_eq!(cache.check_quota(q2, 1), Action::Check(1));

        // Negative state can exist due to refunds.
        cache.update_quota(q1, -123);
        // The cache considers a negative quota like `0`, `100` remaining quota -> 10 (= 10%).
        assert_eq!(cache.check_quota(q1, 10), Action::Accept);
        // Too much, check the entire local usage.
        assert_eq!(cache.check_quota(q1, 1), Action::Check(11));
    }

    #[test]
    fn test_opp_quota_100_percent() {
        let cache = OpportunisticQuotaCache::new(1.0);

        let q1 = simple_quota(100);

        cache.update_quota(q1, 0);
        for _ in 0..100 {
            assert_eq!(cache.check_quota(q1, 1), Action::Accept,);
        }
        assert_eq!(cache.check_quota(q1, 1), Action::Check(101));
    }

    #[test]
    fn test_opp_quota_normalized_to_window() {
        let cache = OpportunisticQuotaCache::new(0.1);

        let q1 = Quota {
            limit: 1000,
            window: 10,
            key: "k1",
            expiry: UnixTimestamp::from_secs(300),
        };

        // First access always needs synchronization.
        assert_eq!(cache.check_quota(q1, 1), Action::Check(1));

        // 700 remaining -> 70 per second -> 7 (10%).
        cache.update_quota(q1, 300);

        // 7 is the exact synchronization breakpoint.
        assert_eq!(cache.check_quota(q1, 8), Action::Check(8));
        // Under 7, but already over consumed before.
        assert_eq!(cache.check_quota(q1, 6), Action::Check(6));

        // Reset.
        cache.update_quota(q1, 300);
        // Under 7 -> that's fine.
        assert_eq!(cache.check_quota(q1, 7), Action::Accept);
        // Way over the limit now.
        assert_eq!(cache.check_quota(q1, 90), Action::Check(97));

        // 100 remaining -> 10 per second -> 1 (10%).
        cache.update_quota(q1, 900);
        assert_eq!(cache.check_quota(q1, 1), Action::Accept);
        assert_eq!(cache.check_quota(q1, 1), Action::Check(2));

        // 90 remaining -> 9 per second -> 0 (10% floored).
        cache.update_quota(q1, 910);
        assert_eq!(cache.check_quota(q1, 1), Action::Check(1));

        // Same for even less remaining.
        cache.update_quota(q1, 999);
        assert_eq!(cache.check_quota(q1, 1), Action::Check(1));

        // Same for nothing remaining.
        cache.update_quota(q1, 1000);
        assert_eq!(cache.check_quota(q1, 1), Action::Check(1));

        // Same for less than nothing remaining.
        cache.update_quota(q1, 1001);
        assert_eq!(cache.check_quota(q1, 1), Action::Check(1));
    }

    #[test]
    fn test_opp_quota_limit_threshold() {
        let cache = OpportunisticQuotaCache::new(0.1).with_limit_threshold(Some(0.7));

        let q1 = simple_quota(100);

        // First access always needs synchronization.
        assert_eq!(cache.check_quota(q1, 1), Action::Check(1));

        // 50 remaining -> 5 (10%), consumption still under limit threshold (70).
        cache.update_quota(q1, 50);
        // Nothing special here.
        assert_eq!(cache.check_quota(q1, 5), Action::Accept);
        assert_eq!(cache.check_quota(q1, 1), Action::Check(6));

        // 31 remaining -> 3 (10%), consumption still under limit threshold (70),
        // but maximum cached consumption would be *above* the threshold, this is currently
        // explicitly not considered (but this behaviour may be changed in the future).
        cache.update_quota(q1, 69);
        assert_eq!(cache.check_quota(q1, 3), Action::Accept);
        assert_eq!(cache.check_quota(q1, 1), Action::Check(4));

        // 30 remaining -> 3 (10%), *but* threshold (70%) is now reached.
        cache.update_quota(q1, 70);
        assert_eq!(cache.check_quota(q1, 1), Action::Check(1));
        // Sanity check, that exhausting the limit fully, still works.
        cache.update_quota(q1, 100);
        assert_eq!(cache.check_quota(q1, 1), Action::Check(1));

        // Resetting consumption to a lower value (refunds) should still work.
        cache.update_quota(q1, 50);
        assert_eq!(cache.check_quota(q1, 5), Action::Accept);
        assert_eq!(cache.check_quota(q1, 1), Action::Check(6));
    }

    #[test]
    fn test_opp_quota_limit_threshold_very_large() {
        let cache = OpportunisticQuotaCache::new(0.1).with_limit_threshold(Some(420.0));

        let q1 = simple_quota(100);

        cache.update_quota(q1, 90);
        assert_eq!(cache.check_quota(q1, 1), Action::Accept);
        assert_eq!(cache.check_quota(q1, 1), Action::Check(2));
    }

    #[test]
    fn test_opp_quota_limit_threshold_very_small() {
        let cache = OpportunisticQuotaCache::new(0.1).with_limit_threshold(Some(-1.0));

        let q1 = simple_quota(100);

        // A negative or `0` limit threshold essentially disables the cache.
        cache.update_quota(q1, 0);
        assert_eq!(cache.check_quota(q1, 1), Action::Check(1));
    }

    /// Negative limits should be considered as infinite.
    #[test]
    fn test_opp_quota_negative_limit() {
        let cache = OpportunisticQuotaCache::new(0.1);

        let q1 = Quota {
            limit: -1,
            window: 10,
            key: "k1",
            expiry: UnixTimestamp::from_secs(300),
        };

        assert_eq!(cache.check_quota(q1, 99999), Action::Accept);
    }

    /// The test asserts the cache behaves correctly if the limit of a quota changes.
    #[test]
    fn test_opp_quota_limit_change() {
        let cache = OpportunisticQuotaCache::new(0.1);
        let window = 3;

        let limit_100 = Quota {
            limit: 100 * window,
            window: window as u64,
            key: "k1",
            expiry: UnixTimestamp::from_secs(300),
        };
        let limit_50 = Quota {
            // Same quota, but a different limit.
            limit: 50 * window,
            ..limit_100
        };

        // Sync internal state to an initial value.
        cache.update_quota(limit_100, 50 * window);

        // With limit 100 there is enough (5) in the cache remaining.
        assert_eq!(cache.check_quota(limit_100, 3), Action::Accept);
        // With limit 50 there is not enough in the cache remaining,  the over accepted amount needs
        // to be checked though.
        assert_eq!(cache.check_quota(limit_50, 1), Action::Check(4));
    }

    /// Tests that even a cache with `0%` over spend acts correctly.
    #[test]
    fn test_opp_quota_zero() {
        let cache = OpportunisticQuotaCache::new(0.0);

        let q1 = simple_quota(100);

        // Not synchronized -> always check.
        assert_eq!(cache.check_quota(q1, 1), Action::Check(1));
        assert_eq!(cache.check_quota(q1, 1), Action::Check(1));

        cache.update_quota(q1, 30_000_000_000);

        // Even after synchronization, we need to check immediately.
        assert_eq!(cache.check_quota(q1, 1), Action::Check(1));
    }

    #[test]
    fn test_opp_quota_vacuum() {
        let cache = OpportunisticQuotaCache::new(0.1);

        let q1 = Quota {
            limit: 100,
            window: 1,
            key: "k1",
            expiry: UnixTimestamp::from_secs(100),
        };
        let q2 = Quota {
            limit: 100,
            window: 1,
            key: "k2",
            expiry: UnixTimestamp::from_secs(200),
        };

        // Initialize the cache.
        cache.update_quota(q1, 0);
        cache.update_quota(q2, 0);

        // Make sure some elements are stored in the cache.
        assert_eq!(cache.check_quota(q1, 9), Action::Accept);
        assert_eq!(cache.check_quota(q2, 9), Action::Accept);

        // This vacuum should succeed, no concurrent vacuums.
        assert!(cache.try_vacuum(UnixTimestamp::from_secs(150)));
        // A vacuum was just ran, there is no need to run it again.
        assert!(!cache.try_vacuum(UnixTimestamp::from_secs(150)));

        // This entry was purged from the cache, now with the same quota, we should no longer have
        // an old entry.
        assert_eq!(cache.check_quota(q1, 6), Action::Check(6));
        // This entry was not purged (different expiry) and should therefor return the full amount.
        assert_eq!(cache.check_quota(q2, 6), Action::Check(15));
    }
}
