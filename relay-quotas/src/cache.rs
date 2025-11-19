use std::num::NonZeroI64;

#[derive(Debug, Clone, Copy)]
pub struct Quota<T> {
    pub limit: i64,
    pub key: T,
    // TODO: the quota needs an expiration for cleanup
}

#[derive(Debug)]
pub struct OpportunisticQuotaCache<T>
where
    T: std::hash::Hash + Eq,
{
    // TODO: expire old entries, maybe Slot needs to be typed better
    // cache: hashbrown::HashMap<QuotaKey<'static>, ()>,
    /// A cache for all currently consumed quotas.
    ///
    /// The cache is keyed with the individual quota, the value is the last known, currently
    /// consumed amount of the quota.
    ///
    /// TODO: maybe the value needs to be (last_known_consumed, opportunisticaly_spent),
    /// based on last known consumed and the limit we can calculate a threshold for the max size of
    /// opportunisticaly_spent
    cache: papaya::HashMap<T, CachedQuota2>,

    /// ...
    ///
    max_over_spend_divisor: NonZeroI64,
}

impl<T> OpportunisticQuotaCache<T>
where
    T: std::hash::Hash + Eq,
{
    pub fn new(max_over_spend_divisor: NonZeroI64) -> Self {
        Self {
            cache: Default::default(),
            max_over_spend_divisor,
        }
    }

    pub fn foobar(&self, quota: Quota<T>, quantity: i64) -> Action {
        // Negative limits are considered infinite already. For a limit of `0`, the cache does not
        // need to be considered.
        if quota.limit <= 0 {
            return Action::Check(quantity);
        }

        let cache = self.cache.pin();

        let value = cache.update(quota.key, |q| {
            let (consumed, local_use) = match q {
                // There is already a sync requested which we need to wait for to get consistent
                // data, we cannot make a decision here at this time.
                CachedQuota2::NeedsSync => return CachedQuota2::NeedsSync,
                // This variant only exists as a way of returning a quantity on the first
                // `NeedsSync` decision. We can safely erase it into `NeedsSync` without a value.
                CachedQuota2::NeedsSyncWithQuantity(_) => return CachedQuota2::NeedsSync,
                CachedQuota2::Active {
                    consumed,
                    local_use,
                } => (*consumed, *local_use),
            };

            let remaining = quota.limit.saturating_sub(consumed).max(0);
            let max_allowed_spend = remaining / self.max_over_spend_divisor.get();

            let total_local_use = local_use + quantity;
            match total_local_use > max_allowed_spend {
                true => NonZeroI64::new(total_local_use)
                    .map(CachedQuota2::NeedsSyncWithQuantity)
                    .unwrap_or(CachedQuota2::NeedsSync),
                false => CachedQuota2::Active {
                    consumed,
                    local_use: total_local_use,
                },
            }
        });

        match value {
            Some(CachedQuota2::NeedsSyncWithQuantity(q)) => Action::Check(q.get()),
            Some(CachedQuota2::NeedsSync) | None => Action::Check(quantity),
            Some(CachedQuota2::Active { .. }) => Action::Accept,
        }
    }

    pub fn update_state(&self, quota: Quota<T>, consumed: i64) {
        let cache = self.cache.pin();

        // TODO: maybe we can do some error reporting? How much we fucked up?
        // TODO: maybe spill over errors to new intervals?

        cache.update_or_insert(
            quota.key,
            |q| match q {
                CachedQuota2::NeedsSync | CachedQuota2::NeedsSyncWithQuantity(_) => {
                    CachedQuota2::Active {
                        consumed,
                        local_use: 0,
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
                CachedQuota2::Active {
                    consumed: _,
                    local_use,
                } => CachedQuota2::Active {
                    consumed,
                    local_use: *local_use,
                },
            },
            CachedQuota2::Active {
                consumed,
                local_use: 0,
            },
        );
    }
}

#[derive(Debug)]
enum CachedQuota2 {
    /// The cache currently cannot make a decision and needs an updated consumption value from the
    /// synchronized store.
    NeedsSync,
    /// Like [`Self::NeedsSync`], but also carries a total quantity which needs to be synchronized
    /// with the store.
    NeedsSyncWithQuantity(NonZeroI64),
    /// The cache is active and can still make decisions without a synchronization.
    Active { consumed: i64, local_use: i64 },
}

#[derive(Debug)]
struct CachedQuota {
    /// The last known, consistently synchronized consumed amount for a specific quota.
    consumed: i64,

    // spent: AtomicI64,
    spent: i64,
}

#[derive(Debug)]
pub enum Action {
    Accept,
    Check(i64),
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
        };
        let q2 = Quota {
            limit: 50,
            key: "k2",
        };

        // First access must always go to synchronized store.
        assert!(matches!(cache.foobar(q1, 10), Action::Check(10)));
        // So does any following access until there was a sync.
        assert!(matches!(cache.foobar(q1, 9), Action::Check(9)));

        // Same for other keys.
        assert!(matches!(cache.foobar(q2, 12), Action::Check(12)));

        // Sync internal state to 30, for this test, there will be 70 remaining, meaning the cache
        // is expected to accept 7 more items without requiring another sync.
        cache.update_state(q1, 30);
        // TODO: test next call with > 7, < 7, = 7, 6 + large

        // A different key still needs a sync.
        assert!(matches!(cache.foobar(q2, 12), Action::Check(12)));

        // 70 remaining, 10% -> 7 accepted.
        assert!(matches!(cache.foobar(q1, 5), Action::Accept));
        assert!(matches!(cache.foobar(q1, 2), Action::Accept));
        // This one goes over the limit, we need to now check a total of 8.
        assert!(matches!(cache.foobar(q1, 1), Action::Check(8)));

        // A new sync is required
        assert!(matches!(cache.foobar(q1, 1), Action::Check(1)));
        assert!(matches!(cache.foobar(q1, 2), Action::Check(2)));
        // A different key is still a different key.
        assert!(matches!(cache.foobar(q2, 3), Action::Check(3)));

        // Consumed state is absolute not relative.
        cache.update_state(q1, 50);
        // This is too much.
        assert!(matches!(cache.foobar(q2, 6), Action::Check(6)));
        // Need another sync again.
        assert!(matches!(cache.foobar(q2, 1), Action::Check(1)));

        // Negative state can exist due to refunds.
        cache.update_state(q1, -100);
        // We now have `200` remaining quota -> 20 (= 10%).
        assert!(matches!(cache.foobar(q1, 20), Action::Accept));
        // Too much, check the entire local usage.
        assert!(matches!(cache.foobar(q1, 1), Action::Check(21)));
    }
}
