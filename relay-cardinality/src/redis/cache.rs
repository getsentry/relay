use std::fmt;
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use relay_common::time::UnixTimestamp;
use relay_statsd::metric;

use crate::redis::QuotaScoping;
use crate::statsd::{CardinalityLimiterCounters, CardinalityLimiterTimers};
use crate::window::Slot;

/// Cached outcome, wether the item can be accepted, rejected or the cache has no information about
/// this hash.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CacheOutcome {
    /// Hash accepted by cache.
    Accepted,
    /// Hash rejected by cache.
    Rejected,
    /// Cache has no information about the hash.
    Unknown,
}

/// Internal cache remembering already accepted elements and current cardinality.
///
/// Only caches for the currently active granule of the sliding window.
pub struct Cache {
    inner: RwLock<Inner>,
}

impl Cache {
    pub fn new(vacuum_interval: Duration) -> Self {
        Self {
            inner: RwLock::new(Inner {
                cache: Default::default(),
                vacuum_interval,
                last_vacuum: UnixTimestamp::from_secs(0),
            }),
        }
    }

    /// Acquires a read lock from the cache and returns a read handle.
    ///
    /// All operations done on the handle share the same lock. To release the lock
    /// the returned [`CacheRead`] must be dropped.
    pub fn read(&self, timestamp: UnixTimestamp) -> CacheRead<'_> {
        let inner = self.inner.read().unwrap_or_else(PoisonError::into_inner);
        CacheRead::new(inner, timestamp)
    }

    /// Acquires a write lock from the cache and returns an update handle.
    ///
    /// All operations done on the handle share the same lock. To release the lock
    /// the returned [`CacheUpdate`] must be dropped.
    pub fn update(&self, scope: QuotaScoping, timestamp: UnixTimestamp) -> CacheUpdate<'_> {
        let mut inner = self.inner.write().unwrap_or_else(PoisonError::into_inner);

        inner.vacuum(timestamp);

        let slot = scope.active_slot(timestamp);
        let cache = inner.cache.entry(scope).or_default();

        // If the slot is older, don't do anything and give up the lock early.
        if slot < cache.current_slot {
            return CacheUpdate::noop();
        }

        // If the slot is newer than the current slot, reset the cache to the new slot.
        if slot > cache.current_slot {
            cache.reset(slot);
        }

        CacheUpdate::new(inner, scope)
    }
}

impl fmt::Debug for Cache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner.read().unwrap_or_else(PoisonError::into_inner);
        f.debug_tuple("Cache").field(&inner.cache).finish()
    }
}

/// Cache read handle.
///
/// Holds a cache read lock, the lock is released on drop.
pub struct CacheRead<'a> {
    inner: RwLockReadGuard<'a, Inner>,
    timestamp: UnixTimestamp,
}

/// Internal state for [`CacheRead`].
impl<'a> CacheRead<'a> {
    /// Creates a new [`CacheRead`] which reads from the cache.
    fn new(inner: RwLockReadGuard<'a, Inner>, timestamp: UnixTimestamp) -> Self {
        Self { inner, timestamp }
    }

    pub fn check(&self, scope: QuotaScoping, hash: u32, limit: u64) -> CacheOutcome {
        let Some(cache) = self.inner.cache.get(&scope) else {
            return CacheOutcome::Unknown;
        };

        let slot = scope.active_slot(self.timestamp);
        cache.check(slot, hash, limit)
    }
}

/// Cache update handle.
///
/// Holds a cache write lock, the lock is released on drop.
pub struct CacheUpdate<'a>(CacheUpdateInner<'a>);

/// Internal state for [`CacheUpdate`].
enum CacheUpdateInner<'a> {
    Noop,
    Cache {
        inner: RwLockWriteGuard<'a, Inner>,
        key: QuotaScoping,
    },
}

impl<'a> CacheUpdate<'a> {
    /// Creates a new [`CacheUpdate`] which operates on the passed cache.
    fn new(inner: RwLockWriteGuard<'a, Inner>, key: QuotaScoping) -> Self {
        Self(CacheUpdateInner::Cache { inner, key })
    }

    /// Creates a new noop [`CacheUpdate`] which does not require a lock.
    fn noop() -> Self {
        Self(CacheUpdateInner::Noop)
    }

    /// Marks a hash as accepted in the cache, future checks of the item will immediately mark the
    /// item as accepted.
    pub fn accept(&mut self, hash: u32) {
        if let CacheUpdateInner::Cache { inner, key } = &mut self.0 {
            if let Some(cache) = inner.cache.get_mut(key) {
                cache.insert(hash);
            }
        }
    }
}

/// Critical section of the [`Cache`].
#[derive(Debug)]
struct Inner {
    cache: hashbrown::HashMap<QuotaScoping, ScopedCache>,
    vacuum_interval: Duration,
    last_vacuum: UnixTimestamp,
}

impl Inner {
    fn vacuum(&mut self, ts: UnixTimestamp) {
        // Debounce the vacuuming.
        let secs_since_last_vacuum = ts.as_secs().saturating_sub(self.last_vacuum.as_secs());
        if secs_since_last_vacuum < self.vacuum_interval.as_secs() {
            return;
        }
        self.last_vacuum = ts;

        let expired = metric!(timer(CardinalityLimiterTimers::CacheVacuum), {
            self.cache
                .drain_filter(|scope, cache| cache.current_slot < scope.active_slot(ts))
                .count()
        });
        metric!(counter(CardinalityLimiterCounters::RedisCacheVacuum) += expired as i64);
    }
}

/// Scope specific information of the cache.
#[derive(Debug, Default)]
struct ScopedCache {
    // Uses hashbrown for a faster hasher `ahash`, benchmarks show about 10% speedup.
    hashes: hashbrown::HashSet<u32>,
    current_slot: Slot,
}

impl ScopedCache {
    fn check(&self, slot: Slot, hash: u32, limit: u64) -> CacheOutcome {
        if slot != self.current_slot {
            return CacheOutcome::Unknown;
        }

        if self.hashes.contains(&hash) {
            // Local cache copy contains the hash -> accept it straight away
            CacheOutcome::Accepted
        } else if self.hashes.len() as u64 >= limit {
            // We have more or the same amount of items in the local cache as the cardinality
            // limit -> this new item/hash is rejected.
            CacheOutcome::Rejected
        } else {
            // Check with Redis.
            CacheOutcome::Unknown
        }
    }

    fn insert(&mut self, hash: u32) {
        self.hashes.insert(hash);
    }

    fn reset(&mut self, slot: Slot) {
        self.current_slot = slot;
        self.hashes.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use relay_base_schema::project::ProjectId;

    use crate::{CardinalityLimit, CardinalityScope, OrganizationId, Scoping, SlidingWindow};

    use super::*;

    fn build_scoping(organization_id: OrganizationId, window: SlidingWindow) -> QuotaScoping {
        QuotaScoping::new(
            Scoping {
                organization_id,
                project_id: ProjectId::new(1),
            },
            &CardinalityLimit {
                id: String::new(),
                window,
                limit: 100,
                scope: CardinalityScope::Organization,
                namespace: None,
            },
        )
        .unwrap()
    }

    #[test]
    fn test_cache() {
        let cache = Cache::new(Duration::from_secs(180));

        let window = SlidingWindow {
            window_seconds: 100,
            granularity_seconds: 10,
        };
        let scope = build_scoping(1, window);
        let now = UnixTimestamp::now();
        let future = now + Duration::from_secs(window.granularity_seconds + 1);

        {
            let cache = cache.read(now);
            assert_eq!(cache.check(scope, 1, 1), CacheOutcome::Unknown);
        }

        {
            let mut cache = cache.update(scope, now);
            cache.accept(1);
            cache.accept(2);
        }

        {
            let r1 = cache.read(now);
            // All in cache, no matter the limit.
            assert_eq!(r1.check(scope, 1, 1), CacheOutcome::Accepted);
            assert_eq!(r1.check(scope, 1, 2), CacheOutcome::Accepted);
            assert_eq!(r1.check(scope, 2, 1), CacheOutcome::Accepted);

            // Not in cache, depends on limit and amount of items in the cache.
            assert_eq!(r1.check(scope, 3, 3), CacheOutcome::Unknown);
            assert_eq!(r1.check(scope, 3, 2), CacheOutcome::Rejected);

            // Read concurrently from a future slot.
            let r2 = cache.read(future);
            assert_eq!(r2.check(scope, 1, 1), CacheOutcome::Unknown);
            assert_eq!(r2.check(scope, 2, 2), CacheOutcome::Unknown);
        }

        {
            // Move the cache into the future.
            let mut cache = cache.update(scope, future);
            cache.accept(1);
        }

        {
            let future = cache.read(future);
            // The future only contains `1`.
            assert_eq!(future.check(scope, 1, 1), CacheOutcome::Accepted);
            assert_eq!(future.check(scope, 2, 1), CacheOutcome::Rejected);

            let past = cache.read(now);
            // The cache has no information about the past.
            assert_eq!(past.check(scope, 1, 1), CacheOutcome::Unknown);
            assert_eq!(past.check(scope, 2, 1), CacheOutcome::Unknown);
            assert_eq!(past.check(scope, 3, 99), CacheOutcome::Unknown);
        }
    }

    #[test]
    fn test_cache_different_scopings() {
        let cache = Cache::new(Duration::from_secs(180));

        let window = SlidingWindow {
            window_seconds: 100,
            granularity_seconds: 10,
        };
        let scope1 = build_scoping(1, window);
        let scope2 = build_scoping(2, window);

        let now = UnixTimestamp::now();

        {
            let mut cache = cache.update(scope1, now);
            cache.accept(1);
        }

        {
            let mut cache = cache.update(scope2, now);
            cache.accept(1);
            cache.accept(2);
        }

        {
            let cache = cache.read(now);
            assert_eq!(cache.check(scope1, 1, 99), CacheOutcome::Accepted);
            assert_eq!(cache.check(scope1, 2, 99), CacheOutcome::Unknown);
            assert_eq!(cache.check(scope1, 3, 99), CacheOutcome::Unknown);
            assert_eq!(cache.check(scope2, 3, 1), CacheOutcome::Rejected);
            assert_eq!(cache.check(scope2, 1, 99), CacheOutcome::Accepted);
            assert_eq!(cache.check(scope2, 2, 99), CacheOutcome::Accepted);
            assert_eq!(cache.check(scope2, 3, 99), CacheOutcome::Unknown);
            assert_eq!(cache.check(scope2, 3, 2), CacheOutcome::Rejected);
        }
    }

    #[test]
    fn test_cache_vacuum() {
        let vacuum_interval = Duration::from_secs(30);
        let cache = Cache::new(vacuum_interval);

        let window = SlidingWindow {
            window_seconds: vacuum_interval.as_secs() * 10,
            granularity_seconds: vacuum_interval.as_secs() * 2,
        };
        let scope1 = build_scoping(1, window);
        let scope2 = build_scoping(2, window);

        let now = UnixTimestamp::now();
        let in_interval = now + Duration::from_secs(vacuum_interval.as_secs() - 1);
        let future = now + Duration::from_secs(vacuum_interval.as_secs() * 3);

        {
            let mut cache = cache.update(scope1, now);
            cache.accept(10);
        }

        {
            let mut cache = cache.update(scope2, now);
            cache.accept(20);
        }

        {
            // Verify entries.
            let cache = cache.read(now);
            assert_eq!(cache.check(scope1, 10, 100), CacheOutcome::Accepted);
            assert_eq!(cache.check(scope2, 20, 100), CacheOutcome::Accepted);
        }

        {
            // Fast forward time a little bit and stay within all bounds.
            let mut cache = cache.update(scope2, in_interval);
            cache.accept(21);
        }

        {
            // Verify entries with old timestamp, values should still be there.
            let cache = cache.read(now);
            assert_eq!(cache.check(scope1, 10, 100), CacheOutcome::Accepted);
        }

        {
            // Fast forward time far in the future, should vacuum old values.
            let mut cache = cache.update(scope2, future);
            cache.accept(22);
        }

        {
            // Verify that there is no data with the original timestamp.
            let cache = cache.read(now);
            assert_eq!(cache.check(scope1, 10, 100), CacheOutcome::Unknown);
            assert_eq!(cache.check(scope1, 11, 100), CacheOutcome::Unknown);
            assert_eq!(cache.check(scope2, 20, 100), CacheOutcome::Unknown);
            assert_eq!(cache.check(scope2, 21, 100), CacheOutcome::Unknown);
        }

        {
            // Make sure the new/current values are cached.
            let cache = cache.read(future);
            assert_eq!(cache.check(scope2, 22, 100), CacheOutcome::Accepted);
        }
    }
}
