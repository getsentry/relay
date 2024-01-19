use std::hash::Hash;
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::OrganizationId;

/// Cached outcome, wether the item can be accepted, rejected or the cache has no information about
/// this hash.
#[derive(Debug)]
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
#[derive(Default)]
pub struct Cache {
    inner: RwLock<Inner>,
}

impl Cache {
    /// Acquires a read lock from the cache and returns a read handle.
    ///
    /// All operations done on the handle share the same lock. To release the lock
    /// the returned [`CacheRead`] must be dropped.
    pub fn read(
        &self,
        organization_id: OrganizationId,
        window: u64,
        limit: usize,
    ) -> CacheRead<'_> {
        let inner = self.inner.read().unwrap_or_else(PoisonError::into_inner);

        // If the window does not match, we can already release the lock.
        if window != inner.current_window {
            return CacheRead::noop();
        }

        CacheRead::new(inner, organization_id, limit)
    }

    /// Acquires a write lock from the cache and returns an update handle.
    ///
    /// All operations done on the handle share the same lock. To release the lock
    /// the returned [`CacheUpdate`] must be dropped.
    pub fn update<'a>(
        &'a self,
        organization_id: OrganizationId,
        item_scope: &'a str,
        window: u64,
    ) -> CacheUpdate<'a> {
        let mut inner = self.inner.write().unwrap_or_else(PoisonError::into_inner);

        // If the window is older don't do anything and give up the lock early.
        if window < inner.current_window {
            return CacheUpdate::noop();
        }

        // If the window is newer then the current window, reset the cache to the new window.
        if window > inner.current_window {
            inner.current_window = window;
            inner.cache.clear();
        }

        let key = KeyRef {
            organization_id,
            item_scope,
        };
        CacheUpdate::new(inner, key)
    }
}

/// Cache read handle.
///
/// Holds a cache read lock, the lock is released on drop.
pub struct CacheRead<'a>(CacheReadInner<'a>);

/// Internal state for [`CacheRead`].
enum CacheReadInner<'a> {
    Noop,
    Cache {
        inner: RwLockReadGuard<'a, Inner>,
        organization_id: OrganizationId,
        limit: usize,
    },
}

impl<'a> CacheRead<'a> {
    /// Creates a new [`CacheRead`] which reads from the cache.
    fn new(
        inner: RwLockReadGuard<'a, Inner>,
        organization_id: OrganizationId,
        limit: usize,
    ) -> Self {
        Self(CacheReadInner::Cache {
            inner,
            organization_id,
            limit,
        })
    }

    /// Creates a new noop [`CacheRead`] which does not require a lock.
    fn noop() -> Self {
        Self(CacheReadInner::Noop)
    }

    pub fn check(&self, item_scope: &str, hash: u32) -> CacheOutcome {
        match &self.0 {
            CacheReadInner::Noop => CacheOutcome::Unknown,
            CacheReadInner::Cache {
                inner,
                organization_id,
                limit,
            } => inner
                .cache
                .get(&KeyRef {
                    organization_id: *organization_id,
                    item_scope,
                })
                .map_or(CacheOutcome::Unknown, |s| s.check(hash, *limit)),
        }
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
        key: KeyRef<'a>,
    },
}

impl<'a> CacheUpdate<'a> {
    /// Creates a new [`CacheUpdate`] which operates on the passed cache.
    fn new(inner: RwLockWriteGuard<'a, Inner>, key: KeyRef<'a>) -> Self {
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
            inner.cache.entry_ref(key).or_default().insert(hash);
        }
    }
}

/// Critical section of the [`Cache`].
#[derive(Debug, Default)]
struct Inner {
    cache: hashbrown::HashMap<Key, ScopedCache>,
    current_window: u64,
}

/// Scope specific information of the cache.
#[derive(Debug, Default)]
struct ScopedCache(
    // Uses hashbrown for a faster hasher `ahash`, benchmarks show about 10% speedup.
    hashbrown::HashSet<u32>,
);

impl ScopedCache {
    fn check(&self, hash: u32, limit: usize) -> CacheOutcome {
        if self.0.contains(&hash) {
            // Local cache copy contains the hash -> accept it straight away
            CacheOutcome::Accepted
        } else if self.0.len() >= limit {
            // We have more or the same amount of items in the local cache as the cardinality
            // limit -> this new item/hash is rejected.
            CacheOutcome::Rejected
        } else {
            // Check with Redis.
            CacheOutcome::Unknown
        }
    }

    fn insert(&mut self, hash: u32) {
        self.0.insert(hash);
    }
}

/// Key/scoping for the cardinality cache.
#[derive(Debug, PartialEq, Eq)]
struct Key {
    organization_id: OrganizationId,
    item_scope: String,
}

impl Hash for Key {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        KeyRef {
            organization_id: self.organization_id,
            item_scope: &self.item_scope,
        }
        .hash(state)
    }
}

impl hashbrown::Equivalent<KeyRef<'_>> for Key {
    fn equivalent(&self, key: &KeyRef<'_>) -> bool {
        let KeyRef {
            organization_id,
            item_scope,
        } = key;

        self.organization_id == *organization_id && &self.item_scope == item_scope
    }
}

impl From<&KeyRef<'_>> for Key {
    fn from(value: &KeyRef<'_>) -> Self {
        Self {
            organization_id: value.organization_id,
            item_scope: value.item_scope.to_owned(),
        }
    }
}

/// A borrowed [`Key`].
#[derive(Debug, PartialEq, Eq, Hash)]
struct KeyRef<'a> {
    organization_id: OrganizationId,
    item_scope: &'a str,
}

impl hashbrown::Equivalent<Key> for KeyRef<'_> {
    fn equivalent(&self, key: &Key) -> bool {
        let Key {
            organization_id,
            item_scope,
        } = key;

        self.organization_id == *organization_id && self.item_scope == item_scope
    }
}
