use std::{
    collections::HashMap,
    hash::Hash,
    sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::SystemTime,
};

use relay_redis::{
    redis::{self, FromRedisValue, Script},
    Connection, RedisPool,
};
use relay_statsd::{metric, CounterMetric};

use crate::{
    limiter::{CardinalityScope, Entry, EntryId, Limiter, Rejection},
    statsd::{CardinalityLimiterCounters, CardinalityLimiterHistograms},
    OrganizationId, Result,
};

/// Key prefix used for Redis keys.
const KEY_PREFIX: &str = "relay:cardinality";

struct CardinalityScript(Script);

/// Status wether an entry/bucket is accepted or rejected by the cardinality limiter.
#[derive(Debug, Clone, Copy)]
enum Status {
    /// Item is rejected.
    Rejected,
    /// Item is accepted.
    Accepted,
}

impl Status {
    fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected)
    }
}

impl FromRedisValue for Status {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let accepted = bool::from_redis_value(v)?;
        Ok(if accepted {
            Self::Accepted
        } else {
            Self::Rejected
        })
    }
}

#[derive(Debug)]
struct CardinalityScriptResult {
    cardinality: u64,
    statuses: Vec<Status>,
}

impl FromRedisValue for CardinalityScriptResult {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let Some(seq) = v.as_sequence() else {
            return Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "Expected a sequence from the cardinality script",
            )));
        };

        let mut iter = seq.iter();

        let cardinality = iter
            .next()
            .ok_or_else(|| {
                redis::RedisError::from((
                    redis::ErrorKind::TypeError,
                    "Expected cardinality as the first result from the cardinality script",
                ))
            })
            .and_then(FromRedisValue::from_redis_value)?;

        let mut statuses = Vec::with_capacity(iter.len());
        for value in iter {
            statuses.push(Status::from_redis_value(value)?);
        }

        Ok(Self {
            cardinality,
            statuses,
        })
    }
}

impl CardinalityScript {
    fn load() -> Self {
        Self(Script::new(include_str!("cardinality.lua")))
    }

    fn invoke(
        &self,
        con: &mut Connection,
        limit: usize,
        expire: u64,
        hashes: impl Iterator<Item = u32>,
        keys: impl Iterator<Item = String>,
    ) -> Result<CardinalityScriptResult> {
        let mut invocation = self.0.prepare_invoke();

        for key in keys {
            invocation.key(key);
        }

        invocation.arg(limit);
        invocation.arg(expire);

        let mut num_hashes = 0;
        for hash in hashes {
            invocation.arg(hash);
            num_hashes += 1;
        }

        let result: CardinalityScriptResult = invocation
            .invoke(con)
            .map_err(relay_redis::RedisError::Redis)?;

        if num_hashes != result.statuses.len() {
            return Err(relay_redis::RedisError::Redis(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Script returned an invalid number of elements",
                format!(
                    "Expected {num_hashes} results, got {}",
                    result.statuses.len()
                ),
            )))
            .into());
        }

        Ok(result)
    }
}

struct CheckedLimits {
    entries: Vec<RedisEntry>,
    statuses: Vec<Status>,
}

impl CheckedLimits {
    fn empty() -> Self {
        Self {
            entries: Vec::new(),
            statuses: Vec::new(),
        }
    }
}

impl IntoIterator for CheckedLimits {
    type Item = (RedisEntry, Status);
    type IntoIter = std::iter::Zip<std::vec::IntoIter<RedisEntry>, std::vec::IntoIter<Status>>;

    fn into_iter(self) -> Self::IntoIter {
        debug_assert_eq!(
            self.entries.len(),
            self.statuses.len(),
            "expected same amount of entries as statuses"
        );
        std::iter::zip(self.entries, self.statuses)
    }
}

/// Implementation uses Redis sets to keep track of cardinality.
pub struct RedisSetLimiter {
    redis: RedisPool,
    window: SlidingWindow,
    script: CardinalityScript,
    cache: Cache,
    #[cfg(test)]
    time_offset: u64,
}

/// A Redis based limiter using Redis sets to track cardinality and membership.
impl RedisSetLimiter {
    /// Creates a new [`RedisSetLimiter`].
    pub fn new(redis: RedisPool, window: SlidingWindow) -> Self {
        Self {
            redis,
            window,
            script: CardinalityScript::load(),
            cache: Cache::default(),
            #[cfg(test)]
            time_offset: 0,
        }
    }

    /// Checks the limits for a specific scope.
    ///
    /// Returns an iterator over all entries which have been accepted.
    fn check_limits(
        &self,
        con: &mut Connection,
        organization_id: OrganizationId,
        item_scope: String,
        entries: Vec<RedisEntry>,
        timestamp: u64,
        limit: usize,
    ) -> Result<CheckedLimits> {
        if entries.is_empty() {
            return Ok(CheckedLimits::empty());
        }

        metric!(
            histogram(CardinalityLimiterHistograms::RedisCheckHashes) = entries.len() as u64,
            scope = &item_scope,
        );

        let keys = self
            .window
            .iter(timestamp)
            .map(|time_bucket| self.to_redis_key(organization_id, &item_scope, time_bucket));

        let hashes = entries.iter().map(|entry| entry.hash);

        // The expiry is a off by `window.granularity_seconds`,
        // but since this is only used for cleanup, this is not an issue.
        let result = self
            .script
            .invoke(con, limit, self.window.window_seconds, hashes, keys)?;

        metric!(
            histogram(CardinalityLimiterHistograms::RedisSetCardinality) = result.cardinality,
            scope = &item_scope,
        );

        Ok(CheckedLimits {
            entries,
            statuses: result.statuses,
        })
    }

    fn to_redis_key(
        &self,
        organization_id: OrganizationId,
        item_scope: &str,
        window: u64,
    ) -> String {
        format!("{KEY_PREFIX}:scope-{{{organization_id}}}-{item_scope}-{window}",)
    }
}

/// Entry used by the Redis limiter.
#[derive(Clone, Copy, Debug)]
struct RedisEntry {
    /// The correlating entry id.
    id: EntryId,
    /// The entry hash.
    hash: u32,
}

impl RedisEntry {
    /// Creates a new Redis entry in the rejected status.
    fn new(id: EntryId, hash: u32) -> Self {
        Self { id, hash }
    }

    /// Turns the item into a rejection.
    fn rejection(self) -> Rejection {
        Rejection { id: self.id }
    }
}

impl Limiter for RedisSetLimiter {
    fn check_cardinality_limits<I, T>(
        &self,
        organization_id: u64,
        entries: I,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Rejection>>>
    where
        I: IntoIterator<Item = Entry<T>>,
        T: CardinalityScope,
    {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Allows to fast forward time in tests.
        #[cfg(test)]
        let timestamp = timestamp + self.time_offset;

        let cache_window = self.window.active_time_bucket(timestamp);

        let mut rejected = Vec::new();
        // There are currently 5 metric namespaces and we know the scope is currently only the
        // metric namespace -> we can be smart about it and pre-allocate the hashmap.
        let mut entries_by_scope = HashMap::with_capacity(5);
        let mut cache_hits = HitCounter::default();
        let mut acceptions_rejections = HitCounter::default();

        let cache = self.cache.read(organization_id, cache_window, limit);
        for Entry { id, scope, hash } in entries {
            let item_scope = scope.to_string();
            match cache.check(&item_scope, hash) {
                CacheOutcome::Accepted => {
                    // Accepted already, nothing to do.
                    cache_hits.hit(&item_scope);
                    acceptions_rejections.hit(&item_scope);
                }
                CacheOutcome::Rejected => {
                    // Rejected, add it to the rejected list and move on.
                    rejected.push(Rejection::new(id));
                    cache_hits.hit(&item_scope);
                    acceptions_rejections.miss(&item_scope);
                }
                CacheOutcome::Unknown => {
                    cache_hits.miss(&item_scope);

                    let key = (organization_id, item_scope);
                    let entry = RedisEntry::new(id, hash);

                    entries_by_scope
                        .entry(key)
                        .or_insert_with(Vec::new)
                        .push(entry)
                }
            }
        }
        // Give up cache lock!
        drop(cache);

        cache_hits.report(
            CardinalityLimiterCounters::RedisCacheHit,
            CardinalityLimiterCounters::RedisCacheMiss,
        );

        let mut client = self.redis.client()?;
        let mut connection = client.connection()?;

        for ((organization_id, item_scope), entries) in entries_by_scope {
            let results = self.check_limits(
                &mut connection,
                organization_id,
                item_scope.clone(),
                entries,
                timestamp,
                limit,
            )?;

            // This always acquires a write lock, but since
            let mut cache = self
                .cache
                .update(organization_id, &item_scope, cache_window);

            for (entry, status) in results {
                if status.is_rejected() {
                    acceptions_rejections.miss(&item_scope);
                    rejected.push(entry.rejection());
                } else {
                    acceptions_rejections.hit(&item_scope);
                    cache.accept(entry.hash);
                }
            }
        }

        if !rejected.is_empty() {
            relay_log::debug!(
                organization_id = organization_id,
                "rejected {} metrics due to cardinality limit",
                rejected.len(),
            );
        }

        acceptions_rejections.report(
            CardinalityLimiterCounters::Accepted,
            CardinalityLimiterCounters::Rejected,
        );

        Ok(Box::new(rejected.into_iter()))
    }
}

/// A sliding window.
#[derive(Debug, Clone, Copy)]
pub struct SlidingWindow {
    /// The number of seconds to apply the limit to.
    pub window_seconds: u64,
    /// A number between 1 and `window_seconds`. Since `window_seconds` is a
    /// sliding window, configure what the granularity of that window is.
    ///
    /// If this is equal to `window_seconds`, the quota resets to 0 every
    /// `window_seconds`.  If this is a very small number, the window slides
    /// "more smoothly" at the expense of having much more redis keys.
    ///
    /// The number of redis keys required to enforce a quota is `window_seconds /
    /// granularity_seconds`.
    pub granularity_seconds: u64,
}

impl SlidingWindow {
    /// Iterate over the quota's window, yielding values representing each
    /// (absolute) granule.
    ///
    /// This function is used to calculate keys for storing the number of
    /// requests made in each granule.
    ///
    /// The iteration is done in reverse-order (newest timestamp to oldest),
    /// starting with the key to which a currently-processed request should be
    /// added. That request's timestamp is `request_timestamp`.
    ///
    /// * `request_timestamp / self.granularity_seconds - 1`
    /// * `request_timestamp / self.granularity_seconds - 2`
    /// * `request_timestamp / self.granularity_seconds - 3`
    /// * ...
    pub fn iter(&self, timestamp: u64) -> impl Iterator<Item = u64> {
        let value = timestamp / self.granularity_seconds;
        (0..self.window_seconds / self.granularity_seconds)
            .map(move |i| value.saturating_sub(i + 1))
    }

    /// The active bucket is the oldest active granule.
    pub fn active_time_bucket(&self, timestamp: u64) -> u64 {
        self.iter(timestamp).last().unwrap_or(0)
    }
}

/// Cached outcome, wether the item can be accepted, rejected or the cache has no information about
/// this hash.
#[derive(Debug)]
enum CacheOutcome {
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
struct Cache {
    inner: RwLock<CacheInner>,
}

impl Cache {
    /// Acquires a read lock from the cache and returns a read handle.
    ///
    /// All operations done on the handle share the same lock. To release the lock
    /// the returned [`CacheRead`] must be dropped.
    fn read(&self, organization_id: OrganizationId, window: u64, limit: usize) -> CacheRead<'_> {
        let inner = self.inner.read().unwrap_or_else(PoisonError::into_inner);

        // If the window does not match, we can already release the lock.
        if window != inner.current_window {
            return CacheRead::Empty;
        }

        CacheRead::Read {
            inner,
            organization_id,
            limit,
        }
    }

    /// Acquires a write lock from the cache and returns an update handle.
    ///
    /// All operations done on the handle share the same lock. To release the lock
    /// the returned [`CacheUpdate`] must be dropped.
    fn update<'a>(
        &'a self,
        organization_id: OrganizationId,
        item_scope: &'a str,
        window: u64,
    ) -> CacheUpdate<'a> {
        let mut inner = self.inner.write().unwrap_or_else(PoisonError::into_inner);

        // If the passed window is newer then the current window, reset the cache to the new window.
        if window > inner.current_window {
            inner.current_window = window;
            inner.cache.clear();
        } else if window != inner.current_window {
            // If the window is older don't do anything and give up the lock early.
            return CacheUpdate::Empty;
        }

        CacheUpdate::Update {
            inner,
            key: CacheKeyRef {
                organization_id,
                item_scope,
            },
        }
    }
}

/// Cache read handle.
///
/// Holds a cache read lock, the lock is released on drop.
enum CacheRead<'a> {
    Empty,
    Read {
        inner: RwLockReadGuard<'a, CacheInner>,
        organization_id: OrganizationId,
        limit: usize,
    },
}

impl<'a> CacheRead<'a> {
    fn check(&self, item_scope: &str, hash: u32) -> CacheOutcome {
        match self {
            CacheRead::Empty => CacheOutcome::Unknown,
            CacheRead::Read {
                inner,
                organization_id,
                limit,
            } => inner
                .cache
                .get(&CacheKeyRef {
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
enum CacheUpdate<'a> {
    Empty,
    Update {
        inner: RwLockWriteGuard<'a, CacheInner>,
        key: CacheKeyRef<'a>,
    },
}

impl<'a> CacheUpdate<'a> {
    /// Marks a hash as accepted in the cache, future checks of the item will immediately mark the
    /// item as accepted.
    pub fn accept(&mut self, hash: u32) {
        if let Self::Update { inner, key } = self {
            inner.cache.entry_ref(key).or_default().0.insert(hash);
        }
    }
}

/// Critical section of the [`Cache`].
#[derive(Debug, Default)]
struct CacheInner {
    cache: hashbrown::HashMap<CacheKey, CacheValue>,
    current_window: u64,
}

/// Scope specific information of the cache.
#[derive(Debug, Default)]
struct CacheValue(hashbrown::HashSet<u32>);

impl CacheValue {
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
}

/// Key/scoping for the cardinality cache.
#[derive(Debug, PartialEq, Eq)]
struct CacheKey {
    organization_id: OrganizationId,
    item_scope: String,
}

impl Hash for CacheKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        CacheKeyRef {
            organization_id: self.organization_id,
            item_scope: &self.item_scope,
        }
        .hash(state)
    }
}

impl hashbrown::Equivalent<CacheKeyRef<'_>> for CacheKey {
    fn equivalent(&self, key: &CacheKeyRef<'_>) -> bool {
        let CacheKeyRef {
            organization_id,
            item_scope,
        } = key;

        self.organization_id == *organization_id && &self.item_scope == item_scope
    }
}

impl From<&CacheKeyRef<'_>> for CacheKey {
    fn from(value: &CacheKeyRef<'_>) -> Self {
        Self {
            organization_id: value.organization_id,
            item_scope: value.item_scope.to_owned(),
        }
    }
}

/// A borrowed [`CacheKey`].
#[derive(Debug, PartialEq, Eq, Hash)]
struct CacheKeyRef<'a> {
    organization_id: OrganizationId,
    item_scope: &'a str,
}

impl hashbrown::Equivalent<CacheKey> for CacheKeyRef<'_> {
    fn equivalent(&self, key: &CacheKey) -> bool {
        let CacheKey {
            organization_id,
            item_scope,
        } = key;

        self.organization_id == *organization_id && self.item_scope == item_scope
    }
}

/// Helper to aggregate cache hits by scope before reporting them to statsd.
#[derive(Debug)]
struct HitCounter(hashbrown::HashMap<String, (i64, i64)>);

impl HitCounter {
    /// Tracks a hit for a scope.
    fn hit(&mut self, scope: &str) {
        self.0.entry_ref(scope).or_default().0 += 1;
    }

    /// Tracks a miss for a scope.
    fn miss(&mut self, scope: &str) {
        self.0.entry_ref(scope).or_default().1 += 1;
    }

    /// Consumes all counters and reports to statsd.
    fn report<H, M>(self, hit: H, miss: M)
    where
        H: CounterMetric,
        M: CounterMetric,
    {
        for (scope, (hits, misses)) in self.0 {
            if hits > 0 {
                metric!(counter(hit) += hits, scope = &scope);
            }
            if misses > 0 {
                metric!(counter(miss) += misses, scope = &scope);
            }
        }
    }
}

impl Default for HitCounter {
    fn default() -> Self {
        // We know that currently the only scope being tracked is a `MetricNamespace`.
        Self(hashbrown::HashMap::with_capacity(5))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::AtomicU64;

    use relay_redis::RedisConfigOptions;

    use crate::limiter::EntryId;
    use crate::OrganizationId;

    use super::*;

    fn build_limiter() -> RedisSetLimiter {
        let url = std::env::var("RELAY_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());

        let redis = RedisPool::single(&url, RedisConfigOptions::default()).unwrap();

        let window = SlidingWindow {
            window_seconds: 3600,
            granularity_seconds: 360,
        };
        RedisSetLimiter::new(redis, window)
    }

    fn new_org(limiter: &RedisSetLimiter) -> OrganizationId {
        static ORGS: AtomicU64 = AtomicU64::new(100);
        let organization_id = ORGS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        limiter.flush_org(organization_id);
        organization_id
    }

    impl RedisSetLimiter {
        /// Remove all redis state for an organization.
        fn flush_org(&self, organization_id: OrganizationId) {
            let pattern = format!("{KEY_PREFIX}:scope-{{{organization_id}}}-*");

            let mut client = self.redis.client().unwrap();
            let mut connection = client.connection().unwrap();

            let mut keys = redis::cmd("KEYS");
            let keys = keys
                .arg(pattern)
                .query::<Vec<String>>(&mut connection)
                .unwrap();

            if !keys.is_empty() {
                let mut del = redis::cmd("DEL");
                for key in keys {
                    del.arg(key);
                }
                del.query::<()>(&mut connection).unwrap();
            }
        }
    }

    #[test]
    fn test_limiter_accept_previously_seen() {
        let limiter = build_limiter();

        let org = new_org(&limiter);
        let entries = [
            Entry::new(EntryId(0), "custom", 0),
            Entry::new(EntryId(1), "custom", 1),
            Entry::new(EntryId(2), "custom", 2),
            Entry::new(EntryId(3), "custom", 3),
            Entry::new(EntryId(4), "custom", 4),
            Entry::new(EntryId(5), "custom", 5),
        ];

        // 6 items, limit is 5 -> 1 rejection.
        let rejected = limiter
            .check_cardinality_limits(org, entries, 5)
            .unwrap()
            .map(|e| e.id)
            .collect::<HashSet<_>>();
        assert_eq!(rejected.len(), 1);

        // We're at the limit but it should still accept already accepted elements, even with a
        // samller limit than previously accepted.
        let rejected2 = limiter
            .check_cardinality_limits(org, entries, 3)
            .unwrap()
            .map(|e| e.id)
            .collect::<HashSet<_>>();
        assert_eq!(rejected2, rejected);

        // A higher limit should accept everthing
        let rejected3 = limiter
            .check_cardinality_limits(org, entries, 10)
            .unwrap()
            .map(|e| e.id)
            .collect::<HashSet<_>>();
        assert_eq!(rejected3.len(), 0);
    }

    #[test]
    fn test_limiter_small_within_limits() {
        let limiter = build_limiter();
        let org = new_org(&limiter);

        let entries = (0..50)
            .map(|i| Entry::new(EntryId(i as usize), "custom", i))
            .collect::<Vec<_>>();
        let rejected = limiter
            .check_cardinality_limits(org, entries, 10_000)
            .unwrap();
        assert_eq!(rejected.count(), 0);

        let entries = (100..150)
            .map(|i| Entry::new(EntryId(i as usize), "custom", i))
            .collect::<Vec<_>>();
        let rejected = limiter
            .check_cardinality_limits(org, entries, 10_000)
            .unwrap();
        assert_eq!(rejected.count(), 0);
    }

    #[test]
    fn test_limiter_big_limit() {
        let limiter = build_limiter();

        let org = new_org(&limiter);
        let entries = (0..100_000)
            .map(|i| Entry::new(EntryId(i as usize), "custom", i))
            .collect::<Vec<_>>();

        let rejected = limiter
            .check_cardinality_limits(org, entries, 80_000)
            .unwrap();
        assert_eq!(rejected.count(), 20_000);
    }

    #[test]
    fn test_limiter_sliding_window() {
        let mut limiter = build_limiter();

        let org = new_org(&limiter);
        let entries1 = [Entry::new(EntryId(0), "custom", 0)];
        let entries2 = [Entry::new(EntryId(1), "custom", 1)];

        // 1 item and limit is 1 -> No rejections.
        let rejected = limiter.check_cardinality_limits(org, entries1, 1).unwrap();
        assert_eq!(rejected.count(), 0);

        for i in 0..limiter.window.window_seconds / limiter.window.granularity_seconds {
            // Fast forward time.
            limiter.time_offset = i * limiter.window.granularity_seconds;

            // Should accept the already inserted item.
            let accepted = limiter.check_cardinality_limits(org, entries1, 1).unwrap();
            assert_eq!(accepted.count(), 0);
            // Should reject the new item.
            let accepted = limiter.check_cardinality_limits(org, entries2, 1).unwrap();
            assert_eq!(accepted.count(), 1);
        }

        // Fast forward time to where we're in the next window.
        limiter.time_offset = limiter.window.window_seconds + 1;
        // Accept the new element.
        let accepted = limiter.check_cardinality_limits(org, entries2, 1).unwrap();
        assert_eq!(accepted.count(), 0);
        // Reject the old element now.
        let accepted = limiter.check_cardinality_limits(org, entries1, 1).unwrap();
        assert_eq!(accepted.count(), 1);
    }

    #[test]
    fn test_quota() {
        let window = SlidingWindow {
            window_seconds: 3600,
            granularity_seconds: 720,
        };

        let timestamp = 1701775000;
        let r = window.iter(timestamp).collect::<Vec<_>>();
        assert_eq!(
            r.len() as u64,
            window.window_seconds / window.granularity_seconds
        );
        assert_eq!(r, vec![2363575, 2363574, 2363573, 2363572, 2363571]);
        assert_eq!(window.active_time_bucket(timestamp), *r.last().unwrap());

        let r2 = window.iter(timestamp + 10).collect::<Vec<_>>();
        assert_eq!(r2, r);

        let r3 = window
            .iter(timestamp + window.granularity_seconds)
            .collect::<Vec<_>>();
        assert_ne!(r3, r);
    }
}
