use std::time::Duration;

use relay_redis::{Connection, RedisPool};
use relay_statsd::metric;

use crate::{
    limiter::{Entry, EntryId, Limiter, Rejections, Scoping},
    redis::{
        cache::{Cache, CacheOutcome},
        quota::QuotaScoping,
        script::{CardinalityScript, Status},
    },
    statsd::{
        CardinalityLimiterCounters, CardinalityLimiterHistograms, CardinalityLimiterSets,
        CardinalityLimiterTimers,
    },
    CardinalityLimit, Result,
};
use relay_common::time::UnixTimestamp;

/// Configuration options for the [`RedisSetLimiter`].
pub struct RedisSetLimiterOptions {
    /// Cache vacuum interval for the in memory cache.
    ///
    /// The cache will scan for expired values based on this interval.
    pub cache_vacuum_interval: Duration,
}

/// Implementation uses Redis sets to keep track of cardinality.
pub struct RedisSetLimiter {
    redis: RedisPool,
    script: CardinalityScript,
    cache: Cache,
    #[cfg(test)]
    time_offset: std::time::Duration,
}

/// A Redis based limiter using Redis sets to track cardinality and membership.
impl RedisSetLimiter {
    /// Creates a new [`RedisSetLimiter`].
    pub fn new(options: RedisSetLimiterOptions, redis: RedisPool) -> Self {
        Self {
            redis,
            script: CardinalityScript::load(),
            cache: Cache::new(options.cache_vacuum_interval),
            #[cfg(test)]
            time_offset: std::time::Duration::from_secs(0),
        }
    }

    /// Checks the limits for a specific scope.
    ///
    /// Returns an iterator over all entries which have been accepted.
    fn check_limits(
        &self,
        con: &mut Connection,
        state: &mut LimitState<'_>,
        timestamp: UnixTimestamp,
    ) -> Result<CheckedLimits> {
        let scope = state.scope;
        let limit = state.limit;
        let entries = state.take_entries();

        metric!(
            histogram(CardinalityLimiterHistograms::RedisCheckHashes) = entries.len() as u64,
            id = &state.id,
        );

        let keys = scope
            .slots(timestamp)
            .map(|slot| scope.into_redis_key(slot));
        let hashes = entries.iter().map(|entry| entry.hash);

        // The expiry is a off by `window.granularity_seconds`,
        // but since this is only used for cleanup, this is not an issue.
        let result = self
            .script
            .invoke(con, limit, scope.redis_key_ttl(), hashes, keys)?;

        metric!(
            histogram(CardinalityLimiterHistograms::RedisSetCardinality) = result.cardinality,
            id = &state.id,
        );

        Ok(CheckedLimits {
            entries,
            statuses: result.statuses,
        })
    }
}

impl Limiter for RedisSetLimiter {
    fn check_cardinality_limits<'a, E, R>(
        &self,
        scoping: Scoping,
        limits: &'a [CardinalityLimit],
        entries: E,
        rejections: &mut R,
    ) -> Result<()>
    where
        E: IntoIterator<Item = Entry>,
        R: Rejections<'a>,
    {
        let timestamp = UnixTimestamp::now();
        // Allows to fast forward time in tests.
        #[cfg(test)]
        let timestamp = timestamp + self.time_offset;

        let mut states = LimitState::from_limits(scoping, limits);

        let cache = self.cache.read(timestamp); // Acquire a read lock.
        for entry in entries {
            for state in states.iter_mut() {
                if !state.scope.matches(&entry) {
                    // Entry not relevant for limit.
                    continue;
                }

                match cache.check(state.scope, entry.hash, state.limit) {
                    CacheOutcome::Accepted => {
                        // Accepted already, nothing to do.
                        state.cache_hit();
                        state.accepted();
                    }
                    CacheOutcome::Rejected => {
                        // Rejected, add it to the rejected list and move on.
                        rejections.reject(state.id, entry.id);
                        state.cache_hit();
                        state.rejected();
                    }
                    CacheOutcome::Unknown => {
                        // Add the entry to the state -> needs to be checked with Redis.
                        state.entries.push(RedisEntry::new(entry.id, entry.hash));
                        state.cache_miss();
                    }
                }
            }
        }
        drop(cache); // Give up the cache lock!

        let mut client = self.redis.client()?;
        let mut connection = client.connection()?;

        for mut state in states {
            if state.entries.is_empty() {
                continue;
            }

            let results = metric!(timer(CardinalityLimiterTimers::Redis), id = state.id, {
                self.check_limits(&mut connection, &mut state, timestamp)
            })?;

            // This always acquires a write lock, but we only hit this
            // if we previously didn't satisfy the request from the cache,
            // -> there is a very high chance we actually need the lock.
            let mut cache = self.cache.update(state.scope, timestamp); // Acquire a write lock.
            for (entry, status) in results {
                if status.is_rejected() {
                    rejections.reject(state.id, entry.id);
                    state.rejected();
                } else {
                    cache.accept(entry.hash);
                    state.accepted();
                }
            }
            drop(cache); // Give up the cache lock!
        }

        Ok(())
    }
}

/// Internal state combining relevant entries for the respective quota.
struct LimitState<'a> {
    /// Id of the original limit.
    pub id: &'a str,
    /// Entries which are relevant for the quota.
    pub entries: Vec<RedisEntry>,
    /// Scoping of the quota.
    pub scope: QuotaScoping,
    /// The limit of the quota.
    pub limit: u64,

    /// The original/full scoping.
    scoping: Scoping,
    /// Amount of cache hits.
    cache_hits: i64,
    /// Amount of cache misses.
    cache_misses: i64,
    /// Amount of accepts,
    accepts: i64,
    /// Amount of rejections.
    rejections: i64,
}

impl<'a> LimitState<'a> {
    pub fn new(scoping: Scoping, limit: &'a CardinalityLimit) -> Option<Self> {
        Some(Self {
            id: &limit.id,
            entries: Vec::new(),
            scope: QuotaScoping::new(scoping, limit)?,
            limit: limit.limit,
            scoping,
            cache_hits: 0,
            cache_misses: 0,
            accepts: 0,
            rejections: 0,
        })
    }

    pub fn from_limits(scoping: Scoping, limits: &'a [CardinalityLimit]) -> Vec<Self> {
        limits
            .iter()
            .filter_map(|limit| LimitState::new(scoping, limit))
            .collect::<Vec<_>>()
    }

    pub fn take_entries(&mut self) -> Vec<RedisEntry> {
        std::mem::take(&mut self.entries)
    }

    pub fn cache_hit(&mut self) {
        self.cache_hits += 1;
    }

    pub fn cache_miss(&mut self) {
        self.cache_misses += 1;
    }

    pub fn accepted(&mut self) {
        self.accepts += 1;
    }

    pub fn rejected(&mut self) {
        self.rejections += 1;
    }
}

impl<'a> Drop for LimitState<'a> {
    fn drop(&mut self) {
        metric!(
            counter(CardinalityLimiterCounters::RedisCacheHit) += self.cache_hits,
            id = self.id
        );
        metric!(
            counter(CardinalityLimiterCounters::RedisCacheMiss) += self.cache_misses,
            id = self.id
        );
        metric!(
            counter(CardinalityLimiterCounters::Accepted) += self.accepts,
            id = self.id
        );
        metric!(
            counter(CardinalityLimiterCounters::Rejected) += self.rejections,
            id = self.id
        );

        let organization_id = self.scoping.organization_id as i64;
        let status = if self.rejections > 0 { "limited" } else { "ok" };
        metric!(
            set(CardinalityLimiterSets::Organizations) = organization_id,
            id = self.id,
            status = status,
        )
    }
}

/// Entry used by the Redis limiter.
#[derive(Clone, Copy, Debug)]
struct RedisEntry {
    /// The correlating entry id.
    pub id: EntryId,
    /// The entry hash.
    pub hash: u32,
}

impl RedisEntry {
    /// Creates a new Redis entry.
    fn new(id: EntryId, hash: u32) -> Self {
        Self { id, hash }
    }
}

struct CheckedLimits {
    entries: Vec<RedisEntry>,
    statuses: Vec<Status>,
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use relay_base_schema::metrics::MetricNamespace;
    use relay_base_schema::project::ProjectId;
    use relay_redis::{redis, RedisConfigOptions};

    use crate::limiter::EntryId;
    use crate::redis::{KEY_PREFIX, KEY_VERSION};
    use crate::{CardinalityScope, SlidingWindow};

    use super::*;

    fn build_limiter() -> RedisSetLimiter {
        let url = std::env::var("RELAY_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());

        let opts = RedisConfigOptions {
            max_connections: 1,
            ..Default::default()
        };
        let redis = RedisPool::single(&url, opts).unwrap();

        RedisSetLimiter::new(
            RedisSetLimiterOptions {
                cache_vacuum_interval: Duration::from_secs(5),
            },
            redis,
        )
    }

    fn new_scoping(limiter: &RedisSetLimiter) -> Scoping {
        static ORGS: AtomicU64 = AtomicU64::new(100);

        let scoping = Scoping {
            organization_id: ORGS.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            project_id: ProjectId::new(1),
        };

        limiter.flush(scoping);

        scoping
    }

    #[derive(Debug, Default, PartialEq, Eq)]
    struct Rejections(HashSet<EntryId>);

    impl<'a> super::Rejections<'a> for Rejections {
        fn reject(&mut self, _limit_id: &'a str, entry_id: EntryId) {
            self.0.insert(entry_id);
        }
    }

    impl std::ops::Deref for Rejections {
        type Target = HashSet<EntryId>;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl RedisSetLimiter {
        /// Remove all redis state for an organization.
        fn flush(&self, scoping: Scoping) {
            let pattern = format!(
                "{KEY_PREFIX}:{KEY_VERSION}:scope-{{{o}-*",
                o = scoping.organization_id
            );

            let mut client = self.redis.client().unwrap();
            let mut connection = client.connection().unwrap();

            let keys = redis::cmd("KEYS")
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

        fn redis_sets(&self, scoping: Scoping) -> Vec<(String, usize)> {
            let pattern = format!(
                "{KEY_PREFIX}:{KEY_VERSION}:scope-{{{o}-*",
                o = scoping.organization_id
            );

            let mut client = self.redis.client().unwrap();
            let mut connection = client.connection().unwrap();

            redis::cmd("KEYS")
                .arg(pattern)
                .query::<Vec<String>>(&mut connection)
                .unwrap()
                .into_iter()
                .map(move |key| {
                    let size = redis::cmd("SCARD")
                        .arg(&key)
                        .query::<usize>(&mut connection)
                        .unwrap();

                    (key, size)
                })
                .collect()
        }

        fn test_limits<I>(
            &self,
            scoping: Scoping,
            limits: &[CardinalityLimit],
            entries: I,
        ) -> Rejections
        where
            I: IntoIterator<Item = Entry>,
        {
            let mut outcomes = Rejections::default();
            self.check_cardinality_limits(scoping, limits, entries, &mut outcomes)
                .unwrap();
            outcomes
        }
    }

    #[test]
    fn test_limiter_accept_previously_seen() {
        let limiter = build_limiter();

        let entries = [
            Entry::new(EntryId(0), MetricNamespace::Custom, 0),
            Entry::new(EntryId(1), MetricNamespace::Custom, 1),
            Entry::new(EntryId(2), MetricNamespace::Custom, 2),
            Entry::new(EntryId(3), MetricNamespace::Custom, 3),
            Entry::new(EntryId(4), MetricNamespace::Custom, 4),
            Entry::new(EntryId(5), MetricNamespace::Custom, 5),
        ];

        let scoping = new_scoping(&limiter);
        let mut limit = CardinalityLimit {
            id: "limit".to_owned(),
            window: SlidingWindow {
                window_seconds: 3600,
                granularity_seconds: 360,
            },
            limit: 5,
            scope: CardinalityScope::Organization,
            namespace: Some(MetricNamespace::Custom),
        };

        // 6 items, limit is 5 -> 1 rejection.
        let rejected = limiter.test_limits(scoping, &[limit.clone()], entries);
        assert_eq!(rejected.len(), 1);

        // We're at the limit but it should still accept already accepted elements, even with a
        // samller limit than previously accepted.
        limit.limit = 3;
        let rejected2 = limiter.test_limits(scoping, &[limit.clone()], entries);
        assert_eq!(rejected2, rejected);

        // A higher limit should accept everthing
        limit.limit = 6;
        let rejected3 = limiter.test_limits(scoping, &[limit], entries);
        assert_eq!(rejected3.len(), 0);
    }

    #[test]
    fn test_limiter_org_based_time_shift() {
        let mut limiter = build_limiter();

        let granularity_seconds = 10_000;

        let scoping1 = Scoping {
            organization_id: granularity_seconds,
            project_id: ProjectId::new(1),
        };
        let scoping2 = Scoping {
            // Time shift relative to `scoping1` should be half the granularity.
            organization_id: granularity_seconds / 2,
            project_id: ProjectId::new(1),
        };

        let limits = &[CardinalityLimit {
            id: "limit".to_owned(),
            window: SlidingWindow {
                window_seconds: granularity_seconds * 3,
                granularity_seconds,
            },
            limit: 1,
            scope: CardinalityScope::Organization,
            namespace: Some(MetricNamespace::Custom),
        }];

        let entries1 = [Entry::new(EntryId(0), MetricNamespace::Custom, 0)];
        assert!(limiter.test_limits(scoping1, limits, entries1).is_empty());
        assert!(limiter.test_limits(scoping2, limits, entries1).is_empty());

        // Make sure `entries2` is not accepted.
        let entries2 = [Entry::new(EntryId(1), MetricNamespace::Custom, 1)];
        assert_eq!(limiter.test_limits(scoping1, limits, entries2).len(), 1);
        assert_eq!(limiter.test_limits(scoping2, limits, entries2).len(), 1);

        let mut scoping1_accept = None;
        let mut scoping2_accept = None;

        // Measure time required until `entries2` is accepted.
        for i in 0..100 {
            let offset = i * granularity_seconds / 10;

            limiter.time_offset = Duration::from_secs(offset);

            if scoping1_accept.is_none()
                && limiter.test_limits(scoping1, limits, entries2).is_empty()
            {
                scoping1_accept = Some(offset as i64);
            }

            if scoping2_accept.is_none()
                && limiter.test_limits(scoping2, limits, entries2).is_empty()
            {
                scoping2_accept = Some(offset as i64);
            }

            if scoping1_accept.is_some() && scoping2_accept.is_some() {
                break;
            }
        }

        let scoping1_accept = scoping1_accept.unwrap();
        let scoping2_accept = scoping2_accept.unwrap();

        let diff = (scoping1_accept - scoping2_accept).abs();
        let expected = granularity_seconds as i64 / 2;
        // Make sure they are perfectly offset.
        assert_eq!(diff, expected);
    }

    #[test]
    fn test_limiter_small_within_limits() {
        let limiter = build_limiter();
        let scoping = new_scoping(&limiter);

        let limits = &[CardinalityLimit {
            id: "limit".to_owned(),
            window: SlidingWindow {
                window_seconds: 3600,
                granularity_seconds: 360,
            },
            limit: 10_000,
            scope: CardinalityScope::Organization,
            namespace: Some(MetricNamespace::Custom),
        }];

        let entries = (0..50)
            .map(|i| Entry::new(EntryId(i as usize), MetricNamespace::Custom, i))
            .collect::<Vec<_>>();
        let rejected = limiter.test_limits(scoping, limits, entries);
        assert_eq!(rejected.len(), 0);

        let entries = (100..150)
            .map(|i| Entry::new(EntryId(i as usize), MetricNamespace::Custom, i))
            .collect::<Vec<_>>();
        let rejected = limiter.test_limits(scoping, limits, entries);
        assert_eq!(rejected.len(), 0);
    }

    #[test]
    fn test_limiter_big_limit() {
        let limiter = build_limiter();

        let scoping = new_scoping(&limiter);
        let limits = &[CardinalityLimit {
            id: "limit".to_owned(),
            window: SlidingWindow {
                window_seconds: 3600,
                granularity_seconds: 360,
            },
            limit: 80_000,
            scope: CardinalityScope::Organization,
            namespace: Some(MetricNamespace::Custom),
        }];

        let entries = (0..100_000)
            .map(|i| Entry::new(EntryId(i as usize), MetricNamespace::Custom, i))
            .collect::<Vec<_>>();

        let rejected = limiter.test_limits(scoping, limits, entries);
        assert_eq!(rejected.len(), 20_000);
    }

    #[test]
    fn test_limiter_sliding_window() {
        let mut limiter = build_limiter();

        let scoping = new_scoping(&limiter);
        let window = SlidingWindow {
            window_seconds: 3600,
            granularity_seconds: 360,
        };
        let limits = &[CardinalityLimit {
            id: "limit".to_owned(),
            window,
            limit: 1,
            scope: CardinalityScope::Organization,
            namespace: Some(MetricNamespace::Custom),
        }];

        let entries1 = [Entry::new(EntryId(0), MetricNamespace::Custom, 0)];
        let entries2 = [Entry::new(EntryId(1), MetricNamespace::Custom, 1)];

        // 1 item and limit is 1 -> No rejections.
        let rejected = limiter.test_limits(scoping, limits, entries1);
        assert_eq!(rejected.len(), 0);

        for i in 0..(window.window_seconds / window.granularity_seconds) * 2 {
            // Fast forward time.
            limiter.time_offset = Duration::from_secs(i * window.granularity_seconds);

            // Should accept the already inserted item.
            let rejected = limiter.test_limits(scoping, limits, entries1);
            assert_eq!(rejected.len(), 0);
            // Should reject the new item.
            let rejected = limiter.test_limits(scoping, limits, entries2);
            assert_eq!(rejected.len(), 1);
        }

        // Fast forward time to a fresh window.
        limiter.time_offset = Duration::from_secs(1 + window.window_seconds * 3);
        // Accept the new element.
        let rejected = limiter.test_limits(scoping, limits, entries2);
        assert_eq!(rejected.len(), 0);
        // Reject the old element now.
        let rejected = limiter.test_limits(scoping, limits, entries1);
        assert_eq!(rejected.len(), 1);
    }

    #[test]
    fn test_limiter_no_namespace_limit_is_shared_limit() {
        let limiter = build_limiter();
        let scoping = new_scoping(&limiter);

        let limits = &[CardinalityLimit {
            id: "limit".to_owned(),
            window: SlidingWindow {
                window_seconds: 3600,
                granularity_seconds: 360,
            },
            limit: 2,
            scope: CardinalityScope::Organization,
            namespace: None,
        }];

        let entries1 = [Entry::new(EntryId(0), MetricNamespace::Custom, 0)];
        let entries2 = [Entry::new(EntryId(0), MetricNamespace::Spans, 1)];
        let entries3 = [Entry::new(EntryId(0), MetricNamespace::Transactions, 2)];

        let rejected = limiter.test_limits(scoping, limits, entries1);
        assert_eq!(rejected.len(), 0);

        let rejected = limiter.test_limits(scoping, limits, entries2);
        assert_eq!(rejected.len(), 0);

        let rejected = limiter.test_limits(scoping, limits, entries3);
        assert_eq!(rejected.len(), 1);
    }

    #[test]
    fn test_limiter_multiple_limits() {
        let limiter = build_limiter();
        let scoping = new_scoping(&limiter);

        let limits = &[
            CardinalityLimit {
                id: "limit1".to_owned(),
                window: SlidingWindow {
                    window_seconds: 3600,
                    granularity_seconds: 360,
                },
                limit: 1,
                scope: CardinalityScope::Organization,
                namespace: Some(MetricNamespace::Custom),
            },
            CardinalityLimit {
                id: "limit2".to_owned(),
                window: SlidingWindow {
                    window_seconds: 3600,
                    granularity_seconds: 360,
                },
                limit: 1,
                scope: CardinalityScope::Organization,
                namespace: Some(MetricNamespace::Custom),
            },
            CardinalityLimit {
                id: "limit3".to_owned(),
                window: SlidingWindow {
                    window_seconds: 3600,
                    granularity_seconds: 360,
                },
                limit: 1,
                scope: CardinalityScope::Organization,
                namespace: Some(MetricNamespace::Spans),
            },
            CardinalityLimit {
                id: "unknown_skipped".to_owned(),
                window: SlidingWindow {
                    window_seconds: 3600,
                    granularity_seconds: 360,
                },
                limit: 1,
                scope: CardinalityScope::Unknown,
                namespace: Some(MetricNamespace::Transactions),
            },
        ];

        let entries = [
            Entry::new(EntryId(0), MetricNamespace::Custom, 0),
            Entry::new(EntryId(1), MetricNamespace::Custom, 1),
            Entry::new(EntryId(2), MetricNamespace::Spans, 2),
            Entry::new(EntryId(3), MetricNamespace::Spans, 3),
            Entry::new(EntryId(4), MetricNamespace::Transactions, 4),
            Entry::new(EntryId(5), MetricNamespace::Transactions, 5),
        ];

        // Run multiple times to make sure caching does not interfere.
        for _ in 0..3 {
            let rejected = limiter.test_limits(scoping, limits, entries);

            // 2 transactions + 1 span + 1 custom (4) accepted -> 2 (6-4) rejected.
            assert_eq!(rejected.len(), 2);
            // 1 custom rejected.
            assert!(rejected.contains(&EntryId(0)) || rejected.contains(&EntryId(1)));
            // 1 span rejected.
            assert!(rejected.contains(&EntryId(2)) || rejected.contains(&EntryId(3)));
            // 2 transactions accepted -> no rejections.
            assert!(!rejected.contains(&EntryId(4)));
            assert!(!rejected.contains(&EntryId(5)));
        }
    }

    #[test]
    fn test_project_limit() {
        let limiter = build_limiter();

        let scoping1 = new_scoping(&limiter);
        let scoping2 = Scoping {
            project_id: ProjectId::new(2),
            ..scoping1
        };

        let limits = &[CardinalityLimit {
            id: "limit".to_owned(),
            window: SlidingWindow {
                window_seconds: 3600,
                granularity_seconds: 360,
            },
            limit: 1,
            scope: CardinalityScope::Project,
            namespace: None,
        }];

        let entries1 = [Entry::new(EntryId(0), MetricNamespace::Custom, 0)];
        let entries2 = [Entry::new(EntryId(0), MetricNamespace::Custom, 1)];

        // Accept different entries for different scopes.
        let rejected = limiter.test_limits(scoping1, limits, entries1);
        assert_eq!(rejected.len(), 0);
        let rejected = limiter.test_limits(scoping2, limits, entries2);
        assert_eq!(rejected.len(), 0);

        // Make sure the other entry is not accepted.
        let rejected = limiter.test_limits(scoping1, limits, entries2);
        assert_eq!(rejected.len(), 1);
        let rejected = limiter.test_limits(scoping2, limits, entries1);
        assert_eq!(rejected.len(), 1);
    }

    #[test]
    fn test_limiter_sliding_window_full() {
        let mut limiter = build_limiter();
        let scoping = new_scoping(&limiter);

        let window = SlidingWindow {
            window_seconds: 300,
            granularity_seconds: 100,
        };
        let limits = &[CardinalityLimit {
            id: "limit".to_owned(),
            window,
            limit: 100,
            scope: CardinalityScope::Organization,
            namespace: Some(MetricNamespace::Custom),
        }];

        macro_rules! test {
            ($r:expr) => {{
                let entries = $r
                    .map(|i| Entry::new(EntryId(i as usize), MetricNamespace::Custom, i))
                    .collect::<Vec<_>>();

                limiter.test_limits(scoping, limits, entries)
            }};
        }

        // Fill the first window with values.
        assert!(test!(0..100).is_empty());

        // Refresh 0..50 - Full.
        limiter.time_offset = Duration::from_secs(1 + window.granularity_seconds);
        assert!(test!(0..50).is_empty());
        assert_eq!(test!(100..125).len(), 25);

        // Refresh 0..50 - Full.
        limiter.time_offset = Duration::from_secs(1 + window.granularity_seconds * 2);
        assert!(test!(0..50).is_empty());
        assert_eq!(test!(125..150).len(), 25);

        // Refresh 0..50 - 50..100 fell out of the window, add 25 (size 50 -> 75).
        // --> Set 4 has size 75.
        limiter.time_offset = Duration::from_secs(1 + window.granularity_seconds * 3);
        assert!(test!(0..50).is_empty());
        assert!(test!(150..175).is_empty());

        // Refresh 0..50 - Still 25 available (size 75 -> 100).
        limiter.time_offset = Duration::from_secs(1 + window.granularity_seconds * 4);
        assert!(test!(0..50).is_empty());
        assert!(test!(175..200).is_empty());

        // From this point on it is repeating:
        //  - Always refresh 0..50.
        //  - First granule is full.
        //  - Next two granules each have space for 25 -> immediately filled.
        for i in 0..21 {
            let start = 200 + i * 25;
            let end = start + 25;

            limiter.time_offset =
                Duration::from_secs(1 + window.granularity_seconds * (i as u64 + 5));
            assert!(test!(0..50).is_empty());

            if i % 3 == 0 {
                assert_eq!(test!(start..end).len(), 25);
            } else {
                assert!(test!(start..end).is_empty());
            }
        }

        let mut sets = limiter.redis_sets(scoping);
        sets.sort();

        let len = sets.len();
        for (i, (_, size)) in sets.into_iter().enumerate() {
            // Set 4 and the last set were never fully filled.
            if i == 3 || i + 1 == len {
                assert_eq!(size, 75);
            } else {
                assert_eq!(size, 100);
            }
        }
    }

    #[test]
    fn test_limiter_sliding_window_perfect() {
        let mut limiter = build_limiter();
        let scoping = new_scoping(&limiter);

        let window = SlidingWindow {
            window_seconds: 300,
            granularity_seconds: 100,
        };
        let limits = &[CardinalityLimit {
            id: "limit".to_owned(),
            window,
            limit: 100,
            scope: CardinalityScope::Organization,
            namespace: Some(MetricNamespace::Custom),
        }];

        macro_rules! test {
            ($r:expr) => {{
                let entries = $r
                    .map(|i| Entry::new(EntryId(i as usize), MetricNamespace::Custom, i))
                    .collect::<Vec<_>>();

                limiter.test_limits(scoping, limits, entries)
            }};
        }

        // Test Case:
        //  1. Fill the window
        //  2. Fast forward one granule and fill that with the same values
        //  3. Assert that values "confirmed" in the first granule are active for a full window
        //
        // [ ][ ][ ]{ }{ }{ }[ ][ ][ ]
        // [A][A][A]{ }{ }{ }[ ][ ][ ]
        //    [A][A]{A}{ }{ }[ ][ ][ ]
        //     |     \-- Assert this value.
        //     \-- Touch this value.

        // Fill the first window with values.
        assert!(test!(0..100).is_empty());

        // Fast forward one granule with the same values.
        limiter.time_offset = Duration::from_secs(1 + window.granularity_seconds);
        assert!(test!(0..100).is_empty());

        // Fast forward to the first granule after a full reset.
        limiter.time_offset = Duration::from_secs(1 + window.window_seconds);
        // Make sure the window is full and does not accept new values.
        assert_eq!(test!(200..300).len(), 100);
        // Assert original values.
        assert!(test!(0..100).is_empty());
    }
}
