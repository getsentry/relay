use std::time::SystemTime;

use relay_redis::{
    redis::{self, FromRedisValue, Script},
    Connection, RedisPool,
};
use relay_statsd::metric;

use crate::{
    cache::{Cache, CacheOutcome},
    limiter::{EntryId, Limiter, Outcomes},
    statsd::{CardinalityLimiterCounters, CardinalityLimiterHistograms, CardinalityLimiterTimers},
    Result,
};
use relay_base_schema::metrics::MetricNamespace;
use relay_base_schema::project::ProjectId;

use crate::limiter::{Entry, Scoping};
use crate::{CardinalityLimit, CardinalityScope, OrganizationId, SlidingWindow};

/// Key prefix used for Redis keys.
const KEY_PREFIX: &str = "relay:cardinality";

/// Implementation uses Redis sets to keep track of cardinality.
pub struct RedisSetLimiter {
    redis: RedisPool,
    script: CardinalityScript,
    cache: Cache,
    #[cfg(test)]
    time_offset: u64,
}

/// A Redis based limiter using Redis sets to track cardinality and membership.
impl RedisSetLimiter {
    /// Creates a new [`RedisSetLimiter`].
    pub fn new(redis: RedisPool) -> Self {
        Self {
            redis,
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
        state: &mut LimitState<'_>,
        timestamp: u64,
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
            .invoke(con, limit, scope.window.window_seconds, hashes, keys)?;

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
    fn check_cardinality_limits<I, T>(
        &self,
        scoping: Scoping,
        limits: &[CardinalityLimit],
        entries: I,
        outcomes: &mut T,
    ) -> Result<()>
    where
        I: IntoIterator<Item = Entry>,
        T: Outcomes,
    {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
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
                        outcomes.reject(entry.id);
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
                    outcomes.reject(entry.id);
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

/// A quota scoping extracted from a [`CardinalityLimit`] and a [`Scoping`].
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) struct QuotaScoping {
    pub window: SlidingWindow,
    pub namespace: Option<MetricNamespace>,
    pub organization_id: Option<OrganizationId>,
    pub project_id: Option<ProjectId>,
}

impl QuotaScoping {
    /// Creates a new [`QuotaScoping`] from a [`Scoping`] and [`CardinalityLimit`].
    ///
    /// Returns `None` for limits with scope [`CardinalityScope::Unknown`].
    pub fn new(scoping: Scoping, limit: &CardinalityLimit) -> Option<Self> {
        let (organization_id, project_id) = match limit.scope {
            CardinalityScope::Organization => (Some(scoping.organization_id), None),
            // Invalid/unknown scope -> ignore the limit.
            CardinalityScope::Unknown => return None,
        };

        Some(Self {
            window: limit.window,
            namespace: limit.namespace,
            organization_id,
            project_id,
        })
    }

    /// Wether the scoping applies to the passed entry.
    pub fn matches(&self, entry: &Entry) -> bool {
        self.namespace.is_none() || self.namespace == Some(entry.namespace)
    }

    /// Returns all slots of the sliding window for a specific timestamp.
    pub fn slots(&self, timestamp: u64) -> impl Iterator<Item = u64> {
        self.window.iter(timestamp)
    }

    /// Turns the scoping into a Redis key for the passed slot.
    fn into_redis_key(self, slot: u64) -> String {
        let organization_id = self.organization_id.unwrap_or(0);
        let project_id = self.project_id.map(|p| p.value()).unwrap_or(0);
        let namespace = self.namespace.map(|ns| ns.as_str()).unwrap_or("");

        format!("{KEY_PREFIX}:scope-{{{organization_id}-{project_id}-{namespace}}}-{slot}")
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

    /// Amount of cache hits `(hits, misses)`.
    cache_hits: (i64, i64),
    /// Amount of accepts and rejections `(accepts, rejections)`.
    accepts_rejections: (i64, i64),
}

impl<'a> LimitState<'a> {
    pub fn new(scoping: Scoping, limit: &'a CardinalityLimit) -> Option<Self> {
        Some(Self {
            id: &limit.id,
            entries: Vec::new(),
            scope: QuotaScoping::new(scoping, limit)?,
            limit: limit.limit,
            cache_hits: (0, 0),
            accepts_rejections: (0, 0),
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
        self.cache_hits.0 += 1;
    }

    pub fn cache_miss(&mut self) {
        self.cache_hits.1 += 1;
    }

    pub fn accepted(&mut self) {
        self.accepts_rejections.0 += 1;
    }

    pub fn rejected(&mut self) {
        self.accepts_rejections.1 += 1;
    }
}

impl<'a> Drop for LimitState<'a> {
    fn drop(&mut self) {
        metric!(
            counter(CardinalityLimiterCounters::RedisCacheHit) += self.cache_hits.0,
            id = self.id
        );
        metric!(
            counter(CardinalityLimiterCounters::RedisCacheMiss) += self.cache_hits.1,
            id = self.id
        );
        metric!(
            counter(CardinalityLimiterCounters::Accepted) += self.accepts_rejections.0,
            id = self.id
        );
        metric!(
            counter(CardinalityLimiterCounters::Rejected) += self.accepts_rejections.1,
            id = self.id
        );
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
}

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
        limit: u64,
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

    use relay_base_schema::metrics::MetricNamespace;
    use relay_base_schema::project::ProjectId;
    use relay_redis::RedisConfigOptions;

    use crate::limiter::EntryId;
    use crate::{CardinalityScope, SlidingWindow};

    use super::*;

    fn build_limiter() -> RedisSetLimiter {
        let url = std::env::var("RELAY_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());

        let redis = RedisPool::single(&url, RedisConfigOptions::default()).unwrap();

        RedisSetLimiter::new(redis)
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
    struct Outcomes(HashSet<EntryId>);

    impl super::Outcomes for Outcomes {
        fn reject(&mut self, entry_id: EntryId) {
            self.0.insert(entry_id);
        }
    }

    impl std::ops::Deref for Outcomes {
        type Target = HashSet<EntryId>;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl RedisSetLimiter {
        /// Remove all redis state for an organization.
        fn flush(&self, scoping: Scoping) {
            let organization_id = scoping.organization_id;
            let pattern = format!("{KEY_PREFIX}:scope-{{{organization_id}-*");

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

        fn test_limits<I>(
            &self,
            scoping: Scoping,
            limits: &[CardinalityLimit],
            entries: I,
        ) -> Outcomes
        where
            I: IntoIterator<Item = Entry>,
        {
            let mut outcomes = Outcomes::default();
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

        for i in 0..window.window_seconds / window.granularity_seconds {
            // Fast forward time.
            limiter.time_offset = i * window.granularity_seconds;

            // Should accept the already inserted item.
            let rejected = limiter.test_limits(scoping, limits, entries1);
            assert_eq!(rejected.len(), 0);
            // Should reject the new item.
            let rejected = limiter.test_limits(scoping, limits, entries2);
            assert_eq!(rejected.len(), 1);
        }

        // Fast forward time to where we're in the next window.
        limiter.time_offset = window.window_seconds + 1;

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
}
