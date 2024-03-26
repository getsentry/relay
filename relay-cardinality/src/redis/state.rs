use relay_statsd::metric;

use crate::{
    limiter::{EntryId, Scoping},
    redis::quota::QuotaScoping,
    statsd::{CardinalityLimiterCounters, CardinalityLimiterSets},
    CardinalityLimit,
};

/// Internal state combining relevant entries for the respective quotas.
///
/// Also tracks amount of cache hits and misses as well as amount of
/// items accepted and rejected. These metrics are reported via statsd on drop.
pub struct LimitState<'a> {
    /// Entries which are relevant for the quota.
    pub entries: Vec<RedisEntry>,
    /// Scoping of the quota.
    pub scope: QuotaScoping,
    /// The limit of the quota.
    pub limit: u64,

    /// The original cardinality limit.
    cardinality_limit: &'a CardinalityLimit,
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
    /// Creates a new limit state from a [`Scoping`] and [`CardinalityLimit`].
    ///
    /// Returns `None` if the cardinality limit scope is [`Unknown`](crate::CardinalityScope::Unknown).
    pub fn new(scoping: Scoping, cardinality_limit: &'a CardinalityLimit) -> Option<Self> {
        Some(Self {
            entries: Vec::new(),
            scope: QuotaScoping::new(scoping, cardinality_limit)?,
            limit: cardinality_limit.limit,
            cardinality_limit,
            scoping,
            cache_hits: 0,
            cache_misses: 0,
            accepts: 0,
            rejections: 0,
        })
    }

    /// Converts a list of limits with the same scope to a vector of limit states.
    ///
    /// All invalid/unknown limits are skipped, see also [`Self::new`].
    pub fn from_limits(scoping: Scoping, limits: &'a [CardinalityLimit]) -> Vec<Self> {
        limits
            .iter()
            .filter_map(|limit| LimitState::new(scoping, limit))
            .collect::<Vec<_>>()
    }

    /// Returns the underlying cardinality limit id.
    pub fn id(&self) -> &'a str {
        &self.cardinality_limit.id
    }

    /// Returns the underlying cardinality limit.
    pub fn cardinality_limit(&self) -> &'a CardinalityLimit {
        self.cardinality_limit
    }

    /// Removes all contained [`RedisEntry`] items and returns them.
    pub fn take_entries(&mut self) -> Vec<RedisEntry> {
        std::mem::take(&mut self.entries)
    }

    /// Increases the cache hit counter.
    pub fn cache_hit(&mut self) {
        self.cache_hits += 1;
    }

    /// Increases the cache miss counter.
    pub fn cache_miss(&mut self) {
        self.cache_misses += 1;
    }

    /// Increases the accepted counter.
    pub fn accepted(&mut self) {
        self.accepts += 1;
    }

    /// Increases the rejected counter.
    pub fn rejected(&mut self) {
        self.rejections += 1;
    }
}

impl<'a> Drop for LimitState<'a> {
    fn drop(&mut self) {
        let passive = if self.cardinality_limit.passive {
            "true"
        } else {
            "false"
        };

        metric!(
            counter(CardinalityLimiterCounters::RedisCacheHit) += self.cache_hits,
            id = &self.cardinality_limit.id,
            passive = passive,
        );
        metric!(
            counter(CardinalityLimiterCounters::RedisCacheMiss) += self.cache_misses,
            id = &self.cardinality_limit.id,
            passive = passive,
        );
        metric!(
            counter(CardinalityLimiterCounters::Accepted) += self.accepts,
            id = &self.cardinality_limit.id,
            passive = passive,
        );
        metric!(
            counter(CardinalityLimiterCounters::Rejected) += self.rejections,
            id = &self.cardinality_limit.id,
            passive = passive,
        );

        let organization_id = self.scoping.organization_id as i64;
        let status = if self.rejections > 0 { "limited" } else { "ok" };
        metric!(
            set(CardinalityLimiterSets::Organizations) = organization_id,
            id = &self.cardinality_limit.id,
            passive = passive,
            status = status,
        )
    }
}

/// Entry used by the Redis limiter.
#[derive(Clone, Copy, Debug)]
pub struct RedisEntry {
    /// The correlating entry id.
    pub id: EntryId,
    /// The entry hash.
    pub hash: u32,
}

impl RedisEntry {
    /// Creates a new Redis entry.
    pub fn new(id: EntryId, hash: u32) -> Self {
        Self { id, hash }
    }
}
