use std::collections::BTreeMap;

use relay_statsd::metric;

use crate::{
    limiter::{Entry, EntryId, Scoping},
    redis::quota::{PartialQuotaScoping, QuotaScoping},
    statsd::{CardinalityLimiterCounters, CardinalityLimiterSets},
    CardinalityLimit,
};

/// Internal state combining relevant entries for the respective quota.
pub struct LimitState<'a> {
    /// The limit of the quota.
    pub limit: u64,

    /// Scoping of the quota.
    scope: PartialQuotaScoping,
    /// Entries grouped by quota scoping.
    ///
    /// Contained scopes are a superset of `scope`.
    sub_scopes: BTreeMap<QuotaScoping, Vec<RedisEntry>>,

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
    pub fn new(scoping: Scoping, cardinality_limit: &'a CardinalityLimit) -> Option<Self> {
        Some(Self {
            scope: PartialQuotaScoping::new(scoping, cardinality_limit)?,
            sub_scopes: BTreeMap::new(),
            limit: cardinality_limit.limit,
            cardinality_limit,
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

    pub fn matches(&self, entry: Entry) -> Option<QuotaScoping> {
        if self.scope.matches(&entry) {
            Some(self.scope.full(entry))
        } else {
            None
        }
    }

    pub fn add(&mut self, scope: QuotaScoping, entry: RedisEntry) {
        self.sub_scopes.entry(scope).or_default().push(entry)
    }

    pub fn id(&self) -> &'a str {
        &self.cardinality_limit.id
    }

    pub fn cardinality_limit(&self) -> &'a CardinalityLimit {
        self.cardinality_limit
    }

    pub fn take_scopes(&mut self) -> impl Iterator<Item = (QuotaScoping, Vec<RedisEntry>)> {
        std::mem::take(&mut self.sub_scopes).into_iter()
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
