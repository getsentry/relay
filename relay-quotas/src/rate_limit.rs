use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use relay_base_schema::data_category::DataCategory;
use relay_base_schema::metrics::MetricNamespace;
use relay_base_schema::organization::OrganizationId;
use relay_base_schema::project::{ProjectId, ProjectKey};
use smallvec::SmallVec;

use crate::quota::{DataCategories, ItemScoping, Quota, QuotaScope, ReasonCode, Scoping};
use crate::{Dimension, DimensionMap, REJECT_ALL_SECS};

/// A monotonic expiration marker for rate limits.
///
/// [`RetryAfter`] represents a point in time when a rate limit expires. It allows checking
/// whether the rate limit is still active or has expired, and calculating the remaining time
/// until expiration.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RetryAfter {
    when: Instant,
}

impl RetryAfter {
    /// Creates a new [`RetryAfter`] instance that expires after the specified number of seconds.
    #[inline]
    pub fn from_secs(seconds: u64) -> Self {
        let now = Instant::now();
        let when = now.checked_add(Duration::from_secs(seconds)).unwrap_or(now);
        Self { when }
    }

    /// Returns the remaining duration until the rate limit expires using the specified instant
    /// as the reference point.
    ///
    /// If the rate limit has already expired at the given instant, returns `None`.
    #[inline]
    pub fn remaining_at(self, at: Instant) -> Option<Duration> {
        if at >= self.when {
            None
        } else {
            Some(self.when - at)
        }
    }

    /// Returns the remaining duration until the rate limit expires.
    ///
    /// This uses the current instant as the reference point. If the rate limit has already
    /// expired, returns `None`.
    #[inline]
    pub fn remaining(self) -> Option<Duration> {
        self.remaining_at(Instant::now())
    }

    /// Returns the remaining seconds until the rate limit expires using the specified instant
    /// as the reference point.
    ///
    /// This method rounds up to the next second to ensure that rate limits are strictly enforced.
    /// If the rate limit has already expired, returns `0`.
    #[inline]
    pub fn remaining_seconds_at(self, at: Instant) -> u64 {
        match self.remaining_at(at) {
            // Compensate for the missing subsec part by adding 1s
            Some(duration) if duration.subsec_nanos() == 0 => duration.as_secs(),
            Some(duration) => duration.as_secs() + 1,
            None => 0,
        }
    }

    /// Returns the remaining seconds until the rate limit expires.
    ///
    /// This uses the current instant as the reference point. If the rate limit
    /// has already expired, returns `0`.
    #[inline]
    pub fn remaining_seconds(self) -> u64 {
        self.remaining_seconds_at(Instant::now())
    }

    /// Returns whether this rate limit has expired at the specified instant.
    #[inline]
    pub fn expired_at(self, at: Instant) -> bool {
        self.remaining_at(at).is_none()
    }

    /// Returns whether this rate limit has expired at the current instant.
    #[inline]
    pub fn expired(self) -> bool {
        self.remaining_at(Instant::now()).is_none()
    }
}

impl fmt::Debug for RetryAfter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.remaining_seconds() {
            0 => write!(f, "RetryAfter(expired)"),
            remaining => write!(f, "RetryAfter({remaining}s)"),
        }
    }
}

#[cfg(test)]
impl serde::Serialize for RetryAfter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeTupleStruct;
        let mut tup = serializer.serialize_tuple_struct("RetryAfter", 1)?;
        tup.serialize_field(&self.remaining_seconds())?;
        tup.end()
    }
}

/// Error that occurs when parsing a [`RetryAfter`] from a string fails.
#[derive(Debug)]
pub enum InvalidRetryAfter {
    /// The supplied delay in seconds was not valid.
    InvalidDelay(std::num::ParseFloatError),
}

impl FromStr for RetryAfter {
    type Err = InvalidRetryAfter;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let float = s.parse::<f64>().map_err(InvalidRetryAfter::InvalidDelay)?;
        let seconds = float.max(0.0).ceil() as u64;
        Ok(RetryAfter::from_secs(seconds))
    }
}

/// The scope that a rate limit applies to.
///
/// Unlike [`QuotaScope`], which only declares the class of the scope, this enum carries
/// the specific identifiers of the individual scopes that a rate limit applies to.
///
/// Rate limits can be applied at different levels of granularity, from global (affecting all data)
/// down to a specific project key.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[cfg_attr(test, derive(serde::Serialize))]
pub enum RateLimitScope {
    /// An organization with identifier.
    Organization(OrganizationId),
    /// A project with identifier.
    Project(ProjectId),
    /// A DSN public key.
    Key(ProjectKey),
}

impl RateLimitScope {
    /// Creates a rate limiting scope from the given item scoping for a specific quota.
    ///
    /// This extracts the appropriate scope identifier based on the quota's scope type.
    /// For unknown scopes, it assumes the most specific scope (Key).
    pub fn for_quota(scoping: &Scoping, scope: QuotaScope) -> Self {
        match scope {
            QuotaScope::Organization => Self::Organization(scoping.organization_id),
            QuotaScope::Project => Self::Project(scoping.project_id),
            QuotaScope::Key => Self::Key(scoping.project_key),
            // For unknown scopes, assume the most specific scope:
            QuotaScope::Unknown => Self::Key(scoping.project_key),
        }
    }

    /// Returns the canonical name of this scope.
    ///
    /// This corresponds to the name of the corresponding [`QuotaScope`].
    pub fn name(&self) -> &'static str {
        match *self {
            Self::Key(_) => QuotaScope::Key.name(),
            Self::Project(_) => QuotaScope::Project.name(),
            Self::Organization(_) => QuotaScope::Organization.name(),
        }
    }
}

/// An active rate limit that restricts data ingestion.
///
/// A rate limit defines restrictions for specific data categories within a particular scope.
/// It includes an expiration time after which the limit is no longer enforced.
///
/// Rate limits can be created from [`Quota`]s or directly constructed with the needed parameters.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct RateLimit {
    /// A set of data categories that this quota applies to. If empty, this rate limit
    /// applies to all data categories.
    pub categories: DataCategories,

    /// The scope of this rate limit.
    pub scope: RateLimitScope,

    /// A machine-readable reason indicating which quota caused this rate limit.
    pub reason_code: Option<ReasonCode>,

    /// A marker when this rate limit expires.
    pub retry_after: RetryAfter,

    /// The metric namespace of this rate limit.
    ///
    /// Only relevant for data categories of type metric bucket. If empty,
    /// this rate limit applies to metrics of all namespaces.
    pub namespaces: SmallVec<[MetricNamespace; 1]>,

    /// The dimensions that this rate-limit applies to.  If empty, this rate limit applies
    /// to any item matching the other properties of this rate limit.
    pub dimensions: DimensionMap,
}

impl RateLimit {
    /// Creates a new rate limit from the given [`Quota`].
    ///
    /// This builds a rate limit with the appropriate scope derived from the quota and scoping
    /// information. The categories and other properties are copied from the quota.
    pub fn from_quota(quota: &Quota, scoping: &ItemScoping, retry_after: RetryAfter) -> Self {
        let dimensions = if let Some(group_by) = &quota.group_by {
            group_by
                .dimensions
                .iter()
                .filter_map(|dim| Some((*dim, scoping.dimensions.get(dim)?.0.clone())))
                .collect()
        } else {
            DimensionMap::default()
        };

        Self {
            categories: quota.categories,
            scope: RateLimitScope::for_quota(scoping, quota.scope),
            reason_code: quota.reason_code.clone(),
            retry_after,
            namespaces: quota.namespace.into_iter().collect(),
            dimensions,
        }
    }

    /// Checks whether this rate limit applies to the given item.
    ///
    /// A rate limit applies if its scope matches the item's scope, and the item's
    /// category and namespace match those of the rate limit.
    pub fn matches(&self, scoping: &ItemScoping) -> bool {
        self.matches_scope(scoping)
            && scoping.matches_categories(self.categories)
            && scoping.matches_namespaces(&self.namespaces)
            && scoping.satisfies_quota_dimensions(&self.dimensions)
    }

    /// Returns `true` if the rate limiting scope matches the given item.
    fn matches_scope(&self, scoping: &ItemScoping) -> bool {
        match self.scope {
            RateLimitScope::Organization(org_id) => scoping.organization_id == org_id,
            RateLimitScope::Project(project_id) => scoping.project_id == project_id,
            RateLimitScope::Key(key) => scoping.project_key == key,
        }
    }
}

/// A wrapper for RateLimits that can only be constructed by [`RateLimits::propagatable`], ensuring
/// that we do not include any dimensioned rate limits in it.
#[derive(Debug)]
pub struct PropagatableRateLimits(RateLimits);

impl Deref for PropagatableRateLimits {
    type Target = RateLimits;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(test, derive(serde::Serialize))]
struct RateLimitKey {
    scope: RateLimitScope,
    dimension_hashes: u64,
}

/// A collection of scoped rate limits.
///
/// [`RateLimits`] manages a set of active rate limits that can be checked against
/// incoming data.
///
/// The collection can be empty, indicated by [`is_ok`](Self::is_ok), meaning no rate limits
/// are currently active.
#[derive(Clone, Debug, Default)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct RateLimits {
    limits: BTreeMap<RateLimitKey, Vec<RateLimit>>,

    dimension_sets: BTreeSet<BTreeSet<Dimension>>,

    expiry_heap: BinaryHeap<HeapItem>,
}

/// An item stored in the expiry heap, ordered on RetryAfter, but also storing a RateLimitKey that
/// is used to index into the limits map.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize))]
struct HeapItem {
    retry_after: RetryAfter,
    rl_key: RateLimitKey,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse for a min heap.
        self.retry_after.cmp(&other.retry_after).reverse()
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl RateLimits {
    /// Creates an empty [`RateLimits`] instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a limit to this collection.
    ///
    /// If a rate limit with an overlapping scope already exists, the `retry_after` count is merged
    /// with the existing limit. Otherwise, the new rate limit is added.
    pub fn add(&mut self, limit: RateLimit) {
        let RateLimit {
            categories,
            scope,
            reason_code: _,
            retry_after: _,
            namespaces,
            dimensions,
        } = &limit;

        let rate_limit_key = RateLimitKey {
            scope: *scope,
            dimension_hashes: dimensions.dimension_hashes(),
        };

        self.dimension_sets
            .insert(dimensions.keys().copied().collect());

        let dims = self.limits.entry(rate_limit_key).or_default();

        let limit_opt = dims.iter_mut().find(|l| {
            *categories == l.categories
                && *namespaces == l.namespaces
                && *dimensions == l.dimensions
        });

        match limit_opt {
            None => {
                self.expiry_heap.push(HeapItem {
                    retry_after: limit.retry_after,
                    rl_key: rate_limit_key,
                });
                dims.push(limit);
            }
            Some(existing) if existing.retry_after < limit.retry_after => {
                // Go ahead and push a heap item with the same RateLimitKey; much cheaper than
                // trying to re-order the heap, and won't do anything when it comes time to pop
                // because we still check the actual RateLimit item to make sure it's expired.
                self.expiry_heap.push(HeapItem {
                    retry_after: limit.retry_after,
                    rl_key: rate_limit_key,
                });
                *existing = limit;
            }
            Some(_) => (), // keep existing, longer limit
        }
    }

    /// Merges all limits from another [`RateLimits`] instance into this one.
    ///
    /// This keeps all existing rate limits, adds new ones, and updates any existing ones
    /// with a later expiration time. The resulting collection contains the maximum
    /// constraints from both instances.
    pub fn merge(&mut self, limits: Self) {
        for limit in limits {
            self.add(limit);
        }
    }

    /// Merges all limits from another [`RateLimits`] with this one.
    ///
    /// See also: [`Self::merge`].
    pub fn merge_with(mut self, other: Self) -> Self {
        self.merge(other);
        self
    }

    /// Returns `true` if this instance contains no active limits.
    ///
    /// This is the opposite of [`is_limited`](Self::is_limited).
    pub fn is_ok(&self) -> bool {
        !self.is_limited()
    }

    /// Returns `true` if this instance contains any active rate limits.
    ///
    /// A rate limit is considered active if it has not yet expired.
    pub fn is_limited(&self) -> bool {
        let now = Instant::now();
        self.iter().any(|limit| !limit.retry_after.expired_at(now))
    }

    /// Returns `true` if this is instance contains any active rate limits
    /// for the specified categories.
    ///
    /// A rate limit is considered active if it has not yet expired.
    pub fn is_any_limited(&self, scopings: &[ItemScoping]) -> bool {
        self.is_any_limited_with_quotas(&[], scopings)
    }

    /// Returns `true` if this is instance contains any active rate limits
    /// for the specified categories.
    ///
    /// This is similar to [`Self::is_limited`], but additionally checks for quotas with a static
    /// limit of `0`, which reject items even if there is no active rate limit in this instance.
    ///
    /// A rate limit is considered active if it has not yet expired.
    pub fn is_any_limited_with_quotas<'a>(
        &self,
        quotas: impl IntoIterator<Item = &'a Quota>,
        scopings: &[ItemScoping],
    ) -> bool {
        for quota in quotas {
            for scoping in scopings {
                if quota.limit == Some(0) && quota.matches(scoping) {
                    return true;
                }
            }
        }

        let now = Instant::now();

        scopings.iter().any(|scoping| {
            self.matching(scoping)
                .any(|limit| !limit.retry_after.expired_at(now))
        })
    }

    /// Removes expired rate limits from this instance.
    pub fn clean_expired(&mut self, now: Instant) {
        if !self.has_expired(now) {
            return;
        }

        while let Some(next) = self.expiry_heap.peek() {
            if !next.retry_after.expired_at(now) {
                break;
            }

            if let Some(limits) = self.limits.get_mut(&next.rl_key) {
                limits.retain(|l| !l.retry_after.expired_at(now));
                if limits.is_empty() {
                    self.limits.remove(&next.rl_key);
                }
            }
            self.expiry_heap.pop();
        }
    }

    /// Returns `true` if any limit has expired at `now`.
    pub fn has_expired(&self, now: Instant) -> bool {
        self.expiry_heap
            .peek()
            .is_some_and(|next| next.retry_after.expired_at(now))
    }

    /// Returns every limit which applies to `scoping`.
    fn matching<'a>(&'a self, scoping: &'a ItemScoping) -> impl Iterator<Item = &'a RateLimit> {
        // Find all the matching dimension hashes of this item ("what RateLimit items have dimensions
        // that could apply to this item?").  Do this by projecting the item's dimensions onto the
        // dimension set of that RateLimit.
        let matching_dimension_hashes = self
            .dimension_sets
            .iter()
            .filter_map(|ds| scoping.dimensions.project(ds));

        matching_dimension_hashes.flat_map(|hash| {
            // Given a matching hash, compute the three RateLimitKeys we need to index into the
            // limit map.  This is needed because an item could match any of RateLimitScope's
            // enumerations: ProjectKey, ProjectId, or OrganizationId.  So, we fan out.
            let rl_keys = [
                RateLimitKey {
                    scope: RateLimitScope::Key(scoping.project_key),
                    dimension_hashes: hash,
                },
                RateLimitKey {
                    scope: RateLimitScope::Organization(scoping.organization_id),
                    dimension_hashes: hash,
                },
                RateLimitKey {
                    scope: RateLimitScope::Project(scoping.project_id),
                    dimension_hashes: hash,
                },
            ];

            rl_keys
                .into_iter()
                .filter_map(|key| self.limits.get(&key))
                .flatten()
                .filter(|limit| limit.matches(scoping))
        })
    }

    /// Returns only the set of rate limits that can be propagated upstream.  At this time,
    /// we cannot propagate dimensioned limits, so these must be filtered out.
    pub fn propagatable(&self) -> PropagatableRateLimits {
        let mut propagatable = RateLimits::new();

        for (k, limits) in self.limits.iter() {
            if k.dimension_hashes == DimensionMap::EMPTY_MAP_HASH {
                for limit in limits {
                    // dimension_hashes is checked to see if this limit's dimensions _looks_ empty,
                    // but we still should be paranoid about collisions, so one last check to be
                    // sure.
                    if limit.dimensions.is_empty() {
                        propagatable.add(limit.clone());
                    }
                }
            }
        }

        PropagatableRateLimits(propagatable)
    }

    /// Checks whether any rate limits apply to the given scoping.
    ///
    /// Returns a new [`RateLimits`] instance containing only the rate limits that match
    /// the provided [`ItemScoping`]. If no limits match, the returned instance will be empty
    /// and [`is_ok`](Self::is_ok) will return `true`.
    pub fn check(&self, scoping: &ItemScoping) -> Self {
        self.check_with_quotas(&[], scoping)
    }

    /// Checks whether any rate limits or static quotas apply to the given scoping.
    ///
    /// This is similar to [`check`](Self::check), but additionally checks for quotas with a static
    /// limit of `0`, which reject items even if there is no active rate limit in this instance.
    ///
    /// Returns a new [`RateLimits`] instance containing the rate limits that match the provided
    /// [`ItemScoping`]. If no limits or quotas match, the returned instance will be empty.
    pub fn check_with_quotas<'a>(
        &self,
        quotas: impl IntoIterator<Item = &'a Quota>,
        scoping: &ItemScoping,
    ) -> Self {
        let mut applied_limits = Self::new();

        for quota in quotas {
            if quota.limit == Some(0) && quota.matches(scoping) {
                let retry_after = RetryAfter::from_secs(REJECT_ALL_SECS);
                applied_limits.add(RateLimit::from_quota(quota, scoping, retry_after));
            }
        }

        for limit in self.matching(scoping) {
            applied_limits.add(limit.clone());
        }

        applied_limits
    }

    /// Returns an iterator over all rate limits in this collection.
    pub fn iter(&self) -> RateLimitsIter<'_> {
        RateLimitsIter {
            iter: std::slice::Iter::default(),
            dimensioned_iter: self.limits.values(),
        }
    }

    /// Returns the rate limit with the latest expiration time.
    ///
    /// If multiple rate limits have the same expiration time, any of them may be returned.
    /// If the collection is empty, returns `None`.
    pub fn longest(&self) -> Option<&RateLimit> {
        self.iter().max_by_key(|limit| limit.retry_after)
    }

    /// Returns `true` if there are no rate limits in this collection.
    ///
    /// Note that an empty collection is not the same as having no active limits.
    /// Use [`is_ok`](Self::is_ok) to check if there are no active limits.
    pub fn is_empty(&self) -> bool {
        self.limits.is_empty()
    }
}

/// An iterator over rate limit references.
///
/// This struct is created by the [`iter`](RateLimits::iter) method on [`RateLimits`].
/// It yields shared references to the rate limits in the collection.
pub struct RateLimitsIter<'a> {
    iter: std::slice::Iter<'a, RateLimit>,
    dimensioned_iter: std::collections::btree_map::Values<'a, RateLimitKey, Vec<RateLimit>>,
}

impl<'a> Iterator for RateLimitsIter<'a> {
    type Item = &'a RateLimit;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(nxt) = self.iter.next() {
                return Some(nxt);
            }

            self.iter = self.dimensioned_iter.next()?.iter();
        }
    }
}

impl IntoIterator for RateLimits {
    type IntoIter = RateLimitsIntoIter;
    type Item = RateLimit;

    fn into_iter(self) -> Self::IntoIter {
        let dimensioned_iter = self.limits.into_values();

        RateLimitsIntoIter {
            iter: std::vec::IntoIter::default(),
            dimensioned_iter,
        }
    }
}

/// An iterator that consumes a [`RateLimits`] collection.
///
/// This struct is created by the `into_iter` method on [`RateLimits`], provided by the
/// [`IntoIterator`] trait. It yields owned rate limits by value.
pub struct RateLimitsIntoIter {
    iter: std::vec::IntoIter<RateLimit>,
    dimensioned_iter: std::collections::btree_map::IntoValues<RateLimitKey, Vec<RateLimit>>,
}

impl Iterator for RateLimitsIntoIter {
    type Item = RateLimit;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let nxt = self.iter.next();
            if nxt.is_some() {
                return nxt;
            }

            self.iter = self.dimensioned_iter.next()?.into_iter();
        }
    }
}

impl<'a> IntoIterator for &'a RateLimits {
    type IntoIter = RateLimitsIter<'a>;
    type Item = &'a RateLimit;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A thread-safe cache of rate limits with automatic expiration handling.
///
/// [`CachedRateLimits`] wraps a [`RateLimits`] collection with a mutex to allow safe
/// concurrent access from multiple threads. It automatically removes expired rate limits
/// when retrieving the current limits.
///
/// This is useful for maintaining a shared set of rate limits across multiple
/// processing threads or tasks.
#[derive(Debug, Default)]
pub struct CachedRateLimits(Mutex<Arc<RateLimits>>);

impl CachedRateLimits {
    /// Creates a new, empty instance without any rate limits.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a rate limit to this collection.
    ///
    /// This is a thread-safe wrapper around [`RateLimits::add`].
    pub fn add(&self, limit: RateLimit) {
        let mut inner = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let current = Arc::make_mut(&mut inner);
        current.add(limit);
    }

    /// Merges rate limits from another collection into this one.
    ///
    /// This is a thread-safe wrapper around [`RateLimits::merge`].
    pub fn merge(&self, limits: RateLimits) {
        if limits.is_empty() {
            return;
        }

        let mut inner = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let current = Arc::make_mut(&mut inner);
        for mut limit in limits {
            // To spice it up, we do have some special casing here for 'inherited categories',
            // e.g. spans and transactions.
            //
            // The tldr; is, as transactions are just containers for spans,
            // we can enforce span limits on transactions but also vice versa.
            //
            // So this is largely an enforcement problem, but since Relay propagates
            // rate limits to clients, we clone the limits with the inherited category.
            // This ensures old SDKs rate limit correctly, but also it simplifies client
            // implementations. Only Relay needs to make this decision.
            for category in limit.categories.iter() {
                for inherited in inherited_categories(&category) {
                    if let Some(categories) = limit.categories.add(*inherited) {
                        limit.categories = categories;
                    }
                }
            }

            current.add(limit);
        }
    }

    /// Returns a reference to the current rate limits.
    ///
    /// This method automatically removes any expired rate limits before returning,
    /// ensuring that only active limits are included in the result.
    pub fn current_limits(&self) -> Arc<RateLimits> {
        let now = Instant::now();
        let mut inner = self.0.lock().unwrap_or_else(PoisonError::into_inner);

        // `Arc::make_mut` clones if there are other pointers to this arc, so only do make_mut if
        // we need to.
        if inner.has_expired(now) {
            Arc::make_mut(&mut inner).clean_expired(now);
        }
        Arc::clone(&inner)
    }
}

/// Returns inherited rate limit categories for the passed category.
///
/// When a rate limit for a category can also be enforced in a different category,
/// then it's an inherited category.
///
/// For example, a transaction rate limit can also be applied to spans and vice versa.
///
/// For a detailed explanation on span/transaction enforcement see:
/// <https://develop.sentry.dev/ingestion/relay/transaction-span-ratelimits/>.
fn inherited_categories(category: &DataCategory) -> &'static [DataCategory] {
    match category {
        DataCategory::Transaction => &[DataCategory::Span],
        DataCategory::Span => &[DataCategory::Transaction],
        _ => &[],
    }
}

#[cfg(test)]
mod tests {

    use smallvec::smallvec;

    use super::*;
    use crate::quota::DataCategory;
    use crate::{DimensionMap, GroupBy, MetricNamespaceScoping};

    #[test]
    fn test_parse_retry_after() {
        // positive float always rounds up to the next integer
        let retry_after = "17.1".parse::<RetryAfter>().expect("parse RetryAfter");
        assert_eq!(retry_after.remaining_seconds(), 18);
        assert!(!retry_after.expired());
        let retry_after = "17.7".parse::<RetryAfter>().expect("parse RetryAfter");
        assert_eq!(retry_after.remaining_seconds(), 18);
        assert!(!retry_after.expired());

        // positive int
        let retry_after = "17".parse::<RetryAfter>().expect("parse RetryAfter");
        assert_eq!(retry_after.remaining_seconds(), 17);
        assert!(!retry_after.expired());

        // negative numbers are treated as zero
        let retry_after = "-2".parse::<RetryAfter>().expect("parse RetryAfter");
        assert_eq!(retry_after.remaining_seconds(), 0);
        assert!(retry_after.expired());
        let retry_after = "-inf".parse::<RetryAfter>().expect("parse RetryAfter");
        assert_eq!(retry_after.remaining_seconds(), 0);
        assert!(retry_after.expired());

        // inf and NaN are valid input and treated as zero
        let retry_after = "inf".parse::<RetryAfter>().expect("parse RetryAfter");
        assert_eq!(retry_after.remaining_seconds(), 0);
        assert!(retry_after.expired());
        let retry_after = "NaN".parse::<RetryAfter>().expect("parse RetryAfter");
        assert_eq!(retry_after.remaining_seconds(), 0);
        assert!(retry_after.expired());

        // large inputs that would overflow are treated as zero
        let retry_after = "100000000000000000000"
            .parse::<RetryAfter>()
            .expect("parse RetryAfter");
        assert_eq!(retry_after.remaining_seconds(), 0);
        assert!(retry_after.expired());

        // invalid strings cause parse error
        "".parse::<RetryAfter>().expect_err("error RetryAfter");
        "nope".parse::<RetryAfter>().expect_err("error RetryAfter");
        " 2 ".parse::<RetryAfter>().expect_err("error RetryAfter");
        "6 0".parse::<RetryAfter>().expect_err("error RetryAfter");
    }

    #[test]
    fn test_rate_limit_matches_categories() {
        let rate_limit = RateLimit {
            categories: [DataCategory::Unknown, DataCategory::Error].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        };

        assert!(rate_limit.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: DimensionMap::default(),
        }));

        assert!(!rate_limit.matches(&ItemScoping {
            category: DataCategory::Transaction,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: DimensionMap::default(),
        }));
    }

    #[test]
    fn test_rate_limit_matches_organization() {
        let rate_limit = RateLimit {
            categories: DataCategories::new(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        };

        assert!(rate_limit.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: DimensionMap::default(),
        }));

        assert!(!rate_limit.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(0),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: DimensionMap::default(),
        }));
    }

    #[test]
    fn test_rate_limit_matches_project() {
        let rate_limit = RateLimit {
            categories: DataCategories::new(),
            scope: RateLimitScope::Project(ProjectId::new(21)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        };

        assert!(rate_limit.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: DimensionMap::default(),
        }));

        assert!(!rate_limit.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(0),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: DimensionMap::default(),
        }));
    }

    #[test]
    fn test_rate_limit_matches_namespaces() {
        let rate_limit = RateLimit {
            categories: Default::default(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![MetricNamespace::Transactions],
            dimensions: DimensionMap::default(),
        };

        let scoping = Scoping {
            organization_id: OrganizationId::new(42),
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: None,
        };

        assert!(rate_limit.matches(&ItemScoping {
            category: DataCategory::MetricBucket,
            scoping,
            namespace: MetricNamespaceScoping::Some(MetricNamespace::Transactions),
            dimensions: DimensionMap::default(),
        }));

        assert!(!rate_limit.matches(&ItemScoping {
            category: DataCategory::MetricBucket,
            scoping,
            namespace: MetricNamespaceScoping::Some(MetricNamespace::Spans),
            dimensions: DimensionMap::default(),
        }));

        let general_rate_limit = RateLimit {
            categories: Default::default(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![], // all namespaces
            dimensions: DimensionMap::default(),
        };

        assert!(general_rate_limit.matches(&ItemScoping {
            category: DataCategory::MetricBucket,
            scoping,
            namespace: MetricNamespaceScoping::Some(MetricNamespace::Spans),
            dimensions: DimensionMap::default(),
        }));

        assert!(general_rate_limit.matches(&ItemScoping {
            category: DataCategory::MetricBucket,
            scoping,
            namespace: MetricNamespaceScoping::None,
            dimensions: DimensionMap::default(),
        }));
    }

    #[test]
    fn test_rate_limit_matches_key() {
        let rate_limit = RateLimit {
            categories: DataCategories::new(),
            scope: RateLimitScope::Key(
                ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            ),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        };

        assert!(rate_limit.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: DimensionMap::default(),
        }));

        assert!(!rate_limit.matches(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(0),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("deadbeefdeadbeefdeadbeefdeadbeef").unwrap(),
                key_id: None,
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: DimensionMap::default(),
        }));
    }

    #[test]
    fn test_rate_limits_add_replacement() {
        let mut rate_limits = RateLimits::new();

        rate_limits.add(RateLimit {
            categories: [DataCategory::Default, DataCategory::Error].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: Some(ReasonCode::new("first")),
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        // longer rate limit shadows shorter one
        rate_limits.add(RateLimit {
            categories: [DataCategory::Error, DataCategory::Default].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: Some(ReasonCode::new("second")),
            retry_after: RetryAfter::from_secs(10),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        insta::assert_ron_snapshot!(rate_limits, @r#"
        RateLimits(
          limits: {
            RateLimitKey(
              scope: Organization(OrganizationId(42)),
              dimension_hashes: 0,
            ): [
              RateLimit(
                categories: [
                  "default",
                  "error",
                ],
                scope: Organization(OrganizationId(42)),
                reason_code: Some(ReasonCode("second")),
                retry_after: RetryAfter(10),
                namespaces: [],
                dimensions: DimensionMap(
                  dimensions: {},
                  hash: 0,
                ),
              ),
            ],
          },
          dimension_sets: [
            [],
          ],
          expiry_heap: [
            HeapItem(
              retry_after: RetryAfter(1),
              rl_key: RateLimitKey(
                scope: Organization(OrganizationId(42)),
                dimension_hashes: 0,
              ),
            ),
            HeapItem(
              retry_after: RetryAfter(10),
              rl_key: RateLimitKey(
                scope: Organization(OrganizationId(42)),
                dimension_hashes: 0,
              ),
            ),
          ],
        )
        "#);
    }

    #[test]
    fn test_rate_limits_add_shadowing() {
        let mut rate_limits = RateLimits::new();

        rate_limits.add(RateLimit {
            categories: [DataCategory::Default, DataCategory::Error].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: Some(ReasonCode::new("first")),
            retry_after: RetryAfter::from_secs(10),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        // shorter rate limit is shadowed by existing one
        rate_limits.add(RateLimit {
            categories: [DataCategory::Error, DataCategory::Default].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: Some(ReasonCode::new("second")),
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        insta::assert_ron_snapshot!(rate_limits, @r#"
        RateLimits(
          limits: {
            RateLimitKey(
              scope: Organization(OrganizationId(42)),
              dimension_hashes: 0,
            ): [
              RateLimit(
                categories: [
                  "default",
                  "error",
                ],
                scope: Organization(OrganizationId(42)),
                reason_code: Some(ReasonCode("first")),
                retry_after: RetryAfter(10),
                namespaces: [],
                dimensions: DimensionMap(
                  dimensions: {},
                  hash: 0,
                ),
              ),
            ],
          },
          dimension_sets: [
            [],
          ],
          expiry_heap: [
            HeapItem(
              retry_after: RetryAfter(10),
              rl_key: RateLimitKey(
                scope: Organization(OrganizationId(42)),
                dimension_hashes: 0,
              ),
            ),
          ],
        )
        "#);
    }

    #[test]
    fn test_rate_limits_add_buckets() {
        let mut rate_limits = RateLimits::new();

        rate_limits.add(RateLimit {
            categories: [DataCategory::Error].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        // Same scope but different categories
        rate_limits.add(RateLimit {
            categories: [DataCategory::Transaction].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        // Same categories but different scope
        rate_limits.add(RateLimit {
            categories: [DataCategory::Error].into(),
            scope: RateLimitScope::Project(ProjectId::new(21)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        insta::assert_ron_snapshot!(rate_limits, @r#"
        RateLimits(
          limits: {
            RateLimitKey(
              scope: Organization(OrganizationId(42)),
              dimension_hashes: 0,
            ): [
              RateLimit(
                categories: [
                  "error",
                ],
                scope: Organization(OrganizationId(42)),
                reason_code: None,
                retry_after: RetryAfter(1),
                namespaces: [],
                dimensions: DimensionMap(
                  dimensions: {},
                  hash: 0,
                ),
              ),
              RateLimit(
                categories: [
                  "transaction",
                ],
                scope: Organization(OrganizationId(42)),
                reason_code: None,
                retry_after: RetryAfter(1),
                namespaces: [],
                dimensions: DimensionMap(
                  dimensions: {},
                  hash: 0,
                ),
              ),
            ],
            RateLimitKey(
              scope: Project(ProjectId(21)),
              dimension_hashes: 0,
            ): [
              RateLimit(
                categories: [
                  "error",
                ],
                scope: Project(ProjectId(21)),
                reason_code: None,
                retry_after: RetryAfter(1),
                namespaces: [],
                dimensions: DimensionMap(
                  dimensions: {},
                  hash: 0,
                ),
              ),
            ],
          },
          dimension_sets: [
            [],
          ],
          expiry_heap: [
            HeapItem(
              retry_after: RetryAfter(1),
              rl_key: RateLimitKey(
                scope: Organization(OrganizationId(42)),
                dimension_hashes: 0,
              ),
            ),
            HeapItem(
              retry_after: RetryAfter(1),
              rl_key: RateLimitKey(
                scope: Organization(OrganizationId(42)),
                dimension_hashes: 0,
              ),
            ),
            HeapItem(
              retry_after: RetryAfter(1),
              rl_key: RateLimitKey(
                scope: Project(ProjectId(21)),
                dimension_hashes: 0,
              ),
            ),
          ],
        )
        "#);
    }

    /// Regression test that ensures namespaces are correctly added to rate limits.
    #[test]
    fn test_rate_limits_add_namespaces() {
        let mut rate_limits = RateLimits::new();

        rate_limits.add(RateLimit {
            categories: [DataCategory::MetricBucket].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![MetricNamespace::Transactions],
            dimensions: DimensionMap::default(),
        });

        // Same category but different namespaces
        rate_limits.add(RateLimit {
            categories: [DataCategory::MetricBucket].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![MetricNamespace::Spans],
            dimensions: DimensionMap::default(),
        });

        insta::assert_ron_snapshot!(rate_limits, @r#"
        RateLimits(
          limits: {
            RateLimitKey(
              scope: Organization(OrganizationId(42)),
              dimension_hashes: 0,
            ): [
              RateLimit(
                categories: [
                  "metric_bucket",
                ],
                scope: Organization(OrganizationId(42)),
                reason_code: None,
                retry_after: RetryAfter(1),
                namespaces: [
                  "transactions",
                ],
                dimensions: DimensionMap(
                  dimensions: {},
                  hash: 0,
                ),
              ),
              RateLimit(
                categories: [
                  "metric_bucket",
                ],
                scope: Organization(OrganizationId(42)),
                reason_code: None,
                retry_after: RetryAfter(1),
                namespaces: [
                  "spans",
                ],
                dimensions: DimensionMap(
                  dimensions: {},
                  hash: 0,
                ),
              ),
            ],
          },
          dimension_sets: [
            [],
          ],
          expiry_heap: [
            HeapItem(
              retry_after: RetryAfter(1),
              rl_key: RateLimitKey(
                scope: Organization(OrganizationId(42)),
                dimension_hashes: 0,
              ),
            ),
            HeapItem(
              retry_after: RetryAfter(1),
              rl_key: RateLimitKey(
                scope: Organization(OrganizationId(42)),
                dimension_hashes: 0,
              ),
            ),
          ],
        )
        "#);
    }

    #[test]
    fn test_rate_limits_longest() {
        let mut rate_limits = RateLimits::new();

        rate_limits.add(RateLimit {
            categories: [DataCategory::Error].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: Some(ReasonCode::new("first")),
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        // Distinct scope to prevent deduplication
        rate_limits.add(RateLimit {
            categories: [DataCategory::Transaction].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: Some(ReasonCode::new("second")),
            retry_after: RetryAfter::from_secs(10),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        let rate_limit = rate_limits.longest().unwrap();
        insta::assert_ron_snapshot!(rate_limit, @r#"
        RateLimit(
          categories: [
            "transaction",
          ],
          scope: Organization(OrganizationId(42)),
          reason_code: Some(ReasonCode("second")),
          retry_after: RetryAfter(10),
          namespaces: [],
          dimensions: DimensionMap(
            dimensions: {},
            hash: 0,
          ),
        )
        "#);
    }

    #[test]
    fn test_rate_limits_clean_expired() {
        let mut rate_limits = RateLimits::new();

        // Active error limit
        rate_limits.add(RateLimit {
            categories: [DataCategory::Error].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        // Inactive error limit with distinct scope
        rate_limits.add(RateLimit {
            categories: [DataCategory::Error].into(),
            scope: RateLimitScope::Project(ProjectId::new(21)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(0),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        // Sanity check before running `clean_expired`
        assert_eq!(rate_limits.iter().count(), 2);

        rate_limits.clean_expired(Instant::now());

        // Check that the expired limit has been removed
        insta::assert_ron_snapshot!(rate_limits, @r#"
        RateLimits(
          limits: {
            RateLimitKey(
              scope: Organization(OrganizationId(42)),
              dimension_hashes: 0,
            ): [
              RateLimit(
                categories: [
                  "error",
                ],
                scope: Organization(OrganizationId(42)),
                reason_code: None,
                retry_after: RetryAfter(1),
                namespaces: [],
                dimensions: DimensionMap(
                  dimensions: {},
                  hash: 0,
                ),
              ),
            ],
          },
          dimension_sets: [
            [],
          ],
          expiry_heap: [
            HeapItem(
              retry_after: RetryAfter(1),
              rl_key: RateLimitKey(
                scope: Organization(OrganizationId(42)),
                dimension_hashes: 0,
              ),
            ),
          ],
        )
        "#);
    }

    #[test]
    fn test_rate_limits_is_any_limited() {
        let mut rate_limits = RateLimits::new();

        // Active error limit
        rate_limits.add(RateLimit {
            categories: [DataCategory::Error].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        // Active transaction limit
        rate_limits.add(RateLimit {
            categories: [DataCategory::Transaction].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        let scoping = Scoping {
            organization_id: OrganizationId::new(42),
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: None,
        };

        let quotas = &[Quota {
            id: None,
            categories: [DataCategory::Attachment].into(),
            scope: QuotaScope::Organization,
            scope_id: Some("42".into()),
            limit: Some(0),
            window: None,
            reason_code: Some(ReasonCode::new("zero")),
            namespace: None,
            group_by: None,
        }];

        assert!(
            rate_limits.is_any_limited_with_quotas(quotas, &[scoping.item(DataCategory::Error)])
        );
        assert!(
            rate_limits
                .is_any_limited_with_quotas(quotas, &[scoping.item(DataCategory::Attachment)])
        );
        assert!(rate_limits.is_any_limited_with_quotas(
            quotas,
            &[
                scoping.item(DataCategory::Replay),
                scoping.item(DataCategory::Error)
            ]
        ));

        assert!(
            !rate_limits.is_any_limited_with_quotas(quotas, &[scoping.item(DataCategory::Replay)])
        );
    }

    #[test]
    fn test_rate_limits_check() {
        let mut rate_limits = RateLimits::new();

        // Active error limit
        rate_limits.add(RateLimit {
            categories: [DataCategory::Error].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        // Active transaction limit
        rate_limits.add(RateLimit {
            categories: [DataCategory::Transaction].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        let applied_limits = rate_limits.check(&ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: DimensionMap::default(),
        });

        // Check that the error limit is applied
        insta::assert_ron_snapshot!(applied_limits, @r#"
        RateLimits(
          limits: {
            RateLimitKey(
              scope: Organization(OrganizationId(42)),
              dimension_hashes: 0,
            ): [
              RateLimit(
                categories: [
                  "error",
                ],
                scope: Organization(OrganizationId(42)),
                reason_code: None,
                retry_after: RetryAfter(1),
                namespaces: [],
                dimensions: DimensionMap(
                  dimensions: {},
                  hash: 0,
                ),
              ),
            ],
          },
          dimension_sets: [
            [],
          ],
          expiry_heap: [
            HeapItem(
              retry_after: RetryAfter(1),
              rl_key: RateLimitKey(
                scope: Organization(OrganizationId(42)),
                dimension_hashes: 0,
              ),
            ),
          ],
        )
        "#);
    }

    #[test]
    fn test_rate_limits_check_quotas() {
        let mut rate_limits = RateLimits::new();

        // Active error limit
        rate_limits.add(RateLimit {
            categories: [DataCategory::Error].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        // Active transaction limit
        rate_limits.add(RateLimit {
            categories: [DataCategory::Transaction].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        let item_scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            },
            namespace: MetricNamespaceScoping::None,
            dimensions: DimensionMap::default(),
        };

        let quotas = &[Quota {
            id: None,
            categories: [DataCategory::Error].into(),
            scope: QuotaScope::Organization,
            scope_id: Some("42".into()),
            limit: Some(0),
            window: None,
            reason_code: Some(ReasonCode::new("zero")),
            namespace: None,
            group_by: None,
        }];

        let applied_limits = rate_limits.check_with_quotas(quotas, &item_scoping);

        insta::assert_ron_snapshot!(applied_limits, @r#"
        RateLimits(
          limits: {
            RateLimitKey(
              scope: Organization(OrganizationId(42)),
              dimension_hashes: 0,
            ): [
              RateLimit(
                categories: [
                  "error",
                ],
                scope: Organization(OrganizationId(42)),
                reason_code: Some(ReasonCode("zero")),
                retry_after: RetryAfter(60),
                namespaces: [],
                dimensions: DimensionMap(
                  dimensions: {},
                  hash: 0,
                ),
              ),
            ],
          },
          dimension_sets: [
            [],
          ],
          expiry_heap: [
            HeapItem(
              retry_after: RetryAfter(60),
              rl_key: RateLimitKey(
                scope: Organization(OrganizationId(42)),
                dimension_hashes: 0,
              ),
            ),
          ],
        )
        "#);
    }

    #[test]
    fn test_rate_limits_merge() {
        let mut rate_limits1 = RateLimits::new();
        let mut rate_limits2 = RateLimits::new();

        rate_limits1.add(RateLimit {
            categories: [DataCategory::Error].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: Some(ReasonCode::new("first")),
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        rate_limits1.add(RateLimit {
            categories: [DataCategory::TransactionIndexed].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        rate_limits2.add(RateLimit {
            categories: [DataCategory::Error].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: Some(ReasonCode::new("second")),
            retry_after: RetryAfter::from_secs(10),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        rate_limits1.merge(rate_limits2);

        insta::assert_ron_snapshot!(rate_limits1, @r#"
        RateLimits(
          limits: {
            RateLimitKey(
              scope: Organization(OrganizationId(42)),
              dimension_hashes: 0,
            ): [
              RateLimit(
                categories: [
                  "error",
                ],
                scope: Organization(OrganizationId(42)),
                reason_code: Some(ReasonCode("second")),
                retry_after: RetryAfter(10),
                namespaces: [],
                dimensions: DimensionMap(
                  dimensions: {},
                  hash: 0,
                ),
              ),
              RateLimit(
                categories: [
                  "transaction_indexed",
                ],
                scope: Organization(OrganizationId(42)),
                reason_code: None,
                retry_after: RetryAfter(1),
                namespaces: [],
                dimensions: DimensionMap(
                  dimensions: {},
                  hash: 0,
                ),
              ),
            ],
          },
          dimension_sets: [
            [],
          ],
          expiry_heap: [
            HeapItem(
              retry_after: RetryAfter(1),
              rl_key: RateLimitKey(
                scope: Organization(OrganizationId(42)),
                dimension_hashes: 0,
              ),
            ),
            HeapItem(
              retry_after: RetryAfter(1),
              rl_key: RateLimitKey(
                scope: Organization(OrganizationId(42)),
                dimension_hashes: 0,
              ),
            ),
            HeapItem(
              retry_after: RetryAfter(10),
              rl_key: RateLimitKey(
                scope: Organization(OrganizationId(42)),
                dimension_hashes: 0,
              ),
            ),
          ],
        )
        "#);
    }

    #[test]
    fn test_cached_rate_limits_expired() {
        let cached = CachedRateLimits::new();

        // Active error limit
        cached.add(RateLimit {
            categories: [DataCategory::Error].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(1),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        // Inactive error limit with distinct scope
        cached.add(RateLimit {
            categories: [DataCategory::Error].into(),
            scope: RateLimitScope::Project(ProjectId::new(21)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(0),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        let rate_limits = cached.current_limits();

        insta::assert_ron_snapshot!(rate_limits, @r#"
        RateLimits(
          limits: {
            RateLimitKey(
              scope: Organization(OrganizationId(42)),
              dimension_hashes: 0,
            ): [
              RateLimit(
                categories: [
                  "error",
                ],
                scope: Organization(OrganizationId(42)),
                reason_code: None,
                retry_after: RetryAfter(1),
                namespaces: [],
                dimensions: DimensionMap(
                  dimensions: {},
                  hash: 0,
                ),
              ),
            ],
          },
          dimension_sets: [
            [],
          ],
          expiry_heap: [
            HeapItem(
              retry_after: RetryAfter(1),
              rl_key: RateLimitKey(
                scope: Organization(OrganizationId(42)),
                dimension_hashes: 0,
              ),
            ),
          ],
        )
        "#);
    }

    fn monitor_quota(group_by: Option<GroupBy>) -> Quota {
        Quota {
            id: Some("quota".into()),
            categories: [DataCategory::Monitor].into(),
            scope: QuotaScope::Project,
            scope_id: None,
            limit: Some(6),
            window: Some(60),
            reason_code: Some(ReasonCode::new("monitor_limit")),
            namespace: None,
            group_by,
        }
    }

    fn group_by_slug() -> Option<GroupBy> {
        Some(GroupBy {
            max_cardinality: 999,
            dimensions: std::collections::BTreeSet::from([crate::Dimension::CheckInSlug]).into(),
        })
    }

    fn monitor_scoping(slug: Option<&str>) -> ItemScoping {
        let scoping = Scoping {
            organization_id: OrganizationId::new(42),
            project_id: ProjectId::new(43),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(44),
        };

        match slug {
            Some(slug) => scoping.item_with_dimensions(
                DataCategory::Monitor,
                DimensionMap::from([(crate::Dimension::CheckInSlug, slug.to_owned())]),
            ),
            None => scoping.item(DataCategory::Monitor),
        }
    }

    /// A limit inherits the dimensionality of the quota which produced it.
    #[test]
    fn test_rate_limit_from_quota_marks_dimensional() {
        let scoping = monitor_scoping(Some("cron1"));
        let retry_after = RetryAfter::from_secs(60);

        let plain = RateLimit::from_quota(&monitor_quota(None), &scoping, retry_after);
        assert!(plain.dimensions.is_empty());

        let dimensional =
            RateLimit::from_quota(&monitor_quota(group_by_slug()), &scoping, retry_after);
        assert!(!dimensional.dimensions.is_empty());
    }

    /// A dimensional limit matches every item in its scope, which is exactly why it must not be
    /// cached. `cacheable` is what keeps it out of the cache.
    #[test]
    fn test_rate_limits_cacheable_drops_dimensional() {
        let scoping = monitor_scoping(Some("cron1"));
        let retry_after = RetryAfter::from_secs(60);

        let mut limits = RateLimits::new();
        limits.add(RateLimit::from_quota(
            &monitor_quota(group_by_slug()),
            &scoping,
            retry_after,
        ));

        // The limit is correctly enforced against the item which produced it, but it also
        // matches an unrelated monitor in the same project, and an item with no dimensions.
        assert!(limits.check(&monitor_scoping(Some("cron1"))).is_limited());
        assert!(!limits.check(&monitor_scoping(Some("cron2"))).is_limited());
        assert!(!limits.check(&monitor_scoping(None)).is_limited());
    }

    /// Filtering keeps the scope-wide limits, so a project which exhausts its total monitor
    /// quota still backs off.
    #[test]
    fn test_rate_limits_cacheable_keeps_plain() {
        let scoping = monitor_scoping(Some("cron1"));
        let retry_after = RetryAfter::from_secs(60);

        let mut limits = RateLimits::new();
        limits.add(RateLimit::from_quota(
            &monitor_quota(group_by_slug()),
            &scoping,
            retry_after,
        ));
        limits.add(RateLimit::from_quota(
            &monitor_quota(None),
            &scoping,
            retry_after,
        ));

        // Both limits are kept apart despite sharing scope, categories and namespaces.
        assert_eq!(limits.limits.len(), 2);

        assert!(limits.check(&monitor_scoping(Some("cron2"))).is_limited());
    }

    /// A dimensional limit must not displace a plain one when both are added.
    #[test]
    fn test_rate_limits_add_keeps_dimensional_separate() {
        let scoping = monitor_scoping(Some("cron1"));

        let mut limits = RateLimits::new();
        // The dimensional limit is the longer of the two, so without the `dimensional` field in
        // the merge key it would overwrite the plain one and be cached in its place.
        limits.add(RateLimit::from_quota(
            &monitor_quota(None),
            &scoping,
            RetryAfter::from_secs(60),
        ));
        limits.add(RateLimit::from_quota(
            &monitor_quota(group_by_slug()),
            &scoping,
            RetryAfter::from_secs(600),
        ));

        assert_eq!(limits.limits.len(), 2);
    }

    fn group_by(dimensions: impl IntoIterator<Item = Dimension>) -> Option<GroupBy> {
        Some(GroupBy {
            max_cardinality: 999,
            dimensions: BTreeSet::from_iter(dimensions).into(),
        })
    }

    fn check_in(environment: &str, slug: &str) -> ItemScoping {
        Scoping {
            organization_id: OrganizationId::new(42),
            project_id: ProjectId::new(43),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(44),
        }
        .item_with_dimensions(
            DataCategory::Monitor,
            DimensionMap::from([
                (Dimension::CheckInEnvironment, environment),
                (Dimension::CheckInSlug, slug),
            ]),
        )
    }

    fn limit_for(quota: &Quota, scoping: &ItemScoping) -> RateLimit {
        RateLimit::from_quota(quota, scoping, RetryAfter::from_secs(60))
    }

    #[test]
    fn test_cached_limit_ignores_unrelated_dimensions() {
        let quota = monitor_quota(group_by([Dimension::CheckInSlug]));

        let prod = check_in("prod", "cron1");
        let dev = check_in("dev", "cron1");

        let mut limits = RateLimits::new();
        limits.add(limit_for(&quota, &prod));

        assert!(limits.check(&prod).is_limited());
        assert!(limits.check(&dev).is_limited());
    }

    #[test]
    fn test_cached_limit_separates_distinct_dimensions() {
        let quota = monitor_quota(group_by([Dimension::CheckInSlug]));

        let mut limits = RateLimits::new();
        limits.add(limit_for(&quota, &check_in("prod", "cron1")));

        assert!(limits.check(&check_in("prod", "cron1")).is_limited());
        assert!(!limits.check(&check_in("prod", "cron2")).is_limited());
    }

    #[test]
    fn test_cached_limit_matches_all_dimensions() {
        let quota = monitor_quota(group_by([
            Dimension::CheckInEnvironment,
            Dimension::CheckInSlug,
        ]));

        let mut limits = RateLimits::new();
        limits.add(limit_for(&quota, &check_in("prod", "cron1")));

        eprintln!("{:?}", limits);
        eprintln!("{:?}", check_in("prod", "cron1"));

        assert!(limits.check(&check_in("prod", "cron1")).is_limited());
        assert!(!limits.check(&check_in("dev", "cron1")).is_limited());
        assert!(!limits.check(&check_in("prod", "cron2")).is_limited());
    }

    #[test]
    fn test_cached_limit_skips_undimensioned_item() {
        let quota = monitor_quota(group_by([Dimension::CheckInSlug]));

        let mut limits = RateLimits::new();
        limits.add(limit_for(&quota, &check_in("prod", "cron1")));

        assert!(!limits.check(&monitor_scoping(None)).is_limited());
    }

    #[test]
    fn test_cached_undimensioned_limit_applies_to_all() {
        let quota = monitor_quota(None);

        let mut limits = RateLimits::new();
        limits.add(limit_for(&quota, &check_in("prod", "cron1")));

        assert!(limits.check(&check_in("prod", "cron1")).is_limited());
        assert!(limits.check(&check_in("dev", "cron2")).is_limited());
        assert!(limits.check(&monitor_scoping(None)).is_limited());
    }

    #[test]
    fn test_cached_limits_mix_dimensioned_and_plain() {
        let mut limits = RateLimits::new();
        limits.add(limit_for(
            &monitor_quota(group_by([Dimension::CheckInSlug])),
            &check_in("prod", "cron1"),
        ));
        limits.add(limit_for(&monitor_quota(None), &check_in("prod", "cron1")));

        assert_eq!(limits.check(&check_in("prod", "cron1")).iter().count(), 2);
        assert_eq!(limits.check(&check_in("prod", "cron2")).iter().count(), 1);
    }

    #[test]
    fn test_is_any_limited_respects_dimensions() {
        let quota = monitor_quota(group_by([Dimension::CheckInSlug]));

        let mut limits = RateLimits::new();
        limits.add(limit_for(&quota, &check_in("prod", "cron1")));

        assert!(limits.is_any_limited(&[check_in("dev", "cron1")]));
        assert!(!limits.is_any_limited(&[check_in("prod", "cron2")]));
    }

    #[test]
    fn test_propagatable_is_empty_without_plain_limits() {
        assert!(RateLimits::new().propagatable().is_empty());

        let mut limits = RateLimits::new();
        limits.add(limit_for(
            &monitor_quota(group_by([Dimension::CheckInSlug])),
            &check_in("prod", "cron1"),
        ));

        let propagatable = limits.propagatable();
        assert!(propagatable.is_empty());
        assert!(!propagatable.is_limited());
        assert!(propagatable.longest().is_none());
    }

    #[test]
    fn test_propagatable_keeps_plain_limits() {
        let mut limits = RateLimits::new();
        limits.add(limit_for(
            &monitor_quota(group_by([Dimension::CheckInSlug])),
            &check_in("prod", "cron1"),
        ));
        limits.add(limit_for(&monitor_quota(None), &check_in("prod", "cron1")));

        let propagatable = limits.propagatable();
        assert_eq!(propagatable.iter().count(), 1);
        assert!(propagatable.iter().all(|limit| limit.dimensions.is_empty()));
    }

    #[test]
    fn test_clean_expired_drops_dimension_buckets() {
        let quota = monitor_quota(group_by([Dimension::CheckInSlug]));
        let scoping = check_in("prod", "cron1");

        let mut limits = RateLimits::new();
        limits.add(RateLimit::from_quota(
            &quota,
            &scoping,
            RetryAfter::from_secs(0),
        ));
        limits.add(limit_for(&quota, &check_in("prod", "cron2")));

        limits.clean_expired(Instant::now());

        assert_eq!(limits.iter().count(), 1);
        assert!(!limits.check(&scoping).is_limited());
        assert!(limits.check(&check_in("prod", "cron2")).is_limited());
    }

    #[test]
    fn test_has_expired_tracks_earliest_limit() {
        let quota = monitor_quota(group_by([Dimension::CheckInSlug]));

        let mut limits = RateLimits::new();
        assert!(!limits.has_expired(Instant::now()));

        limits.add(limit_for(&quota, &check_in("prod", "cron1")));
        assert!(!limits.has_expired(Instant::now()));

        limits.add(RateLimit::from_quota(
            &quota,
            &check_in("prod", "cron2"),
            RetryAfter::from_secs(0),
        ));
        assert!(limits.has_expired(Instant::now()));

        limits.clean_expired(Instant::now());
        assert!(!limits.has_expired(Instant::now()));
        assert_eq!(limits.iter().count(), 1);
    }

    #[test]
    fn test_cached_rate_limits_round_trip_dimensions() {
        let quota = monitor_quota(group_by([Dimension::CheckInSlug]));

        let cached = CachedRateLimits::new();
        cached.add(limit_for(&quota, &check_in("prod", "cron1")));

        let current = cached.current_limits();
        assert!(current.check(&check_in("dev", "cron1")).is_limited());
        assert!(!current.check(&check_in("prod", "cron2")).is_limited());
        assert!(current.propagatable().is_empty());
    }
}
