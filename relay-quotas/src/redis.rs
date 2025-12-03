use std::fmt::{self, Debug};
use std::sync::Arc;

use relay_base_schema::metrics::MetricNamespace;
use relay_base_schema::organization::OrganizationId;
use relay_common::time::UnixTimestamp;
use relay_log::protocol::value;
use relay_redis::redis::{self, FromRedisValue, Script};
use relay_redis::{AsyncRedisClient, RedisError, RedisScripts};
use thiserror::Error;

use crate::cache::OpportunisticQuotaCache;
use crate::global::GlobalLimiter;
use crate::quota::{ItemScoping, Quota, QuotaScope};
use crate::rate_limit::{RateLimit, RateLimits, RetryAfter};
use crate::statsd::{QuotaCounters, QuotaTimers};
use crate::{REJECT_ALL_SECS, cache};

/// The `grace` period allows accommodating for clock drift in TTL
/// calculation since the clock on the Redis instance used to store quota
/// metrics may not be in sync with the computer running this code.
const GRACE: u64 = 60;

/// An error returned by [`RedisRateLimiter`].
#[derive(Debug, Error)]
pub enum RateLimitingError {
    /// Failed to communicate with Redis.
    #[error("failed to communicate with redis")]
    Redis(
        #[from]
        #[source]
        RedisError,
    ),

    /// Failed to check global rate limits via the service.
    #[error("failed to check global rate limits")]
    UnreachableGlobalRateLimits,
}

/// Creates a refund key for a given counter key.
///
/// Refund keys are used to track credits that should be applied to a quota,
/// allowing for more flexible quota management.
fn get_refunded_quota_key(counter_key: &str) -> String {
    format!("r:{counter_key}")
}

/// A transparent wrapper around an Option that only displays `Some`.
struct OptionalDisplay<T>(Option<T>);

impl<T> fmt::Display for OptionalDisplay<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(ref value) => write!(f, "{value}"),
            None => Ok(()),
        }
    }
}

/// Owned version of [`RedisQuota`].
#[derive(Debug, Clone)]
pub struct OwnedRedisQuota {
    /// The original quota.
    quota: Quota,
    /// Scopes of the item being tracked.
    scoping: ItemScoping,
    /// The Redis key prefix mapped from the quota id.
    prefix: Arc<str>,
    /// The Redis window in seconds mapped from the quota.
    window: u64,
    /// The quantity being checked.
    quantity: u64,
    /// The ingestion timestamp determining the rate limiting bucket.
    timestamp: UnixTimestamp,
}

impl OwnedRedisQuota {
    /// Returns an instance of [`RedisQuota`] which borrows from this [`OwnedRedisQuota`].
    pub fn build_ref(&self) -> RedisQuota<'_> {
        RedisQuota {
            quota: &self.quota,
            scoping: self.scoping,
            prefix: Arc::clone(&self.prefix),
            window: self.window,
            quantity: self.quantity,
            timestamp: self.timestamp,
        }
    }
}

/// Reference to information required for tracking quotas in Redis.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RedisQuota<'a> {
    /// The original quota.
    quota: &'a Quota,
    /// Scopes of the item being tracked.
    scoping: ItemScoping,
    /// The Redis key prefix mapped from the quota id.
    prefix: Arc<str>,
    /// The Redis window in seconds mapped from the quota.
    window: u64,
    /// The quantity being checked.
    quantity: u64,
    /// The ingestion timestamp determining the rate limiting bucket.
    timestamp: UnixTimestamp,
}

impl<'a> RedisQuota<'a> {
    /// Creates a new [`RedisQuota`] from a [`Quota`], item scoping, and timestamp.
    ///
    /// Returns `None` if the quota cannot be tracked in Redis because it's missing
    /// required fields (ID or window). This allows forward compatibility with
    /// future quota types.
    pub fn new(
        quota: &'a Quota,
        quantity: u64,
        scoping: ItemScoping,
        timestamp: UnixTimestamp,
    ) -> Option<Self> {
        // These fields indicate that we *can* track this quota.
        let prefix = quota.id.clone()?;
        let window = quota.window?;

        Some(Self {
            quota,
            scoping,
            prefix,
            quantity,
            window,
            timestamp,
        })
    }

    /// Converts this [`RedisQuota`] to an [`OwnedRedisQuota`] leaving the original
    /// struct in place.
    pub fn build_owned(&self) -> OwnedRedisQuota {
        OwnedRedisQuota {
            quota: self.quota.clone(),
            scoping: self.scoping,
            prefix: Arc::clone(&self.prefix),
            window: self.window,
            quantity: self.quantity,
            timestamp: self.timestamp,
        }
    }

    /// Returns the window size of the quota in seconds.
    pub fn window(&self) -> u64 {
        self.window
    }

    /// Returns the prefix of the quota used for Redis key generation.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Returns the quantity to rate limit.
    pub fn quantity(&self) -> u64 {
        self.quantity
    }

    /// Returns the limit value formatted for Redis.
    ///
    /// Returns `-1` for unlimited quotas or when the limit doesn't fit into an `i64`.
    /// Otherwise, returns the limit value as an `i64`.
    pub fn limit(&self) -> i64 {
        self.limit
            // If it does not fit into i64, treat as unlimited:
            .and_then(|limit| limit.try_into().ok())
            .unwrap_or(-1)
    }

    fn shift(&self) -> u64 {
        if self.quota.scope == QuotaScope::Global {
            0
        } else {
            self.scoping.organization_id.value() % self.window
        }
    }

    /// Returns the current time slot of the quota based on the timestamp.
    ///
    /// Slots are used to determine the time bucket for rate limiting.
    pub fn slot(&self) -> u64 {
        (self.timestamp.as_secs() - self.shift()) / self.window
    }

    /// Returns the timestamp when the current quota window will expire.
    pub fn expiry(&self) -> UnixTimestamp {
        let next_slot = self.slot() + 1;
        let next_start = next_slot * self.window + self.shift();
        UnixTimestamp::from_secs(next_start)
    }

    /// Returns when the Redis key should expire.
    ///
    /// This is the expiry time plus a grace period.
    pub fn key_expiry(&self) -> u64 {
        self.expiry().as_secs() + GRACE
    }

    /// Returns the Redis key for this quota.
    ///
    /// The key includes the quota ID, organization ID, and other scoping information
    /// based on the quota's scope type. Keys are structured to ensure proper isolation
    /// between different organizations and scopes.
    pub fn key(&self) -> QuotaCacheKey {
        // The subscope id is only formatted into the key if the quota is not organization-scoped.
        // The organization id is always included.
        let subscope = match self.quota.scope {
            QuotaScope::Global => None,
            QuotaScope::Organization => None,
            scope => self.scoping.scope_id(scope),
        };

        QuotaCacheKey {
            id: Arc::clone(&self.prefix),
            org: self.scoping.organization_id,
            subscope,
            namespace: self.namespace,
            slot: self.slot(),
        }
    }

    /// Returns a [`cache::Quota`] built from this [`RedisQuota`].
    fn for_cache(&self) -> cache::Quota<QuotaCacheKey> {
        cache::Quota {
            limit: self.limit(),
            window: self.window,
            key: self.key(),
            expiry: UnixTimestamp::from_secs(self.key_expiry()),
        }
    }
}

impl std::ops::Deref for RedisQuota<'_> {
    type Target = Quota;

    fn deref(&self) -> &Self::Target {
        self.quota
    }
}

/// A key which uniquely identifies a quota.
///
/// Can be used as a Redis cache key by using the [`fmt::Display`] trait.
///
/// See also: [`RedisQuota::key`].
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct QuotaCacheKey {
    id: Arc<str>,
    org: OrganizationId,
    subscope: Option<u64>,
    namespace: Option<MetricNamespace>,
    slot: u64,
}

impl fmt::Display for QuotaCacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "quota:{id}{{{org}}}{subscope}{namespace}:{slot}",
            id = self.id,
            org = self.org,
            subscope = OptionalDisplay(self.subscope),
            namespace = OptionalDisplay(self.namespace),
            slot = self.slot,
        )
    }
}

/// A service that executes quotas and checks for rate limits in a shared cache.
///
/// Quotas handle tracking a project's usage and respond whether a project has been
/// configured to throttle incoming data if they go beyond the specified quota.
///
/// Quotas can specify a window to be tracked in, such as per minute or per hour. Additionally,
/// quotas allow to specify the data categories they apply to, for example error events or
/// attachments. For more information on quota parameters, see [`Quota`].
///
/// Requires the `redis` feature.
#[derive(Clone)]
pub struct RedisRateLimiter<T> {
    client: AsyncRedisClient,
    cache: Option<Arc<OpportunisticQuotaCache<QuotaCacheKey>>>,
    script: &'static Script,
    max_limit: Option<u64>,
    global_limiter: T,
}

impl<T: GlobalLimiter> RedisRateLimiter<T> {
    /// Creates a new [`RedisRateLimiter`] instance.
    pub fn new(client: AsyncRedisClient, global_limiter: T) -> Self {
        RedisRateLimiter {
            client,
            cache: None,
            script: RedisScripts::load_is_rate_limited(),
            max_limit: None,
            global_limiter,
        }
    }

    /// Sets the maximum rate limit in seconds.
    ///
    /// By default, this rate limiter will return rate limits based on the quotas' `window` fields.
    /// If a maximum rate limit is set, the returned rate limit will be bounded by this value.
    pub fn max_limit(mut self, max_limit: Option<u64>) -> Self {
        self.max_limit = max_limit;
        self
    }

    /// Enables an opportunistic cache for quotas.
    ///
    /// The opportunistic cache, opportunistically serves quotas from a local cache, reducing the
    /// load on Redis heavily.
    ///
    /// Caching considers a ratio of the remaining quota to be available and periodically
    /// synchronizes with Redis.
    pub fn cache(
        mut self,
        max_over_spend_ratio: Option<f32>,
        limit_threshold: Option<f32>,
    ) -> Self {
        self.cache = max_over_spend_ratio
            .map(OpportunisticQuotaCache::new)
            .map(|c| c.with_limit_threshold(limit_threshold))
            .map(Arc::new);

        self
    }

    /// Checks whether any of the quotas in effect have been exceeded and records consumption.
    ///
    /// By invoking this method, the caller signals that data is being ingested and needs to be
    /// counted against the quota. This increment happens atomically if none of the quotas have been
    /// exceeded. Otherwise, a rate limit is returned and data is not counted against the quotas.
    ///
    /// If no key is specified, then only organization-wide and project-wide quotas are checked. If
    /// a key is specified, then key-quotas are also checked.
    ///
    /// When `over_accept_once` is set to `true` and the current quota would be exceeded by the
    /// provided `quantity`, the data is accepted once and subsequent requests will be rejected
    /// until the quota refreshes.
    ///
    /// A `quantity` of `0` can be used to check if the quota limit has been reached or exceeded
    /// without incrementing it in the success case. This is useful for checking quotas in a different
    /// data category.
    pub async fn is_rate_limited<'a>(
        &self,
        quotas: impl IntoIterator<Item = &'a Quota>,
        item_scoping: ItemScoping,
        quantity: usize,
        over_accept_once: bool,
    ) -> Result<RateLimits, RateLimitingError> {
        let timestamp = UnixTimestamp::now();
        let mut invocation = self.script.prepare_invoke();
        let mut tracked_quotas = Vec::new();
        let mut rate_limits = RateLimits::new();

        let mut global_quotas = vec![];

        let quantity = u64::try_from(quantity).unwrap_or(u64::MAX);

        for quota in quotas {
            if !quota.matches(item_scoping) {
                // Silently skip all quotas that do not apply to this item.
            } else if quota.limit == Some(0) {
                // A zero-sized quota is strongest. Do not call into Redis at all, and do not
                // increment any keys, as one quota has reached capacity (this is how regular quotas
                // behave as well).
                let retry_after = self.retry_after(REJECT_ALL_SECS);
                rate_limits.add(RateLimit::from_quota(quota, *item_scoping, retry_after));
            } else if let Some(mut quota) =
                RedisQuota::new(quota, quantity, item_scoping, timestamp)
            {
                if quota.scope == QuotaScope::Global {
                    global_quotas.push(quota);
                } else {
                    if let Some(cache) = &self.cache {
                        quota.quantity = match cache.check_quota(quota.for_cache(), quantity) {
                            cache::Action::Accept => continue,
                            cache::Action::Check(quantity) => quantity,
                        };
                    }

                    let redis_key = quota.key().to_string();
                    // Remaining quotas are expected to be track-able in Redis.
                    let refund_key = get_refunded_quota_key(&redis_key);

                    invocation.key(redis_key);
                    invocation.key(refund_key);

                    invocation.arg(quota.limit());
                    invocation.arg(quota.key_expiry());
                    invocation.arg(quota.quantity);
                    invocation.arg(over_accept_once);

                    tracked_quotas.push(quota);
                }
            } else {
                // This quota is neither a static reject-all, nor can it be tracked in Redis due to
                // missing fields. We're skipping this for forward-compatibility.
                relay_log::with_scope(
                    |scope| scope.set_extra("quota", value::to_value(quota).unwrap()),
                    || relay_log::warn!("skipping unsupported quota"),
                )
            }
        }

        // We check the global rate limits before the other limits. This step must be separate from
        // checking the other rate limits, since those are checked with a Redis script that works
        // under the invariant that all keys are within the same Redis instance (given their partitioning).
        // Global keys on the other hand are always on the same instance, so if they were to be mixed
        // with normal keys the script will end up referencing keys from multiple instances, making it
        // impossible for the script to work.
        let rate_limited_global_quotas = self
            .global_limiter
            .check_global_rate_limits(&global_quotas)
            .await?;

        for quota in rate_limited_global_quotas {
            let retry_after = self.retry_after((quota.expiry() - timestamp).as_secs());
            rate_limits.add(RateLimit::from_quota(quota, *item_scoping, retry_after));
        }

        // Either there are no quotas to run against Redis, or we already have a rate limit from a
        // zero-sized quota. In either cases, skip invoking the script and return early.
        if tracked_quotas.is_empty() || rate_limits.is_limited() {
            return Ok(rate_limits);
        }

        // We get the Redis client after the global rate limiting since we don't want to hold the
        // client across await points, otherwise it might be held for too long, and we will run out
        // of connections.
        let mut connection = self.client.get_connection().await?;
        let result: ScriptResult = invocation
            .invoke_async(&mut connection)
            .await
            .map_err(RedisError::Redis)?;

        for (quota, state) in tracked_quotas.iter().zip(result.0) {
            if state.is_rejected {
                // We can calculate the error by comparing how much the cache added to the
                // quantity with remaining difference of the consumption and limit.
                let cache_error = {
                    let remaining = quota.limit().saturating_sub(state.consumed).max(0) as u64;
                    let cache_quantity = quota.quantity.saturating_sub(quantity);

                    cache_quantity.saturating_sub(remaining)
                };
                relay_statsd::metric!(
                    counter(QuotaCounters::CacheError) += cache_error,
                    category = item_scoping.category.name(),
                );

                let retry_after = self.retry_after((quota.expiry() - timestamp).as_secs());
                rate_limits.add(RateLimit::from_quota(quota, *item_scoping, retry_after));
            } else if let Some(cache) = &self.cache {
                // Only update the cache if it's really necessary. Quotas which are being rejected,
                // will not be able to be handled from the cache anyways.
                cache.update_quota(quota.for_cache(), state.consumed);
            }
        }

        if let Some(cache) = &self.cache {
            let vacuum_start = std::time::Instant::now();
            if cache.try_vacuum(timestamp) {
                relay_statsd::metric!(
                    timer(QuotaTimers::CacheVacuumDuration) = vacuum_start.elapsed()
                );
            }
        }

        Ok(rate_limits)
    }

    /// Creates a [`RetryAfter`] value that is bounded by the configured [`max_limit`](Self::max_limit).
    ///
    /// If a maximum rate limit has been set, the returned value will not exceed that limit.
    fn retry_after(&self, mut seconds: u64) -> RetryAfter {
        if let Some(max_limit) = self.max_limit {
            seconds = std::cmp::min(seconds, max_limit);
        }

        RetryAfter::from_secs(seconds)
    }
}

/// The result returned from the rate limiting Redis script.
#[derive(Debug)]
struct ScriptResult(Vec<QuotaState>);

impl FromRedisValue for ScriptResult {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let Some(seq) = v.as_sequence() else {
            return Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "Expected a sequence from the rate limiting script",
                format!("{v:?}"),
            )));
        };

        let (chunks, rem) = seq.as_chunks();
        if !rem.is_empty() {
            return Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "Expected an even number of values from the rate limiting script",
                format!("{v:?}"),
            )));
        }

        let mut quotas = Vec::with_capacity(chunks.len());
        for [is_rejected, consumed] in chunks {
            quotas.push(QuotaState {
                is_rejected: bool::from_redis_value(is_rejected)?,
                consumed: i64::from_redis_value(consumed)?,
            });
        }

        Ok(Self(quotas))
    }
}

/// The state returned from the rate limiting script for a single quota.
#[derive(Debug)]
struct QuotaState {
    /// Whether the quota rejects the request.
    is_rejected: bool,
    /// How much of the quota has already been consumed.
    consumed: i64,
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::quota::{DataCategories, DataCategory, ReasonCode, Scoping};
    use crate::rate_limit::RateLimitScope;
    use crate::{GlobalRateLimiter, MetricNamespaceScoping};
    use relay_base_schema::metrics::MetricNamespace;
    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_redis::RedisConfigOptions;
    use relay_redis::redis::AsyncCommands;
    use smallvec::smallvec;
    use tokio::sync::Mutex;

    struct MockGlobalLimiter {
        client: AsyncRedisClient,
        global_rate_limiter: Mutex<GlobalRateLimiter>,
    }

    impl GlobalLimiter for MockGlobalLimiter {
        async fn check_global_rate_limits<'a>(
            &self,
            global_quotas: &'a [RedisQuota<'a>],
        ) -> Result<Vec<&'a RedisQuota<'a>>, RateLimitingError> {
            self.global_rate_limiter
                .lock()
                .await
                .filter_rate_limited(&self.client, global_quotas)
                .await
        }
    }

    fn build_rate_limiter() -> RedisRateLimiter<MockGlobalLimiter> {
        let url = std::env::var("RELAY_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
        let client =
            AsyncRedisClient::single("test", &url, &RedisConfigOptions::default()).unwrap();

        let global_limiter = MockGlobalLimiter {
            client: client.clone(),
            global_rate_limiter: Mutex::new(GlobalRateLimiter::default()),
        };

        RedisRateLimiter {
            client,
            cache: None,
            script: RedisScripts::load_is_rate_limited(),
            max_limit: None,
            global_limiter,
        }
    }

    #[tokio::test]
    async fn test_zero_size_quotas() {
        let quotas = &[
            Quota {
                id: None,
                categories: DataCategories::new(),
                scope: QuotaScope::Organization,
                scope_id: None,
                limit: Some(0),
                window: None,
                reason_code: Some(ReasonCode::new("get_lost")),
                namespace: None,
            },
            Quota {
                id: Some("42".into()),
                categories: DataCategories::new(),
                scope: QuotaScope::Organization,
                scope_id: None,
                limit: None,
                window: Some(42),
                reason_code: Some(ReasonCode::new("unlimited")),
                namespace: None,
            },
        ];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
            namespace: MetricNamespaceScoping::None,
        };

        let rate_limits: Vec<RateLimit> = build_rate_limiter()
            .is_rate_limited(quotas, scoping, 1, false)
            .await
            .expect("rate limiting failed")
            .into_iter()
            .collect();

        assert_eq!(
            rate_limits,
            vec![RateLimit {
                categories: DataCategories::new(),
                scope: RateLimitScope::Organization(OrganizationId::new(42)),
                reason_code: Some(ReasonCode::new("get_lost")),
                retry_after: rate_limits[0].retry_after,
                namespaces: smallvec![],
            }]
        );
    }

    /// Tests that a quota with and without namespace are counted separately.
    #[tokio::test]
    async fn test_non_global_namespace_quota() {
        let quota_limit = 5;
        let get_quota = |namespace: Option<MetricNamespace>| -> Quota {
            Quota {
                id: Some(format!("test_simple_quota_{}", uuid::Uuid::new_v4()).into()),
                categories: DataCategories::new(),
                scope: QuotaScope::Organization,
                scope_id: None,
                limit: Some(quota_limit),
                window: Some(600),
                reason_code: Some(ReasonCode::new(format!("ns: {namespace:?}"))),
                namespace,
            }
        };

        let quotas = &[get_quota(None)];
        let quota_with_namespace = &[get_quota(Some(MetricNamespace::Transactions))];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
            namespace: MetricNamespaceScoping::Some(MetricNamespace::Transactions),
        };

        let rate_limiter = build_rate_limiter();

        // First confirm normal behaviour without namespace.
        for i in 0..10 {
            let rate_limits: Vec<RateLimit> = rate_limiter
                .is_rate_limited(quotas, scoping, 1, false)
                .await
                .expect("rate limiting failed")
                .into_iter()
                .collect();

            if i < quota_limit {
                assert_eq!(rate_limits, vec![]);
            } else {
                assert_eq!(
                    rate_limits[0].reason_code,
                    Some(ReasonCode::new("ns: None"))
                );
            }
        }

        // Then, send identical quota with namespace and confirm it counts separately.
        for i in 0..10 {
            let rate_limits: Vec<RateLimit> = rate_limiter
                .is_rate_limited(quota_with_namespace, scoping, 1, false)
                .await
                .expect("rate limiting failed")
                .into_iter()
                .collect();

            if i < quota_limit {
                assert_eq!(rate_limits, vec![]);
            } else {
                assert_eq!(
                    rate_limits[0].reason_code,
                    Some(ReasonCode::new("ns: Some(Transactions)"))
                );
            }
        }
    }

    #[tokio::test]
    async fn test_simple_quota() {
        let quotas = &[Quota {
            id: Some(format!("test_simple_quota_{}", uuid::Uuid::new_v4()).into()),
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(5),
            window: Some(60),
            reason_code: Some(ReasonCode::new("get_lost")),
            namespace: None,
        }];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
            namespace: MetricNamespaceScoping::None,
        };

        let rate_limiter = build_rate_limiter();

        for i in 0..10 {
            let rate_limits: Vec<RateLimit> = rate_limiter
                .is_rate_limited(quotas, scoping, 1, false)
                .await
                .expect("rate limiting failed")
                .into_iter()
                .collect();

            if i >= 5 {
                assert_eq!(
                    rate_limits,
                    vec![RateLimit {
                        categories: DataCategories::new(),
                        scope: RateLimitScope::Organization(OrganizationId::new(42)),
                        reason_code: Some(ReasonCode::new("get_lost")),
                        retry_after: rate_limits[0].retry_after,
                        namespaces: smallvec![],
                    }]
                );
            } else {
                assert_eq!(rate_limits, vec![]);
            }
        }
    }

    #[tokio::test]
    async fn test_simple_global_quota() {
        let quotas = &[Quota {
            id: Some(format!("test_simple_global_quota_{}", uuid::Uuid::new_v4()).into()),
            categories: DataCategories::new(),
            scope: QuotaScope::Global,
            scope_id: None,
            limit: Some(5),
            window: Some(60),
            reason_code: Some(ReasonCode::new("get_lost")),
            namespace: None,
        }];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
            namespace: MetricNamespaceScoping::None,
        };

        let rate_limiter = build_rate_limiter();

        for i in 0..10 {
            let rate_limits: Vec<RateLimit> = rate_limiter
                .is_rate_limited(quotas, scoping, 1, false)
                .await
                .expect("rate limiting failed")
                .into_iter()
                .collect();

            if i >= 5 {
                assert_eq!(
                    rate_limits,
                    vec![RateLimit {
                        categories: DataCategories::new(),
                        scope: RateLimitScope::Global,
                        reason_code: Some(ReasonCode::new("get_lost")),
                        retry_after: rate_limits[0].retry_after,
                        namespaces: smallvec![],
                    }]
                );
            } else {
                assert_eq!(rate_limits, vec![]);
            }
        }
    }

    #[tokio::test]
    async fn test_quantity_0() {
        let quotas = &[Quota {
            id: Some(format!("test_quantity_0_{}", uuid::Uuid::new_v4()).into()),
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(1),
            window: Some(60),
            reason_code: Some(ReasonCode::new("get_lost")),
            namespace: None,
        }];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
            namespace: MetricNamespaceScoping::None,
        };

        let rate_limiter = build_rate_limiter();

        // limit is 1, so first call not rate limited
        assert!(
            !rate_limiter
                .is_rate_limited(quotas, scoping, 1, false)
                .await
                .unwrap()
                .is_limited()
        );

        // quota is now exhausted
        assert!(
            rate_limiter
                .is_rate_limited(quotas, scoping, 1, false)
                .await
                .unwrap()
                .is_limited()
        );

        // quota is exhausted, regardless of the quantity
        assert!(
            rate_limiter
                .is_rate_limited(quotas, scoping, 0, false)
                .await
                .unwrap()
                .is_limited()
        );

        // quota is exhausted, regardless of the quantity
        assert!(
            rate_limiter
                .is_rate_limited(quotas, scoping, 1, false)
                .await
                .unwrap()
                .is_limited()
        );
    }

    #[tokio::test]
    async fn test_quota_go_over() {
        let quotas = &[Quota {
            id: Some(format!("test_quota_go_over{}", uuid::Uuid::new_v4()).into()),
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(2),
            window: Some(60),
            reason_code: Some(ReasonCode::new("get_lost")),
            namespace: None,
        }];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
            namespace: MetricNamespaceScoping::None,
        };

        let rate_limiter = build_rate_limiter();

        // limit is 2, so first call not rate limited
        let is_limited = rate_limiter
            .is_rate_limited(quotas, scoping, 1, true)
            .await
            .unwrap()
            .is_limited();
        assert!(!is_limited);

        // go over limit, but first call is over-accepted
        let is_limited = rate_limiter
            .is_rate_limited(quotas, scoping, 2, true)
            .await
            .unwrap()
            .is_limited();
        assert!(!is_limited);

        // quota is exhausted, regardless of the quantity
        let is_limited = rate_limiter
            .is_rate_limited(quotas, scoping, 0, true)
            .await
            .unwrap()
            .is_limited();
        assert!(is_limited);

        // quota is exhausted, regardless of the quantity
        let is_limited = rate_limiter
            .is_rate_limited(quotas, scoping, 1, true)
            .await
            .unwrap()
            .is_limited();
        assert!(is_limited);
    }

    #[tokio::test]
    async fn test_bails_immediately_without_any_quota() {
        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
            namespace: MetricNamespaceScoping::None,
        };

        let rate_limits: Vec<RateLimit> = build_rate_limiter()
            .is_rate_limited(&[], scoping, 1, false)
            .await
            .expect("rate limiting failed")
            .into_iter()
            .collect();

        assert_eq!(rate_limits, vec![]);
    }

    #[tokio::test]
    async fn test_limited_with_unlimited_quota() {
        let quotas = &[
            Quota {
                id: Some("q0".into()),
                categories: DataCategories::new(),
                scope: QuotaScope::Organization,
                scope_id: None,
                limit: None,
                window: Some(1),
                reason_code: Some(ReasonCode::new("project_quota0")),
                namespace: None,
            },
            Quota {
                id: Some("q1".into()),
                categories: DataCategories::new(),
                scope: QuotaScope::Organization,
                scope_id: None,
                limit: Some(1),
                window: Some(1),
                reason_code: Some(ReasonCode::new("project_quota1")),
                namespace: None,
            },
        ];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
            namespace: MetricNamespaceScoping::None,
        };

        let rate_limiter = build_rate_limiter();

        for i in 0..1 {
            let rate_limits: Vec<RateLimit> = rate_limiter
                .is_rate_limited(quotas, scoping, 1, false)
                .await
                .expect("rate limiting failed")
                .into_iter()
                .collect();

            if i == 0 {
                assert_eq!(rate_limits, &[]);
            } else {
                assert_eq!(
                    rate_limits,
                    vec![RateLimit {
                        categories: DataCategories::new(),
                        scope: RateLimitScope::Organization(OrganizationId::new(42)),
                        reason_code: Some(ReasonCode::new("project_quota1")),
                        retry_after: rate_limits[0].retry_after,
                        namespaces: smallvec![],
                    }]
                );
            }
        }
    }

    #[tokio::test]
    async fn test_quota_with_quantity() {
        let quotas = &[Quota {
            id: Some(format!("test_quantity_quota_{}", uuid::Uuid::new_v4()).into()),
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(500),
            window: Some(60),
            reason_code: Some(ReasonCode::new("get_lost")),
            namespace: None,
        }];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
            namespace: MetricNamespaceScoping::None,
        };

        let rate_limiter = build_rate_limiter();

        for i in 0..10 {
            let rate_limits: Vec<RateLimit> = rate_limiter
                .is_rate_limited(quotas, scoping, 100, false)
                .await
                .expect("rate limiting failed")
                .into_iter()
                .collect();

            if i >= 5 {
                assert_eq!(
                    rate_limits,
                    vec![RateLimit {
                        categories: DataCategories::new(),
                        scope: RateLimitScope::Organization(OrganizationId::new(42)),
                        reason_code: Some(ReasonCode::new("get_lost")),
                        retry_after: rate_limits[0].retry_after,
                        namespaces: smallvec![],
                    }]
                );
            } else {
                assert_eq!(rate_limits, vec![]);
            }
        }
    }

    #[tokio::test]
    async fn test_get_redis_key_scoped() {
        let quota = Quota {
            id: Some("foo".into()),
            categories: DataCategories::new(),
            scope: QuotaScope::Project,
            scope_id: Some("42".into()),
            window: Some(2),
            limit: Some(0),
            reason_code: None,
            namespace: None,
        };

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(69420),
                project_id: ProjectId::new(42),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(4711),
            },
            namespace: MetricNamespaceScoping::None,
        };

        let timestamp = UnixTimestamp::from_secs(123_123_123);
        let redis_quota = RedisQuota::new(&quota, 0, scoping, timestamp).unwrap();
        assert_eq!(redis_quota.key().to_string(), "quota:foo{69420}42:61561561");
    }

    #[tokio::test]
    async fn test_get_redis_key_unscoped() {
        let quota = Quota {
            id: Some("foo".into()),
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            window: Some(10),
            limit: Some(0),
            reason_code: None,
            namespace: None,
        };

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(69420),
                project_id: ProjectId::new(42),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(4711),
            },
            namespace: MetricNamespaceScoping::None,
        };

        let timestamp = UnixTimestamp::from_secs(234_531);
        let redis_quota = RedisQuota::new(&quota, 0, scoping, timestamp).unwrap();
        assert_eq!(redis_quota.key().to_string(), "quota:foo{69420}:23453");
    }

    #[tokio::test]
    async fn test_large_redis_limit_large() {
        let quota = Quota {
            id: Some("foo".into()),
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            window: Some(10),
            limit: Some(9223372036854775808), // i64::MAX + 1
            reason_code: None,
            namespace: None,
        };

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(69420),
                project_id: ProjectId::new(42),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(4711),
            },
            namespace: MetricNamespaceScoping::None,
        };

        let timestamp = UnixTimestamp::from_secs(234_531);
        let redis_quota = RedisQuota::new(&quota, 0, scoping, timestamp).unwrap();
        assert_eq!(redis_quota.limit(), -1);
    }

    #[tokio::test]
    async fn test_is_rate_limited_script() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap();

        let rate_limiter = build_rate_limiter();
        let mut conn = rate_limiter.client.get_connection().await.unwrap();

        // define a few keys with random seed such that they do not collide with repeated test runs
        let foo = format!("foo___{now}");
        let r_foo = format!("r:foo___{now}");
        let bar = format!("bar___{now}");
        let r_bar = format!("r:bar___{now}");
        let apple = format!("apple___{now}");
        let orange = format!("orange___{now}");
        let baz = format!("baz___{now}");

        let script = RedisScripts::load_is_rate_limited();

        macro_rules! assert_invocation {
            ($invocation:expr, $($tt:tt)*) => {{
                let result = $invocation
                    .invoke_async::<ScriptResult>(&mut conn)
                    .await
                    .unwrap();

                insta::assert_debug_snapshot!(result, $($tt)*);
            }};
        }

        let mut invocation = script.prepare_invoke();
        invocation
            .key(&foo) // key
            .key(&r_foo) // refund key
            .key(&bar) // key
            .key(&r_bar) // refund key
            .arg(1) // limit
            .arg(now + 60) // expiry
            .arg(1) // quantity
            .arg(false) // over accept once
            .arg(2) // limit
            .arg(now + 120) // expiry
            .arg(1) // quantity
            .arg(false); // over accept once

        // Craft a new invocation similar to the previous one, but it only applies to the quota
        // with a higher limit (2).
        let mut invocation2 = script.prepare_invoke();
        invocation2
            .key(&bar) // key
            .key(&r_bar) // refund key
            .arg(2) // limit
            .arg(now + 120) // expiry
            .arg(1) // quantity
            .arg(false); // over accept once

        // 1 quantity used from both quotas.
        assert_invocation!(invocation, @r"
        ScriptResult(
            [
                QuotaState {
                    is_rejected: false,
                    consumed: 1,
                },
                QuotaState {
                    is_rejected: false,
                    consumed: 1,
                },
            ],
        )
        "
        );

        // This invocation fails the rate limit on the first quota.
        // -> No changes are made to the counters.
        assert_invocation!(invocation, @r"
        ScriptResult(
            [
                QuotaState {
                    is_rejected: true,
                    consumed: 1,
                },
                QuotaState {
                    is_rejected: false,
                    consumed: 1,
                },
            ],
        )
        "
        );

        // Another call, same result as before, just making sure there were no changes applied.
        assert_invocation!(invocation, @r"
        ScriptResult(
            [
                QuotaState {
                    is_rejected: true,
                    consumed: 1,
                },
                QuotaState {
                    is_rejected: false,
                    consumed: 1,
                },
            ],
        )
        "
        );

        // Using the second invocation which only considers a quota with a higher limit, usage for
        // that quota is now 2.
        assert_invocation!(invocation2, @r"
        ScriptResult(
            [
                QuotaState {
                    is_rejected: false,
                    consumed: 2,
                },
            ],
        )
        "
        );

        // Same invocation, but this time the limit is reached, quota should not increase.
        assert_invocation!(invocation2, @r"
        ScriptResult(
            [
                QuotaState {
                    is_rejected: true,
                    consumed: 2,
                },
            ],
        )
        "
        );

        // Check again with the original invocation, this now yields `[1, 2]`.
        assert_invocation!(invocation, @r"
        ScriptResult(
            [
                QuotaState {
                    is_rejected: true,
                    consumed: 1,
                },
                QuotaState {
                    is_rejected: true,
                    consumed: 2,
                },
            ],
        )
        "
        );

        assert_eq!(conn.get::<_, String>(&foo).await.unwrap(), "1");
        let ttl: u64 = conn.ttl(&foo).await.unwrap();
        assert!(ttl >= 59);
        assert!(ttl <= 60);

        assert_eq!(conn.get::<_, String>(&bar).await.unwrap(), "2");
        let ttl: u64 = conn.ttl(&bar).await.unwrap();
        assert!(ttl >= 119);
        assert!(ttl <= 120);

        // make sure "refund/negative" keys haven't been incremented
        let () = conn.get(r_foo).await.unwrap();
        let () = conn.get(r_bar).await.unwrap();

        // Test that refunded quotas work
        let () = conn.set(&apple, 5).await.unwrap();

        let mut invocation = script.prepare_invoke();
        invocation
            .key(&orange) // key
            .key(&baz) // refund key
            .arg(1) // limit
            .arg(now + 60) // expiry
            .arg(1) // quantity
            .arg(false);

        // increment, current quota usage is 1.
        assert_invocation!(invocation, @r"
        ScriptResult(
            [
                QuotaState {
                    is_rejected: false,
                    consumed: 1,
                },
            ],
        )
        "
        );

        // test that it's rate limited without refund.
        assert_invocation!(invocation, @r"
        ScriptResult(
            [
                QuotaState {
                    is_rejected: true,
                    consumed: 1,
                },
            ],
        )
        "
        );

        // Make sure, the counter wasn't incremented.
        assert_invocation!(invocation, @r"
        ScriptResult(
            [
                QuotaState {
                    is_rejected: true,
                    consumed: 1,
                },
            ],
        )
        "
        );

        let mut invocation = script.prepare_invoke();
        invocation
            .key(&orange) // key
            .key(&apple) // refund key
            .arg(1) // limit
            .arg(now + 60) // expiry
            .arg(1) // quantity
            .arg(false);

        // test that refund key is used
        assert_invocation!(invocation, @r"
        ScriptResult(
            [
                QuotaState {
                    is_rejected: false,
                    consumed: -3,
                },
            ],
        )
        "
        );
    }

    /// Usual rate limiting with a cache should just work as expected.
    #[tokio::test]
    async fn test_quota_with_cache() {
        let quotas = &[Quota {
            id: Some(format!("test_simple_quota_{}", uuid::Uuid::new_v4()).into()),
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(50),
            window: Some(60),
            reason_code: Some(ReasonCode::new("get_lost")),
            namespace: None,
        }];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
            namespace: MetricNamespaceScoping::None,
        };

        // For this test, with only a single rate limiter accessing Redis and always a quantity of
        // `1`, the parameters for the cache should not make any difference.
        let rate_limiter = build_rate_limiter().cache(Some(0.1), Some(0.9));

        for _ in 0..50 {
            let rate_limits = rate_limiter
                .is_rate_limited(quotas, scoping, 1, false)
                .await
                .unwrap();

            assert!(rate_limits.is_empty());
        }

        let rate_limits: Vec<RateLimit> = rate_limiter
            .is_rate_limited(quotas, scoping, 1, false)
            .await
            .expect("rate limiting failed")
            .into_iter()
            .collect();

        assert_eq!(
            rate_limits,
            vec![RateLimit {
                categories: DataCategories::new(),
                scope: RateLimitScope::Organization(OrganizationId::new(42)),
                reason_code: Some(ReasonCode::new("get_lost")),
                retry_after: rate_limits[0].retry_after,
                namespaces: smallvec![],
            }]
        );
    }

    #[tokio::test]
    async fn test_quota_with_cache_slightly_over_account() {
        let window = 60;
        let limit = 50 * window;

        let quotas = &[Quota {
            id: Some(format!("test_simple_quota_{}", uuid::Uuid::new_v4()).into()),
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(limit),
            window: Some(window),
            reason_code: Some(ReasonCode::new("get_lost")),
            namespace: None,
        }];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: Scoping {
                organization_id: OrganizationId::new(42),
                project_id: ProjectId::new(43),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(44),
            },
            namespace: MetricNamespaceScoping::None,
        };

        // 10% Quota cache.
        let rate_limiter1 = build_rate_limiter().cache(Some(0.1), None);
        let rate_limiter2 = build_rate_limiter().cache(Some(0.1), None);

        // Prime the cache.
        let rate_limits = rate_limiter1
            .is_rate_limited(quotas, scoping, 1, false)
            .await
            .unwrap();
        assert!(rate_limits.is_empty());
        // Reserve 3 out 5 in the cache.
        let rate_limits = rate_limiter1
            .is_rate_limited(quotas, scoping, 3, false)
            .await
            .unwrap();
        assert!(rate_limits.is_empty());

        // Consume right up to the limit on the other limiter
        let rate_limits = rate_limiter2
            .is_rate_limited(quotas, scoping, limit as usize - 1, false)
            .await
            .unwrap();
        assert!(rate_limits.is_empty());

        // There is still one more slot in the cache.
        let rate_limits = rate_limiter1
            .is_rate_limited(quotas, scoping, 1, false)
            .await
            .unwrap();
        assert!(rate_limits.is_empty());

        // This should now rate limit, as the cache is exhausted and Redis is checked.
        let rate_limits: Vec<RateLimit> = rate_limiter1
            .is_rate_limited(quotas, scoping, 1, false)
            .await
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(
            rate_limits,
            vec![RateLimit {
                categories: DataCategories::new(),
                scope: RateLimitScope::Organization(OrganizationId::new(42)),
                reason_code: Some(ReasonCode::new("get_lost")),
                retry_after: rate_limits[0].retry_after,
                namespaces: smallvec![],
            }]
        );
    }
}
