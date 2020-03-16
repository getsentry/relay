use std::fmt;
use std::sync::Arc;

use failure::Fail;

use relay_common::UnixTimestamp;
use relay_redis::{redis::Script, RedisError, RedisPool};
use sentry::protocol::value;

use crate::types::{ItemScoping, Quota, QuotaScope, RateLimit, RateLimits, RetryAfter};

/// The `grace` period allows accomodating for clock drift in TTL
/// calculation since the clock on the Redis instance used to store quota
/// metrics may not be in sync with the computer running this code.
const GRACE: u64 = 60;

/// The default timeout to apply when a scope is fully rejected. This
/// typically happens for disabled keys, projects, or organizations.
const REJECT_ALL_SECS: u64 = 60;

/// An error returned by `RateLimiter`.
#[derive(Debug, Fail)]
pub enum RateLimitingError {
    /// Failed to communicate with Redis.
    #[fail(display = "failed to communicate with redis")]
    Redis(#[cause] RedisError),
}

fn load_lua_script() -> Script {
    Script::new(include_str!("is_rate_limited.lua"))
}

fn get_refunded_quota_key(counter_key: &str) -> String {
    format!("r:{}", counter_key)
}

/// A transparent wrapper around an Option that only displays `Some`.
struct OptionalDisplay<T>(Option<T>);

impl<T> fmt::Display for OptionalDisplay<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(ref value) => write!(f, "{}", value),
            None => Ok(()),
        }
    }
}

/// Reference to information required for tracking quotas in Redis.
#[derive(Debug)]
struct RedisQuota<'a> {
    /// The original quota.
    quota: &'a Quota,
    /// Scopes of the item being tracked.
    scoping: &'a ItemScoping,
    /// The Redis key prefix mapped from the quota id.
    prefix: &'a str,
    /// The redis window in seconds mapped from the quota.
    window: u64,
    /// The ingestion timestamp determining the rate limiting bucket.
    timestamp: UnixTimestamp,
}

impl<'a> RedisQuota<'a> {
    fn new(quota: &'a Quota, scoping: &'a ItemScoping, timestamp: UnixTimestamp) -> Option<Self> {
        // These fields indicate that we *can* track this quota.
        let prefix = quota.id.as_deref()?;
        let window = quota.window?;

        Some(Self {
            quota,
            scoping,
            prefix,
            window,
            timestamp,
        })
    }

    /// Returns the limit value for Redis (`-1` for unlimited, otherwise the limit value).
    fn limit(&self) -> i64 {
        self.limit.map(i64::from).unwrap_or(-1)
    }

    fn shift(&self) -> u64 {
        self.scoping.organization_id % self.window
    }

    fn slot(&self) -> u64 {
        (self.timestamp.as_secs() - self.shift()) / self.window
    }

    fn expiry(&self) -> UnixTimestamp {
        let next_slot = self.slot() + 1;
        let next_start = next_slot * self.window + self.shift();
        UnixTimestamp::from_secs(next_start + GRACE)
    }

    fn key(&self) -> String {
        // The subscope id is only formatted into the key if the quota is not organization-scoped.
        // The organization id is always included.
        let subscope = match self.quota.scope {
            QuotaScope::Organization => None,
            scope => self.scoping.scope_id(scope),
        };

        format!(
            "quota:{id}{{{org}}}{subscope}:{slot}",
            id = self.prefix,
            org = self.scoping.organization_id,
            subscope = OptionalDisplay(subscope),
            slot = self.slot(),
        )
    }
}

impl std::ops::Deref for RedisQuota<'_> {
    type Target = Quota;

    fn deref(&self) -> &Self::Target {
        self.quota
    }
}

/// A service that executes quotas and checks for rate limits in a shared cache.
///
/// Quotas handle tracking a project's usage and respond whether or not a project has been
/// configured to throttle incoming data if they go beyond the specified quota.
///
/// Quotas can specify a window to be tracked in, such as per minute or per hour. Additionally,
/// quotas allow to specify the data categories they apply to, for example error events or
/// attachments. For more information on quota parameters, see `QuotaConfig`.
///
/// Requires the `rate-limiter` feature.
#[derive(Clone)]
pub struct RateLimiter {
    pool: RedisPool,
    script: Arc<Script>,
    max_limit: Option<u64>,
}

impl RateLimiter {
    /// Creates a new `RateLimiter` instance.
    pub fn new(pool: RedisPool) -> Self {
        RateLimiter {
            pool,
            script: Arc::new(load_lua_script()),
            max_limit: None,
        }
    }

    /// Sets the maximum rate limit in seconds.
    ///
    /// By default, this rate limiter will return rate limits based on the quotas' `window` fields.
    /// If a maximum rate limit is set, this limit is bounded.
    pub fn max_limit(mut self, max_limit: Option<u64>) -> Self {
        self.max_limit = max_limit;
        self
    }

    /// Checks whether any of the quotas in effect for the given project and project key has been
    /// exceeded and records consumption of the quota.
    ///
    /// By invoking this method, the caller signals that data is being ingested and needs to be
    /// counted against the quota. This increment happens atomically if none of the quotas have been
    /// exceeded. Otherwise, a rate limit is returned and data is not counted against the quotas.
    ///
    /// If no key is specified, then only organization-wide and project-wide quotas are checked. If
    /// a key is specified, then key-quotas are also checked.
    pub fn is_rate_limited(
        &self,
        quotas: &[Quota],
        scoping: &ItemScoping,
    ) -> Result<RateLimits, RateLimitingError> {
        let timestamp = UnixTimestamp::now();

        let mut invocation = self.script.prepare_invoke();
        let mut tracked_quotas = Vec::new();
        let mut rate_limits = RateLimits::new();

        for quota in quotas {
            if !quota.matches(scoping) {
                // Silently skip all quotas that do not apply to this item.
            } else if quota.limit == Some(0) {
                // A zero-sized quota is strongest. Do not call into Redis at all, and do not
                // increment any keys, as one quota has reached capacity (this is how regular quotas
                // behave as well).
                let retry_after = self.retry_after(REJECT_ALL_SECS);
                rate_limits.add(RateLimit::from_quota(quota, scoping, retry_after));
            } else if let Some(quota) = RedisQuota::new(quota, scoping, timestamp) {
                // Remaining quotas are expected to be trackable in Redis.
                let key = quota.key();
                let refund_key = get_refunded_quota_key(&key);

                invocation.key(key);
                invocation.key(refund_key);

                invocation.arg(quota.limit());
                invocation.arg(quota.expiry().as_secs());

                tracked_quotas.push(quota);
            } else {
                // This quota is neither a static reject-all, nor can it be tracked in Redis due to
                // missing fields. We're skipping this for forward-compatibility.
                sentry::with_scope(
                    |scope| scope.set_extra("quota", value::to_value(quota).unwrap()),
                    || log::warn!("skipping unsupported quota"),
                )
            }
        }

        // Either there are no quotas to run against Redis, or we already have a rate limit from a
        // zero-sized quota. In either cases, skip invoking the script and return early.
        if tracked_quotas.is_empty() || rate_limits.is_limited() {
            return Ok(rate_limits);
        }

        let mut client = self.pool.client().map_err(RateLimitingError::Redis)?;
        let rejections: Vec<bool> = invocation
            .invoke(&mut client.connection())
            .map_err(RedisError::Redis)
            .map_err(RateLimitingError::Redis)?;

        for (quota, is_rejected) in tracked_quotas.iter().zip(rejections) {
            if is_rejected {
                let retry_after = self.retry_after((quota.expiry() - timestamp).as_secs());
                rate_limits.add(RateLimit::from_quota(&*quota, scoping, retry_after));
            }
        }

        Ok(rate_limits)
    }

    /// Creates a rate limit bounded by `max_limit`.
    fn retry_after(&self, mut seconds: u64) -> RetryAfter {
        if let Some(max_limit) = self.max_limit {
            seconds = std::cmp::min(seconds, max_limit);
        }

        RetryAfter::from_secs(seconds)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use relay_common::ProjectId;
    use relay_redis::redis::Commands;

    use crate::types::{DataCategories, DataCategory, RateLimitScope, ReasonCode};

    use super::*;

    lazy_static::lazy_static! {
        static ref RATE_LIMITER: RateLimiter = RateLimiter {
            pool: RedisPool::single("redis://127.0.0.1").unwrap(),
            script: Arc::new(load_lua_script()),
            max_limit: None,
        };
    }

    #[test]
    fn test_zero_size_quotas() {
        let quotas = &[
            Quota {
                id: None,
                categories: DataCategories::new(),
                scope: QuotaScope::Organization,
                scope_id: None,
                limit: Some(0),
                window: None,
                reason_code: Some(ReasonCode::new("get_lost")),
            },
            Quota {
                id: Some("42".to_owned()),
                categories: DataCategories::new(),
                scope: QuotaScope::Organization,
                scope_id: None,
                limit: None,
                window: Some(42),
                reason_code: Some(ReasonCode::new("unlimited")),
            },
        ];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            organization_id: 42,
            project_id: ProjectId::new(43),
            public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
            key_id: Some(44),
        };

        let rate_limits: Vec<RateLimit> = RATE_LIMITER
            .is_rate_limited(quotas, &scoping)
            .expect("rate limiting failed")
            .into_iter()
            .collect();

        assert_eq!(
            rate_limits,
            vec![RateLimit {
                categories: DataCategories::new(),
                scope: RateLimitScope::Organization(42),
                reason_code: Some(ReasonCode::new("get_lost")),
                retry_after: rate_limits[0].retry_after,
            }]
        );
    }

    #[test]
    fn test_simple_quota() {
        let quotas = &[Quota {
            id: Some(format!("test_simple_quota_{:?}", SystemTime::now())),
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: Some(5),
            window: Some(60),
            reason_code: Some(ReasonCode::new("get_lost")),
        }];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            organization_id: 42,
            project_id: ProjectId::new(43),
            public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
            key_id: Some(44),
        };

        for i in 0..10 {
            let rate_limits: Vec<RateLimit> = RATE_LIMITER
                .is_rate_limited(quotas, &scoping)
                .expect("rate limiting failed")
                .into_iter()
                .collect();

            if i >= 5 {
                assert_eq!(
                    rate_limits,
                    vec![RateLimit {
                        categories: DataCategories::new(),
                        scope: RateLimitScope::Organization(42),
                        reason_code: Some(ReasonCode::new("get_lost")),
                        retry_after: rate_limits[0].retry_after,
                    }]
                );
            } else {
                assert_eq!(rate_limits, vec![]);
            }
        }
    }

    #[test]
    fn test_bails_immediately_without_any_quota() {
        let scoping = ItemScoping {
            category: DataCategory::Error,
            organization_id: 42,
            project_id: ProjectId::new(43),
            public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
            key_id: Some(44),
        };

        let rate_limits: Vec<RateLimit> = RATE_LIMITER
            .is_rate_limited(&[], &scoping)
            .expect("rate limiting failed")
            .into_iter()
            .collect();

        assert_eq!(rate_limits, vec![]);
    }

    #[test]
    fn test_limited_with_unlimited_quota() {
        let quotas = &[
            Quota {
                id: Some("q0".to_string()),
                categories: DataCategories::new(),
                scope: QuotaScope::Organization,
                scope_id: None,
                limit: None,
                window: Some(1),
                reason_code: Some(ReasonCode::new("project_quota0")),
            },
            Quota {
                id: Some("q1".to_string()),
                categories: DataCategories::new(),
                scope: QuotaScope::Organization,
                scope_id: None,
                limit: Some(1),
                window: Some(1),
                reason_code: Some(ReasonCode::new("project_quota1")),
            },
        ];

        let scoping = ItemScoping {
            category: DataCategory::Error,
            organization_id: 42,
            project_id: ProjectId::new(43),
            public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
            key_id: Some(44),
        };

        for i in 0..1 {
            let rate_limits: Vec<RateLimit> = RATE_LIMITER
                .is_rate_limited(quotas, &scoping)
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
                        scope: RateLimitScope::Organization(42),
                        reason_code: Some(ReasonCode::new("project_quota1")),
                        retry_after: rate_limits[0].retry_after,
                    }]
                );
            }
        }
    }

    #[test]
    fn test_get_redis_key_scoped() {
        let quota = Quota {
            id: Some("foo".to_owned()),
            categories: DataCategories::new(),
            scope: QuotaScope::Project,
            scope_id: Some("42".to_owned()),
            window: Some(2),
            limit: Some(0),
            reason_code: None,
        };

        let scoping = ItemScoping {
            category: DataCategory::Error,
            organization_id: 69420,
            project_id: ProjectId::new(42),
            public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
            key_id: Some(4711),
        };

        let timestamp = UnixTimestamp::from_secs(123_123_123);
        let redis_quota = RedisQuota::new(&quota, &scoping, timestamp).unwrap();
        assert_eq!(redis_quota.key(), "quota:foo{69420}42:61561561");
    }

    #[test]
    fn test_get_redis_key_unscoped() {
        let quota = Quota {
            id: Some("foo".to_owned()),
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            window: Some(10),
            limit: Some(0),
            reason_code: None,
        };

        let scoping = ItemScoping {
            category: DataCategory::Error,
            organization_id: 69420,
            project_id: ProjectId::new(42),
            public_key: "a94ae32be2584e0bbd7a4cbb95971fee".to_owned(),
            key_id: Some(4711),
        };

        let timestamp = UnixTimestamp::from_secs(234_531);
        let redis_quota = RedisQuota::new(&quota, &scoping, timestamp).unwrap();
        assert_eq!(redis_quota.key(), "quota:foo{69420}:23453");
    }

    #[test]
    #[allow(clippy::blacklisted_name, clippy::let_unit_value)]
    fn test_is_rate_limited_script() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap();

        let mut client = RATE_LIMITER.pool.client().expect("get client");
        let mut conn = client.connection();

        // define a few keys with random seed such that they do not collide with repeated test runs
        let foo = format!("foo___{}", now);
        let r_foo = format!("r:foo___{}", now);
        let bar = format!("bar___{}", now);
        let r_bar = format!("r:bar___{}", now);
        let apple = format!("apple___{}", now);
        let orange = format!("orange___{}", now);
        let baz = format!("baz___{}", now);

        let script = load_lua_script();

        let mut invocation = script.prepare_invoke();
        invocation
            .key(&foo)
            .key(&r_foo)
            .key(&bar)
            .key(&r_bar)
            .arg(1)
            .arg(now + 60)
            .arg(2)
            .arg(now + 120);

        // The item should not be rate limited by either key.
        assert_eq!(
            invocation.invoke::<Vec<bool>>(&mut conn).unwrap(),
            vec![false, false]
        );

        // The item should be rate limited by the first key (1).
        assert_eq!(
            invocation.invoke::<Vec<bool>>(&mut conn).unwrap(),
            vec![true, false]
        );

        // The item should still be rate limited by the first key (1), but *not*
        // rate limited by the second key (2) even though this is the third time
        // we've checked the quotas. This ensures items that are rejected by a lower
        // quota don't affect unrelated items that share a parent quota.
        assert_eq!(
            invocation.invoke::<Vec<bool>>(&mut conn).unwrap(),
            vec![true, false]
        );

        assert_eq!(conn.get::<_, String>(&foo).unwrap(), "1");
        let ttl: u64 = conn.ttl(&foo).unwrap();
        assert!(ttl >= 59);
        assert!(ttl <= 60);

        assert_eq!(conn.get::<_, String>(&bar).unwrap(), "1");
        let ttl: u64 = conn.ttl(&bar).unwrap();
        assert!(ttl >= 119);
        assert!(ttl <= 120);

        // make sure "refund/negative" keys haven't been incremented
        let () = conn.get(r_foo).unwrap();
        let () = conn.get(r_bar).unwrap();

        // Test that refunded quotas work
        let () = conn.set(&apple, 5).unwrap();

        let mut invocation = script.prepare_invoke();
        invocation.key(&orange).key(&baz).arg(1).arg(now + 60);

        // increment
        assert_eq!(
            invocation.invoke::<Vec<bool>>(&mut conn).unwrap(),
            vec![false]
        );

        // test that it's rate limited without refund
        assert_eq!(
            invocation.invoke::<Vec<bool>>(&mut conn).unwrap(),
            vec![true]
        );

        let mut invocation = script.prepare_invoke();
        invocation.key(&orange).key(&apple).arg(1).arg(now + 60);

        // test that refund key is used
        assert_eq!(
            invocation.invoke::<Vec<bool>>(&mut conn).unwrap(),
            vec![false]
        );
    }
}
