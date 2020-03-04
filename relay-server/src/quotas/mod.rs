use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use failure::Fail;

use relay_config::Config;

use crate::actors::project::{Quota, RedisQuota, RejectAllQuota, RetryAfter};
use crate::utils::{RedisError, RedisPool};

/// The `grace` period allows accomodating for clock drift in TTL
/// calculation since the clock on the Redis instance used to store quota
/// metrics may not be in sync with the computer running this code.
const GRACE: u64 = 60;

/// The default timeout to apply when a scope is fully rejected. This
/// typically happens for disabled keys, projects, or organizations.
const REJECT_ALL_SECS: u64 = 60;

#[derive(Debug, Fail)]
pub enum QuotasError {
    #[fail(display = "failed to talk to redis")]
    Redis(#[cause] crate::utils::RedisError),
}

#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
struct UnixTimestamp(u64);

fn load_lua_script() -> redis::Script {
    redis::Script::new(include_str!("is_rate_limited.lua"))
}

#[derive(Clone)]
pub struct RateLimiter {
    config: Arc<Config>,
    pool: RedisPool,
    script: Arc<redis::Script>,
}

impl RateLimiter {
    pub fn new(config: Arc<Config>, pool: RedisPool) -> Self {
        RateLimiter {
            config,
            pool,
            script: Arc::new(load_lua_script()),
        }
    }

    pub fn is_rate_limited(
        &self,
        quotas: &[Quota],
        organization_id: u64,
    ) -> Result<Option<RetryAfter>, QuotasError> {
        let timestamp = UnixTimestamp(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );

        let mut invocation = self.script.prepare_invoke();
        let mut redis_quotas = Vec::new();

        for quota in quotas {
            match *quota {
                Quota::RejectAll(RejectAllQuota { ref reason_code }) => {
                    // A zero-sized quota is the absolute worst-case. Do not call
                    // into Redis at all, and do not increment any keys, as one
                    // quota has reached capacity (this is how regular quotas behave
                    // as well).
                    return Ok(Some(self.retry_after(REJECT_ALL_SECS, reason_code.clone())));
                }
                Quota::Redis(ref redis_quota) => {
                    let shift = organization_id % redis_quota.window;

                    let key = get_redis_key(redis_quota, timestamp, shift, organization_id);
                    let return_key = get_refunded_quota_key(&key);
                    invocation.key(key);
                    invocation.key(return_key);

                    let expiry = get_next_period_start(redis_quota, timestamp, shift);
                    let lua_quota = redis_quota.limit.map(|x| x as i64).unwrap_or(-1);
                    invocation.arg(lua_quota);
                    invocation.arg(expiry.0);
                    redis_quotas.push(redis_quota);
                }
            }
        }

        if redis_quotas.is_empty() {
            return Ok(None);
        }

        let rejections: Vec<bool> = match self.pool {
            RedisPool::Cluster(ref pool) => {
                let mut client = pool
                    .get()
                    .map_err(RedisError::RedisPool)
                    .map_err(QuotasError::Redis)?;
                invocation
                    .invoke(&mut *client)
                    .map_err(RedisError::Redis)
                    .map_err(QuotasError::Redis)?
            }
            RedisPool::Single(ref pool) => {
                let mut client = pool
                    .get()
                    .map_err(RedisError::RedisPool)
                    .map_err(QuotasError::Redis)?;
                invocation
                    .invoke(&mut *client)
                    .map_err(RedisError::Redis)
                    .map_err(QuotasError::Redis)?
            }
        };

        let mut worst_case = None;

        for (quota, is_rejected) in redis_quotas.into_iter().zip(rejections) {
            if !is_rejected {
                continue;
            }

            let shift = organization_id % quota.window;
            let delay = get_next_period_start(quota, timestamp, shift).0 - timestamp.0;

            if worst_case
                .as_ref()
                .map_or(true, |(worst_delay, _)| delay > *worst_delay)
            {
                worst_case = Some((delay, quota.reason_code.clone()));
            }
        }

        Ok(worst_case.map(|(delay, reason_code)| self.retry_after(delay, reason_code)))
    }

    /// Creates a rate limit bounded by `config.max_rate_limit`.
    fn retry_after(&self, mut seconds: u64, reason_code: Option<String>) -> RetryAfter {
        if let Some(max_rate_limit) = self.config.max_rate_limit() {
            seconds = std::cmp::min(seconds, max_rate_limit);
        }

        RetryAfter::with_reason(seconds, reason_code)
    }
}

fn get_redis_key(
    quota: &RedisQuota,
    timestamp: UnixTimestamp,
    shift: u64,
    organization_id: u64,
) -> String {
    format!(
        "quota:{}{{{}}}{}:{}",
        quota.prefix,
        organization_id,
        quota.subscope.as_deref().unwrap_or(""),
        (timestamp.0 - shift) / quota.window
    )
}

fn get_refunded_quota_key(counter_key: &str) -> String {
    format!("r:{}", counter_key)
}

fn get_next_period_start(
    quota: &RedisQuota,
    timestamp: UnixTimestamp,
    shift: u64,
) -> UnixTimestamp {
    let interval = quota.window;
    let current_period = (timestamp.0 - shift) / interval;
    UnixTimestamp((current_period + 1) * interval + shift + GRACE)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use redis::Commands;

    use super::*;

    lazy_static::lazy_static! {
        static ref RATE_LIMITER: RateLimiter = RateLimiter {
            config: Arc::default(),
            pool: RedisPool::single("redis://127.0.0.1").unwrap(),
            script: Arc::new(load_lua_script())
        };
    }

    #[test]
    fn test_zero_size_quotas() {
        let quotas = &[
            Quota::RejectAll(RejectAllQuota {
                reason_code: Some("get_lost".to_owned()),
            }),
            Quota::Redis(RedisQuota {
                limit: None,
                reason_code: Some("unlimited".to_owned()),
                window: 42,
                prefix: "42".to_owned(),
                subscope: None,
            }),
        ];

        let retry_after = RATE_LIMITER
            .is_rate_limited(quotas, 42)
            .expect("rate limiting failed")
            .expect("expected to get a rate limit");

        assert_eq!(retry_after.reason_code(), Some("get_lost"));
    }

    #[test]
    fn test_simple_quota() {
        let quotas = &[Quota::Redis(RedisQuota {
            prefix: format!("test_simple_quota_{:?}", SystemTime::now()),
            limit: Some(5),
            window: 60,
            reason_code: Some("get_lost".to_owned()),
            subscope: None,
        })];

        for i in 0..10 {
            let retry_after = RATE_LIMITER
                .is_rate_limited(quotas, 42)
                .expect("rate limiting failed");

            if i >= 5 {
                let retry_after = retry_after.expect("expected to get a rate limit");
                assert_eq!(retry_after.reason_code(), Some("get_lost"));
            } else {
                assert!(retry_after.is_none());
            }
        }
    }

    #[test]
    #[allow(clippy::blacklisted_name, clippy::let_unit_value)]
    fn test_is_rate_limited_script() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap();

        let conn_guard = match &RATE_LIMITER.pool {
            RedisPool::Single(ref conn_guard) => conn_guard,
            _ => unreachable!(),
        };

        let mut conn = conn_guard.get().unwrap();

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
            invocation.invoke::<Vec<bool>>(&mut *conn).unwrap(),
            vec![false, false]
        );

        // The item should be rate limited by the first key (1).
        assert_eq!(
            invocation.invoke::<Vec<bool>>(&mut *conn).unwrap(),
            vec![true, false]
        );

        // The item should still be rate limited by the first key (1), but *not*
        // rate limited by the second key (2) even though this is the third time
        // we've checked the quotas. This ensures items that are rejected by a lower
        // quota don't affect unrelated items that share a parent quota.
        assert_eq!(
            invocation.invoke::<Vec<bool>>(&mut *conn).unwrap(),
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
            invocation.invoke::<Vec<bool>>(&mut *conn).unwrap(),
            vec![false]
        );

        // test that it's rate limited without refund
        assert_eq!(
            invocation.invoke::<Vec<bool>>(&mut *conn).unwrap(),
            vec![true]
        );

        let mut invocation = script.prepare_invoke();
        invocation.key(&orange).key(&apple).arg(1).arg(now + 60);

        // test that refund key is used
        assert_eq!(
            invocation.invoke::<Vec<bool>>(&mut *conn).unwrap(),
            vec![false]
        );
    }

    #[test]
    fn test_bails_immediately_without_any_quota() {
        let limit = RATE_LIMITER
            .is_rate_limited(&[], 0)
            .expect("rate limiting failed");

        assert_eq!(limit, None);
    }

    #[test]
    fn test_limited_with_unlimited_quota() {
        let quotas = &[
            Quota::Redis(RedisQuota {
                prefix: "test_limited_with_unlimited_quota".to_string(),
                subscope: Some("1".to_owned()),
                limit: None,
                window: 1,
                reason_code: Some("project_quota0".to_owned()),
            }),
            Quota::Redis(RedisQuota {
                prefix: "test_limited_with_unlimited_quota".to_string(),
                subscope: Some("2".to_owned()),
                limit: Some(1),
                window: 1,
                reason_code: Some("project_quota1".to_owned()),
            }),
        ];

        for i in 0..1 {
            let result = RATE_LIMITER
                .is_rate_limited(quotas, 0)
                .expect("rate limiting failed");

            if i == 0 {
                assert_eq!(result, None);
            } else {
                let result = result.unwrap();
                assert_eq!(result.reason_code(), Some("project_quota1"));
            }
        }
    }

    #[test]
    fn test_get_redis_key() {
        // These were manually checked to work the same in Python (the input values are random and
        // meaningless)

        assert_eq!(
            get_redis_key(
                &RedisQuota {
                    prefix: "foo".to_owned(),
                    subscope: Some("42".to_owned()),
                    window: 2,
                    limit: Some(0),
                    reason_code: None
                },
                UnixTimestamp(123_123_123),
                123_123,
                42
            ),
            "quota:foo{42}42:61500000"
        );

        assert_eq!(
            get_redis_key(
                &RedisQuota {
                    prefix: "foo".to_owned(),
                    subscope: None,
                    window: 10,
                    limit: Some(0),
                    reason_code: None
                },
                UnixTimestamp(234_531),
                134_451,
                69420
            ),
            "quota:foo{69420}:10008"
        );
    }
}
