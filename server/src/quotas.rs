use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use failure::Fail;
use r2d2::Pool;

use crate::actors::project::{Quota, RetryAfter};

/// The ``grace`` period allows accomodating for clock drift in TTL
/// calculation since the clock on the Redis instance used to store quota
/// metrics may not be in sync with the computer running this code.
static GRACE: u64 = 60;

#[derive(Debug, Clone, Copy)]
struct UnixTimestamp(u64);

#[derive(Clone)]
pub enum RedisPool {
    Cluster(Pool<redis::cluster::ClusterClient>),
    Single(Pool<redis::Client>),
}

impl RedisPool {
    pub fn cluster(servers: Vec<&str>) -> Result<Self, QuotasError> {
        Ok(RedisPool::Cluster(
            Pool::builder()
                .max_size(24)
                .build(redis::cluster::ClusterClient::open(servers).map_err(QuotasError::Redis)?)
                .map_err(QuotasError::RedisPool)?,
        ))
    }

    pub fn single(server: &str) -> Result<Self, QuotasError> {
        Ok(RedisPool::Single(
            Pool::builder()
                .max_size(24)
                .build(redis::Client::open(server).map_err(QuotasError::Redis)?)
                .map_err(QuotasError::RedisPool)?,
        ))
    }
}

#[derive(Debug, Fail)]
pub enum QuotasError {
    #[fail(display = "failed to connect to redis")]
    RedisPool(#[cause] r2d2::Error),

    #[fail(display = "failed to talk to redis")]
    Redis(#[cause] redis::RedisError),
}

lazy_static::lazy_static! {
    static ref IS_RATE_LIMITED: redis::Script = redis::Script::new(include_str!("is_rate_limited.lua"));
}

pub fn is_rate_limited(
    redis_pool: &RedisPool,
    quotas: &[Quota],
    organization_id: u64,
) -> Result<Option<RetryAfter>, QuotasError> {
    let timestamp = UnixTimestamp(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .as_ref()
            .map(Duration::as_secs)
            .unwrap_or(0),
    );

    let mut invocation = IS_RATE_LIMITED.prepare_invoke();
    let mut empty_invocation = true;

    for quota in quotas {
        if quota.limit == Some(0) {
            // A zero-sized quota is the absolute worst-case. Do not call
            // into Redis at all, and do not increment any keys, as one
            // quota has reached capacity (this is how regular quotas behave
            // as well).

            debug_assert!(quota.window.is_none());
            debug_assert!(!quota.should_track());
            return Ok(Some(RetryAfter {
                when: Instant::now() + Duration::from_secs(60),
                reason_code: quota.reason_code.clone(),
            }));
        }

        debug_assert!(quota.should_track());

        empty_invocation = false;

        let shift = organization_id % quota.window.unwrap();

        let key = get_redis_key(quota, timestamp, shift, organization_id);
        let return_key = get_refunded_quota_key(&key);
        invocation.key(key);
        invocation.key(return_key);

        let expiry = get_next_period_start(quota, timestamp, shift);
        let lua_quota = quota.limit.map(|x| x as i64).unwrap_or(-1);
        invocation.arg(lua_quota);
        invocation.arg(expiry.0);
    }

    if empty_invocation {
        return Ok(None);
    }

    let rejections: Vec<bool> = match redis_pool {
        RedisPool::Cluster(ref pool) => {
            let mut client = pool.get().map_err(QuotasError::RedisPool)?;
            invocation
                .invoke(&mut *client)
                .map_err(QuotasError::Redis)?
        }
        RedisPool::Single(ref pool) => {
            let mut client = pool.get().map_err(QuotasError::RedisPool)?;
            invocation
                .invoke(&mut *client)
                .map_err(QuotasError::Redis)?
        }
    };

    let mut worst_case = None;

    for (quota, is_rejected) in quotas.iter().zip(rejections) {
        if !is_rejected {
            continue;
        }

        let shift = organization_id % quota.window.unwrap();
        let delay = get_next_period_start(quota, timestamp, shift).0 - timestamp.0;

        if worst_case
            .as_ref()
            .map(|(worst_delay, _)| delay > *worst_delay)
            .unwrap_or(true)
        {
            worst_case = Some((delay, quota.reason_code.clone()));
        }
    }

    Ok(worst_case.map(|(delay, reason_code)| RetryAfter {
        when: Instant::now() + Duration::from_secs(delay),
        reason_code,
    }))
}

fn get_redis_key(
    quota: &Quota,
    timestamp: UnixTimestamp,
    shift: u64,
    organization_id: u64,
) -> String {
    format!(
        "quota:{}{{{}}}{}:{}",
        quota.prefix.as_ref().unwrap(),
        organization_id,
        quota.subscope.as_ref().map(String::as_str).unwrap_or(""),
        (timestamp.0 - shift) / quota.window.unwrap()
    )
}

fn get_refunded_quota_key(counter_key: &str) -> String {
    format!("r:{}", counter_key)
}

fn get_next_period_start(quota: &Quota, timestamp: UnixTimestamp, shift: u64) -> UnixTimestamp {
    let interval = quota.window.unwrap();
    let current_period = (timestamp.0 - shift) / interval;
    UnixTimestamp((current_period + 1) * interval + shift + GRACE)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use redis::Commands;

    use super::{is_rate_limited, Quota, RedisPool, IS_RATE_LIMITED};

    lazy_static::lazy_static! {
        static ref REDIS_CONN: RedisPool = RedisPool::single("redis://127.0.0.1").unwrap();
    }

    #[test]
    fn test_zero_size_quotas() {
        let retry_after = is_rate_limited(
            &*REDIS_CONN,
            &[
                Quota {
                    limit: Some(0),
                    reason_code: Some("get_lost".to_owned()),
                    ..Default::default()
                },
                Quota {
                    limit: None,
                    reason_code: Some("unlimited".to_owned()),
                    ..Default::default()
                },
                Quota {
                    limit: Some(42),
                    reason_code: Some("unlimited".to_owned()),
                    ..Default::default()
                },
            ],
            42,
        )
        .unwrap()
        .expect("expected to get a rate limit");

        assert_eq!(retry_after.reason_code, Some("get_lost".to_owned()));
    }

    #[test]
    fn test_simple_quota() {
        let prefix = Some(format!("test_simple_quota_{:?}", SystemTime::now()));
        for i in 0..10 {
            let retry_after = is_rate_limited(
                &*REDIS_CONN,
                &[Quota {
                    prefix: prefix.clone(),
                    limit: Some(5),
                    window: Some(60),
                    reason_code: Some("get_lost".to_owned()),
                    ..Default::default()
                }][..],
                42,
            )
            .unwrap();

            if i >= 5 {
                let retry_after = retry_after.expect("expected to get a rate limit");
                assert_eq!(retry_after.reason_code, Some("get_lost".to_owned()));
            } else {
                assert!(retry_after.is_none());
            }
        }
    }

    #[test]
    fn test_is_rate_limited_script() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .as_ref()
            .map(Duration::as_secs)
            .unwrap();

        let conn_guard = match *REDIS_CONN {
            RedisPool::Single(ref conn) => conn,
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

        let mut invocation = IS_RATE_LIMITED.prepare_invoke();
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

        let mut invocation = IS_RATE_LIMITED.prepare_invoke();
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

        let mut invocation = IS_RATE_LIMITED.prepare_invoke();
        invocation.key(&orange).key(&apple).arg(1).arg(now + 60);

        // test that refund key is used
        assert_eq!(
            invocation.invoke::<Vec<bool>>(&mut *conn).unwrap(),
            vec![false]
        );
    }

    #[test]
    fn test_bails_immediately_without_any_quota() {
        assert_eq!(is_rate_limited(&*REDIS_CONN, &[], 0).unwrap(), None);
    }

    #[test]
    fn test_limited_with_unlimited_quota() {
        for i in 0..1 {
            let result = is_rate_limited(
                &*REDIS_CONN,
                &[
                    Quota {
                        prefix: Some("test_limited_with_unlimited_quota".to_string()),
                        subscope: Some("1".to_owned()),
                        limit: None,
                        window: Some(1),
                        reason_code: Some("project_quota0".to_owned()),
                    },
                    Quota {
                        prefix: Some("test_limited_with_unlimited_quota".to_string()),
                        subscope: Some("2".to_owned()),
                        limit: Some(1),
                        window: Some(1),
                        reason_code: Some("project_quota1".to_owned()),
                    },
                ],
                0,
            )
            .unwrap();

            if i == 0 {
                assert_eq!(result, None);
            } else {
                let result = result.unwrap();
                assert_eq!(result.reason_code, Some("project_quota1".to_owned()));
            }
        }
    }
}
