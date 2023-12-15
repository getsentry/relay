use std::time::SystemTime;

use itertools::Itertools;
use relay_redis::{
    redis::{self, FromRedisValue, Script},
    Connection, RedisPool,
};
use relay_statsd::metric;

use crate::{
    limiter::{CardinalityScope, Entry, EntryId, Limiter, Rejection},
    statsd::{CardinalityLimiterCounters, CardinalityLimiterHistograms},
    OrganizationId, Result,
};

/// Key prefix used for Redis keys.
const KEY_PREFIX: &str = "relay:cardinality";

struct CardinalityScript(Script);

/// Status wether an entry/bucket is accepted or rejected by the cardinality limiter.
#[derive(Clone, Copy, Debug)]
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
    rejected: usize,
    stati: Vec<Status>,
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

        let mut stati = Vec::with_capacity(iter.len());
        let mut rejected = 0;
        for value in iter {
            let status = Status::from_redis_value(value)?;
            if status.is_rejected() {
                rejected += 1;
            }
            stati.push(status);
        }

        Ok(Self {
            cardinality,
            rejected,
            stati,
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
        keys: impl Iterator<Item = String>,
        hashes: impl Iterator<Item = u32>,
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

        if num_hashes != result.stati.len() {
            return Err(relay_redis::RedisError::Redis(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Script returned an invalid number of elements",
                format!("Expected {num_hashes} results, got {}", result.stati.len()),
            )))
            .into());
        }

        Ok(result)
    }
}

/// Implementation uses Redis sets to keep track of cardinality.
pub struct RedisSetLimiter {
    redis: RedisPool,
    window: SlidingWindow,
    script: CardinalityScript,
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
    ) -> Result<impl Iterator<Item = Rejection>> {
        metric!(
            counter(CardinalityLimiterCounters::RedisRead) += 1,
            scope = &item_scope,
        );
        metric!(
            counter(CardinalityLimiterCounters::RedisHashCheck) += entries.len() as i64,
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
            .invoke(con, limit, self.window.window_seconds, keys, hashes)?;

        metric!(
            histogram(CardinalityLimiterHistograms::RedisSetCardinality) = result.cardinality,
            scope = &item_scope,
        );

        if result.rejected > 0 {
            relay_log::debug!(
                organization_id = organization_id,
                scope = &item_scope,
                "rejected {} metrics due to cardinality limit",
                result.rejected,
            );
            metric!(
                counter(CardinalityLimiterCounters::Rejected) += result.rejected as i64,
                scope = &item_scope,
            );
        }

        let result = std::iter::zip(entries, result.stati)
            .filter_map(|(entry, status)| status.is_rejected().then_some(entry.rejection()));

        Ok(result)
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
        let entries = entries
            .into_iter()
            .map(|entry| {
                let Entry { id, scope, hash } = entry;

                let key = (organization_id, scope);
                let entry = RedisEntry::new(id, hash);

                (key, entry)
            })
            .into_group_map();

        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Allows to fast forward time in tests.
        #[cfg(test)]
        let timestamp = timestamp + self.time_offset;

        let mut client = self.redis.client()?;
        let mut connection = client.connection()?;

        let mut rejected = Vec::new();
        for ((organization_id, item_scope), entries) in entries {
            rejected.push(self.check_limits(
                &mut connection,
                organization_id,
                item_scope.to_string(),
                entries,
                timestamp,
                limit,
            )?);
        }

        Ok(Box::new(rejected.into_iter().flatten()))
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
