use std::{collections::HashSet, time::SystemTime};

use itertools::Itertools;
use relay_redis::{
    redis::{self, ToRedisArgs},
    Connection, RedisPool,
};
use relay_statsd::metric;

use crate::{
    limiter::{CardinalityScope, Entry, EntryId, Limiter, Rejection},
    statsd::CardinalityLimiterCounters,
    OrganizationId, Result,
};

/// Key prefix used for Redis keys.
const KEY_PREFIX: &str = "relay:cardinality";
/// Maximum amount of arguments for a Redis `SADD` call.
const MAX_SADD_ARGS: usize = 1000;

/// Implementation uses Redis sets to keep track of cardinality.
pub struct RedisSetLimiter {
    redis: RedisPool,
    window: SlidingWindow,
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
            #[cfg(test)]
            time_offset: 0,
        }
    }

    /// Checks the limits for a specific scope.
    ///
    /// Returns an iterator over all entries which have been accepted.
    fn check_limits(
        &self,
        client: &mut RedisHelper,
        organization_id: OrganizationId,
        item_scope: String,
        mut entries: Vec<RedisEntry>,
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

        let active_window = client.read_set(&self.to_redis_key(
            organization_id,
            &item_scope,
            self.window.active_time_bucket(timestamp),
        ))?;
        let mut new_hashes = HashSet::new();

        let mut new_cardinality = active_window.len();
        let mut rejected = 0;

        for entry in &mut entries {
            if active_window.contains(&entry.hash) {
                entry.accept();
            } else if new_cardinality < limit {
                entry.accept();
                new_hashes.insert(entry.hash);
                new_cardinality += 1;
            } else {
                entry.reject();
                rejected += 1;
            }
        }

        if rejected > 0 {
            relay_log::debug!(
                organization_id = organization_id,
                scope = &item_scope,
                "rejected {rejected} metrics due to cardinality limit"
            );
            metric!(
                counter(CardinalityLimiterCounters::Rejected) += rejected,
                scope = &item_scope,
            );
        }

        if !new_hashes.is_empty() {
            for time_bucket in self.window.iter(timestamp) {
                let key = self.to_redis_key(organization_id, &item_scope, time_bucket);

                // The expiry is a off by `window.granularity_seconds`,
                // but since this is only used for cleanup, this is not an issue.
                client.add_to_set(&key, &new_hashes, self.window.window_seconds)?;
            }

            metric!(
                counter(CardinalityLimiterCounters::RedisHashUpdate) += new_hashes.len() as i64,
                scope = &item_scope,
            );
        }

        Ok(entries.into_iter().filter_map(|entry| entry.rejection()))
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

/// Status wether an entry/bucket is accepted or rejected by the cardinality limiter.
#[derive(Clone, Copy, Debug)]
enum Status {
    /// Item is rejected.
    Rejected,
    /// Item is accepted.
    Accepted,
}

#[derive(Clone, Copy, Debug)]
struct RedisEntry {
    /// The correlating entry id.
    id: EntryId,
    /// The entry hash.
    hash: u32,
    /// The current status.
    status: Status,
}

impl RedisEntry {
    /// Creates a new Redis entry in the rejected status.
    fn new(id: EntryId, hash: u32) -> Self {
        Self {
            id,
            hash,
            status: Status::Rejected,
        }
    }

    /// Marks the entry as accepted.
    fn accept(&mut self) {
        self.status = Status::Accepted;
    }

    /// Marks the entry as rejected.
    fn reject(&mut self) {
        self.status = Status::Rejected;
    }

    /// Turns the item into a rejection when the status is rejected.
    fn rejection(self) -> Option<Rejection> {
        matches!(self.status, Status::Rejected).then_some(Rejection { id: self.id })
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
        let mut client = RedisHelper(client.connection()?);

        let mut rejected = Vec::new();
        for ((organization_id, item_scope), entries) in entries {
            // dbg!((organization_id, item_scope.to_string()), &entries);
            rejected.push(self.check_limits(
                &mut client,
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

/// Internal Redis client wrapper to abstract the issuing of commands.
struct RedisHelper<'a>(Connection<'a>);

impl<'a> RedisHelper<'a> {
    fn read_set(&mut self, name: &str) -> Result<HashSet<u32>> {
        let result = redis::cmd("SMEMBERS")
            .arg(name)
            .query(&mut self.0)
            .map_err(relay_redis::RedisError::Redis)?;

        Ok(result)
    }

    fn add_to_set<I: ToRedisArgs + std::fmt::Debug>(
        &mut self,
        name: &str,
        values: impl IntoIterator<Item = I>,
        expire: u64,
    ) -> Result<()> {
        let mut pipeline = redis::pipe();

        for values in &values.into_iter().chunks(MAX_SADD_ARGS) {
            let cmd = pipeline.cmd("SADD").arg(name);

            for arg in values {
                cmd.arg(arg);
            }

            cmd.ignore();
        }

        pipeline
            .cmd("EXPIRE")
            .arg(name)
            .arg(expire)
            // .arg("NX") - NX not supported for Redis < 7
            .ignore()
            .query(&mut self.0)
            .map_err(relay_redis::RedisError::Redis)?;

        Ok(())
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
    use std::sync::atomic::AtomicU64;

    use relay_metrics::MetricNamespace;
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
            Entry::new(EntryId(0), MetricNamespace::Custom, 0),
            Entry::new(EntryId(1), MetricNamespace::Custom, 1),
            Entry::new(EntryId(2), MetricNamespace::Custom, 2),
            Entry::new(EntryId(3), MetricNamespace::Custom, 3),
            Entry::new(EntryId(4), MetricNamespace::Custom, 4),
            Entry::new(EntryId(5), MetricNamespace::Custom, 5),
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
    fn test_limiter_big_limit() {
        let limiter = build_limiter();

        let org = new_org(&limiter);
        let entries = (0..100_000)
            .map(|i| Entry::new(EntryId(i as usize), MetricNamespace::Custom, i))
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
        let entries1 = [Entry::new(EntryId(0), MetricNamespace::Custom, 0)];
        let entries2 = [Entry::new(EntryId(1), MetricNamespace::Custom, 1)];

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
