use std::{collections::HashSet, time::SystemTime};

use itertools::Itertools;
use relay_metrics::MetricNamespace;
use relay_redis::{
    redis::{self, ToRedisArgs},
    Connection, RedisPool,
};
use relay_statsd::metric;

use crate::{
    limiter::{Entry, Limiter},
    statsd::CardinalityLimiterCounters,
    OrganizationId, Result,
};

/// Key prefix used for Redis keys.
const REDIS_KEY_PREFIX: &str = "relay:cardinality";
/// Maximum amount of arguments for a Redis `SADD` call.
const REDIS_MAX_SADD_ARGS: usize = 1000;

/// Scope for which cardinality will be tracked.
///
/// Currently this includes the organization and [namespace](`MetricNamespace`).
/// Eventually we want to track cardinality also on more fine grained namespaces,
/// e.g. on individual metric basis.
#[derive(Clone, Copy, Debug, Hash, PartialEq, PartialOrd, Eq, Ord)]
struct Scope {
    /// The organization.
    pub organization_id: OrganizationId,
    /// The metric namespace, extracted from the metric bucket name.
    pub namespace: MetricNamespace,
}

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
}

impl RedisSetLimiter {
    /// Checks the limits for a specific scope.
    ///
    /// Returns an iterator over all entries which have been accepted.
    fn check_limits(
        &self,
        client: &mut RedisHelper,
        scope: Scope,
        mut entries: Vec<Entry>,
        timestamp: u64,
        limit: usize,
    ) -> Result<impl Iterator<Item = Entry>> {
        metric!(
            counter(CardinalityLimiterCounters::RedisRead) += 1,
            namespace = scope.namespace.as_str()
        );
        metric!(
            counter(CardinalityLimiterCounters::RedisHashCheck) += entries.len() as i64,
            namespace = scope.namespace.as_str()
        );

        let active_window = client
            .read_set(&self.to_redis_key(&scope, self.window.active_time_bucket(timestamp)))?;
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
                organization_id = scope.organization_id,
                namespace = scope.namespace.as_str(),
                "rejected {rejected} metrics due to cardinality limit"
            );
            metric!(
                counter(CardinalityLimiterCounters::Rejected) += rejected,
                namespace = scope.namespace.as_str()
            );
        }

        if !new_hashes.is_empty() {
            for time_bucket in self.window.iter(timestamp) {
                let key = self.to_redis_key(&scope, time_bucket);

                // The expiry is a off by `window.granularity_seconds`,
                // but since this is only used for cleanup, this is not an issue.
                client.add_to_set(&key, &new_hashes, self.window.window_seconds)?;
            }

            metric!(
                counter(CardinalityLimiterCounters::RedisHashUpdate) += new_hashes.len() as i64,
                namespace = scope.namespace.as_str()
            );
        }

        Ok(entries.into_iter().filter(|entry| entry.is_accepted()))
    }

    fn to_redis_key(&self, scope: &Scope, window: u64) -> String {
        // Pattern match here, to not forget to update the key, when modifying the scope.
        let Scope {
            organization_id,
            namespace,
        } = scope;

        format!("{REDIS_KEY_PREFIX}:scope-{{{organization_id}}}-{namespace}-{window}",)
    }
}

impl Limiter for RedisSetLimiter {
    fn check_cardinality_limits<I>(
        &self,
        organization_id: u64,
        entries: I,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Entry>>>
    where
        I: IntoIterator<Item = Entry>,
    {
        let entries = entries.into_iter().into_group_map_by(|f| Scope {
            organization_id,
            namespace: f.namespace,
        });

        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Allows to fast forward time in tests.
        #[cfg(test)]
        let timestamp = timestamp + self.time_offset;

        let mut client = self.redis.client()?;
        let mut client = RedisHelper(client.connection()?);

        let mut accepted = Vec::new();
        for (scope, entries) in entries {
            accepted.push(self.check_limits(&mut client, scope, entries, timestamp, limit)?);
        }

        Ok(Box::new(accepted.into_iter().flatten()))
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
        for values in &values.into_iter().chunks(REDIS_MAX_SADD_ARGS) {
            let mut cmd = redis::cmd("SADD");
            cmd.arg(name);

            for arg in values {
                cmd.arg(arg);
            }

            cmd.query(&mut self.0)
                .map_err(relay_redis::RedisError::Redis)?;
        }

        redis::cmd("EXPIRE")
            .arg(name)
            .arg(expire)
            // .arg("NX") - NX not supported for Redis < 7
            .query(&mut self.0)
            .map_err(relay_redis::RedisError::Redis)?;

        Ok(())
    }
}

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
            let pattern = format!("{REDIS_KEY_PREFIX}:scope-{{{organization_id}}}-*");

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

        let accepted = limiter
            .check_cardinality_limits(org, entries, 5)
            .unwrap()
            .map(|e| e.id)
            .collect::<HashSet<_>>();
        assert_eq!(accepted.len(), 5);

        // We're at the limit but it should still accept already accepted elements, even with a
        // samller limit than previously accepted.
        let accepted2 = limiter
            .check_cardinality_limits(org, entries, 3)
            .unwrap()
            .map(|e| e.id)
            .collect::<HashSet<_>>();
        assert_eq!(accepted2, accepted);

        // A higher limit should accept everthing
        let accepted3 = limiter
            .check_cardinality_limits(org, entries, 10)
            .unwrap()
            .map(|e| e.id)
            .collect::<HashSet<_>>();
        assert_eq!(accepted3, entries.iter().map(|e| e.id).collect());
    }

    #[test]
    fn test_limiter_big_limit() {
        let limiter = build_limiter();

        let org = new_org(&limiter);
        let entries = (0..100_000)
            .map(|i| Entry::new(EntryId(i as usize), MetricNamespace::Custom, i))
            .collect::<Vec<_>>();

        let accepted = limiter
            .check_cardinality_limits(org, entries, 80_000)
            .unwrap();
        assert_eq!(accepted.count(), 80_000);
    }

    #[test]
    fn test_limiter_sliding_window() {
        let mut limiter = build_limiter();

        let org = new_org(&limiter);
        let entries1 = [Entry::new(EntryId(0), MetricNamespace::Custom, 0)];
        let entries2 = [Entry::new(EntryId(1), MetricNamespace::Custom, 1)];

        let accepted = limiter.check_cardinality_limits(org, entries1, 1).unwrap();
        assert_eq!(accepted.count(), 1);

        for i in 0..limiter.window.window_seconds / limiter.window.granularity_seconds {
            // Fast forward time.
            limiter.time_offset = i * limiter.window.granularity_seconds;

            // Should accept the already inserted item.
            let accepted = limiter.check_cardinality_limits(org, entries1, 1).unwrap();
            assert_eq!(accepted.count(), 1);
            // Should reject the new item.
            let accepted = limiter.check_cardinality_limits(org, entries2, 1).unwrap();
            assert_eq!(accepted.count(), 0);
        }

        // Fast forward time to where we're in the next window.
        limiter.time_offset = limiter.window.window_seconds + 1;
        // Accept the new element.
        let accepted = limiter.check_cardinality_limits(org, entries2, 1).unwrap();
        assert_eq!(accepted.count(), 1);
        // Reject the old element now.
        let accepted = limiter.check_cardinality_limits(org, entries1, 1).unwrap();
        assert_eq!(accepted.count(), 0);
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
