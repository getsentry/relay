use std::time::Duration;

use relay_redis::{Connection, RedisPool};
use relay_statsd::metric;

use crate::{
    limiter::{CardinalityReport, Entry, Limiter, Reporter, Scoping},
    redis::{
        cache::{Cache, CacheOutcome},
        quota::QuotaScoping,
        script::{CardinalityScript, CardinalityScriptResult, Status},
        state::{LimitState, RedisEntry},
    },
    statsd::{CardinalityLimiterHistograms, CardinalityLimiterTimers},
    CardinalityLimit, Result,
};
use relay_common::time::UnixTimestamp;

/// Configuration options for the [`RedisSetLimiter`].
pub struct RedisSetLimiterOptions {
    /// Cache vacuum interval for the in memory cache.
    ///
    /// The cache will scan for expired values based on this interval.
    pub cache_vacuum_interval: Duration,
}

/// Implementation uses Redis sets to keep track of cardinality.
pub struct RedisSetLimiter {
    redis: RedisPool,
    script: CardinalityScript,
    cache: Cache,
    #[cfg(test)]
    timestamp: UnixTimestamp,
    #[cfg(test)]
    time_offset: std::time::Duration,
}

/// A Redis based limiter using Redis sets to track cardinality and membership.
impl RedisSetLimiter {
    /// Creates a new [`RedisSetLimiter`].
    pub fn new(options: RedisSetLimiterOptions, redis: RedisPool) -> Self {
        Self {
            redis,
            script: CardinalityScript::load(),
            cache: Cache::new(options.cache_vacuum_interval),
            #[cfg(test)]
            timestamp: UnixTimestamp::now(),
            #[cfg(test)]
            time_offset: std::time::Duration::from_secs(0),
        }
    }

    /// Checks the limits for a specific scope.
    ///
    /// Returns an iterator over all entries which have been accepted.
    fn check_limits(
        &self,
        connection: &mut Connection<'_>,
        state: &mut LimitState<'_>,
        timestamp: UnixTimestamp,
    ) -> Result<Vec<CheckedLimits>> {
        let limit = state.limit;

        let scopes = state.take_scopes();

        let mut num_hashes: u64 = 0;

        let mut pipeline = self.script.pipe();
        for (scope, entries) in &scopes {
            let keys = scope.slots(timestamp).map(|slot| scope.to_redis_key(slot));

            let hashes = entries.iter().map(|entry| entry.hash);
            num_hashes += hashes.len() as u64;

            // The expiry is a off by `window.granularity_seconds`,
            // but since this is only used for cleanup, this is not an issue.
            pipeline.add_invocation(limit, scope.redis_key_ttl(), hashes, keys);
        }

        metric!(
            histogram(CardinalityLimiterHistograms::RedisCheckHashes) = num_hashes,
            id = state.id(),
        );

        let results = pipeline.invoke(connection)?;

        debug_assert_eq!(results.len(), scopes.len());
        scopes
            .into_iter()
            .zip(results)
            .inspect(|(_, result)| {
                metric!(
                    histogram(CardinalityLimiterHistograms::RedisSetCardinality) =
                        result.cardinality,
                    id = state.id(),
                );
            })
            .map(|((scope, entries), result)| CheckedLimits::new(scope, entries, result))
            .collect()
    }
}

impl Limiter for RedisSetLimiter {
    fn check_cardinality_limits<'a, 'b, E, R>(
        &self,
        scoping: Scoping,
        limits: &'a [CardinalityLimit],
        entries: E,
        reporter: &mut R,
    ) -> Result<()>
    where
        E: IntoIterator<Item = Entry<'b>>,
        R: Reporter<'a>,
    {
        #[cfg(not(test))]
        let timestamp = UnixTimestamp::now();
        // Allows to fast forward time in tests using a fixed offset.
        #[cfg(test)]
        let timestamp = self.timestamp + self.time_offset;

        let mut states = LimitState::from_limits(scoping, limits);

        let cache = self.cache.read(timestamp); // Acquire a read lock.
        for entry in entries {
            for state in states.iter_mut() {
                let Some(scope) = state.matching_scope(entry) else {
                    // Entry not relevant for limit.
                    continue;
                };

                match cache.check(&scope, entry.hash, state.limit) {
                    CacheOutcome::Accepted => {
                        // Accepted already, nothing to do.
                        state.cache_hit();
                        state.accepted();
                    }
                    CacheOutcome::Rejected => {
                        // Rejected, add it to the rejected list and move on.
                        reporter.reject(state.cardinality_limit(), entry.id);
                        state.cache_hit();
                        state.rejected();
                    }
                    CacheOutcome::Unknown => {
                        // Add the entry to the state -> needs to be checked with Redis.
                        state.add(scope, RedisEntry::new(entry.id, entry.hash));
                        state.cache_miss();
                    }
                }
            }
        }
        drop(cache); // Give up the cache lock!

        let mut client = self.redis.client()?;
        let mut connection = client.connection()?;

        for mut state in states {
            if state.is_empty() {
                continue;
            }

            let results = metric!(timer(CardinalityLimiterTimers::Redis), id = state.id(), {
                self.check_limits(&mut connection, &mut state, timestamp)
            })?;

            for result in results {
                reporter.cardinality(state.cardinality_limit(), result.to_report());

                // This always acquires a write lock, but we only hit this
                // if we previously didn't satisfy the request from the cache,
                // -> there is a very high chance we actually need the lock.
                let mut cache = self.cache.update(&result.scope, timestamp); // Acquire a write lock.
                for (entry, status) in result {
                    if status.is_rejected() {
                        reporter.reject(state.cardinality_limit(), entry.id);
                        state.rejected();
                    } else {
                        cache.accept(entry.hash);
                        state.accepted();
                    }
                }
                drop(cache); // Give up the cache lock!
            }
        }

        Ok(())
    }
}

struct CheckedLimits {
    scope: QuotaScoping,
    cardinality: u64,
    entries: Vec<RedisEntry>,
    statuses: Vec<Status>,
}

impl CheckedLimits {
    fn new(
        scope: QuotaScoping,
        entries: Vec<RedisEntry>,
        result: CardinalityScriptResult,
    ) -> Result<Self> {
        result.validate(entries.len())?;

        Ok(Self {
            scope,
            entries,
            cardinality: result.cardinality,
            statuses: result.statuses,
        })
    }

    fn to_report(&self) -> CardinalityReport {
        CardinalityReport {
            organization_id: self.scope.organization_id,
            project_id: self.scope.project_id,
            name: self.scope.name.clone(),
            cardinality: self.cardinality,
        }
    }
}

impl IntoIterator for CheckedLimits {
    type Item = (RedisEntry, Status);
    type IntoIter = std::iter::Zip<std::vec::IntoIter<RedisEntry>, std::vec::IntoIter<Status>>;

    fn into_iter(self) -> Self::IntoIter {
        debug_assert_eq!(
            self.entries.len(),
            self.statuses.len(),
            "expected same amount of entries as statuses"
        );
        std::iter::zip(self.entries, self.statuses)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashSet};
    use std::sync::atomic::AtomicU64;

    use relay_base_schema::metrics::{MetricName, MetricNamespace::*};
    use relay_base_schema::project::ProjectId;
    use relay_redis::{redis, RedisConfigOptions};

    use crate::limiter::EntryId;
    use crate::redis::{KEY_PREFIX, KEY_VERSION};
    use crate::{CardinalityScope, SlidingWindow};

    use super::*;

    fn build_limiter() -> RedisSetLimiter {
        let url = std::env::var("RELAY_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());

        let opts = RedisConfigOptions {
            max_connections: 1,
            ..Default::default()
        };
        let redis = RedisPool::single(&url, opts).unwrap();

        RedisSetLimiter::new(
            RedisSetLimiterOptions {
                cache_vacuum_interval: Duration::from_secs(5),
            },
            redis,
        )
    }

    fn new_scoping(limiter: &RedisSetLimiter) -> Scoping {
        static ORGS: AtomicU64 = AtomicU64::new(100);

        let scoping = Scoping {
            organization_id: ORGS.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            project_id: ProjectId::new(1),
        };

        limiter.flush(scoping);

        scoping
    }

    #[derive(Debug, Default, PartialEq, Eq)]
    struct TestReporter {
        entries: HashSet<EntryId>,
        reports: BTreeMap<CardinalityLimit, Vec<CardinalityReport>>,
    }

    impl TestReporter {
        fn contains_any(&self, ids: impl IntoIterator<Item = usize>) -> bool {
            ids.into_iter()
                .any(|id| self.entries.contains(&EntryId(id)))
        }

        #[track_caller]
        fn assert_cardinality(&self, limit: &CardinalityLimit, cardinality: u64) {
            let Some(r) = self.reports.get(limit) else {
                panic!("expected cardinality report for limit {limit:?}");
            };
            assert_eq!(r.len(), 1, "expected one cardinality report");
            assert_eq!(r[0].cardinality, cardinality);
        }
    }

    impl<'a> super::Reporter<'a> for TestReporter {
        fn reject(&mut self, _limit: &'a CardinalityLimit, entry_id: EntryId) {
            self.entries.insert(entry_id);
        }

        fn cardinality(&mut self, limit: &'a CardinalityLimit, report: CardinalityReport) {
            let reports = self.reports.entry(limit.clone()).or_default();
            reports.push(report);
            reports.sort();
        }
    }

    impl std::ops::Deref for TestReporter {
        type Target = HashSet<EntryId>;

        fn deref(&self) -> &Self::Target {
            &self.entries
        }
    }

    impl RedisSetLimiter {
        /// Remove all redis state for an organization.
        fn flush(&self, scoping: Scoping) {
            let pattern = format!(
                "{KEY_PREFIX}:{KEY_VERSION}:scope-{{{o}-*",
                o = scoping.organization_id
            );

            let mut client = self.redis.client().unwrap();
            let mut connection = client.connection().unwrap();

            let keys = redis::cmd("KEYS")
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

        fn redis_sets(&self, scoping: Scoping) -> Vec<(String, usize)> {
            let pattern = format!(
                "{KEY_PREFIX}:{KEY_VERSION}:scope-{{{o}-*",
                o = scoping.organization_id
            );

            let mut client = self.redis.client().unwrap();
            let mut connection = client.connection().unwrap();

            redis::cmd("KEYS")
                .arg(pattern)
                .query::<Vec<String>>(&mut connection)
                .unwrap()
                .into_iter()
                .map(move |key| {
                    let size = redis::cmd("SCARD")
                        .arg(&key)
                        .query::<usize>(&mut connection)
                        .unwrap();

                    (key, size)
                })
                .collect()
        }

        fn test_limits<'a, I>(
            &self,
            scoping: Scoping,
            limits: &'a [CardinalityLimit],
            entries: I,
        ) -> TestReporter
        where
            I: IntoIterator<Item = Entry<'a>>,
        {
            let mut reporter = TestReporter::default();
            self.check_cardinality_limits(scoping, limits, entries, &mut reporter)
                .unwrap();
            reporter
        }
    }

    #[test]
    fn test_limiter_accept_previously_seen() {
        let limiter = build_limiter();

        let m0 = MetricName::from("a");
        let m1 = MetricName::from("b");
        let m2 = MetricName::from("c");
        let m3 = MetricName::from("d");
        let m4 = MetricName::from("e");
        let m5 = MetricName::from("f");

        let entries = [
            Entry::new(EntryId(0), Custom, &m0, 0),
            Entry::new(EntryId(1), Custom, &m1, 1),
            Entry::new(EntryId(2), Custom, &m2, 2),
            Entry::new(EntryId(3), Custom, &m3, 3),
            Entry::new(EntryId(4), Custom, &m4, 4),
            Entry::new(EntryId(5), Custom, &m5, 5),
        ];

        let scoping = new_scoping(&limiter);
        let mut limit = CardinalityLimit {
            id: "limit".to_owned(),
            passive: false,
            report: false,
            window: SlidingWindow {
                window_seconds: 3600,
                granularity_seconds: 360,
            },
            limit: 5,
            scope: CardinalityScope::Organization,
            namespace: Some(Custom),
        };

        // 6 items, limit is 5 -> 1 rejection.
        let rejected = limiter.test_limits(scoping, &[limit.clone()], entries);
        assert_eq!(rejected.len(), 1);

        // We're at the limit but it should still accept already accepted elements, even with a
        // samller limit than previously accepted.
        limit.limit = 3;
        let rejected2 = limiter.test_limits(scoping, &[limit.clone()], entries);
        assert_eq!(rejected2.entries, rejected.entries);

        // A higher limit should accept everthing
        limit.limit = 6;
        let rejected3 = limiter.test_limits(scoping, &[limit], entries);
        assert_eq!(rejected3.len(), 0);
    }

    #[test]
    fn test_limiter_name_limit() {
        let limiter = build_limiter();

        let m0 = MetricName::from("a");
        let m1 = MetricName::from("b");

        let entries = [
            Entry::new(EntryId(0), Custom, &m0, 0),
            Entry::new(EntryId(1), Custom, &m0, 1),
            Entry::new(EntryId(2), Custom, &m0, 2),
            Entry::new(EntryId(3), Custom, &m1, 3),
            Entry::new(EntryId(4), Custom, &m1, 4),
            Entry::new(EntryId(5), Custom, &m1, 5),
        ];

        let scoping = new_scoping(&limiter);
        let limit = CardinalityLimit {
            id: "limit".to_owned(),
            passive: false,
            report: true,
            window: SlidingWindow {
                window_seconds: 3600,
                granularity_seconds: 360,
            },
            limit: 2,
            scope: CardinalityScope::Name,
            namespace: Some(Custom),
        };

        let rejected = limiter.test_limits(scoping, &[limit.clone()], entries);
        assert_eq!(rejected.len(), 2);
        assert!(rejected.contains_any([0, 1, 2]));
        assert!(rejected.contains_any([3, 4, 5]));

        assert_eq!(rejected.reports.len(), 1);
        let reports = rejected.reports.get(&limit).unwrap();
        assert_eq!(
            reports,
            &[
                CardinalityReport {
                    organization_id: Some(scoping.organization_id),
                    project_id: Some(scoping.project_id),
                    name: Some(m0),
                    cardinality: 2,
                },
                CardinalityReport {
                    organization_id: Some(scoping.organization_id),
                    project_id: Some(scoping.project_id),
                    name: Some(m1),
                    cardinality: 2,
                },
            ]
        );
    }

    #[test]
    fn test_limiter_org_based_time_shift() {
        let mut limiter = build_limiter();

        let granularity_seconds = 10_000;

        let scoping1 = Scoping {
            organization_id: granularity_seconds,
            project_id: ProjectId::new(1),
        };
        let scoping2 = Scoping {
            // Time shift relative to `scoping1` should be half the granularity.
            organization_id: granularity_seconds / 2,
            project_id: ProjectId::new(1),
        };

        let limits = &[CardinalityLimit {
            id: "limit".to_owned(),
            passive: false,
            report: false,
            window: SlidingWindow {
                window_seconds: granularity_seconds * 3,
                granularity_seconds,
            },
            limit: 1,
            scope: CardinalityScope::Organization,
            namespace: Some(Custom),
        }];

        let m = MetricName::from("a");

        let entries1 = [Entry::new(EntryId(0), Custom, &m, 0)];
        assert!(limiter.test_limits(scoping1, limits, entries1).is_empty());
        assert!(limiter.test_limits(scoping2, limits, entries1).is_empty());

        // Make sure `entries2` is not accepted.
        let entries2 = [Entry::new(EntryId(1), Custom, &m, 1)];
        assert_eq!(limiter.test_limits(scoping1, limits, entries2).len(), 1);
        assert_eq!(limiter.test_limits(scoping2, limits, entries2).len(), 1);

        let mut scoping1_accept = None;
        let mut scoping2_accept = None;

        // Measure time required until `entries2` is accepted.
        for i in 0..100 {
            let offset = i * granularity_seconds / 10;

            limiter.time_offset = Duration::from_secs(offset);

            if scoping1_accept.is_none()
                && limiter.test_limits(scoping1, limits, entries2).is_empty()
            {
                scoping1_accept = Some(offset as i64);
            }

            if scoping2_accept.is_none()
                && limiter.test_limits(scoping2, limits, entries2).is_empty()
            {
                scoping2_accept = Some(offset as i64);
            }

            if scoping1_accept.is_some() && scoping2_accept.is_some() {
                break;
            }
        }

        let scoping1_accept = scoping1_accept.unwrap();
        let scoping2_accept = scoping2_accept.unwrap();

        let diff = (scoping1_accept - scoping2_accept).abs();
        let expected = granularity_seconds as i64 / 2;
        // Make sure they are perfectly offset.
        assert_eq!(diff, expected);
    }

    #[test]
    fn test_limiter_small_within_limits() {
        let limiter = build_limiter();
        let scoping = new_scoping(&limiter);

        let limits = &[CardinalityLimit {
            id: "limit".to_owned(),
            passive: false,
            report: false,
            window: SlidingWindow {
                window_seconds: 3600,
                granularity_seconds: 360,
            },
            limit: 10_000,
            scope: CardinalityScope::Organization,
            namespace: Some(Custom),
        }];

        let m = MetricName::from("a");

        let entries = (0..50)
            .map(|i| Entry::new(EntryId(i as usize), Custom, &m, i))
            .collect::<Vec<_>>();
        let rejected = limiter.test_limits(scoping, limits, entries);
        assert_eq!(rejected.len(), 0);

        let entries = (100..150)
            .map(|i| Entry::new(EntryId(i as usize), Custom, &m, i))
            .collect::<Vec<_>>();
        let rejected = limiter.test_limits(scoping, limits, entries);
        assert_eq!(rejected.len(), 0);
    }

    #[test]
    fn test_limiter_big_limit() {
        let limiter = build_limiter();

        let scoping = new_scoping(&limiter);
        let limits = &[CardinalityLimit {
            id: "limit".to_owned(),
            passive: false,
            report: false,
            window: SlidingWindow {
                window_seconds: 3600,
                granularity_seconds: 360,
            },
            limit: 80_000,
            scope: CardinalityScope::Organization,
            namespace: Some(Custom),
        }];

        let m = MetricName::from("a");

        let entries = (0..100_000)
            .map(|i| Entry::new(EntryId(i as usize), Custom, &m, i))
            .collect::<Vec<_>>();

        let rejected = limiter.test_limits(scoping, limits, entries);
        assert_eq!(rejected.len(), 20_000);
    }

    #[test]
    fn test_limiter_sliding_window() {
        let mut limiter = build_limiter();

        let scoping = new_scoping(&limiter);
        let window = SlidingWindow {
            window_seconds: 3600,
            granularity_seconds: 360,
        };
        let limits = &[CardinalityLimit {
            id: "limit".to_owned(),
            passive: false,
            report: false,
            window,
            limit: 1,
            scope: CardinalityScope::Organization,
            namespace: Some(Custom),
        }];

        let m0 = MetricName::from("a");
        let m1 = MetricName::from("b");

        let entries1 = [Entry::new(EntryId(0), Custom, &m0, 0)];
        let entries2 = [Entry::new(EntryId(1), Custom, &m1, 1)];

        // 1 item and limit is 1 -> No rejections.
        let rejected = limiter.test_limits(scoping, limits, entries1);
        assert_eq!(rejected.len(), 0);

        for i in 0..(window.window_seconds / window.granularity_seconds) * 2 {
            // Fast forward time.
            limiter.time_offset = Duration::from_secs(i * window.granularity_seconds);

            // Should accept the already inserted item.
            let rejected = limiter.test_limits(scoping, limits, entries1);
            assert_eq!(rejected.len(), 0);
            // Should reject the new item.
            let rejected = limiter.test_limits(scoping, limits, entries2);
            assert_eq!(rejected.len(), 1);
        }

        // Fast forward time to a fresh window.
        limiter.time_offset = Duration::from_secs(1 + window.window_seconds * 3);
        // Accept the new element.
        let rejected = limiter.test_limits(scoping, limits, entries2);
        assert_eq!(rejected.len(), 0);
        // Reject the old element now.
        let rejected = limiter.test_limits(scoping, limits, entries1);
        assert_eq!(rejected.len(), 1);
    }

    #[test]
    fn test_limiter_no_namespace_limit_is_shared_limit() {
        let limiter = build_limiter();
        let scoping = new_scoping(&limiter);

        let limits = &[CardinalityLimit {
            id: "limit".to_owned(),
            passive: false,
            report: false,
            window: SlidingWindow {
                window_seconds: 3600,
                granularity_seconds: 360,
            },
            limit: 2,
            scope: CardinalityScope::Organization,
            namespace: None,
        }];

        let m0 = MetricName::from("a");
        let m1 = MetricName::from("b");
        let m2 = MetricName::from("c");

        let entries1 = [Entry::new(EntryId(0), Custom, &m0, 0)];
        let entries2 = [Entry::new(EntryId(0), Spans, &m1, 1)];
        let entries3 = [Entry::new(EntryId(0), Transactions, &m2, 2)];

        let rejected = limiter.test_limits(scoping, limits, entries1);
        assert_eq!(rejected.len(), 0);

        let rejected = limiter.test_limits(scoping, limits, entries2);
        assert_eq!(rejected.len(), 0);

        let rejected = limiter.test_limits(scoping, limits, entries3);
        assert_eq!(rejected.len(), 1);
    }

    #[test]
    fn test_limiter_multiple_limits() {
        let limiter = build_limiter();
        let scoping = new_scoping(&limiter);

        let limits = &[
            CardinalityLimit {
                id: "limit1".to_owned(),
                passive: false,
                report: false,
                window: SlidingWindow {
                    window_seconds: 3600,
                    granularity_seconds: 360,
                },
                limit: 1,
                scope: CardinalityScope::Organization,
                namespace: Some(Custom),
            },
            CardinalityLimit {
                id: "limit2".to_owned(),
                passive: false,
                report: false,
                window: SlidingWindow {
                    window_seconds: 3600,
                    granularity_seconds: 360,
                },
                limit: 1,
                scope: CardinalityScope::Organization,
                namespace: Some(Custom),
            },
            CardinalityLimit {
                id: "limit3".to_owned(),
                passive: false,
                report: false,
                window: SlidingWindow {
                    window_seconds: 3600,
                    granularity_seconds: 360,
                },
                limit: 1,
                scope: CardinalityScope::Project,
                namespace: Some(Spans),
            },
            CardinalityLimit {
                id: "unknown_skipped".to_owned(),
                passive: false,
                report: false,
                window: SlidingWindow {
                    window_seconds: 3600,
                    granularity_seconds: 360,
                },
                limit: 1,
                scope: CardinalityScope::Unknown,
                namespace: Some(Transactions),
            },
        ];

        let m0 = MetricName::from("a");
        let m1 = MetricName::from("b");
        let m2 = MetricName::from("c");
        let m3 = MetricName::from("d");
        let m4 = MetricName::from("e");
        let m5 = MetricName::from("f");

        let entries = [
            Entry::new(EntryId(0), Custom, &m0, 0),
            Entry::new(EntryId(1), Custom, &m1, 1),
            Entry::new(EntryId(2), Spans, &m2, 2),
            Entry::new(EntryId(3), Spans, &m3, 3),
            Entry::new(EntryId(4), Transactions, &m4, 4),
            Entry::new(EntryId(5), Transactions, &m5, 5),
        ];

        // Run multiple times to make sure caching does not interfere.
        for i in 0..3 {
            let rejected = limiter.test_limits(scoping, limits, entries);

            // 2 transactions + 1 span + 1 custom (4) accepted -> 2 (6-4) rejected.
            assert_eq!(rejected.len(), 2);
            // 1 custom rejected.
            assert!(rejected.contains_any([0, 1]));
            // 1 span rejected.
            assert!(rejected.contains_any([2, 3]));
            // 2 transactions accepted -> no rejections.
            assert!(!rejected.contains(&EntryId(4)));
            assert!(!rejected.contains(&EntryId(5)));

            // Cardinality reports are only generated for items not coming from the cache,
            // after the first iteration items may be coming from the cache.
            if i == 0 {
                assert_eq!(rejected.reports.len(), 3);
                assert_eq!(
                    rejected.reports.get(&limits[0]).unwrap(),
                    &[CardinalityReport {
                        organization_id: Some(scoping.organization_id),
                        project_id: None,
                        name: None,
                        cardinality: 1
                    }]
                );
                assert_eq!(
                    rejected.reports.get(&limits[1]).unwrap(),
                    &[CardinalityReport {
                        organization_id: Some(scoping.organization_id),
                        project_id: None,
                        name: None,
                        cardinality: 1
                    }]
                );
                assert_eq!(
                    rejected.reports.get(&limits[2]).unwrap(),
                    &[CardinalityReport {
                        organization_id: Some(scoping.organization_id),
                        project_id: Some(scoping.project_id),
                        name: None,
                        cardinality: 1
                    }]
                );
                assert!(rejected.reports.get(&limits[3]).is_none());
            } else {
                // Coming from cache.
                assert!(rejected.reports.is_empty());
            }
        }
    }

    #[test]
    fn test_project_limit() {
        let limiter = build_limiter();

        let scoping1 = new_scoping(&limiter);
        let scoping2 = Scoping {
            project_id: ProjectId::new(2),
            ..scoping1
        };

        let limits = &[CardinalityLimit {
            id: "limit".to_owned(),
            passive: false,
            report: false,
            window: SlidingWindow {
                window_seconds: 3600,
                granularity_seconds: 360,
            },
            limit: 1,
            scope: CardinalityScope::Project,
            namespace: None,
        }];

        let m1 = MetricName::from("a");
        let m2 = MetricName::from("b");
        let entries1 = [Entry::new(EntryId(0), Custom, &m1, 0)];
        let entries2 = [Entry::new(EntryId(0), Custom, &m2, 1)];

        // Accept different entries for different scopes.
        let rejected = limiter.test_limits(scoping1, limits, entries1);
        assert_eq!(rejected.len(), 0);
        let rejected = limiter.test_limits(scoping2, limits, entries2);
        assert_eq!(rejected.len(), 0);

        // Make sure the other entry is not accepted.
        let rejected = limiter.test_limits(scoping1, limits, entries2);
        assert_eq!(rejected.len(), 1);
        let rejected = limiter.test_limits(scoping2, limits, entries1);
        assert_eq!(rejected.len(), 1);
    }

    #[test]
    fn test_limiter_sliding_window_full() {
        let mut limiter = build_limiter();
        let scoping = new_scoping(&limiter);

        let window = SlidingWindow {
            window_seconds: 300,
            granularity_seconds: 100,
        };
        let limits = &[CardinalityLimit {
            id: "limit".to_owned(),
            passive: false,
            report: false,
            window,
            limit: 100,
            scope: CardinalityScope::Organization,
            namespace: Some(Custom),
        }];

        let m = MetricName::from("foo");
        macro_rules! test {
            ($r:expr) => {{
                let entries = $r
                    .map(|i| Entry::new(EntryId(i as usize), Custom, &m, i))
                    .collect::<Vec<_>>();

                limiter.test_limits(scoping, limits, entries)
            }};
        }

        macro_rules! assert_test {
            ($v:expr, $rejected:expr) => {{
                let report = test!($v);
                assert_eq!(report.len(), $rejected);
            }};
            ($v:expr, $rejected:expr, $cardinality:expr) => {{
                let report = test!($v);
                assert_eq!(report.len(), $rejected);
                report.assert_cardinality(&limits[0], $cardinality);
            }};
        }

        // Fill the first window with values.
        assert_test!(0..100, 0, 100);

        // Refresh 0..50 - Full.
        limiter.time_offset = Duration::from_secs(window.granularity_seconds);
        assert_test!(0..50, 0, 100);
        assert_test!(100..125, 25, 100);

        // Refresh 0..50 - Full.
        limiter.time_offset = Duration::from_secs(window.granularity_seconds * 2);
        assert_test!(0..50, 0, 100);
        assert_test!(125..150, 25, 100);

        // Refresh 0..50 - 50..100 fell out of the window, add 25 (size 50 -> 75).
        // --> Set 4 has size 75.
        limiter.time_offset = Duration::from_secs(window.granularity_seconds * 3);
        assert_test!(0..50, 0, 50);
        assert_test!(150..175, 0, 75);

        // Refresh 0..50 - Still 25 available (size 75 -> 100).
        limiter.time_offset = Duration::from_secs(window.granularity_seconds * 4);
        assert_test!(0..50, 0, 75);
        assert_test!(175..200, 0, 100);

        // From this point on it is repeating:
        //  - Always refresh 0..50.
        //  - First granule is full.
        //  - Next two granules each have space for 25 -> immediately filled.
        for i in 0..21 {
            let start = 200 + i * 25;
            let end = start + 25;

            limiter.time_offset = Duration::from_secs(window.granularity_seconds * (i as u64 + 5));

            if i % 3 == 0 {
                assert_test!(0..50, 0, 100);
                assert_test!(start..end, 25, 100);
            } else {
                assert_test!(0..50, 0, 75);
                assert_test!(start..end, 0, 100);
            }
        }

        let mut sets = limiter.redis_sets(scoping);
        sets.sort();

        let len = sets.len();
        for (i, (_, size)) in sets.into_iter().enumerate() {
            // Set 4 and the last set were never fully filled.
            if i == 3 || i + 1 == len {
                assert_eq!(size, 75);
            } else {
                assert_eq!(size, 100);
            }
        }
    }

    #[test]
    fn test_limiter_sliding_window_perfect() {
        let mut limiter = build_limiter();
        let scoping = new_scoping(&limiter);

        let window = SlidingWindow {
            window_seconds: 300,
            granularity_seconds: 100,
        };
        let limits = &[CardinalityLimit {
            id: "limit".to_owned(),
            passive: false,
            report: false,
            window,
            limit: 100,
            scope: CardinalityScope::Organization,
            namespace: Some(Custom),
        }];

        let m = MetricName::from("foo");
        macro_rules! test {
            ($r:expr) => {{
                let entries = $r
                    .map(|i| Entry::new(EntryId(i as usize), Custom, &m, i))
                    .collect::<Vec<_>>();

                limiter.test_limits(scoping, limits, entries)
            }};
        }

        // Test Case:
        //  1. Fill the window
        //  2. Fast forward one granule and fill that with the same values
        //  3. Assert that values "confirmed" in the first granule are active for a full window
        //
        // [ ][ ][ ]{ }{ }{ }[ ][ ][ ]
        // [A][A][A]{ }{ }{ }[ ][ ][ ]
        //    [A][A]{A}{ }{ }[ ][ ][ ]
        //     |     \-- Assert this value.
        //     \-- Touch this value.

        // Fill the first window with values.
        let report = test!(0..100);
        assert!(report.is_empty());
        report.assert_cardinality(&limits[0], 100);

        // Fast forward one granule with the same values.
        limiter.time_offset = Duration::from_secs(window.granularity_seconds);
        let report = test!(0..100);
        assert!(report.is_empty());
        report.assert_cardinality(&limits[0], 100);

        // Fast forward to the first granule after a full reset.
        limiter.time_offset = Duration::from_secs(window.window_seconds);
        // Make sure the window is full and does not accept new values.
        assert_eq!(test!(200..300).len(), 100);
        // Assert original values.
        assert!(test!(0..100).is_empty());
    }
}
