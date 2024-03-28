use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

use criterion::{BatchSize, BenchmarkId, Criterion};
use relay_base_schema::{
    metrics::{MetricName, MetricNamespace},
    project::ProjectId,
};
use relay_cardinality::{
    limiter::{Entry, EntryId, Limiter, Reporter, Scoping},
    CardinalityLimit, CardinalityReport, CardinalityScope, RedisSetLimiter, RedisSetLimiterOptions,
    SlidingWindow,
};
use relay_redis::{redis, RedisConfigOptions, RedisPool};

fn build_redis() -> RedisPool {
    let url =
        std::env::var("RELAY_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());

    RedisPool::single(&url, RedisConfigOptions::default()).unwrap()
}

fn build_limiter(redis: RedisPool, reset_redis: bool) -> RedisSetLimiter {
    let mut client = redis.client().unwrap();
    let mut connection = client.connection().unwrap();

    if reset_redis {
        redis::cmd("FLUSHALL").execute(&mut connection);
    }

    RedisSetLimiter::new(
        RedisSetLimiterOptions {
            cache_vacuum_interval: Duration::from_secs(180),
        },
        redis,
    )
}

struct NoopReporter;

impl<'a> Reporter<'a> for NoopReporter {
    fn reject(&mut self, _limit_id: &'a CardinalityLimit, _entry_id: EntryId) {}

    fn cardinality(&mut self, _limit: &'a CardinalityLimit, _report: CardinalityReport) {}
}

#[derive(Debug)]
struct Params {
    limits: Vec<CardinalityLimit>,
    scoping: Scoping,

    rounds: usize,
    num_hashes: usize,

    name: MetricName,
}

impl Params {
    fn new(limit: u32, rounds: usize, num_hashes: usize) -> Self {
        Self {
            limits: vec![CardinalityLimit {
                id: "limit".to_owned(),
                passive: false,
                report: false,
                window: SlidingWindow {
                    window_seconds: 3600,
                    granularity_seconds: 360,
                },
                limit,
                scope: CardinalityScope::Organization,
                namespace: None,
            }],
            scoping: Scoping {
                organization_id: 1,
                project_id: ProjectId::new(100),
            },
            rounds,
            num_hashes,
            name: MetricName::from("foo"),
        }
    }

    #[inline(always)]
    fn run<'a>(&self, limiter: &RedisSetLimiter, entries: impl IntoIterator<Item = Entry<'a>>) {
        limiter
            .check_cardinality_limits(self.scoping, &self.limits, entries, &mut NoopReporter)
            .unwrap();
    }

    /// Every round contains the same hashes.
    fn rounds(&self) -> Vec<Vec<Entry<'_>>> {
        let entries = (0..self.num_hashes)
            .map(|i| {
                Entry::new(
                    EntryId(i),
                    MetricNamespace::Custom,
                    &self.name,
                    u32::MAX - (i as u32),
                )
            })
            .collect::<Vec<_>>();

        (0..self.rounds)
            .map(move |_| entries.clone())
            .collect::<Vec<_>>()
    }

    /// High cardinality, every round contains unique hashes.
    fn rounds_unique(&self) -> Vec<Vec<Entry<'_>>> {
        let hash = AtomicU32::new(u32::MAX);

        (0..self.rounds)
            .map(move |_| {
                (0..self.num_hashes)
                    .map(|i| {
                        Entry::new(
                            EntryId(i),
                            MetricNamespace::Custom,
                            &self.name,
                            hash.fetch_sub(1, Ordering::SeqCst),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    }

    /// Entry which is never generated by either [`Self::rounds`] or [`Self::rounds_unique`].
    fn never_entry(&self) -> Entry<'_> {
        Entry::new(EntryId(usize::MAX), MetricNamespace::Custom, &self.name, 0)
    }

    /// A vector of entries which is never generated by either [`Self::rounds`] or [`Self::rounds_unique`].
    fn never_entries(&self) -> Vec<Entry<'_>> {
        (0..self.limits[0].limit)
            .map(|i| {
                Entry::new(
                    EntryId(usize::MAX - i as usize),
                    MetricNamespace::Custom,
                    &self.name,
                    i,
                )
            })
            .collect::<Vec<_>>()
    }
}

impl std::fmt::Display for Params {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub fn bench_simple(c: &mut Criterion) {
    let params = [
        Params::new(10_000, 1000, 50),
        Params::new(10_000, 1000, 500),
        Params::new(10_000, 50, 10_000),
    ];

    let redis = build_redis();

    let mut g = c.benchmark_group("Simple");

    for params in params {
        g.throughput(criterion::Throughput::Elements(params.rounds as u64));
        g.bench_with_input(BenchmarkId::new("simple", &params), &params, |b, params| {
            b.iter_batched(
                || {
                    let limiter = build_limiter(redis.clone(), true);
                    (limiter, params.rounds())
                },
                |(limiter, rounds)| {
                    for entries in rounds {
                        params.run(&limiter, entries);
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
}

pub fn bench_big_set_small_queries(c: &mut Criterion) {
    let redis = build_redis();

    let params = Params::new(10_000, 1000, 50);

    let mut g = c.benchmark_group("Big set small queries");
    g.throughput(criterion::Throughput::Elements(params.rounds as u64));
    g.bench_function("big_set_small_queries", |b| {
        b.iter_batched(
            || {
                let limiter = build_limiter(redis.clone(), true);
                let rounds = params.rounds();

                // Seed with the round data
                params.run(&limiter, rounds[0].clone());

                (limiter, rounds)
            },
            |(limiter, rounds)| {
                for entries in rounds {
                    params.run(&limiter, entries);
                }
            },
            BatchSize::SmallInput,
        )
    });
}

pub fn bench_high_cardinality(c: &mut Criterion) {
    let redis = build_redis();

    let params = Params::new(10_000, 1000, 50);

    let mut g = c.benchmark_group("High Cardinality");
    g.throughput(criterion::Throughput::Elements(params.rounds as u64));
    g.bench_function("high_cardinality", |b| {
        b.iter_batched(
            || {
                let limiter = build_limiter(redis.clone(), true);
                (limiter, params.rounds_unique())
            },
            |(limiter, rounds)| {
                for entries in rounds {
                    params.run(&limiter, entries);
                }
            },
            BatchSize::SmallInput,
        )
    });
}

pub fn bench_cache_never_full(c: &mut Criterion) {
    let redis = build_redis();

    let params = Params::new(10_000, 1_000, 50);

    let mut g = c.benchmark_group("Cache Never Full");
    g.throughput(criterion::Throughput::Elements(params.rounds as u64));
    g.bench_function("cache_never_full", |b| {
        b.iter_batched(
            || {
                let limiter = build_limiter(redis.clone(), true);
                params.run(&limiter, vec![params.never_entry()]);

                // New limiter to reset cache.
                let limiter = build_limiter(redis.clone(), false);
                (limiter, params.rounds_unique())
            },
            |(limiter, rounds)| {
                for entries in rounds {
                    params.run(&limiter, entries);
                }
            },
            BatchSize::SmallInput,
        )
    });
}

pub fn bench_cache_worst_case(c: &mut Criterion) {
    let redis = build_redis();

    let params = Params::new(10_000, 1000, 50);

    let mut g = c.benchmark_group("Cache Worst Case");
    g.throughput(criterion::Throughput::Elements(params.rounds as u64));
    g.bench_function("cache_worst_case", |b| {
        b.iter_batched(
            || {
                let limiter = build_limiter(redis.clone(), true);
                params.run(&limiter, params.never_entries());

                // New limiter to reset cache.
                let limiter = build_limiter(redis.clone(), false);
                (limiter, params.rounds_unique())
            },
            |(limiter, rounds)| {
                for entries in rounds {
                    params.run(&limiter, entries);
                }
            },
            BatchSize::SmallInput,
        )
    });
}
