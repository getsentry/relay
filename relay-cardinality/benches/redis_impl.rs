use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

use criterion::{BatchSize, BenchmarkId, Criterion};
use relay_base_schema::{
    metrics::{MetricName, MetricNamespace},
    organization::OrganizationId,
    project::ProjectId,
};
use relay_cardinality::{
    limiter::{Entry, EntryId, Limiter, Reporter, Scoping},
    CardinalityLimit, CardinalityReport, CardinalityScope, RedisSetLimiter, RedisSetLimiterOptions,
    SlidingWindow,
};
use relay_redis::{AsyncRedisClient, RedisConfigOptions};

// Async helper functions remain unchanged
fn build_redis_client() -> AsyncRedisClient {
    let url =
        std::env::var("RELAY_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());

    AsyncRedisClient::single(&url, &RedisConfigOptions::default()).unwrap()
}

async fn build_limiter(client: AsyncRedisClient, reset_redis: bool) -> RedisSetLimiter {
    let mut connection = client.get_connection().await.unwrap();
    if reset_redis {
        relay_redis::redis::cmd("FLUSHALL")
            .exec_async(&mut connection)
            .await
            .unwrap();
    }

    RedisSetLimiter::new(
        RedisSetLimiterOptions {
            cache_vacuum_interval: Duration::from_secs(180),
        },
        client,
    )
}

struct NoopReporter;

impl<'a> Reporter<'a> for NoopReporter {
    fn reject(&mut self, _limit_id: &'a CardinalityLimit, _entry_id: EntryId) {}
    fn report_cardinality(&mut self, _limit: &'a CardinalityLimit, _report: CardinalityReport) {}
}

#[derive(Debug)]
struct Params {
    limits: Vec<CardinalityLimit>,
    scoping: Scoping,
    rounds: usize,
    num_hashes: usize,
    names: Vec<MetricName>,
}

impl Params {
    fn new(limit: u32, rounds: usize, num_hashes: usize, names: usize) -> Self {
        let scope = if names > 0 {
            CardinalityScope::Name
        } else {
            CardinalityScope::Organization
        };
        let names = (0..names + 1)
            .map(|i| MetricName::from(format!("eins_metric_{i}")))
            .collect();
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
                scope,
                namespace: None,
            }],
            scoping: Scoping {
                organization_id: OrganizationId::new(1),
                project_id: ProjectId::new(100),
            },
            rounds,
            num_hashes,
            names,
        }
    }

    #[inline(always)]
    async fn run<'a>(
        &self,
        limiter: &RedisSetLimiter,
        entries: impl IntoIterator<Item = Entry<'a>> + Send,
    ) {
        limiter
            .check_cardinality_limits(self.scoping, &self.limits, entries, &mut NoopReporter)
            .await
            .unwrap();
    }

    fn rounds(&self) -> Vec<Vec<Entry<'_>>> {
        let entries = (0..self.num_hashes)
            .map(|i| {
                Entry::new(
                    EntryId(i),
                    MetricNamespace::Custom,
                    &self.names[i % self.names.len()],
                    u32::MAX - (i as u32),
                )
            })
            .collect::<Vec<_>>();
        (0..self.rounds).map(move |_| entries.clone()).collect()
    }

    fn rounds_unique(&self) -> Vec<Vec<Entry<'_>>> {
        let hash = AtomicU32::new(u32::MAX);
        (0..self.rounds)
            .map(move |_| {
                (0..self.num_hashes)
                    .map(|i| {
                        Entry::new(
                            EntryId(i),
                            MetricNamespace::Custom,
                            &self.names[i % self.names.len()],
                            hash.fetch_sub(1, Ordering::SeqCst),
                        )
                    })
                    .collect()
            })
            .collect()
    }

    fn never_entry(&self) -> Entry<'_> {
        Entry::new(
            EntryId(usize::MAX),
            MetricNamespace::Custom,
            &self.names[0],
            0,
        )
    }

    fn never_entries(&self) -> Vec<Entry<'_>> {
        (0..self.limits[0].limit as usize)
            .map(|i| {
                Entry::new(
                    EntryId(usize::MAX - i),
                    MetricNamespace::Custom,
                    &self.names[i % self.names.len()],
                    i as u32,
                )
            })
            .collect()
    }
}

impl std::fmt::Display for Params {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let limit = self.limits[0].limit;
        let rounds = self.rounds;
        let hashes = self.num_hashes;
        let names = self.names.len() - 1;
        write!(
            f,
            "{{Limit: {limit}, Rounds: {rounds}, Hashes: {hashes}, Names: {names}}}"
        )
    }
}

pub fn bench_simple(c: &mut Criterion) {
    let simple: &[Params] = &[
        Params::new(10_000, 1000, 50, 0),
        Params::new(10_000, 1000, 500, 0),
        Params::new(10_000, 50, 10_000, 0),
    ];
    let names: &[Params] = &[
        Params::new(10_000, 50, 1_000, 2),
        Params::new(10_000, 50, 1_000, 30),
        Params::new(10_000, 50, 1_000, 1000),
    ];

    // Create Tokio runtime
    let rt = tokio::runtime::Runtime::new().unwrap();
    let redis = build_redis_client();

    for (name, params) in [("Simple", simple), ("Names", names)] {
        let mut g = c.benchmark_group(name);
        for params in params {
            g.throughput(criterion::Throughput::Elements(params.rounds as u64));
            g.bench_with_input(BenchmarkId::new("simple", params), params, |b, params| {
                b.iter_batched(
                    || {
                        let limiter = rt.block_on(build_limiter(redis.clone(), true));
                        (limiter, params.rounds())
                    },
                    |(limiter, rounds)| {
                        for entries in rounds {
                            rt.block_on(params.run(&limiter, entries));
                        }
                    },
                    BatchSize::SmallInput,
                )
            });
        }
    }
}

pub fn bench_big_set_small_queries(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = build_redis_client();
    let params = Params::new(10_000, 1000, 50, 0);

    let mut g = c.benchmark_group("Big set small queries");
    g.throughput(criterion::Throughput::Elements(params.rounds as u64));
    g.bench_function("big_set_small_queries", |b| {
        b.iter_batched(
            || {
                let limiter = rt.block_on(build_limiter(client.clone(), true));
                let rounds = params.rounds();
                rt.block_on(params.run(&limiter, rounds[0].clone()));
                (limiter, rounds)
            },
            |(limiter, rounds)| {
                for entries in rounds {
                    rt.block_on(params.run(&limiter, entries));
                }
            },
            BatchSize::SmallInput,
        )
    });
}

pub fn bench_high_cardinality(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = build_redis_client();
    let params = Params::new(10_000, 1000, 50, 0);

    let mut g = c.benchmark_group("High Cardinality");
    g.throughput(criterion::Throughput::Elements(params.rounds as u64));
    g.bench_function("high_cardinality", |b| {
        b.iter_batched(
            || {
                let limiter = rt.block_on(build_limiter(client.clone(), true));
                (limiter, params.rounds_unique())
            },
            |(limiter, rounds)| {
                for entries in rounds {
                    rt.block_on(params.run(&limiter, entries));
                }
            },
            BatchSize::SmallInput,
        )
    });
}

pub fn bench_cache_never_full(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = build_redis_client();
    let params = Params::new(10_000, 1_000, 50, 0);

    let mut g = c.benchmark_group("Cache Never Full");
    g.throughput(criterion::Throughput::Elements(params.rounds as u64));
    g.bench_function("cache_never_full", |b| {
        b.iter_batched(
            || {
                let limiter = rt.block_on(build_limiter(client.clone(), true));
                rt.block_on(params.run(&limiter, vec![params.never_entry()]));
                let limiter = rt.block_on(build_limiter(client.clone(), false));
                (limiter, params.rounds_unique())
            },
            |(limiter, rounds)| {
                for entries in rounds {
                    rt.block_on(params.run(&limiter, entries));
                }
            },
            BatchSize::SmallInput,
        )
    });
}

pub fn bench_cache_worst_case(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = build_redis_client();
    let params = Params::new(10_000, 1000, 50, 0);

    let mut g = c.benchmark_group("Cache Worst Case");
    g.throughput(criterion::Throughput::Elements(params.rounds as u64));
    g.bench_function("cache_worst_case", |b| {
        b.iter_batched(
            || {
                let limiter = rt.block_on(build_limiter(client.clone(), true));
                rt.block_on(params.run(&limiter, params.never_entries()));
                let limiter = rt.block_on(build_limiter(client.clone(), false));
                (limiter, params.rounds_unique())
            },
            |(limiter, rounds)| {
                for entries in rounds {
                    rt.block_on(params.run(&limiter, entries));
                }
            },
            BatchSize::SmallInput,
        )
    });
}
