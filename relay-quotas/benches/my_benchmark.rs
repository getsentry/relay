use std::sync::Arc;
use std::thread;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::Rng;
use relay_base_schema::project::{ProjectId, ProjectKey};
use relay_common::time::UnixTimestamp;
use relay_quotas::{
    DataCategories, DataCategory, GlobalCounters, ItemScoping, Quota, QuotaScope, RedisQuota,
    Scoping,
};
use relay_redis::{PooledClient, RedisConfigOptions, RedisPool};

fn redis_quota_dummy(window: u64, limit: u64) -> RedisQuota<'static> {
    let quota = Quota {
        id: Some("foo".to_owned()),
        categories: DataCategories::new(),
        scope: QuotaScope::Global,
        scope_id: None,
        window: Some(window),
        limit: Some(limit),
        reason_code: None,
    };

    let inner_scoping = Scoping {
        organization_id: 69420,
        project_id: ProjectId::new(42),
        project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        key_id: Some(4711),
    };

    let static_inner_scoping: &'static Scoping = Box::leak(Box::new(inner_scoping));
    let static_quota: &'static Quota = Box::leak(Box::new(quota));

    let scoping = ItemScoping {
        category: DataCategory::MetricBucket,
        scoping: static_inner_scoping,
    };

    RedisQuota::new(static_quota, scoping, UnixTimestamp::now()).unwrap()
}

fn multi_threaded_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Multi-threaded Benchmark");

    // Adjust the sample size and measurement time
    group.sample_size(50); // Limit the number of iterations

    group.bench_with_input(
        BenchmarkId::new("multi_threaded_whatever", 12),
        &12,
        |b, &num_threads| {
            let counter = Arc::new(GlobalCounters::default());
            let pool = Arc::new(
                RedisPool::single("redis://127.0.0.1:6379", RedisConfigOptions::default()).unwrap(),
            );

            let window: u64 = rand::thread_rng().gen_range(10..100000);

            b.iter(|| {
                let handles: Vec<_> = (0..num_threads)
                    .map(|_| {
                        let pool_clone = pool.clone();
                        let counter_clone = counter.clone();
                        thread::spawn(move || {
                            let mut client = pool_clone.client().unwrap();
                            decrement_until_ratelimited(&mut client, counter_clone, window as u64);
                        })
                    })
                    .collect();

                for handle in handles {
                    handle.join().unwrap();
                }
            });
        },
    );

    group.finish();
}

fn decrement_until_ratelimited(
    client: &mut PooledClient,
    counter: Arc<GlobalCounters>,
    window: u64,
) {
    let quota = redis_quota_dummy(window, 100_000);
    let mut cnt = 1;

    while !counter.is_rate_limited(client, &quota, cnt % 15).unwrap() {
        cnt += 1;
    }
}

criterion_group!(benches, multi_threaded_benchmark);
criterion_main!(benches);
