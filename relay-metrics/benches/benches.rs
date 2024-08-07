use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use rand::distributions::Uniform;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use relay_base_schema::project::ProjectKey;
use relay_common::time::UnixTimestamp;
use relay_metrics::{
    aggregator::{Aggregator, AggregatorConfig},
    Bucket, BucketValue, DistributionValue, FiniteF64,
};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;

struct NumbersGenerator {
    min: usize,
    max: usize,
    generator: RefCell<SmallRng>,
}

impl NumbersGenerator {
    fn new(range: Range<usize>) -> Self {
        Self {
            min: range.start,
            max: range.end,
            generator: RefCell::new(SmallRng::seed_from_u64(
                (range.start + range.end / 2) as u64,
            )),
        }
    }

    fn next(&self) -> usize {
        let dist = Uniform::new(self.min, self.max + 1);
        let value = self.generator.borrow_mut().sample(dist);

        value
    }
}

struct BucketsGenerator {
    percentage_backdated: f32,
    num_buckets: usize,
    metric_ids_generator: NumbersGenerator,
    project_keys_generator: NumbersGenerator,
    timestamp_shifts_generator: NumbersGenerator,
}
impl BucketsGenerator {
    fn get_buckets(&self, base_timestamp: UnixTimestamp) -> Vec<(ProjectKey, Bucket)> {
        let mut buckets = Vec::with_capacity(self.num_buckets);

        let backdated =
            ((self.num_buckets as f32 * self.percentage_backdated) as usize).min(self.num_buckets);
        let non_backdated = self.num_buckets - backdated;

        for _ in 0..backdated {
            buckets.push(self.build_bucket(base_timestamp, true));
        }

        for _ in 0..non_backdated {
            buckets.push(self.build_bucket(base_timestamp, false));
        }

        buckets
    }

    fn build_bucket(
        &self,
        base_timestamp: UnixTimestamp,
        is_backdated: bool,
    ) -> (ProjectKey, Bucket) {
        let time_shift = self.timestamp_shifts_generator.next();
        let timestamp = if is_backdated {
            base_timestamp.as_secs() - (time_shift as u64)
        } else {
            base_timestamp.as_secs() + (time_shift as u64)
        };
        let name = format!("c:transactions/foo_{}", self.metric_ids_generator.next());
        let bucket = Bucket {
            timestamp: UnixTimestamp::from_secs(timestamp),
            width: 0,
            name: name.into(),
            value: BucketValue::counter(42.into()),
            tags: BTreeMap::new(),
            metadata: Default::default(),
        };

        let key_id = self.project_keys_generator.next();
        let project_key = ProjectKey::parse(&format!("{key_id:0width$x}", width = 32)).unwrap();

        (project_key, bucket)
    }
}

impl fmt::Display for BucketsGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} buckets", self.num_buckets,)
    }
}

fn bench_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("DistributionValue");

    for size in [1, 10, 100, 1000, 10_000, 100_000, 1_000_000] {
        let values = std::iter::from_fn(|| Some(rand::random()))
            .filter_map(FiniteF64::new)
            .take(size as usize)
            .collect::<Vec<FiniteF64>>();

        group.throughput(criterion::Throughput::Elements(size));
        group.bench_with_input(BenchmarkId::from_parameter(size), &values, |b, values| {
            b.iter(|| DistributionValue::from_iter(black_box(values.iter().copied())))
        });
    }

    group.finish();
}

fn bench_insert_and_flush(c: &mut Criterion) {
    let config = AggregatorConfig {
        bucket_interval: 10,
        initial_delay: 0,
        ..Default::default()
    };

    let inputs = vec![
        (
            "multiple metrics of the same project",
            BucketsGenerator {
                percentage_backdated: 0.5,
                num_buckets: 100_000,
                metric_ids_generator: NumbersGenerator::new(1..100),
                project_keys_generator: NumbersGenerator::new(1..1),
                timestamp_shifts_generator: NumbersGenerator::new(1..10),
            },
        ),
        (
            "same metric on different projects",
            BucketsGenerator {
                percentage_backdated: 0.5,
                num_buckets: 100_000,
                metric_ids_generator: NumbersGenerator::new(1..1),
                project_keys_generator: NumbersGenerator::new(1..100),
                timestamp_shifts_generator: NumbersGenerator::new(1..10),
            },
        ),
        (
            "all backdated metrics",
            BucketsGenerator {
                percentage_backdated: 1.0,
                num_buckets: 100_000,
                metric_ids_generator: NumbersGenerator::new(1..100),
                project_keys_generator: NumbersGenerator::new(1..100),
                timestamp_shifts_generator: NumbersGenerator::new(10..50),
            },
        ),
        (
            "all non-backdated metrics",
            BucketsGenerator {
                percentage_backdated: 0.0,
                num_buckets: 100_000,
                metric_ids_generator: NumbersGenerator::new(1..100),
                project_keys_generator: NumbersGenerator::new(1..100),
                timestamp_shifts_generator: NumbersGenerator::new(10..50),
            },
        ),
        (
            "a lot of metrics from a lot of projects with mix of backdated buckets and non",
            BucketsGenerator {
                percentage_backdated: 0.5,
                num_buckets: 1_000_000,
                metric_ids_generator: NumbersGenerator::new(1..1_000_000),
                project_keys_generator: NumbersGenerator::new(1..1_000_000),
                timestamp_shifts_generator: NumbersGenerator::new(10..50),
            },
        ),
    ];

    for (input_name, input) in &inputs {
        {
            let mut group = c.benchmark_group("insert");
            group.throughput(criterion::Throughput::Elements(input.num_buckets as u64));
            group.bench_with_input(
                BenchmarkId::new("bench_insert_metrics", input_name),
                &input,
                |b, input| {
                    b.iter_batched(
                        || {
                            let timestamp = UnixTimestamp::now();
                            let aggregator: Aggregator = Aggregator::new(config.clone());
                            (aggregator, input.get_buckets(timestamp))
                        },
                        |(mut aggregator, buckets)| {
                            for (project_key, bucket) in buckets {
                                #[allow(clippy::unit_arg)]
                                black_box(
                                    aggregator
                                        .merge(black_box(project_key), black_box(bucket))
                                        .unwrap(),
                                );
                            }
                        },
                        BatchSize::SmallInput,
                    )
                },
            );
        }

        let mut group = c.benchmark_group("flush");
        group.bench_with_input(
            BenchmarkId::new("bench_flush_metrics", input_name),
            &input,
            |b, &input| {
                b.iter_batched(
                    || {
                        let timestamp = UnixTimestamp::now();
                        let mut aggregator: Aggregator = Aggregator::new(config.clone());
                        for (project_key, bucket) in input.get_buckets(timestamp) {
                            aggregator.merge(project_key, bucket).unwrap();
                        }
                        aggregator
                    },
                    |mut aggregator| {
                        // XXX: Ideally we'd want to test the entire try_flush here, but spawning
                        // a service is too much work here.
                        black_box(aggregator.pop_flush_buckets(black_box(false)));
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }
}

criterion_group!(benches, bench_insert_and_flush, bench_distribution);
criterion_main!(benches);
