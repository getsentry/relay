use std::collections::BTreeMap;
use std::fmt;

use actix::prelude::*;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};

use relay_common::{ProjectKey, UnixTimestamp};
use relay_metrics::{Aggregator, AggregatorConfig};
use relay_metrics::{Bucket, FlushBuckets, Metric, MetricUnit, MetricValue};

#[derive(Clone, Default)]
struct TestReceiver;

impl Actor for TestReceiver {
    type Context = Context<Self>;
}

impl Handler<FlushBuckets> for TestReceiver {
    type Result = Result<(), Vec<Bucket>>;

    fn handle(&mut self, _msg: FlushBuckets, _ctx: &mut Self::Context) -> Self::Result {
        Ok(())
    }
}

/// Struct representing a testcase for which insert + flush are timed.
struct MetricInput {
    num_metrics: usize,
    num_project_keys: usize,
    metric: Metric,
}

impl MetricInput {
    // Separate from actual metric insertion as we do not want to time this function, its logic and
    // especially not creation and destruction of a large vector.
    //
    // In theory we could also create all vectors upfront instead of having a MetricInput struct,
    // but that would take a lot of memory. This way we can at least free some RAM between
    // benchmarks.
    fn get_metrics(&self) -> Vec<(ProjectKey, Metric)> {
        let mut rv = Vec::new();

        for i in 0..self.num_metrics {
            let key_id = i % self.num_project_keys;
            let key = ProjectKey::parse(&format!("{:0width$x}", key_id, width = 32)).unwrap();
            rv.push((key, self.metric.clone()));
        }

        rv
    }
}

impl fmt::Display for MetricInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {:?} metrics, {} keys",
            self.num_metrics,
            self.metric.value.ty(),
            self.num_project_keys
        )
    }
}

lazy_static::lazy_static! {
    static ref COUNTER_METRIC: Metric = Metric {
        name: "foo".to_owned(),
        unit: MetricUnit::None,
        value: MetricValue::Counter(42.),
        timestamp: UnixTimestamp::now(),
        tags: BTreeMap::new(),
    };

    static ref METRIC_INPUTS: [MetricInput; 4] = [
        MetricInput {
            num_metrics: 1,
            metric: COUNTER_METRIC.clone(),
            num_project_keys: 1,
        },
        MetricInput {
            num_metrics: 1000,
            metric: COUNTER_METRIC.clone(),
            num_project_keys: 1,
        },
        MetricInput {
            num_metrics: 1000,
            metric: COUNTER_METRIC.clone(),
            num_project_keys: 1000,
        },
        MetricInput {
            num_metrics: 10000,
            metric: COUNTER_METRIC.clone(),
            num_project_keys: 10000,
        },
    ];
}

fn bench_insert_and_flush(c: &mut Criterion) {
    let config = AggregatorConfig {
        bucket_interval: 1000,
        initial_delay: 0,
        debounce_delay: 0,
    };

    let flush_receiver = TestReceiver.start().recipient();

    for input in &*METRIC_INPUTS {
        c.bench_with_input(
            BenchmarkId::new("bench_insert_metrics", input),
            &input,
            |b, &input| {
                b.iter_batched(
                    || {
                        (
                            Aggregator::new(config.clone(), flush_receiver.clone()),
                            input.get_metrics(),
                        )
                    },
                    |(mut aggregator, metrics)| {
                        for (project_key, metric) in metrics {
                            aggregator.insert(project_key, metric).unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        c.bench_with_input(
            BenchmarkId::new("bench_flush_metrics", input),
            &input,
            |b, &input| {
                b.iter_batched(
                    || {
                        let mut aggregator =
                            Aggregator::new(config.clone(), flush_receiver.clone());
                        for (project_key, metric) in input.get_metrics() {
                            aggregator.insert(project_key, metric).unwrap();
                        }
                        aggregator
                    },
                    |mut aggregator| {
                        // XXX: Ideally we'd want to test the entire try_flush here, but spawning
                        // an actor is too much work here.
                        aggregator.pop_flush_buckets();
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }
}

criterion_group!(benches, bench_insert_and_flush);
criterion_main!(benches);
