use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use futures::future::{BoxFuture, FutureExt};
use relay_threading::AsyncPoolBuilder;
use tokio::runtime::Runtime;
use tokio::sync::Semaphore;

struct BenchBarrier {
    semaphore: Arc<Semaphore>,
    count: usize,
}

impl BenchBarrier {
    fn new(count: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(count)),
            count,
        }
    }

    async fn spawn<F, Fut>(&self, pool: &relay_threading::AsyncPool<BoxFuture<'static, ()>>, f: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let semaphore = self.semaphore.clone();
        let permit = semaphore.acquire_owned().await.unwrap();
        pool.spawn_async(
            async move {
                f().await;
                drop(permit);
            }
            .boxed(),
        )
        .await;
    }

    async fn wait(&self) {
        let _ = self
            .semaphore
            .acquire_many(self.count as u32)
            .await
            .unwrap();
    }
}

fn create_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

async fn run_benchmark(pool: &relay_threading::AsyncPool<BoxFuture<'static, ()>>, count: usize) {
    let counter = Arc::new(AtomicUsize::new(0));
    let barrier = BenchBarrier::new(count);

    // Spawn tasks
    for _ in 0..count {
        let counter = counter.clone();
        barrier
            .spawn(pool, move || async move {
                // Simulate some work
                tokio::time::sleep(Duration::from_micros(50)).await;
                counter.fetch_add(1, Ordering::SeqCst);
            })
            .await;
    }

    // Wait for all tasks to complete
    barrier.wait().await;
    assert_eq!(counter.load(Ordering::SeqCst), count);
}

fn bench_pool_scaling(c: &mut Criterion) {
    let runtime = create_runtime();
    let mut group = c.benchmark_group("pool_scaling");
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(10));

    // Test with different numbers of threads
    for threads in [1, 2, 4, 8].iter() {
        let pool = AsyncPoolBuilder::new(runtime.handle().clone())
            .num_threads(*threads)
            .max_concurrency(100)
            .build()
            .unwrap();

        // Test with different task counts
        for tasks in [100, 1000, 10000].iter() {
            group.bench_with_input(
                BenchmarkId::new(format!("threads_{threads}"), tasks),
                tasks,
                |b, &tasks| {
                    b.to_async(&runtime).iter(|| run_benchmark(&pool, tasks));
                },
            );
        }
    }

    group.finish();
}

fn bench_multi_threaded_spawn(c: &mut Criterion) {
    let runtime = create_runtime();
    let mut group = c.benchmark_group("multi_threaded_spawn");
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(10));

    // Test with different numbers of spawning threads
    for spawn_threads in [2, 4, 8].iter() {
        // Test with different task counts
        for tasks in [1000, 10000].iter() {
            group.bench_with_input(
                BenchmarkId::new(format!("spawn_threads_{spawn_threads}"), tasks),
                tasks,
                |b, &tasks| {
                    b.to_async(&runtime).iter(|| async {
                        let pool = Arc::new(
                            AsyncPoolBuilder::new(runtime.handle().clone())
                                .num_threads(4) // Fixed number of worker threads
                                .max_concurrency(100)
                                .build()
                                .unwrap(),
                        );

                        let tasks_per_thread = tasks / spawn_threads;
                        let mut handles = Vec::new();

                        // Spawn tasks from multiple threads
                        for _ in 0..*spawn_threads {
                            let runtime = runtime.handle().clone();
                            let pool = pool.clone();
                            let handle = std::thread::spawn(move || {
                                runtime.block_on(run_benchmark(&pool, tasks_per_thread));
                            });
                            handles.push(handle);
                        }

                        // Wait for all spawning threads to complete
                        for handle in handles {
                            handle.join().unwrap();
                        }
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_pool_scaling, bench_multi_threaded_spawn);
criterion_main!(benches);
