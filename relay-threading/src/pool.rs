use std::future::Future;
use std::io;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::FutureExt;

use crate::builder::AsyncPoolBuilder;
use crate::metrics::AsyncPoolMetrics;
use crate::multiplexing::Multiplexed;
use crate::{PanicHandler, ThreadMetrics};

/// Default name of the pool.
const DEFAULT_POOL_NAME: &str = "unnamed";

/// [`AsyncPool`] is a thread-based executor that runs asynchronous tasks on dedicated worker threads.
///
/// The pool collects tasks through a bounded channel and distributes them among threads, each of which runs its own
/// Tokio executor. This design enables controlled concurrency and efficient use of system resources.
#[derive(Debug)]
pub struct AsyncPool<F> {
    /// Name of the pool.
    name: &'static str,
    /// Transmission containing all futures.
    tx: flume::Sender<F>,
    /// The maximum number of tasks that are expected to run concurrently at any point in time.
    max_tasks: u64,
    /// Vector containing all the metrics collected individually in each thread.
    threads_metrics: Arc<Vec<Arc<ThreadMetrics>>>,
}

impl<F> AsyncPool<F> {
    /// Returns the `name` of the [`AsyncPool`].
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Returns the [`AsyncPoolMetrics`] that are updated by the pool.
    pub fn metrics(&self) -> AsyncPoolMetrics {
        AsyncPoolMetrics {
            max_tasks: self.max_tasks,
            queue_size: self.tx.len() as u64,
            threads_metrics: &self.threads_metrics,
        }
    }
}

impl<F> AsyncPool<F>
where
    F: Future<Output = ()> + Send + 'static,
{
    /// Creates a new [`AsyncPool`] based on the configuration specified by [`AsyncPoolBuilder`].
    ///
    /// This method initializes the dedicated worker threads and configures each executor with the defined
    /// concurrency limits.
    pub fn new<S>(mut builder: AsyncPoolBuilder<S>) -> io::Result<Self>
    where
        S: ThreadSpawn,
    {
        let pool_name = builder.pool_name.unwrap_or(DEFAULT_POOL_NAME);
        let (tx, rx) = flume::bounded(builder.num_threads * 2);
        let mut threads_metrics = Vec::with_capacity(builder.num_threads);

        for thread_id in 0..builder.num_threads {
            let rx = rx.clone();
            let thread_name: Option<String> = builder.thread_name.as_mut().map(|f| f(thread_id));

            let metrics = Arc::new(ThreadMetrics::default());
            let thread = Thread {
                id: thread_id,
                max_concurrency: builder.max_concurrency,
                name: thread_name.clone(),
                runtime: builder.runtime.clone(),
                panic_handler: builder.thread_panic_handler.clone(),
                task: Multiplexed::new(
                    pool_name,
                    builder.max_concurrency,
                    rx.into_stream(),
                    builder.task_panic_handler.clone(),
                    metrics.clone(),
                )
                .boxed(),
            };

            threads_metrics.push(metrics);

            builder.spawn_handler.spawn(thread)?;
        }

        Ok(Self {
            name: pool_name,
            tx,
            max_tasks: (builder.num_threads * builder.max_concurrency) as u64,
            threads_metrics: Arc::new(threads_metrics),
        })
    }

    /// Schedules a future for execution within the [`AsyncPool`].
    ///
    /// The task is added to the pool's internal queue to be executed by an available worker thread.
    ///
    /// # Panics
    ///
    /// This method panics if all receivers have been dropped which can happen when all threads of
    /// the pool panicked.
    pub fn spawn(&self, future: F) {
        assert!(
            self.tx.send(future).is_ok(),
            "failed to schedule task: all worker threads have terminated (either none were spawned or all have panicked)"
        );
    }

    /// Asynchronously enqueues a future for execution within the [`AsyncPool`].
    ///
    /// This method awaits until the task is successfully added to the internal queue.
    ///
    /// # Panics
    ///
    /// This method panics if all receivers have been dropped which can happen when all threads of
    /// the pool panicked.
    pub async fn spawn_async(&self, future: F) {
        assert!(
            self.tx.send_async(future).await.is_ok(),
            "failed to schedule task: all worker threads have terminated (either none were spawned or all have panicked)"
        );
    }
}

impl<F> Clone for AsyncPool<F> {
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            tx: self.tx.clone(),
            max_tasks: self.max_tasks,
            threads_metrics: self.threads_metrics.clone(),
        }
    }
}

/// [`Thread`] represents a dedicated worker thread within an [`AsyncPool`] that executes scheduled tasks.
pub struct Thread {
    id: usize,
    max_concurrency: usize,
    name: Option<String>,
    runtime: tokio::runtime::Handle,
    panic_handler: Option<Arc<PanicHandler>>,
    task: BoxFuture<'static, ()>,
}

impl Thread {
    /// Returns the unique index assigned to this [`Thread`].
    ///
    /// The index can help identify the thread during debugging or logging.
    pub fn id(&self) -> usize {
        self.id
    }

    /// Returns the maximum number of concurrent tasks permitted on this [`Thread`].
    ///
    /// This reflects the concurrency limit configured via the [`AsyncPoolBuilder`].
    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }

    /// Returns the human-readable name of this [`Thread`], if one was set.
    ///
    /// Thread names can assist in monitoring and debugging the execution environment.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

impl Thread {
    /// Runs the task multiplexer associated with this [`Thread`].
    ///
    /// This method drives the execution of tasks on the worker thread.
    ///
    /// # Panics
    ///
    /// Panics are either handled by the custom handler or propagated if no handler is specified.
    pub fn run(self) {
        let result =
            std::panic::catch_unwind(AssertUnwindSafe(|| self.runtime.block_on(self.task)));

        match (self.panic_handler, result) {
            // Panic handler and error, we swallow the panic and invoke the callback.
            (Some(panic_handler), Err(error)) => {
                panic_handler(error);
            }
            // No panic handler and error, we propagate the panic.
            (None, Err(error)) => {
                std::panic::resume_unwind(error);
            }
            // Otherwise, we do nothing.
            (_, Ok(())) => {}
        }
    }
}

/// [`ThreadSpawn`] defines how threads are spawned in an [`AsyncPool`].
///
/// This trait allows customization of thread creation (for example, setting names or adjusting stack sizes)
/// without altering the core functionality of the pool.
pub trait ThreadSpawn {
    /// Spawns a new thread using the provided configuration.
    fn spawn(&mut self, thread: Thread) -> io::Result<()>;
}

/// [`DefaultSpawn`] is the default implementation of [`ThreadSpawn`] that delegates to the system's
/// standard thread creation mechanism.
///
/// It applies any provided thread name using the standard thread builder.
#[derive(Clone)]
pub struct DefaultSpawn;

impl ThreadSpawn for DefaultSpawn {
    fn spawn(&mut self, thread: Thread) -> io::Result<()> {
        let mut b = std::thread::Builder::new();
        if let Some(name) = thread.name() {
            b = b.name(name.to_owned());
        }
        b.spawn(|| thread.run())?;

        Ok(())
    }
}

/// [`CustomSpawn`] is an alternative implementation of [`ThreadSpawn`] that uses a user-supplied closure
/// for custom thread configuration.
///
/// This allows for fine-grained control over thread properties, enabling application-specific setups.
#[derive(Clone)]
pub struct CustomSpawn<B>(B);

impl<B> CustomSpawn<B> {
    /// Creates a new instance of [`CustomSpawn`] with the specified configuration closure.
    pub fn new(spawn_handler: B) -> Self {
        CustomSpawn(spawn_handler)
    }
}

impl<B> ThreadSpawn for CustomSpawn<B>
where
    B: FnMut(Thread) -> io::Result<()>,
{
    /// Applies the custom configuration closure when spawning a new thread.
    fn spawn(&mut self, thread: Thread) -> io::Result<()> {
        self.0(thread)
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::panic::AssertUnwindSafe;
    use std::sync::atomic::AtomicBool;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::{Duration, Instant};

    use futures::future::BoxFuture;
    use futures::FutureExt;
    use tokio::runtime::Runtime;
    use tokio::sync::Semaphore;
    use tokio::{runtime::Handle, time::sleep};

    use crate::builder::AsyncPoolBuilder;
    use crate::{AsyncPool, Thread};

    struct TestBarrier {
        semaphore: Arc<Semaphore>,
        count: u32,
    }

    impl TestBarrier {
        async fn new(count: u32) -> Self {
            Self {
                semaphore: Arc::new(Semaphore::new(count as usize)),
                count,
            }
        }

        async fn spawn<F, Fut>(&self, pool: &AsyncPool<BoxFuture<'static, ()>>, f: F)
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
            let _ = self.semaphore.acquire_many(self.count).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_async_pool_executes_all_tasks() {
        let pool = AsyncPoolBuilder::new(Handle::current())
            .num_threads(1)
            .max_concurrency(2)
            .build()
            .unwrap();
        let counter = Arc::new(AtomicUsize::new(0));
        let barrier = TestBarrier::new(20).await;

        // Spawn 20 tasks that wait briefly and then update the counter.
        for _ in 0..20 {
            let counter_clone = counter.clone();
            barrier
                .spawn(&pool, move || async move {
                    sleep(Duration::from_millis(50)).await;
                    counter_clone.fetch_add(1, Ordering::SeqCst);
                })
                .await;
        }

        barrier.wait().await;
        assert_eq!(counter.load(Ordering::SeqCst), 20);
    }

    #[tokio::test]
    async fn test_async_pool_executes_all_tasks_concurrently_with_single_thread() {
        let pool = AsyncPoolBuilder::new(Handle::current())
            .num_threads(1)
            .max_concurrency(2)
            .build()
            .unwrap();

        let start = Instant::now();
        let barrier = TestBarrier::new(2).await;

        // Spawn 2 tasks that each sleep for 200ms.
        for _ in 0..2 {
            barrier
                .spawn(&pool, || async {
                    sleep(Duration::from_millis(200)).await;
                })
                .await;
        }

        barrier.wait().await;

        let elapsed = start.elapsed();
        // If running concurrently, the overall time should be near 200ms (with some allowance).
        assert!(
            elapsed < Duration::from_millis(250),
            "Elapsed time was too high: {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn test_async_pool_executes_all_tasks_concurrently_with_multiple_threads() {
        let pool = AsyncPoolBuilder::new(Handle::current())
            .num_threads(2)
            .max_concurrency(1)
            .build()
            .unwrap();

        let start = Instant::now();
        let barrier = TestBarrier::new(2).await;

        // Spawn 2 tasks that each sleep for 200ms.
        for _ in 0..2 {
            barrier
                .spawn(&pool, || async {
                    sleep(Duration::from_millis(200)).await;
                })
                .await;
        }

        barrier.wait().await;

        let elapsed = start.elapsed();
        // If running concurrently, the overall time should be near 200ms (with some allowance).
        assert!(
            elapsed < Duration::from_millis(250),
            "Elapsed time was too high: {:?}",
            elapsed
        );
    }

    #[test]
    fn test_thread_panic_handling() {
        let runtime = Runtime::new().unwrap();
        let has_panicked = Arc::new(AtomicBool::new(false));
        let has_panicked_clone = has_panicked.clone();
        let panic_handler = move |_| {
            has_panicked_clone.store(true, Ordering::SeqCst);
        };

        Thread {
            id: 0,
            max_concurrency: 1,
            name: Some("test-thread".into()),
            runtime: runtime.handle().clone(),
            panic_handler: Some(Arc::new(panic_handler)),
            task: async move {
                panic!("panicked");
            }
            .boxed(),
        }
        .run();

        assert!(has_panicked.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_spawn_panics_if_no_threads_are_available() {
        let pool = AsyncPoolBuilder::new(Handle::current())
            .num_threads(0)
            .max_concurrency(1)
            .build()
            .unwrap();

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            pool.spawn(async move {});
        }));

        assert!(result.is_err());
    }
}
