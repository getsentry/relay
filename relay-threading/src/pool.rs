use std::any::Any;
use std::future::Future;
use std::io;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::FutureExt;

use crate::builder::AsyncPoolBuilder;
use crate::multiplexing::Multiplexed;

/// A thread-based asynchronous pool for executing futures concurrently.
///
/// [`AsyncPool`] enables scheduling asynchronous tasks that are executed across a set of dedicated threads.
/// Each thread runs its own asynchronous executor executing tasks concurrently up to a configurable limit.
/// This design isolates task execution to dedicated threads while remaining safe for scheduling from multiple threads.
#[derive(Debug)]
pub struct AsyncPool<F> {
    tx: flume::Sender<F>,
}

impl<F> AsyncPool<F>
where
    F: Future<Output = ()> + Send + 'static,
{
    /// Constructs a new [`AsyncPool`] using the configuration specified by [`AsyncPoolBuilder`].
    ///
    /// This method creates the thread pool with the appropriate number of threads and configures each executor,
    /// thus enabling efficient processing of asynchronous tasks.
    pub fn new<S>(mut builder: AsyncPoolBuilder<S>) -> io::Result<Self>
    where
        S: ThreadSpawn,
    {
        let (tx, rx) = flume::bounded(builder.num_threads * 2);

        for index in 0..builder.num_threads {
            let rx = rx.clone();

            let thread = Thread {
                index,
                max_concurrency: builder.max_concurrency,
                name: builder.thread_name.as_mut().map(|f| f(index)),
                runtime: builder.runtime.clone(),
                panic_handler: builder.thread_panic_handler.clone(),
                task: Multiplexed::new(
                    builder.max_concurrency,
                    rx.into_stream(),
                    builder.task_panic_handler.clone(),
                )
                .boxed(),
            };

            builder.spawn_handler.spawn(thread)?;
        }

        Ok(Self { tx })
    }
}

impl<F> AsyncPool<F>
where
    F: Future<Output = ()>,
{
    /// Schedules a future for asynchronous execution within the pool.
    ///
    /// This method adds the given future to the pool's task queue where it will be executed by an available thread.
    pub fn spawn(&self, future: F) {
        assert!(
            self.tx.send(future).is_ok(),
            "receiver never exits before sender"
        );
    }

    /// Schedules a future for asynchronous execution, awaiting until it is enqueued.
    ///
    /// Use this asynchronous method when you need assurance that the task has been successfully queued.
    pub async fn spawn_async(&self, future: F) {
        assert!(
            self.tx.send_async(future).await.is_ok(),
            "receiver never exits before sender"
        );
    }
}

/// Represents a dedicated thread running asynchronous tasks within an [`AsyncPool`].
pub struct Thread {
    index: usize,
    max_concurrency: usize,
    name: Option<String>,
    runtime: tokio::runtime::Handle,
    #[allow(clippy::type_complexity)]
    panic_handler: Option<Arc<dyn Fn(Box<dyn Any + Send>) + Send + Sync>>,
    task: BoxFuture<'static, ()>,
}

impl Thread {
    /// Returns the identifier assigned to this thread.
    ///
    /// The identifier is useful for debugging or tracing task execution across threads.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Returns the maximum concurrency for this thread.
    ///
    /// The maximum concurrency determines how many futures will be polled concurrently by the
    /// thread.
    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }

    /// Returns the name of this thread, if one was provided.
    ///
    /// Thread names can aid in logging and debugging by providing a human-readable identifier.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

impl Thread {
    /// Runs the [`Multiplexed`] future associated with this thread that executes incoming futures.
    ///
    /// If there is a panic during execution, the `panic_handler` will be called.
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

/// A trait for customizing the spawning of threads in an [`AsyncPool`].
///
/// Implement [`ThreadSpawn`] to modify thread settings—such as the thread name or stack size—prior to creation,
/// allowing the thread to be tailored for the requirements of your application.
pub trait ThreadSpawn {
    /// Spawns a new thread using the provided configuration.
    fn spawn(&mut self, thread: Thread) -> io::Result<()>;
}

/// A default implementation of [`ThreadSpawn`] that uses system defaults.
///
/// [`DefaultSpawn`] does not alter thread settings and relies on the standard behavior of the operating system.
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

/// A flexible [`ThreadSpawn`] implementation that uses a closure for dynamic thread configuration.
///
/// Use [`CustomSpawn`] to provide custom settings for thread creation via a user-supplied closure.
#[derive(Clone)]
pub struct CustomSpawn<B>(B);

impl<B> CustomSpawn<B> {
    /// Creates a new instance of [`CustomSpawn`] with the provided configuration closure.
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
            index: 0,
            max_concurrency: 1,
            name: Some("test-thread".to_owned()),
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
}
