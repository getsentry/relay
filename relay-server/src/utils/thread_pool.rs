use std::sync::Arc;
use std::thread;
use tokio::runtime::Handle;

pub use rayon::{ThreadPool, ThreadPoolBuildError};
use tokio::sync::Semaphore;

/// Used to create a new [`ThreadPool`] thread pool.
pub struct ThreadPoolBuilder {
    name: &'static str,
    runtime: Option<Handle>,
    num_threads: usize,
}

impl ThreadPoolBuilder {
    /// Creates a new named thread pool builder.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            runtime: None,
            num_threads: 0,
        }
    }

    /// Sets the number of threads to be used in the rayon threadpool.
    ///
    /// See also [`rayon::ThreadPoolBuilder::num_threads`].
    pub fn num_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = num_threads;
        self
    }

    /// Sets the tokio runtime which will be made available in the workers.
    pub fn runtime(mut self, runtime: Handle) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Creates and returns the thread pool.
    pub fn build(self) -> Result<ThreadPool, ThreadPoolBuildError> {
        rayon::ThreadPoolBuilder::new()
            .num_threads(self.num_threads)
            .thread_name(move |id| format!("pool-{name}-{id}", name = self.name))
            // In case of panic, log that there was a panic but keep the thread alive and don't
            // exist.
            .panic_handler(move |_panic| {
                relay_log::error!("thread in pool {name} paniced!", name = self.name)
            })
            .spawn_handler(|thread| {
                let mut b = thread::Builder::new();
                if let Some(name) = thread.name() {
                    b = b.name(name.to_owned());
                }
                if let Some(stack_size) = thread.stack_size() {
                    b = b.stack_size(stack_size);
                }
                let runtime = self.runtime.clone();
                b.spawn(move || {
                    let _guard = runtime.as_ref().map(|runtime| runtime.enter());
                    thread.run()
                })?;
                Ok(())
            })
            .build()
    }
}

/// A [`WorkerGroup`] adds an async brackpressure mechanism to a [`ThreadPool`].
pub struct WorkerGroup {
    pool: ThreadPool,
    semaphore: Arc<Semaphore>,
}

impl WorkerGroup {
    /// Creates a new worker group from a thread pool.
    pub fn new(pool: ThreadPool) -> Self {
        // Use `current_num_threads() * 2` to guarantee all threads immediately have a new item to work on.
        let semaphore = Arc::new(Semaphore::new(pool.current_num_threads() * 2));
        Self { pool, semaphore }
    }

    /// Spawns an asynchronous task on the thread pool.
    ///
    /// If the thread pool is saturated the returned future is pending until
    /// the thread pool has capacity to work on the task.
    ///
    /// # Examples:
    ///
    /// ```ignore
    /// # async fn test(mut messages: tokio::sync::mpsc::Receiver<()>) {
    /// # use relay_server::utils::{WorkerGroup, ThreadPoolBuilder};
    /// # use std::thread;
    /// # use std::time::Duration;
    /// # let pool = ThreadPoolBuilder::new("test").num_threads(1).build().unwrap();
    /// let workers = WorkerGroup::new(pool);
    ///
    /// while let Some(message) = messages.recv().await {
    ///     workers.spawn(move || {
    ///         thread::sleep(Duration::from_secs(1));
    ///         println!("worked on message {message:?}")
    ///     }).await;
    /// }
    /// # }
    /// ```
    pub async fn spawn(&self, op: impl FnOnce() + Send + 'static) {
        let semaphore = Arc::clone(&self.semaphore);
        let permit = semaphore
            .acquire_owned()
            .await
            .expect("the semaphore is never closed");

        self.pool.spawn(move || {
            op();
            drop(permit);
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::time::Duration;

    use futures::FutureExt;

    use super::*;

    #[test]
    fn test_thread_pool_num_threads() {
        let pool = ThreadPoolBuilder::new("s").num_threads(3).build().unwrap();
        assert_eq!(pool.current_num_threads(), 3);
    }

    #[test]
    fn test_thread_pool_runtime() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let pool = ThreadPoolBuilder::new("s")
            .num_threads(1)
            .runtime(rt.handle().clone())
            .build()
            .unwrap();

        let has_runtime = pool.install(|| tokio::runtime::Handle::try_current().is_ok());
        assert!(has_runtime);
    }

    #[test]
    fn test_thread_pool_no_runtime() {
        let pool = ThreadPoolBuilder::new("s").num_threads(1).build().unwrap();

        let has_runtime = pool.install(|| tokio::runtime::Handle::try_current().is_ok());
        assert!(!has_runtime);
    }

    #[test]
    fn test_thread_pool_panic() {
        let pool = ThreadPoolBuilder::new("s").num_threads(1).build().unwrap();
        let barrier = Arc::new(Barrier::new(2));

        pool.spawn({
            let barrier = Arc::clone(&barrier);
            move || {
                barrier.wait();
                panic!();
            }
        });
        barrier.wait();

        pool.spawn({
            let barrier = Arc::clone(&barrier);
            move || {
                barrier.wait();
            }
        });
        barrier.wait();
    }

    #[test]
    fn test_worker_group_backpressure() {
        let pool = ThreadPoolBuilder::new("s").num_threads(1).build().unwrap();
        let workers = WorkerGroup::new(pool);

        // Num Threads * 2 is the limit after backpressure kicks in
        let barrier = Arc::new(Barrier::new(2));

        let spawn = || {
            let barrier = Arc::clone(&barrier);
            workers
                .spawn(move || {
                    barrier.wait();
                })
                .now_or_never()
                .is_some()
        };

        for _ in 0..15 {
            // Pool should accept two immediately.
            assert!(spawn());
            assert!(spawn());
            // Pool should reject because there are already 2 tasks active.
            assert!(!spawn());

            // Unblock the barrier
            barrier.wait(); // first spawn
            barrier.wait(); // second spawn

            // wait a tiny bit to make sure the semaphore handle is dropped
            thread::sleep(Duration::from_millis(50));
        }
    }
}
