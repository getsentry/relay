use std::sync::Arc;
use std::thread;
use tokio::runtime::Handle;

pub use rayon::{ThreadPool, ThreadPoolBuildError};
use tokio::sync::Semaphore;

/// A thread kind.
///
/// The thread kind has an effect on how threads are prioritized and scheduled.
#[derive(Default, Debug, Clone, Copy)]
pub enum ThreadKind {
    /// The default kind, just a thread like any other without any special configuration.
    #[default]
    Default,
    /// A worker thread is a CPU intensive task with a lower priority than the [`Self::Default`] kind.
    Worker,
}

/// Used to create a new [`ThreadPool`] thread pool.
pub struct ThreadPoolBuilder {
    name: &'static str,
    runtime: Option<Handle>,
    num_threads: usize,
    kind: ThreadKind,
}

impl ThreadPoolBuilder {
    /// Creates a new named thread pool builder.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            runtime: None,
            num_threads: 0,
            kind: ThreadKind::Default,
        }
    }

    /// Sets the number of threads to be used in the rayon thread-pool.
    ///
    /// See also [`rayon::ThreadPoolBuilder::num_threads`].
    pub fn num_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = num_threads;
        self
    }

    /// Configures the [`ThreadKind`] for all threads spawned in the pool.
    pub fn thread_kind(mut self, kind: ThreadKind) -> Self {
        self.kind = kind;
        self
    }

    /// Sets the Tokio runtime which will be made available in the workers.
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
                    set_current_thread_priority(self.kind);
                    let _guard = runtime.as_ref().map(|runtime| runtime.enter());
                    thread.run()
                })?;
                Ok(())
            })
            .build()
    }
}

/// A [`WorkerGroup`] adds an async back-pressure mechanism to a [`ThreadPool`].
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

#[cfg(unix)]
fn set_current_thread_priority(kind: ThreadKind) {
    // Lower priorities cause more favorable scheduling.
    // Higher priorities cause less favorable scheduling.
    //
    // For details see `man setpriority(2)`.
    let prio = match kind {
        // The default priority needs no change, and defaults to `0`.
        ThreadKind::Default => return,
        // Set a priority of `10` for worker threads,
        // it's just important that this is a higher priority than default.
        ThreadKind::Worker => 10,
    };
    if unsafe { libc::setpriority(libc::PRIO_PROCESS, 0, prio) } != 0 {
        // Clear the `errno` and log it.
        let error = std::io::Error::last_os_error();
        relay_log::warn!(
            error = &error as &dyn std::error::Error,
            "failed to set thread priority for a {kind:?} thread: {error:?}"
        );
    };
}

#[cfg(not(unix))]
fn set_current_thread_priority(_kind: ThreadKind) {
    // Ignored for non-Unix platforms.
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
    #[cfg(unix)]
    fn test_thread_pool_priority() {
        fn get_current_priority() -> i32 {
            unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) }
        }

        let default_prio = get_current_priority();

        {
            let pool = ThreadPoolBuilder::new("s").num_threads(1).build().unwrap();
            let prio = pool.install(get_current_priority);
            // Default pool priority must match current priority.
            assert_eq!(prio, default_prio);
        }

        {
            let pool = ThreadPoolBuilder::new("s")
                .num_threads(1)
                .thread_kind(ThreadKind::Worker)
                .build()
                .unwrap();
            let prio = pool.install(get_current_priority);
            // Worker must be higher than the default priority (higher number = lower priority).
            assert!(prio > default_prio);
        }
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
