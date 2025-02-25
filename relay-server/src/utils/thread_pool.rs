use std::future::Future;
use std::{io, thread};

use tokio::runtime::Handle;

use relay_threading::{AsyncPool, AsyncPoolBuilder};

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

/// Used to create a new [`AsyncPool`] thread pool.
pub struct ThreadPoolBuilder {
    name: &'static str,
    runtime: Handle,
    num_threads: usize,
    max_concurrency: usize,
    kind: ThreadKind,
}

impl ThreadPoolBuilder {
    /// Creates a new named thread pool builder.
    pub fn new(name: &'static str, runtime: Handle) -> Self {
        Self {
            name,
            runtime,
            num_threads: 0,
            max_concurrency: 1,
            kind: ThreadKind::Default,
        }
    }

    /// Sets the number of threads to be used in the rayon thread-pool.
    ///
    /// See also [`AsyncPoolBuilder::num_threads`].
    pub fn num_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = num_threads;
        self
    }

    /// Sets the maximum number of tasks that can run concurrently per thread.
    ///
    /// See also [`AsyncPoolBuilder::max_concurrency`].
    pub fn max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.max_concurrency = max_concurrency;
        self
    }

    /// Configures the [`ThreadKind`] for all threads spawned in the pool.
    pub fn thread_kind(mut self, kind: ThreadKind) -> Self {
        self.kind = kind;
        self
    }

    /// Creates and returns the thread pool.
    pub fn build<F>(self) -> Result<AsyncPool<F>, io::Error>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        AsyncPoolBuilder::new(self.runtime)
            .pool_name(self.name)
            .thread_name(move |id| format!("pool-{name}-{id}", name = self.name))
            .num_threads(self.num_threads)
            .max_concurrency(self.max_concurrency)
            // In case of panic in a task sent to the pool, we catch it to continue the remaining
            // work and just log an error.
            .task_panic_handler(move |_panic| {
                relay_log::error!(
                    "task in pool {name} panicked, other tasks will continue execution",
                    name = self.name
                );
            })
            // In case of panic in the thread, log it. After a panic in the thread, it will stop.
            .thread_panic_handler(move |panic| {
                relay_log::error!("thread in pool {name} panicked", name = self.name);
                std::panic::resume_unwind(panic);
            })
            .spawn_handler(|thread| {
                let mut b = thread::Builder::new();
                if let Some(name) = thread.name() {
                    b = b.name(name.to_owned());
                }
                b.spawn(move || {
                    set_current_thread_priority(self.kind);
                    thread.run()
                })?;

                Ok(())
            })
            .build()
    }
}

#[cfg(unix)]
fn set_current_thread_priority(kind: ThreadKind) {
    // Lower priorities cause more favorable scheduling.
    // Higher priorities cause less favorable scheduling.
    //
    // The relative niceness between threads determines their relative
    // priority. The formula to map a nice value to a weight is approximately
    // `1024 / (1.25 ^ nice)`.
    //
    // More information can be found:
    //  - https://www.kernel.org/doc/Documentation/scheduler/sched-nice-design.txt
    //  - https://oakbytes.wordpress.com/2012/06/06/linux-scheduler-cfs-and-nice/
    //  - `man setpriority(2)`
    let prio = match kind {
        // The default priority needs no change, and defaults to `0`.
        ThreadKind::Default => return,
        // Set a priority of `10` for worker threads.
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
    use crate::utils::{ThreadKind, ThreadPoolBuilder};
    use futures::FutureExt;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;
    use tokio::runtime::Handle;
    use tokio::sync::Barrier;

    #[tokio::test]
    async fn test_thread_pool_panic() {
        let pool = ThreadPoolBuilder::new("s", Handle::current())
            .num_threads(1)
            .build()
            .unwrap();
        let barrier = Arc::new(Barrier::new(2));

        let barrier_clone = barrier.clone();
        pool.spawn(
            async move {
                barrier_clone.wait().await;
                panic!();
            }
            .boxed(),
        );
        barrier.wait().await;

        let barrier_clone = barrier.clone();
        pool.spawn(
            async move {
                barrier_clone.wait().await;
            }
            .boxed(),
        );
        barrier.wait().await;
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn test_thread_pool_priority() {
        fn get_current_priority() -> i32 {
            unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) }
        }

        let default_priority = get_current_priority();

        {
            let pool = ThreadPoolBuilder::new("s", Handle::current())
                .num_threads(1)
                .build()
                .unwrap();

            let barrier = Arc::new(Barrier::new(2));
            let priority = Arc::new(AtomicI32::new(0));
            let barrier_clone = barrier.clone();
            let priority_clone = priority.clone();
            pool.spawn(async move {
                priority_clone.store(get_current_priority(), Ordering::SeqCst);
                barrier_clone.wait().await;
            });
            barrier.wait().await;

            // Default pool priority must match current priority.
            assert_eq!(priority.load(Ordering::SeqCst), default_priority);
        }

        {
            let pool = ThreadPoolBuilder::new("s", Handle::current())
                .num_threads(1)
                .thread_kind(ThreadKind::Worker)
                .build()
                .unwrap();

            let barrier = Arc::new(Barrier::new(2));
            let priority = Arc::new(AtomicI32::new(0));
            let barrier_clone = barrier.clone();
            let priority_clone = priority.clone();
            pool.spawn(async move {
                priority_clone.store(get_current_priority(), Ordering::SeqCst);
                barrier_clone.wait().await;
            });
            barrier.wait().await;

            // Worker must be higher than the default priority (higher number = lower priority).
            assert!(priority.load(Ordering::SeqCst) > default_priority);
        }
    }
}
