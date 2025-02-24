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

    /// Sets the maximum number of concurrent tasks per thread.
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
            .num_threads(self.num_threads)
            .max_concurrency(self.max_concurrency)
            .thread_name(move |id| format!("pool-{name}-{id}", name = self.name))
            // In case of panic in a task sent to the pool, we catch it to continue the remaining
            // work and just log an error.
            .task_panic_handler(move |_panic| {
                relay_log::error!(
                    "task in pool {name} panicked, other tasks will continue execution",
                    name = self.name
                )
            })
            // In case of panic in the thread, log it. After a panic in the thread, it will stop.
            .thread_panic_handler(move |_panic| {
                relay_log::error!("thread in pool {name} panicked", name = self.name)
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

    // #[test]
    // fn test_thread_pool_num_threads() {
    //     let pool = ThreadPoolBuilder::new("s").num_threads(3).build().unwrap();
    //     assert_eq!(pool.current_num_threads(), 3);
    // }
    //
    // #[test]
    // fn test_thread_pool_runtime() {
    //     let rt = tokio::runtime::Runtime::new().unwrap();
    //
    //     let pool = ThreadPoolBuilder::new("s")
    //         .num_threads(1)
    //         .runtime(rt.handle().clone())
    //         .build()
    //         .unwrap();
    //
    //     let has_runtime = pool.install(|| tokio::runtime::Handle::try_current().is_ok());
    //     assert!(has_runtime);
    // }
    //
    // #[test]
    // fn test_thread_pool_no_runtime() {
    //     let pool = ThreadPoolBuilder::new("s").num_threads(1).build().unwrap();
    //
    //     let has_runtime = pool.install(|| tokio::runtime::Handle::try_current().is_ok());
    //     assert!(!has_runtime);
    // }
    //
    // #[test]
    // fn test_thread_pool_panic() {
    //     let pool = ThreadPoolBuilder::new("s").num_threads(1).build().unwrap();
    //     let barrier = Arc::new(Barrier::new(2));
    //
    //     pool.spawn({
    //         let barrier = Arc::clone(&barrier);
    //         move || {
    //             barrier.wait();
    //             panic!();
    //         }
    //     });
    //     barrier.wait();
    //
    //     pool.spawn({
    //         let barrier = Arc::clone(&barrier);
    //         move || {
    //             barrier.wait();
    //         }
    //     });
    //     barrier.wait();
    // }
    //
    // #[test]
    // #[cfg(unix)]
    // fn test_thread_pool_priority() {
    //     fn get_current_priority() -> i32 {
    //         unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) }
    //     }
    //
    //     let default_prio = get_current_priority();
    //
    //     {
    //         let pool = ThreadPoolBuilder::new("s").num_threads(1).build().unwrap();
    //         let prio = pool.install(get_current_priority);
    //         // Default pool priority must match current priority.
    //         assert_eq!(prio, default_prio);
    //     }
    //
    //     {
    //         let pool = ThreadPoolBuilder::new("s")
    //             .num_threads(1)
    //             .thread_kind(ThreadKind::Worker)
    //             .build()
    //             .unwrap();
    //         let prio = pool.install(get_current_priority);
    //         // Worker must be higher than the default priority (higher number = lower priority).
    //         assert!(prio > default_prio);
    //     }
    // }
    //
    // #[test]
    // fn test_worker_group_backpressure() {
    //     let pool = ThreadPoolBuilder::new("s").num_threads(1).build().unwrap();
    //     let workers = WorkerGroup::new(pool);
    //
    //     // Num Threads * 2 is the limit after backpressure kicks in
    //     let barrier = Arc::new(Barrier::new(2));
    //
    //     let spawn = || {
    //         let barrier = Arc::clone(&barrier);
    //         workers
    //             .spawn(move || {
    //                 barrier.wait();
    //             })
    //             .now_or_never()
    //             .is_some()
    //     };
    //
    //     for _ in 0..15 {
    //         // Pool should accept two immediately.
    //         assert!(spawn());
    //         assert!(spawn());
    //         // Pool should reject because there are already 2 tasks active.
    //         assert!(!spawn());
    //
    //         // Unblock the barrier
    //         barrier.wait(); // first spawn
    //         barrier.wait(); // second spawn
    //
    //         // wait a tiny bit to make sure the semaphore handle is dropped
    //         thread::sleep(Duration::from_millis(50));
    //     }
    // }
}
