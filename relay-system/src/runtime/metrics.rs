use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Handle to the [`Runtime`](crate::Runtime)'s metrics.
///
/// Unlike [`tokio::runtime::RuntimeMetrics`] this handle returns the relative
/// increase in values since the last time the specific metric was queried.
///
/// This makes it possible to emit these metrics as counters more easily through statsd.
///
/// It also exposes some other runtime metrics which Tokio does not directly
/// expose through the [`tokio::runtime::RuntimeMetrics`] handle.
#[derive(Debug)]
pub struct RuntimeMetrics {
    name: &'static str,
    cb: Arc<TokioCallbackMetrics>,
    tokio: tokio::runtime::RuntimeMetrics,
    // Metric state:
    budget_forced_yield_count: AtomicU64,
    worker_state: Box<[WorkerState]>,
}

macro_rules! impl_diff_metric {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        ///
        #[doc = concat!("See also: [`tokio::runtime::RuntimeMetrics::", stringify!($name), "`]")]
        ///
        /// Returns the difference since the function was last called.
        #[track_caller]
        pub fn $name(&self) -> u64 {
            let stat = self.tokio.$name() as u64;
            let prev = self.$name.swap(stat, Ordering::Relaxed);
            stat.saturating_sub(prev)
        }
    };
    (worker: $(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        ///
        #[doc = concat!("See also: [`tokio::runtime::RuntimeMetrics::", stringify!($name), "`]")]
        ///
        /// Returns the difference since the function was last called.
        #[track_caller]
        pub fn $name(&self, worker: usize) -> u64 {
            let stat = self.tokio.$name(worker) as u64;
            let prev = self.worker_state[worker].$name.swap(stat, Ordering::Relaxed);
            stat.saturating_sub(prev)
        }
    };
}

impl RuntimeMetrics {
    /// Returns the runtime name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Returns the amount of current threads idle.
    pub fn num_idle_threads(&self) -> usize {
        self.cb.idle_threads.load(Ordering::Relaxed)
    }

    /// Returns the amount of currently alive tasks in the runtime.
    ///
    /// See also: [`tokio::runtime::RuntimeMetrics::num_alive_tasks`].
    pub fn num_alive_tasks(&self) -> usize {
        self.tokio.num_alive_tasks()
    }

    /// Returns the number of tasks currently scheduled in the blocking
    /// thread pool, spawned using `spawn_blocking`.
    ///
    /// See also: [`tokio::runtime::RuntimeMetrics::blocking_queue_depth`].
    pub fn blocking_queue_depth(&self) -> usize {
        self.tokio.blocking_queue_depth()
    }

    impl_diff_metric!(
        /// Returns the number of times that tasks have been forced to yield back to the scheduler
        /// after exhausting their task budgets.
        budget_forced_yield_count
    );

    /// Returns the number of additional threads spawned by the runtime.
    ///
    /// See also: [`tokio::runtime::RuntimeMetrics::num_blocking_threads`].
    pub fn num_blocking_threads(&self) -> usize {
        self.tokio.num_blocking_threads()
    }

    /// Returns the number of idle threads, which have spawned by the runtime
    /// for `spawn_blocking` calls.
    ///
    /// See also: [`tokio::runtime::RuntimeMetrics::num_idle_blocking_threads`].
    pub fn num_idle_blocking_threads(&self) -> usize {
        self.tokio.num_idle_blocking_threads()
    }

    /// Returns the number of worker threads used by the runtime.
    ///
    /// See also: [`tokio::runtime::RuntimeMetrics::num_workers`].
    pub fn num_workers(&self) -> usize {
        self.tokio.num_workers()
    }

    /// Returns the number of tasks currently scheduled in the given worker's
    /// local queue.
    ///
    /// See also: [`tokio::runtime::RuntimeMetrics::worker_local_queue_depth`].
    pub fn worker_local_queue_depth(&self, worker: usize) -> usize {
        self.tokio.worker_local_queue_depth(worker)
    }

    /// Returns the mean duration of task polls, in nanoseconds.
    ///
    /// See also: [`tokio::runtime::RuntimeMetrics::worker_mean_poll_time`].
    pub fn worker_mean_poll_time(&self, worker: usize) -> Duration {
        self.tokio.worker_mean_poll_time(worker)
    }

    impl_diff_metric!(
        worker:
        /// Returns the number of tasks scheduled from **within** the runtime on the
        /// given worker's local queue.
        worker_local_schedule_count
    );
    impl_diff_metric!(
        worker:
        /// Returns the number of times the given worker thread unparked but
        /// performed no work before parking again.
        worker_noop_count
    );
    impl_diff_metric!(
        worker:
        /// Returns the number of times the given worker thread saturated its local
        /// queue.
        worker_overflow_count
    );
    impl_diff_metric!(
        worker:
        /// Returns the total number of times the given worker thread has parked.
        worker_park_count
    );
    impl_diff_metric!(
        worker:
        /// Returns the number of tasks the given worker thread has polled.
        worker_poll_count
    );
    impl_diff_metric!(
        worker:
        /// Returns the number of tasks the given worker thread stole from
        /// another worker thread.
        worker_steal_count
    );
    impl_diff_metric!(
        worker:
        /// Returns the number of times the given worker thread stole tasks from
        /// another worker thread.
        worker_steal_operations
    );

    /// Returns the amount of time the given worker thread has been busy, in seconds.
    ///
    /// See also: [`tokio::runtime::RuntimeMetrics::worker_total_busy_duration`].
    ///
    /// Returns the difference since the function was last called.
    pub fn worker_total_busy_duration(&self, worker: usize) -> Duration {
        let stat = self.tokio.worker_total_busy_duration(worker).as_secs_f64();
        let prev = self.worker_state[worker]
            .worker_total_busy_duration
            .swap(stat.to_bits(), Ordering::Relaxed);
        Duration::from_secs_f64(stat - f64::from_bits(prev))
    }
}

#[derive(Debug, Default)]
struct WorkerState {
    worker_local_schedule_count: AtomicU64,
    worker_noop_count: AtomicU64,
    worker_overflow_count: AtomicU64,
    worker_park_count: AtomicU64,
    worker_poll_count: AtomicU64,
    worker_steal_count: AtomicU64,
    worker_steal_operations: AtomicU64,
    worker_total_busy_duration: AtomicU64,
}

impl WorkerState {
    pub fn for_workers(num: usize) -> Box<[WorkerState]> {
        let mut v = Vec::with_capacity(num);
        v.resize_with(num, WorkerState::default);
        v.into_boxed_slice()
    }
}

/// Keeps track of Tokio's callback metrics.
#[derive(Debug, Default)]
pub struct TokioCallbackMetrics {
    idle_threads: AtomicUsize,
}

impl TokioCallbackMetrics {
    pub fn register(self: &Arc<Self>, builder: &mut tokio::runtime::Builder) {
        builder.on_thread_park({
            let this = Arc::clone(self);
            move || {
                this.idle_threads.fetch_add(1, Ordering::Relaxed);
            }
        });
        builder.on_thread_unpark({
            let this = Arc::clone(self);
            move || {
                this.idle_threads.fetch_sub(1, Ordering::Relaxed);
            }
        });
    }

    pub fn into_metrics(
        self: Arc<Self>,
        name: &'static str,
        tokio: tokio::runtime::RuntimeMetrics,
    ) -> RuntimeMetrics {
        let workers = tokio.num_workers();
        RuntimeMetrics {
            name,
            cb: self,
            tokio,
            budget_forced_yield_count: AtomicU64::new(0),
            worker_state: WorkerState::for_workers(workers),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")] // Test relies on Tokio/Platform specific internals.
    use super::*;

    #[test]
    #[cfg(target_os = "linux")] // Test relies on Tokio/Platform specific internals.
    fn test_metric_diff() {
        let rt = crate::Runtime::builder("test").worker_threads(1).build();

        let metrics = rt.metrics();

        rt.block_on(async move {
            let tokio_metrics = tokio::runtime::Handle::current().metrics();

            assert_eq!(metrics.num_workers(), 1);
            assert_eq!(tokio_metrics.num_workers(), 1);

            assert_eq!(metrics.worker_local_schedule_count(0), 0);
            assert_eq!(tokio_metrics.worker_local_schedule_count(0), 0);

            // Increase local worker schedule count by awaiting a timer.
            crate::spawn!(tokio::time::sleep(Duration::from_nanos(10)))
                .await
                .unwrap();

            assert_eq!(metrics.worker_local_schedule_count(0), 1);
            assert_eq!(tokio_metrics.worker_local_schedule_count(0), 1);

            // Increase it again.
            crate::spawn!(tokio::time::sleep(Duration::from_nanos(10)))
                .await
                .unwrap();

            // The difference is `1`.
            assert_eq!(metrics.worker_local_schedule_count(0), 1);
            // The total count is `2`.
            assert_eq!(tokio_metrics.worker_local_schedule_count(0), 2);
        });
    }
}
