use relay_system::TimedFutureReceiver;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

#[derive(Debug)]
struct Inner {
    /// Number of futures that are currently being polled concurrently by the thread in the pool.
    active_tasks: AtomicU64,
    /// Number of tasks that have been successfully driven to completion.
    ///
    /// This number will monotonically grow if not reset.
    finished_tasks: AtomicU64,
    /// The utilization is the amount of busy work performed by each thread when polling the
    /// futures.
    utilization: AtomicU8,
}

/// Metrics for a single thread in an asynchronous pool.
///
/// This struct provides a way to track and query metrics specific to an individual thread, such as
/// the number of futures it has polled. It is designed for safe concurrent access across threads.
#[derive(Debug, Clone)]
pub(crate) struct ThreadMetrics(Arc<Inner>);

impl ThreadMetrics {
    /// Sets the number of active tasks.
    pub fn set_active_tasks(&self, active_tasks: u64) {
        self.0.active_tasks.store(active_tasks, Ordering::Release);
    }

    /// Increments the number of finished tasks by `finished_tasks`.
    pub fn increment_finished_tasks(&self, finished_tasks: u64) {
        self.0
            .finished_tasks
            .fetch_add(finished_tasks, Ordering::AcqRel);
    }

    /// Returns the number of active tasks.
    pub fn active_tasks(&self) -> u64 {
        self.0.active_tasks.load(Ordering::Relaxed)
    }

    /// Returns the number of finished tasks.
    pub fn finished_tasks(&self) -> u64 {
        self.0.finished_tasks.load(Ordering::Relaxed)
    }

    /// Resets metrics that are monotonically increasing.
    pub fn reset(&self) {
        self.0.finished_tasks.store(0, Ordering::Relaxed);
    }
}

impl TimedFutureReceiver for ThreadMetrics {
    fn on_new_utilization(&self, utilization: u8) {
        self.0.utilization.store(utilization, Ordering::Release);
    }
}

impl Default for ThreadMetrics {
    fn default() -> Self {
        Self(Arc::new(Inner {
            active_tasks: AtomicU64::new(0),
            finished_tasks: AtomicU64::new(0),
            utilization: AtomicU8::new(0),
        }))
    }
}

/// Metrics for the asynchronous pool.
#[derive(Debug)]
pub struct AsyncPoolMetrics<'a> {
    pub(crate) max_tasks: u64,
    pub(crate) queue_size: u64,
    pub(crate) threads_metrics: &'a [ThreadMetrics],
}

impl AsyncPoolMetrics<'_> {
    /// Returns the amount of tasks in the pool's queue.
    pub fn queue_size(&self) -> u64 {
        self.queue_size
    }

    /// Returns the total number of finished tasks since the last poll.
    pub fn finished_tasks(&self) -> u64 {
        let total_finished_tasks: u64 = self
            .threads_metrics
            .iter()
            .map(|m| {
                let finished_tasks = m.finished_tasks();
                m.reset();

                finished_tasks
            })
            .sum();

        total_finished_tasks
    }

    /// Returns the utilization metric for the pool.
    ///
    /// The utilization is measured as the amount of busy work performed by each thread when polling
    /// the futures.
    ///
    /// A utilization of 100% indicates that the pool has been doing CPU-bound work for the duration
    /// of the measurement.
    /// A utilization of 0% indicates that the pool didn't do any CPU-bound work for the duration
    /// of the measurement.
    ///
    /// Note that this metric is collected and updated for each thread when the main future is polled,
    /// thus if no work is being done, it will not be updated.
    pub fn utilization(&self) -> u8 {
        // TODO: replace with utilization metric.
        0
    }

    /// Returns the activity metric for the pool.
    ///
    /// The activity is measure as the amount of active tasks in the pool versus the maximum amount
    /// of tasks that the pool can have active at the same time.
    ///
    /// An activity of 100% indicates that the pool is driving the maximum number of tasks that it
    /// can.
    /// An activity of 0% indicates that the pool is not driving any tasks.
    pub fn activity(&self) -> f32 {
        let total_polled_futures: u64 = self.threads_metrics.iter().map(|m| m.active_tasks()).sum();

        (total_polled_futures as f32 / self.max_tasks as f32).clamp(0.0, 1.0) * 100.0
    }
}
