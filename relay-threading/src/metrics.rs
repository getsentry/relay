use relay_system::RawMetrics;
use std::cmp::max;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Metrics for a single thread in an asynchronous pool.
///
/// This struct provides a way to track and query metrics specific to an individual thread, such as
/// the number of futures it has polled. It is designed for safe concurrent access across threads.
#[derive(Debug, Default)]
pub(crate) struct ThreadMetrics {
    /// Number of futures that are currently being polled concurrently by the thread in the pool.
    pub(crate) active_tasks: AtomicU64,
    /// Number of tasks that have been successfully driven to completion.
    ///
    /// This number will monotonically grow if not reset.
    pub(crate) finished_tasks: AtomicU64,
    /// The raw metrics collected by the timed future.
    pub(crate) raw_metrics: Arc<RawMetrics>,
}

impl ThreadMetrics {
    /// Resets metrics that are monotonically increasing.
    pub fn reset(&self) {
        self.finished_tasks.store(0, Ordering::Relaxed);
    }
}

/// Metrics for the asynchronous pool.
#[derive(Debug)]
pub struct AsyncPoolMetrics<'a> {
    pub(crate) max_tasks: u64,
    pub(crate) queue_size: u64,
    pub(crate) threads_metrics: &'a [Arc<ThreadMetrics>],
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
                let finished_tasks = m.finished_tasks.load(Ordering::Relaxed);
                m.reset();

                finished_tasks
            })
            .sum();

        total_finished_tasks
    }

    /// Returns the utilization metric for the pool.
    ///
    /// The cpu utilization is measured as the amount of busy work performed by each thread when polling
    /// the futures.
    ///
    /// A cpu utilization of 100% indicates that the pool has been doing CPU-bound work for the duration
    /// of the measurement.
    /// A cpu utilization of 0% indicates that the pool didn't do any CPU-bound work for the duration
    /// of the measurement.
    ///
    /// Note that this metric is collected and updated for each thread when the main future is polled,
    /// thus if no work is being done, it will not be updated.
    pub fn cpu_utilization(&self) -> u8 {
        self.threads_metrics
            .iter()
            .map(|m| m.raw_metrics.utilization.load(Ordering::Relaxed))
            .max()
            .unwrap_or(100)
    }

    /// Returns the overall utilization of the pool.
    ///
    /// This metric provides a high-level view of how busy the pool is, combining both CPU-bound and
    /// I/O-bound workloads into a single value. It is intended as a general signal of system load
    /// and should be preferred when making scaling decisions.
    ///
    /// Unlike [`Self::cpu_utilization`] or [`Self::activity`], which reflect specific aspects of pool usage,
    /// this method captures the maximum pressure observed in either dimension. This makes it more robust
    /// in edge cases such as:
    /// - High activity with low CPU utilization (e.g., I/O-bound workloads with many tasks waiting).
    /// - Low activity with high CPU utilization (e.g., a few threads performing heavy computation).
    pub fn total_utilization(&self) -> u8 {
        max(self.cpu_utilization(), self.activity())
    }

    /// Returns the activity metric for the pool.
    ///
    /// The activity is measure as the amount of active tasks in the pool versus the maximum amount
    /// of tasks that the pool can have active at the same time.
    ///
    /// An activity of 100% indicates that the pool is driving the maximum number of tasks that it
    /// can.
    /// An activity of 0% indicates that the pool is not driving any tasks.
    pub fn activity(&self) -> u8 {
        let total_polled_futures: u64 = self
            .threads_metrics
            .iter()
            .map(|m| m.active_tasks.load(Ordering::Relaxed))
            .sum();

        (total_polled_futures as f32 / self.max_tasks as f32).clamp(0.0, 1.0) as u8 * 100
    }
}
