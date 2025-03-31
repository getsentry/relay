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
    pub fn utilization(&self) -> f32 {
        let total_polled_futures: u64 = self
            .threads_metrics
            .iter()
            .map(|m| m.active_tasks.load(Ordering::Relaxed))
            .sum();

        (total_polled_futures as f32 / self.max_tasks as f32).clamp(0.0, 1.0) * 100.0
    }
}
