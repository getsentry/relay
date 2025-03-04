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
}

/// Metrics for the asynchronous pool.
#[derive(Debug)]
pub struct AsyncPoolMetrics<'a> {
    pub(crate) max_expected_futures: u64,
    pub(crate) queue_size: u64,
    pub(crate) threads_metrics: &'a [Arc<ThreadMetrics>],
}

impl AsyncPoolMetrics<'_> {
    /// Returns the amount of futures in the pool's queue.
    pub fn queue_size(&self) -> u64 {
        self.queue_size
    }

    /// Returns the utilization metric for the pool.
    pub fn utilization(&self) -> f32 {
        let total_polled_futures: u64 = self
            .threads_metrics
            .iter()
            .map(|m| m.active_tasks.load(Ordering::Relaxed))
            .sum();

        (total_polled_futures as f32 / self.max_expected_futures as f32).clamp(0.0, 1.0)
    }
}
